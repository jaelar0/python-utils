/*
  adherence_daily
  ---------------
  One row per agent per rostered work day.
  Measures whether each agent followed their rostered schedule:
    - During rostered activities (breaks, lunches, meetings): excluded from adherence calc.
    - During productive scheduled time: agent should be in Waiting, Connected,
      After Call Work, or Dialer Login.

  Adherence % = Adherent Seconds / (Adherent Seconds + Non-Adherent Seconds)

  All duration columns are formatted as HH:MM:SS strings for direct use in Power BI.
  Calculations use seconds throughout to preserve sub-minute precision.

  How intervals are built:
    - adh_log_raw has no end times; LEAD() derives each interval's end as the
      next event timestamp for that agent on the same day.
    - Each interval is clipped to the roster window (roster_start → roster_finish).
    - The portion of each interval that overlaps with any rostered activity
      (roster_breaks_raw) is excluded — adherence only measures productive time.
    - Remaining time is classified adherent (productive status) or non-adherent.

  Grain: payroll × work_date
  Supports team or per-agent aggregation in Power BI via team/lob columns.
*/

with adh_raw as (

    select
        payroll,
        date(tstamp)                                                    as work_date,
        tstamp                                                          as status_start,
        lead(tstamp) over (
            partition by payroll, date(tstamp)
            order by tstamp
        )                                                               as status_end,
        newstatus

    from {{ source('raw_shifttrack', 'adh_log_raw') }}

),

-- Drop the last record for each agent-day (no end time known)
adh_intervals as (

    select *
    from adh_raw
    where status_end is not null

),

roster as (

    select
        payroll,
        date(rdate)                                                     as work_date,
        datetime(date(rdate) || ' ' || time(start))                    as roster_start,
        datetime(date(rdate) || ' ' || time(finish))                   as roster_finish,
        -- Convert source decimal hours to whole seconds for consistent arithmetic
        cast(round(hours * 3600) as integer)                           as rostered_seconds

    from {{ source('raw_shifttrack', 'roster_staff_raw') }}

),

-- Total rostered activity time per agent per day (breaks + lunches + meetings), in seconds
rostered_activity_agg as (

    select
        person                                                          as payroll,
        date("from")                                                    as work_date,
        cast(round(
            sum((julianday("to") - julianday("from")) * 86400)
        ) as integer)                                                   as total_activity_seconds

    from {{ source('raw_shifttrack', 'roster_breaks_raw') }}
    group by person, date("from")

),

-- Clip each status interval to the roster window
-- Intervals entirely outside the roster window are excluded
clipped_intervals as (

    select
        a.payroll,
        a.work_date,
        a.newstatus,
        -- SQLite multi-arg max/min used as scalar GREATEST/LEAST
        max(a.status_start, r.roster_start)                             as clipped_start,
        min(a.status_end,   r.roster_finish)                           as clipped_end

    from adh_intervals a
    inner join roster r
        on  a.payroll   = r.payroll
        and a.work_date = r.work_date
    where a.status_start < r.roster_finish
      and a.status_end   > r.roster_start

),

-- For each clipped interval calculate:
--   interval_seconds           — total duration inside the roster window
--   activity_overlap_seconds   — portion that overlaps with ANY rostered activity
--   is_productive              — whether the status is a productive phone state
interval_detail as (

    select
        ci.payroll,
        ci.work_date,
        ci.newstatus,
        (julianday(ci.clipped_end) - julianday(ci.clipped_start)) * 86400
                                                                        as interval_seconds,

        -- Correlated subquery: sum of overlaps with all rostered activities
        -- for this agent on this day that intersect the clipped interval.
        -- max(0.0, ...) guards against floating-point drift producing negatives.
        coalesce((
            select sum(
                max(0.0,
                    (julianday(min(ci.clipped_end,   rb."to"))
                     - julianday(max(ci.clipped_start, rb."from"))) * 86400
                )
            )
            from {{ source('raw_shifttrack', 'roster_breaks_raw') }} rb
            where rb.person       = ci.payroll
              and date(rb."from") = ci.work_date
              and rb."from"       < ci.clipped_end
              and rb."to"         > ci.clipped_start
        ), 0)                                                           as activity_overlap_seconds,

        case
            when ci.newstatus in ('Waiting', 'Connected', 'After Call Work', 'Dialer Login')
                then 1
            else 0
        end                                                             as is_productive

    from clipped_intervals ci

),

-- Net productive-window time per interval (interval minus any rostered-activity overlap)
interval_productive as (

    select
        payroll,
        work_date,
        newstatus,
        is_productive,
        max(0.0, interval_seconds - activity_overlap_seconds)           as productive_window_seconds

    from interval_detail

),

-- Aggregate to agent × day
daily_adherence as (

    select
        payroll,
        work_date,
        cast(round(
            sum(case when is_productive = 1 then productive_window_seconds else 0 end)
        ) as integer)                                                   as adherent_seconds,
        cast(round(
            sum(case when is_productive = 0 then productive_window_seconds else 0 end)
        ) as integer)                                                   as non_adherent_seconds

    from interval_productive
    group by payroll, work_date

)

select
    r.work_date,
    r.payroll,
    p.firstname,
    p.surname,
    p.team,
    p.lob,

    -- Raw seconds (integer) — use for aggregation and custom calculations in Power BI
    r.rostered_seconds,
    coalesce(ra.total_activity_seconds, 0)                              as rostered_activity_seconds,
    r.rostered_seconds - coalesce(ra.total_activity_seconds, 0)        as scheduled_productive_seconds,
    coalesce(da.adherent_seconds, 0)                                    as adherent_seconds,
    coalesce(da.non_adherent_seconds, 0)                                as non_adherent_seconds,

    -- HH:MM:SS formatted strings — use for display columns in Power BI
    {{ seconds_to_hhmmss('r.rostered_seconds') }}
                                                                        as rostered_time,
    {{ seconds_to_hhmmss('coalesce(ra.total_activity_seconds, 0)') }}
                                                                        as rostered_activity_time,
    {{ seconds_to_hhmmss('r.rostered_seconds - coalesce(ra.total_activity_seconds, 0)') }}
                                                                        as scheduled_productive_time,
    {{ seconds_to_hhmmss('coalesce(da.adherent_seconds, 0)') }}
                                                                        as adherent_time,
    {{ seconds_to_hhmmss('coalesce(da.non_adherent_seconds, 0)') }}
                                                                        as non_adherent_time,

    case
        when coalesce(da.adherent_seconds, 0) + coalesce(da.non_adherent_seconds, 0) > 0
            then round(
                coalesce(da.adherent_seconds, 0)
                / (coalesce(da.adherent_seconds, 0) + coalesce(da.non_adherent_seconds, 0)),
                4
            )
        else null
    end                                                                 as adherence_pct

from roster r

left join rostered_activity_agg ra
    on  r.payroll   = ra.payroll
    and r.work_date = ra.work_date

left join daily_adherence da
    on  r.payroll   = da.payroll
    and r.work_date = da.work_date

left join {{ source('raw_shifttrack', 'personnel_raw') }} p
    on r.payroll = p.payroll
