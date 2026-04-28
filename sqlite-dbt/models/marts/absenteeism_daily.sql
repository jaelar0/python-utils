/*
  absenteeism_daily
  -----------------
  One row per agent per rostered work day.
  Captures total absenteeism hours (full-day leave + partial leave) and the
  absenteeism percentage against scheduled hours.

  Scheduled Hours = Rostered Hours - Breaks - Lunch
  Absenteeism %   = Absenteeism Hours / Scheduled Hours

  Grain: payroll × work_date
  Supports team or per-agent aggregation in Power BI via team/lob columns.
*/

with roster as (

    select
        rs.roster_key,
        rs.payroll,
        date(rs.rdate)                                                  as work_date,
        -- start/finish store only the time portion under a dummy 1899 date;
        -- rebuild proper datetimes using the actual work date from rdate.
        datetime(date(rs.rdate) || ' ' || time(rs.start))              as roster_start,
        datetime(date(rs.rdate) || ' ' || time(rs.finish))             as roster_finish,
        rs.hours                                                        as rostered_hours

    from {{ source('raw_shifttrack', 'roster_staff_raw') }} rs

),

-- Breaks and lunches to subtract from rostered hours (break = 1).
-- Meetings (break = 0) are rostered activities but are NOT subtracted here
-- per the Scheduled Hours definition: Rostered Hours - Breaks - Lunch.
breaks_lunch_agg as (

    select
        rb.roster_key,
        rb.person                                                       as payroll,
        date(rb."from")                                                 as work_date,
        round(
            sum((julianday(rb."to") - julianday(rb."from")) * 24),
            4
        )                                                               as break_lunch_hours

    from {{ source('raw_shifttrack', 'roster_breaks_raw') }} rb
    where rb.break = 1
    group by rb.roster_key, rb.person, date(rb."from")

),

-- Full-day leave (hours pre-calculated in source table)
full_leave as (

    select
        l.payroll,
        date(l.start)                                                   as work_date,
        sum(l.hours)                                                    as leave_hours,
        group_concat(l.type, ' | ')                                     as leave_types

    from {{ source('raw_shifttrack', 'leave_raw') }} l
    group by l.payroll, date(l.start)

),

-- Partial-day leave (late arrivals, early departures, partial PTO, etc.)
partial_leave as (

    select
        pl.payroll,
        date(pl.leavestart)                                             as work_date,
        round(
            -- Guard against bad data where leavefinish < leavestart
            sum(max(0.0, (julianday(pl.leavefinish) - julianday(pl.leavestart)) * 24)),
            4
        )                                                               as leave_hours,
        group_concat(pl.leavetype, ' | ')                               as leave_types

    from {{ source('raw_shifttrack', 'partial_leave_raw') }} pl
    group by pl.payroll, date(pl.leavestart)

),

all_leave as (

    select payroll, work_date, leave_hours, leave_types from full_leave
    union all
    select payroll, work_date, leave_hours, leave_types from partial_leave

),

-- Combined absenteeism (full-day + partial-day) per agent per day
leave_agg as (

    select
        payroll,
        work_date,
        round(sum(leave_hours), 4)                                      as absenteeism_hours,
        group_concat(leave_types, ' | ')                                as leave_types

    from all_leave
    group by payroll, work_date

)

select
    r.work_date,
    r.payroll,
    p.firstname,
    p.surname,
    p.team,
    p.lob,
    r.rostered_hours,
    round(coalesce(bl.break_lunch_hours, 0), 4)                         as break_lunch_hours,
    round(r.rostered_hours - coalesce(bl.break_lunch_hours, 0), 4)      as scheduled_hours,
    round(coalesce(l.absenteeism_hours, 0), 4)                          as absenteeism_hours,
    coalesce(l.leave_types, '')                                         as leave_types,
    case
        when r.rostered_hours - coalesce(bl.break_lunch_hours, 0) > 0
            then round(
                coalesce(l.absenteeism_hours, 0)
                / (r.rostered_hours - coalesce(bl.break_lunch_hours, 0)),
                4
            )
        else null
    end                                                                 as absenteeism_pct

from roster r

left join breaks_lunch_agg bl
    on  r.roster_key = bl.roster_key
    and r.payroll    = bl.payroll
    and r.work_date  = bl.work_date

left join leave_agg l
    on  r.payroll   = l.payroll
    and r.work_date = l.work_date

left join {{ source('raw_shifttrack', 'personnel_raw') }} p
    on r.payroll = p.payroll
