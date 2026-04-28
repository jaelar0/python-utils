/*
  absenteeism_by_type
  -------------------
  One row per agent per rostered work day per leave type.
  Enables leave-type breakdowns in Power BI
  (e.g., "LOB BNA has 100 hours of Late Arrival this month").

  Use absenteeism_daily for overall daily totals per agent.
  Use this model when slicing or filtering by leave type.

  Grain: payroll × work_date × leave_type
  Supports team, LOB, or per-agent aggregation in Power BI via team/lob columns.
*/

with roster as (

    select
        rs.roster_key,
        rs.payroll,
        date(rs.rdate)                                                  as work_date,
        rs.hours                                                        as rostered_hours

    from {{ source('raw_shifttrack', 'roster_staff_raw') }} rs

),

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

-- Scheduled hours per agent per day (shared denominator for all leave types that day)
scheduled as (

    select
        r.roster_key,
        r.payroll,
        r.work_date,
        r.rostered_hours,
        round(r.rostered_hours - coalesce(bl.break_lunch_hours, 0), 4) as scheduled_hours

    from roster r

    left join breaks_lunch_agg bl
        on  r.roster_key = bl.roster_key
        and r.payroll    = bl.payroll
        and r.work_date  = bl.work_date

),

-- Full-day leave: compute duration from start/stop then subtract any overlap
-- with rostered breaks/lunches so scheduled break time is never counted as absent.
full_leave_net as (

    select
        l.payroll,
        date(l.start)                                                   as work_date,
        l.type                                                          as leave_type,
        max(0.0,
            (julianday(l.stop) - julianday(l.start)) * 24
            - coalesce((
                select sum(
                    max(0.0,
                        (julianday(min(l.stop,   rb."to"))
                         - julianday(max(l.start, rb."from"))) * 24
                    )
                )
                from {{ source('raw_shifttrack', 'roster_breaks_raw') }} rb
                where rb.person       = l.payroll
                  and date(rb."from") = date(l.start)
                  and rb.break        = 1
                  and rb."from"       < l.stop
                  and rb."to"         > l.start
            ), 0)
        )                                                               as net_leave_hours

    from {{ source('raw_shifttrack', 'leave_raw') }} l

),

full_leave_by_type as (

    select
        payroll,
        work_date,
        leave_type,
        round(sum(net_leave_hours), 4)                                  as leave_hours

    from full_leave_net
    group by payroll, work_date, leave_type

),

-- Partial-day leave: same break-overlap subtraction applied to leavestart/leavefinish.
partial_leave_net as (

    select
        pl.payroll,
        date(pl.leavestart)                                             as work_date,
        pl.leavetype                                                    as leave_type,
        max(0.0,
            (julianday(pl.leavefinish) - julianday(pl.leavestart)) * 24
            - coalesce((
                select sum(
                    max(0.0,
                        (julianday(min(pl.leavefinish, rb."to"))
                         - julianday(max(pl.leavestart, rb."from"))) * 24
                    )
                )
                from {{ source('raw_shifttrack', 'roster_breaks_raw') }} rb
                where rb.person       = pl.payroll
                  and date(rb."from") = date(pl.leavestart)
                  and rb.break        = 1
                  and rb."from"       < pl.leavefinish
                  and rb."to"         > pl.leavestart
            ), 0)
        )                                                               as net_leave_hours

    from {{ source('raw_shifttrack', 'partial_leave_raw') }} pl

),

partial_leave_by_type as (

    select
        payroll,
        work_date,
        leave_type,
        round(sum(net_leave_hours), 4)                                  as leave_hours

    from partial_leave_net
    group by payroll, work_date, leave_type

),

all_leave_by_type as (

    select payroll, work_date, leave_type, leave_hours from full_leave_by_type
    union all
    select payroll, work_date, leave_type, leave_hours from partial_leave_by_type

),

-- One row per agent × day × leave_type
leave_by_type as (

    select
        payroll,
        work_date,
        leave_type,
        round(sum(leave_hours), 4)                                      as absenteeism_hours

    from all_leave_by_type
    group by payroll, work_date, leave_type

)

select
    s.work_date,
    s.payroll,
    p.firstname,
    p.surname,
    p.team,
    p.lob,
    l.leave_type,
    s.rostered_hours,
    s.scheduled_hours,
    l.absenteeism_hours,
    case
        when s.scheduled_hours > 0
            then round(l.absenteeism_hours / s.scheduled_hours, 4)
        else null
    end                                                                 as absenteeism_pct

from leave_by_type l

-- Inner join: only rows that have leave and a matching roster entry
inner join scheduled s
    on  l.payroll   = s.payroll
    and l.work_date = s.work_date

left join {{ source('raw_shifttrack', 'personnel_raw') }} p
    on s.payroll = p.payroll
