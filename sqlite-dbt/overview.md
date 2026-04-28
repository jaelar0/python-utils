# Task Overview

I need you to create two tables that are ready to be ingested into PowerBI for analytics. Use the `Database Info` section below to guide you in writing SQL code. I only need SQL code for a SQLite dbt project.

## First Table

- This table will capture **absenteeism** hours per day 
  - Absenteeism includes full and partial leave for every day
  - Things to note:
    - End-users want to see Absenteeism percentage (Absenteeism Hours / Scheduled Hours)
    - When we calculate this ratio, Scheduled Hours = Rostered Hours - Breaks - Lunch 
    - Absenteeism Hours include `types` like any PTO, Call Outs, Late Arrivals, etc.
- The table should allow for team aggregation or per agent basis within Power BI

## Second Table

- This table will capture **adherence** per day 
  - Adherence revolves around what activities are scheduled per the agent roster, and whether that agent actually followed that rostered schedule
  - Rostered activities include any breaks or scheduled activities (ie. Meetings) for the day
  - Agent should be dialing, waiting for calls, connected, or after call work during times not in a rostered activity
  - Things to note about `adh_log_raw`:
    - The fields `oldstatus` and `newstatus` are arbitrary and only `newstatus` should be used as the main type of activity
    - There are no start or end times, so some type of lag may be needed
    - This is a large table
- The table should allow for team aggregation or per agent basis within Power BI


# Database Info

SQLite database named `raw_shifttrack` with below tables.

```
adh_log_raw
roster_breaks_raw
roster_staff_raw
partial_leave_raw
leave_raw
personnel_raw
```

## Previews of each table are outlined below.

- **adh_log_raw**:
```sql
select *
from adh_log_raw
where payroll = 'ABAL'
and date(tstamp) = '2026-04-21'
order by tstamp
```

| tstamp | payroll | oldstatus | newstatus |
| :--- | :--- | :--- | :--- |
| 2026-04-21 08:01:52 | ABAL | Dialer Login | (Logged Out) |
| 2026-04-21 08:02:32 | ABAL | Waiting | Waiting |
| 2026-04-21 08:02:54 | ABAL | Connected | Connected |
| 2026-04-21 08:08:46 | ABAL | After Call Work | After Call Work |
| 2026-04-21 08:08:53 | ABAL | Waiting | Waiting |
| 2026-04-21 08:09:24 | ABAL | Connected | Connected |
| 2026-04-21 08:16:22 | ABAL | After Call Work | After Call Work |
| 2026-04-21 08:17:04 | ABAL | Waiting | Waiting |
| 2026-04-21 08:17:58 | ABAL | Connected | Connected |
| 2026-04-21 08:20:35 | ABAL | After Call Work | After Call Work |
| 2026-04-21 08:20:42 | ABAL | Waiting | Waiting |
| 2026-04-21 08:21:03 | ABAL | Connected | Connected |
| 2026-04-21 08:24:27 | ABAL | After Call Work | After Call Work |
| 2026-04-21 08:25:14 | ABAL | Waiting | Waiting |
| 2026-04-21 08:25:43 | ABAL | Connected | Connected |
| 2026-04-21 08:26:08 | ABAL | After Call Work | After Call Work |
| 2026-04-21 08:26:10 | ABAL | Waiting | Waiting |
| 2026-04-21 08:26:46 | ABAL | Connected | Connected |
| 2026-04-21 08:27:07 | ABAL | After Call Work | After Call Work |
| 2026-04-21 08:27:09 | ABAL | Waiting | Waiting |
| 2026-04-21 08:27:45 | ABAL | Connected | Connected |
| 2026-04-21 08:27:57 | ABAL | After Call Work | After Call Work |
| 2026-04-21 08:27:59 | ABAL | Waiting | Waiting |
| 2026-04-21 08:28:16 | ABAL | Connected | Connected |
| 2026-04-21 08:30:53 | ABAL | After Call Work | After Call Work |
| 2026-04-21 08:31:53 | ABAL | Waiting | Waiting |
| 2026-04-21 08:32:06 | ABAL | Connected | Connected |


- **roster_breaks_raw**: 
Snapshot obtained by running below
```sql
select *
from roster_breaks_raw
where person = 'ABAL'
and date("from") = '2026-04-21'
```

| roster_key | person | from | to | break | label | notes |
| ---------- | ------- | ------- | ------- | ------- | ------- | ------- |
| 89183  | ABAL | 2026-04-21 14:45:00 | 2026-04-21 15:00:00 | 1 | Break |  |
| 89183  | ABAL | 2026-04-21 09:45:00 | 2026-04-21 10:00:00 | 1 | Break |  |
| 89183  | ABAL | 2026-04-21 10:00:00 | 2026-04-21 11:00:00 | 0 | Meeting | Townhall |
| 89183  | ABAL | 2026-04-21 11:45:00 | 2026-04-21 12:45:00 | 1 | Lunch |  |

- **roster_staff_raw**:

Snapshot obtained by running below
```sql
select *
from roster_staff_raw
limit 3
```
| roster_key | payroll | start | finish | hours | lastmodify | rdate |
| ---------- | ------- | ------- | ------- | ------- | ------- | ------- |
| 89183  | ABAL | 1899-12-30 08:00:00 | 1899-12-30 17:00:00 | 8.0 | 2026-04-16 10:30:31 | 2026-04-16 00:00:00 |
| 88827  | ERVA | 1899-12-30 08:00:00 | 1899-12-30 17:00:00 | 8.0 | 2026-04-08 09:51:28 | 2026-04-17 00:00:00 |
| 89120  | JERA | 1899-12-30 12:00:00 | 1899-12-30 21:00:00 | 8.0 | 2026-04-15 15:54:01 | 2026-04-20 00:00:00 |

- **partial_leave_raw**:
```sql
select *
from partial_leave_raw
limit 3
```
| payroll | leavestart | leavefinish | leavetype |
| ---------- | ------- | ------- | ------- |
| 1548  | 2026-04-21 15:00:00 | 2026-04-17 17:00:00 | PTO - Vacation |
| ANDO  | 2026-04-16 15:04:00 | 2026-04-16 17:00:00 | Early Dept - Flex |
| AAAD  | 2026-04-21 08:00:00 | 2026-04-17 08:45:00 | Late Arrival |

- **leave_raw**:
```sql
select *
from leave_raw
limit 3
```
| payroll | start | stop | days | hours | type |
| ---------- | ------- | ------- | ------- | ------- | ------- |
| NPA  | 2026-04-17 08:00:00 | 2026-04-17 17:00:00 | 1 | 8.0 | Protected Leave |
| ALMU  | 2026-04-16 12:00:00 | 2026-04-16 21:00:00 | 1 | 8.0 | PTO - Vacation |
| CHVA  | 2026-04-17 11:00:00 | 2026-04-17 20:00:00 | 1 | 8.0 | PTO - Vacation |

- **personnel_raw**:
```sql
select *
from personnel_raw
limit 3
```
| payroll | surname | firstname | team | lob |
| ---------- | ------- | ------- | ------- | ------- |
| KIMR  | Ross | Fred | Young, F. | BNA |
| CAZO  | Catrina | Kim | Fredericks, S. | USBC |
| JRF  | Smith | Johnny | Castille, J. | USBC |
