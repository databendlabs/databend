---
title: Date & Time
description: Basic Date and Time data type.
---

## Date and Time Data Types

|  Name      | Aliases   | Storage Size |  Resolution  | Min Value             | Max Value                      | Description
|----------- | --------- |  ----------- | -------------|-----------------------| -----------------------------  |
|  DATE      |           | 4 bytes      |  day         | 1000-01-01            | 9999-12-31                     | YYYY-MM-DD             |
|  TIMESTAMP |  DATETIME | 8 bytes      |  microsecond | 1000-01-01 00:00:00   | 9999-12-31 23:59:59.999999 UTC | YYYY-MM-DD hh:mm:ss[.fraction], up to microseconds (6 digits) precision

## Example

```sql
CREATE TABLE test_dt
  (
     date DATE,
     ts   TIMESTAMP
  );

DESC test_dt;
+-------+--------------+------+---------+-------+
| Field | Type         | Null | Default | Extra |
+-------+--------------+------+---------+-------+
| date  | DATE         | NO   | 0       |       |
| ts    | TIMESTAMP    | NO   | 0       |       |
+-------+--------------+------+---------+-------+

-- A TIMESTAMP value can optionally include a trailing fractional seconds part in up to microseconds (6 digits) precision.

INSERT INTO test_dt
VALUES      ('2022-04-07',
             '2022-04-07 01:01:01.123456'),
            ('2022-04-08',
             '2022-04-08 01:01:01');

SELECT *
FROM   test_dt;
+------------+----------------------------+
| date       | ts                         |
+------------+----------------------------+
| 2022-04-07 | 2022-04-07 01:01:01.123456 |
| 2022-04-08 | 2022-04-08 01:01:01.000000 |
+------------+----------------------------+

-- Databend recognizes TIMESTAMP values in several formats.

CREATE TABLE test_formats
  (
     id INT,
     a  TIMESTAMP
  );

INSERT INTO test_formats
VALUES      (1,
             '2022-01-01 02:00:11'),
            (2,
             '2022-01-02T02:00:22'),
            (3,
             '2022-02-02T04:00:03+00:00'),
            (4,
             '2022-02-03');

SELECT *
FROM   test_formats;

 ----
 1  2022-01-01 02:00:11.000000
 2  2022-01-02 02:00:22.000000
 3  2022-02-02 04:00:03.000000
 4  2022-02-03 00:00:00.000000

-- Databend automatically adjusts and shows TIMESTAMP values based on your current timezone.

CREATE TABLE test_tz
  (
     id INT,
     t  TIMESTAMP
  );

SET timezone='UTC';

INSERT INTO test_tz
VALUES      (1,
             '2022-02-03T03:00:00'),
            (2,
             '2022-02-03T03:00:00+08:00'),
            (3,
             '2022-02-03T03:00:00-08:00'),
            (4,
             '2022-02-03'),
            (5,
             '2022-02-03T03:00:00+09:00'),
            (6,
             '2022-02-03T03:00:00+06:00');

SELECT *
FROM   test_tz;

 ----
 1  2022-02-03 03:00:00.000000
 2  2022-02-02 19:00:00.000000
 3  2022-02-03 11:00:00.000000
 4  2022-02-03 00:00:00.000000
 5  2022-02-02 18:00:00.000000
 6  2022-02-02 21:00:00.000000

SET timezone='Asia/Shanghai';

SELECT *
FROM   test_tz;

 ----
 1  2022-02-03 11:00:00.000000
 2  2022-02-03 03:00:00.000000
 3  2022-02-03 19:00:00.000000
 4  2022-02-03 08:00:00.000000
 5  2022-02-03 02:00:00.000000
 6  2022-02-03 05:00:00.000000
```

## Functions

See [Date & Time Functions](/doc/reference/functions/datetime-functions).

### Formatting Date and Time

In Databend, certain date and time functions like [TO_DATE](../../15-sql-functions/30-datetime-functions/todate.md) and [TO_TIMESTAMP](../../15-sql-functions/30-datetime-functions/totimestamp.md) require you to specify the desired format for date and time values. To handle date and time formatting, Databend makes use of the chrono::format::strftime module, which is a standard module provided by the chrono library in Rust. This module enables precise control over the formatting of dates and times. The following content is excerpted from https://docs.rs/chrono/latest/chrono/format/strftime/index.html:

| Spec. | Example                          | Description                                                                                                                                                                         |
|-------|----------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|       |                                  | DATE SPECIFIERS:                                                                                                                                                                    |
| %Y    | 2001                             | The full proleptic Gregorian year, zero-padded to 4 digits. chrono supports years from -262144 to 262143. Note: years before 1 BCE or after 9999 CE, require an initial sign (+/-). |
| %C    | 20                               | The proleptic Gregorian year divided by 100, zero-padded to 2 digits.                                                                                                               |
| %y    | 01                               | The proleptic Gregorian year modulo 100, zero-padded to 2 digits.                                                                                                                   |
| %m    | 07                               | Month number (01–12), zero-padded to 2 digits.                                                                                                                                      |
| %b    | Jul                              | Abbreviated month name. Always 3 letters.                                                                                                                                           |
| %B    | July                             | Full month name. Also accepts corresponding abbreviation in parsing.                                                                                                                |
| %h    | Jul                              | Same as %b.                                                                                                                                                                         |
| %d    | 08                               | Day number (01–31), zero-padded to 2 digits.                                                                                                                                        |
| %e    |  8                               | Same as %d but space-padded. Same as %_d.                                                                                                                                           |
| %a    | Sun                              | Abbreviated weekday name. Always 3 letters.                                                                                                                                         |
| %A    | Sunday                           | Full weekday name. Also accepts corresponding abbreviation in parsing.                                                                                                              |
| %w    | 0                                | Sunday = 0, Monday = 1, …, Saturday = 6.                                                                                                                                            |
| %u    | 7                                | Monday = 1, Tuesday = 2, …, Sunday = 7. (ISO 8601)                                                                                                                                  |
| %U    | 28                               | Week number starting with Sunday (00–53), zero-padded to 2 digits.                                                                                                                  |
| %W    | 27                               | Same as %U, but week 1 starts with the first Monday in that year instead.                                                                                                           |
| %G    | 2001                             | Same as %Y but uses the year number in ISO 8601 week date.                                                                                                                          |
| %g    | 01                               | Same as %y but uses the year number in ISO 8601 week date.                                                                                                                          |
| %V    | 27                               | Same as %U but uses the week number in ISO 8601 week date (01–53).                                                                                                                  |
| %j    | 189                              | Day of the year (001–366), zero-padded to 3 digits.                                                                                                                                 |
| %D    | 07/08/01                         | Month-day-year format. Same as %m/%d/%y.                                                                                                                                            |
| %x    | 07/08/01                         | Locale’s date representation (e.g., 12/31/99).                                                                                                                                      |
| %F    | 2001-07-08                       | Year-month-day format (ISO 8601). Same as %Y-%m-%d.                                                                                                                                 |
| %v    |  8-Jul-2001                      | Day-month-year format. Same as %e-%b-%Y.                                                                                                                                            |
|       |                                  | TIME SPECIFIERS:                                                                                                                                                                    |
| %H    | 00                               | Hour number (00–23), zero-padded to 2 digits.                                                                                                                                       |
| %k    |  0                               | Same as %H but space-padded. Same as %_H.                                                                                                                                           |
| %I    | 12                               | Hour number in 12-hour clocks (01–12), zero-padded to 2 digits.                                                                                                                     |
| %l    | 12                               | Same as %I but space-padded. Same as %_I.                                                                                                                                           |
| %P    | am                               | am or pm in 12-hour clocks.                                                                                                                                                         |
| %p    | AM                               | AM or PM in 12-hour clocks.                                                                                                                                                         |
| %M    | 34                               | Minute number (00–59), zero-padded to 2 digits.                                                                                                                                     |
| %S    | 60                               | Second number (00–60), zero-padded to 2 digits.                                                                                                                                     |
| %f    | 026490000                        | The fractional seconds (in nanoseconds) since last whole second.                                                                                                                    |
| %.f   | .026490                          | Similar to .%f but left-aligned. These all consume the leading dot.                                                                                                                 |
| %.3f  | .026                             | Similar to .%f but left-aligned but fixed to a length of 3.                                                                                                                         |
| %.6f  | .026490                          | Similar to .%f but left-aligned but fixed to a length of 6.                                                                                                                         |
| %.9f  | .026490000                       | Similar to .%f but left-aligned but fixed to a length of 9.                                                                                                                         |
| %3f   | 026                              | Similar to %.3f but without the leading dot.                                                                                                                                        |
| %6f   | 026490                           | Similar to %.6f but without the leading dot.                                                                                                                                        |
| %9f   | 026490000                        | Similar to %.9f but without the leading dot.                                                                                                                                        |
| %R    | 00:34                            | Hour-minute format. Same as %H:%M.                                                                                                                                                  |
| %T    | 00:34:60                         | Hour-minute-second format. Same as %H:%M:%S.                                                                                                                                        |
| %X    | 00:34:60                         | Locale’s time representation (e.g., 23:13:48).                                                                                                                                      |
| %r    | 12:34:60 AM                      | Hour-minute-second format in 12-hour clocks. Same as %I:%M:%S %p.                                                                                                                   |
|       |                                  | TIME ZONE SPECIFIERS:                                                                                                                                                               |
| %Z    | ACST                             | Local time zone name. Skips all non-whitespace characters during parsing.                                                                                                           |
| %z    | +0930                            | Offset from the local time to UTC (with UTC being +0000).                                                                                                                           |
| %:z   | +09:30                           | Same as %z but with a colon.                                                                                                                                                        |
| %::z  | +09:30:00                        | Offset from the local time to UTC with seconds.                                                                                                                                     |
| %:::z | +09                              | Offset from the local time to UTC without minutes.                                                                                                                                  |
| %#z   | +09                              | Parsing only: Same as %z but allows minutes to be missing or present.                                                                                                               |
|       |                                  | DATE & TIME SPECIFIERS:                                                                                                                                                             |
| %c    | Sun Jul  8 00:34:60 2001         | Locale’s date and time (e.g., Thu Mar 3 23:05:25 2005).                                                                                                                             |
| %+    | 2001-07-08T00:34:60.026490+09:30 | ISO 8601 / RFC 3339 date & time format.                                                                                                                                             |
| %s    | 994518299                        | UNIX timestamp, the number of seconds since 1970-01-01 00:00 UTC.                                                                                                                   |
|       |                                  | SPECIAL SPECIFIERS:                                                                                                                                                                 |
| %t    |                                  | Literal tab (\t).                                                                                                                                                                   |
| %n    |                                  | Literal newline (\n).                                                                                                                                                               |
| %%    |                                  | Literal percent sign.                                                                                                                                                               |

It is possible to override the default padding behavior of numeric specifiers %?. This is not allowed for other specifiers and will result in the BAD_FORMAT error.

| Modifier | Description                                                                   |
|----------|-------------------------------------------------------------------------------|
| %-?      | Suppresses any padding including spaces and zeroes. (e.g. %j = 012, %-j = 12) |
| %_?      | Uses spaces as a padding. (e.g. %j = 012, %_j =  12)                          |
| %0?      | Uses zeroes as a padding. (e.g. %e =  9, %0e = 09)                            |

- %C, %y: This is floor division, so 100 BCE (year number -99) will print -1 and 99 respectively. 

- %U: Week 1 starts with the first Sunday in that year. It is possible to have week 0 for days before the first Sunday. 

- %G, %g, %V: Week 1 is the first week with at least 4 days in that year. Week 0 does not exist, so this should be used with %G or %g. 

- %S: It accounts for leap seconds, so 60 is possible. 

- %f, %.f, %.3f, %.6f, %.9f, %3f, %6f, %9f:
  The default %f is right-aligned and always zero-padded to 9 digits for the compatibility with glibc and others, so it always counts the number of nanoseconds since the last whole second. E.g. 7ms after the last second will print 007000000, and parsing 7000000 will yield the same.

  The variant %.f is left-aligned and print 0, 3, 6 or 9 fractional digits according to the precision. E.g. 70ms after the last second under %.f will print .070 (note: not .07), and parsing .07, .070000 etc. will yield the same. Note that they can print or read nothing if the fractional part is zero or the next character is not ..

  The variant %.3f, %.6f and %.9f are left-aligned and print 3, 6 or 9 fractional digits according to the number preceding f. E.g. 70ms after the last second under %.3f will print .070 (note: not .07), and parsing .07, .070000 etc. will yield the same. Note that they can read nothing if the fractional part is zero or the next character is not . however will print with the specified length.

  The variant %3f, %6f and %9f are left-aligned and print 3, 6 or 9 fractional digits according to the number preceding f, but without the leading dot. E.g. 70ms after the last second under %3f will print 070 (note: not 07), and parsing 07, 070000 etc. will yield the same. Note that they can read nothing if the fractional part is zero. 

- %Z: Offset will not be populated from the parsed data, nor will it be validated. Timezone is completely ignored. Similar to the glibc strptime treatment of this format code.

  It is not possible to reliably convert from an abbreviation to an offset, for example CDT can mean either Central Daylight Time (North America) or China Daylight Time.

- %+: Same as %Y-%m-%dT%H:%M:%S%.f%:z, i.e. 0, 3, 6 or 9 fractional digits for seconds and colons in the time zone offset.

  This format also supports having a Z or UTC in place of %:z. They are equivalent to +00:00.

  Note that all T, Z, and UTC are parsed case-insensitively.

  The typical strftime implementations have different (and locale-dependent) formats for this specifier. While Chrono’s format for %+ is far more stable, it is best to avoid this specifier if you want to control the exact output. 

- %s: This is not padded and can be negative. For the purpose of Chrono, it only accounts for non-leap seconds so it slightly differs from ISO C strftime behavior. 