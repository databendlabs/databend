# DuckDB Migration Issues

- Generated at (UTC): 2026-03-10T07:48:07.309244+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 5
- Files with issues: 1
- Unknown function TODOs: 5
- Unsupported feature TODOs: 0
- Unknown function unique issues: 2
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `approx_top_k` | 4 | sql/aggregate/aggregates/approx_top_k.test |  |
| `list_sort` | 1 | sql/aggregate/aggregates/approx_top_k.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-10T07:48:18.542574+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 3
- Files with issues: 2
- Unknown function TODOs: 5
- Unsupported feature TODOs: 3
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `arg_max` | `unsupported feature` | 3 | sql/aggregate/aggregates/arg_min_max_n.test | `query I<br>SELECT arg_max(even_groups, i, 3) FROM t2;<br>----<br>[4, 3, 2]` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-10T07:48:20.934137+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 10
- Files with issues: 3
- Unknown function TODOs: 5
- Unsupported feature TODOs: 13
- Unknown function unique issues: 0
- Unsupported feature unique issues: 5

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `compute_bottom_k` | `unsupported feature` | 2 | sql/aggregate/aggregates/arg_min_max_n_tpch.test | `query II nosort bottom_resultset<br>SELECT * FROM compute_bottom_k(lineitem, l_returnflag, l_orderkey, 3);` |  |
| `compute_top_k` | `unsupported feature` | 2 | sql/aggregate/aggregates/arg_min_max_n_tpch.test | `query II nosort top_resultset<br>SELECT * FROM compute_top_k(lineitem, l_returnflag, l_orderkey, 3);` |  |
| `dbgen` | `unsupported feature` | 1 | sql/aggregate/aggregates/arg_min_max_n_tpch.test | `statement ok<br>CALL dbgen(sf=0.001);` |  |
| `max` | `unsupported feature` | 3 | sql/aggregate/aggregates/arg_min_max_n_tpch.test | `query I<br>select max(l_orderkey, 3) from lineitem;<br>----<br>[5988, 5987, 5987]` |  |
| `min` | `unsupported feature` | 2 | sql/aggregate/aggregates/arg_min_max_n_tpch.test | `query I<br>select min(l_orderkey, 3) from lineitem;<br>----<br>[1, 1, 1]` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-10T07:48:23.823022+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 11
- Files with issues: 4
- Unknown function TODOs: 5
- Unsupported feature TODOs: 24
- Unknown function unique issues: 0
- Unsupported feature unique issues: 3

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `arg_max_nulls_last` | `unsupported feature` | 5 | sql/aggregate/aggregates/arg_min_max_nulls_last.test | `query I<br>SELECT arg_max_nulls_last(arg, val) FROM tbl<br>----<br>NULL` |  |
| `arg_min_nulls_last` | `unsupported feature` | 5 | sql/aggregate/aggregates/arg_min_max_nulls_last.test | `query I<br>SELECT arg_min_nulls_last(arg, val) FROM tbl<br>----<br>5` |  |
| `t` | `unsupported feature` | 1 | sql/aggregate/aggregates/arg_min_max_nulls_last.test | `statement ok<br>CREATE TABLE tbl AS SELECT * FROM VALUES (1, 5, 1), (1, NULL, 2), (1, 3, NULL), (2, NULL, NULL), (3, 1, NULL) t(grp, arg, val)` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-10T07:48:31.958233+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 56
- Files with issues: 5
- Unknown function TODOs: 61
- Unsupported feature TODOs: 24
- Unknown function unique issues: 2
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `equi_width_bins` | 54 | sql/aggregate/aggregates/binning.test |  |
| `unnest` | 2 | sql/aggregate/aggregates/binning.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-10T07:48:38.113391+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 6
- Unknown function TODOs: 62
- Unsupported feature TODOs: 24
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `bitstring_agg` | 1 | sql/aggregate/aggregates/bitstring_agg_empty.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-10T07:48:43.392979+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 12
- Files with issues: 7
- Unknown function TODOs: 69
- Unsupported feature TODOs: 29
- Unknown function unique issues: 2
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `histogram_exact` | 4 | sql/aggregate/aggregates/histogram_exact.test |  |
| `is_histogram_other_bin` | 3 | sql/aggregate/aggregates/histogram_exact.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `histogram_exact` | `unsupported feature` | 2 | sql/aggregate/aggregates/histogram_exact.test | `query I<br>SELECT histogram_exact([n], [[x] for x in [10, 20, 30, 40, 50]]) FROM obs<br>----<br>{[10]=0, [20]=1, [30]=0, [40]=0, [50]=0, []=14}` |  |
| `is_histogram_other_bin` | `unsupported feature` | 3 | sql/aggregate/aggregates/histogram_exact.test | `query I<br>SELECT is_histogram_other_bin([]::INT[][][])<br>----<br>1` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-10T07:48:53.209297+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 9
- Files with issues: 8
- Unknown function TODOs: 69
- Unsupported feature TODOs: 38
- Unknown function unique issues: 0
- Unsupported feature unique issues: 3

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `count` | `unsupported feature` | 3 | sql/aggregate/aggregates/histogram_table_function.test | `query II<br>SELECT COUNT(*), AVG(count) FROM histogram_values(integers, i, technique := 'equi-height')<br>----<br>10	13` |  |
| `histogram` | `unsupported feature` | 1 | sql/aggregate/aggregates/histogram_table_function.test | `query II<br>SELECT bin, count FROM histogram(integers, i, bin_count := 10, technique := 'equi-width')<br>----<br>x <= 12	13<br>12 < x <= 25	13<br>25 < x <= 37	12<br>37 < x <= 50	14<br>50 < x <= 63	13<br>63 < x <= 75	12<br>75 < x <= 88	14<br>88 < x <= 100	12<br>100 < x <= 113	13<br>113 < x <= 126	13` |  |
| `histogram_values` | `unsupported feature` | 5 | sql/aggregate/aggregates/histogram_table_function.test | `query II<br>SELECT * FROM histogram_values(booleans, b::INTEGER)<br>----<br>0	75<br>1	25` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-10T07:49:00.540719+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 27
- Files with issues: 9
- Unknown function TODOs: 69
- Unsupported feature TODOs: 92
- Unknown function unique issues: 0
- Unsupported feature unique issues: 3

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `range` | `unsupported feature` | 13 | sql/aggregate/aggregates/max_n_all_types_grouped.test | `statement ok<br>CREATE OR REPLACE TABLE tbl AS SELECT * FROM<br>		(SELECT int as val_col FROM all_types)<br>	CROSS JOIN (SELECT i % 2 as grp_col FROM range(0, 5) as r(i));` |  |
| `test_all_types` | `unsupported feature` | 1 | sql/aggregate/aggregates/max_n_all_types_grouped.test | `statement ok<br>create table all_types as from test_all_types()` |  |
| `unsupported feature` | `unsupported feature` | 13 | sql/aggregate/aggregates/max_n_all_types_grouped.test | `query II<br>SELECT * FROM (SELECT * FROM window_table ORDER BY rowid) EXCEPT SELECT * FROM (SELECT * FROM agg_table ORDER BY rowid);<br>----` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T05:36:13.452731+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 71
- Files with issues: 11
- Unknown function TODOs: 70
- Unsupported feature TODOs: 165
- Unknown function unique issues: 1
- Unsupported feature unique issues: 7

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `typeof` | 1 | sql/aggregate/aggregates/test_aggregate_state_type.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `any_value` | `unsupported feature` | 18 | sql/aggregate/aggregates/test_aggregate_state_type.test<br>sql/aggregate/aggregates/test_any_value.test | `query I<br>SELECT ANY_VALUE(i ORDER BY 5-i) FROM five<br>----<br>5` |  |
| `avg` | `unsupported feature` | 17 | sql/aggregate/aggregates/test_aggregate_state_type.test | `statement ok<br>CREATE TABLE t1_DOUBLE AS SELECT avg(range::DOUBLE) EXPORT_STATE as state FROM range(0, 10);` |  |
| `finalize` | `unsupported feature` | 15 | sql/aggregate/aggregates/test_aggregate_state_type.test | `query R<br>SELECT finalize(combine(state, state)) FROM t1_DOUBLE;<br>----<br>4.500000` |  |
| `first` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_aggregate_state_type.test | `query T<br>SELECT (first(42) export_state)::struct("value" integer, is_set boolean, is_null boolean);<br>----<br>{'value': 42, 'is_set': 1, 'is_null': 0}` |  |
| `last` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_aggregate_state_type.test | `query T<br>SELECT (last(42) export_state)::struct("value" integer, is_set boolean, is_null boolean);<br>----<br>{'value': 42, 'is_set': 1, 'is_null': 0}` |  |
| `product` | `unsupported feature` | 2 | sql/aggregate/aggregates/test_aggregate_state_type.test | `statement ok<br>CREATE TABLE t3_product AS SELECT product(range::DOUBLE) EXPORT_STATE as state FROM range(1, 6);` |  |
| `typeof` | `unsupported feature` | 16 | sql/aggregate/aggregates/test_aggregate_state_type.test | `query T<br>SELECT typeof(min(1::BIGINT) EXPORT_STATE);<br>----<br>LEGACY_AGGREGATE_STATE<min(BIGINT)::BIGINT>` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T05:44:54.226416+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 5
- Files with issues: 10
- Unknown function TODOs: 80
- Unsupported feature TODOs: 158
- Unknown function unique issues: 1
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `finalize` | 1 | sql/aggregate/aggregates/test_aggregate_state_type.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `regr_avgx` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_aggregate_state_type.test | `query T<br>SELECT (regr_avgx(1.0, 2.0) export_state)::struct(sum double, count ubigint);<br>----<br>{'sum': 2.0, 'count': 1}` |  |
| `regr_count` | `unsupported feature` | 3 | sql/aggregate/aggregates/test_aggregate_state_type.test | `query T<br>SELECT (regr_count(1.0, 2.0) export_state)::struct(count ubigint);<br>----<br>{'count': 1}` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T05:46:07.485964+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 12
- Unknown function TODOs: 80
- Unsupported feature TODOs: 167
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `string_agg` | `unsupported feature` | 2 | sql/aggregate/aggregates/test_aggregate_types_scalar.test | `query T<br>SELECT STRING_AGG(DISTINCT val::VARCHAR ORDER BY val::VARCHAR DESC) from test_val;<br>----<br>3,2,1` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T05:48:55.436238+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 13
- Unknown function TODOs: 80
- Unsupported feature TODOs: 204
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `interval` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_any_value.test | `statement ok<br>CREATE TABLE five_dates AS<br>        SELECT 1 AS i,<br>               NULL::DATE AS d,<br>               NULL::TIMESTAMP AS dt,<br>               NULL::TIME AS t,<br>               NULL::INTERVAL AS s<br>        UNION ALL<br>	SELECT<br>		i::integer AS i,<br>		'2021-08-20'::DATE + i::INTEGER AS d,<br>		'2021-08-20'::TIMESTAMP + INTERVAL (i) HOUR AS dt,<br>		'14:59:37'::TIME + INTERVAL (i) MINUTE AS t,<br>		INTERVAL (i) SECOND AS s<br>	FROM range(1, 6, 1) t1(i)` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T05:49:05.219782+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 28
- Files with issues: 14
- Unknown function TODOs: 88
- Unsupported feature TODOs: 225
- Unknown function unique issues: 2
- Unsupported feature unique issues: 4

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `approx_quantile` | 6 | sql/aggregate/aggregates/test_approx_quantile.test |  |
| `reservoir_quantile` | 2 | sql/aggregate/aggregates/test_approx_quantile.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `approx_quantile` | `unsupported feature` | 6 | sql/aggregate/aggregates/test_approx_quantile.test | `query I<br>SELECT approx_quantile('1:02:03.000000+05:30'::TIMETZ, 0.5);<br>----<br>01:02:42+05:30:39` |  |
| `between` | `unsupported feature` | 10 | sql/aggregate/aggregates/test_approx_quantile.test | `query I<br>SELECT CASE<br>	  WHEN ( approx_quantile between (true_quantile - 100) and (true_quantile + 100) )<br>		  THEN TRUE<br>		  ELSE FALSE<br>	  END<br>	  FROM (SELECT approx_quantile(r, 0.5) as approx_quantile ,quantile(r,0.5) as true_quantile FROM quantile) AS T<br>----<br>1` |  |
| `random` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_approx_quantile.test | `statement ok<br>create table quantile as select range r, random() from range(0, 10000) union all values (NULL, 0.1), (NULL, 0.5), (NULL, 0.9) order by 2;` |  |
| `reservoir_quantile` | `unsupported feature` | 3 | sql/aggregate/aggregates/test_approx_quantile.test | `statement ok<br>SELECT reservoir_quantile(r, 0.9)  from quantile` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T05:49:43.987108+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 4
- Files with issues: 15
- Unknown function TODOs: 88
- Unsupported feature TODOs: 231
- Unknown function unique issues: 0
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `approx_count_distinct` | `unsupported feature` | 3 | sql/aggregate/aggregates/test_approximate_distinct_count.test | `statement ok<br>SELECT approx_count_distinct(c0 ORDER BY (c0, 1)) FROM issue5259;` |  |
| `mod` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_approximate_distinct_count.test | `statement ok<br>create  table t as select range a, mod(range,10) b from range(0, 2000);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T05:50:09.815965+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 24
- Files with issues: 16
- Unknown function TODOs: 108
- Unsupported feature TODOs: 238
- Unknown function unique issues: 4
- Unsupported feature unique issues: 3

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `argmax` | 5 | sql/aggregate/aggregates/test_arg_min_max.test |  |
| `argmin` | 9 | sql/aggregate/aggregates/test_arg_min_max.test |  |
| `max_by` | 3 | sql/aggregate/aggregates/test_arg_min_max.test |  |
| `min_by` | 3 | sql/aggregate/aggregates/test_arg_min_max.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `argmin` | `unsupported feature` | 2 | sql/aggregate/aggregates/test_arg_min_max.test | `query II<br>select argmin(a,b), argmax(a,b)  from blobs;<br>----<br>5	20` |  |
| `blobs` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_arg_min_max.test | `statement ok<br>CREATE TABLE blobs (b BYTEA, a BIGINT);` |  |
| `hugeints` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_arg_min_max.test | `statement ok<br>CREATE TABLE hugeints (z HUGEINT);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T05:50:39.042288+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 18
- Files with issues: 17
- Unknown function TODOs: 124
- Unsupported feature TODOs: 242
- Unknown function unique issues: 2
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `arg_max_null` | 5 | sql/aggregate/aggregates/test_arg_min_max_null.test |  |
| `arg_min_null` | 11 | sql/aggregate/aggregates/test_arg_min_max_null.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `arg_min_null` | `unsupported feature` | 2 | sql/aggregate/aggregates/test_arg_min_max_null.test | `query II<br>select arg_min_null(a,b), arg_max_null(a,b)  from blobs;<br>----<br>5	20` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T05:50:54.192594+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 18
- Unknown function TODOs: 124
- Unsupported feature TODOs: 259
- Unknown function unique issues: 0
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `timetzs` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_avg.test | `statement ok<br>CREATE TABLE timetzs (ttz TIMETZ);` |  |
| `vals` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_avg.test | `statement ok<br>CREATE TABLE vals(i INTEGER, j DOUBLE, k HUGEINT);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T05:50:55.339662+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 19
- Unknown function TODOs: 124
- Unsupported feature TODOs: 262
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `bigints` | `unsupported feature` | 2 | sql/aggregate/aggregates/test_bigint_avg.test | `statement ok<br>CREATE TABLE bigints(n HUGEINT);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T05:51:08.415536+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 9
- Files with issues: 21
- Unknown function TODOs: 124
- Unsupported feature TODOs: 290
- Unknown function unique issues: 0
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `bit_and` | `unsupported feature` | 8 | sql/aggregate/aggregates/test_bit_and.test | `query I<br>SELECT BIT_AND(nextval('seq'))<br>----<br>1` |  |
| `bits` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_bit_and.test | `statement ok<br>CREATE TABLE bits(b BIT);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T05:51:21.016438+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 8
- Files with issues: 22
- Unknown function TODOs: 124
- Unsupported feature TODOs: 301
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `bit_or` | `unsupported feature` | 8 | sql/aggregate/aggregates/test_bit_or.test | `query I<br>SELECT BIT_OR(nextval('seq'))<br>----<br>1` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T05:51:28.446200+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 9
- Files with issues: 23
- Unknown function TODOs: 124
- Unsupported feature TODOs: 313
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `bit_xor` | `unsupported feature` | 9 | sql/aggregate/aggregates/test_bit_xor.test | `query I<br>SELECT BIT_XOR(nextval('seq'))<br>----<br>1` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T05:52:25.875090+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 11
- Files with issues: 24
- Unknown function TODOs: 145
- Unsupported feature TODOs: 321
- Unknown function unique issues: 1
- Unsupported feature unique issues: 3

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `bit_count` | 7 | sql/aggregate/aggregates/test_bitstring_agg.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `bit_count` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_bitstring_agg.test | `query I nosort distinct_hugeints<br>SELECT bit_count(BITSTRING_AGG(i)) FROM hugeints<br>----` |  |
| `bit_length` | `unsupported feature` | 2 | sql/aggregate/aggregates/test_bitstring_agg.test | `query I<br>SELECT bit_length(BITSTRING_AGG(i)) FROM hugeints<br>----<br>56068` |  |
| `uhugeints` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_bitstring_agg.test | `statement ok<br>CREATE TABLE uhugeints(i UHUGEINT);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T05:52:54.754550+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 3
- Files with issues: 25
- Unknown function TODOs: 145
- Unsupported feature TODOs: 338
- Unknown function unique issues: 0
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `as` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_combine_aggr.test | `statement ok<br>CREATE TABLE legacy_states AS (SELECT g, (min(d) export_state) state FROM dummy GROUP BY g);` |  |
| `combine_aggr` | `unsupported feature` | 2 | sql/aggregate/aggregates/test_combine_aggr.test | `statement ok<br>create view combined_view as select combine_aggr(state) as global_state from states;` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T05:53:03.802564+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 5
- Files with issues: 26
- Unknown function TODOs: 149
- Unsupported feature TODOs: 339
- Unknown function unique issues: 1
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `corr` | 4 | sql/aggregate/aggregates/test_corr.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `corr` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_corr.test | `query II<br>select k, corr(v, v2) from aggr group by k ORDER BY ALL;<br>----<br>1	NULL<br>2	0.9988445981` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T05:53:20.430075+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 29
- Unknown function TODOs: 149
- Unsupported feature TODOs: 361
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `list_aggr` | `unsupported feature` | 2 | sql/aggregate/aggregates/test_count_all_types.test | `query I nosort count_int_list<br>SELECT list_aggr(n, 'count') FROM test_vector_types(NULL::INT[], all_flat=true) t(n);<br>----` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T05:53:54.058544+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 30
- Unknown function TODOs: 149
- Unsupported feature TODOs: 365
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `covar_pop` | `unsupported feature` | 2 | sql/aggregate/aggregates/test_covar.test | `query R<br>SELECT COVAR_POP(nextval('seqx'),nextval('seqy'))<br>----<br>0.000000` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T05:54:40.220409+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 10
- Files with issues: 32
- Unknown function TODOs: 158
- Unsupported feature TODOs: 367
- Unknown function unique issues: 1
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `entropy` | 9 | sql/aggregate/aggregates/test_entropy.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `entropy` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_entropy.test | `query I<br>select entropy(k) from aggr group by k%2 order by all<br>----<br>1.000000<br>1.584963` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T05:54:48.411362+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 34
- Unknown function TODOs: 159
- Unsupported feature TODOs: 383
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `arbitrary` | 1 | sql/aggregate/aggregates/test_first_noninlined.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T05:55:06.836563+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 35
- Unknown function TODOs: 159
- Unsupported feature TODOs: 392
- Unknown function unique issues: 0
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `enum` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_histogram.test | `statement ok<br>CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')` |  |
| `enums` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_histogram.test | `statement ok<br>CREATE TABLE enums (e mood)` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T05:56:17.646182+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 37
- Unknown function TODOs: 161
- Unsupported feature TODOs: 393
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `favg` | 1 | sql/aggregate/aggregates/test_kahan_avg.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T05:56:39.316256+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 3
- Files with issues: 38
- Unknown function TODOs: 164
- Unsupported feature TODOs: 393
- Unknown function unique issues: 3
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `fsum` | 1 | sql/aggregate/aggregates/test_kahan_sum.test |  |
| `kahan_sum` | 1 | sql/aggregate/aggregates/test_kahan_sum.test |  |
| `sumkahan` | 1 | sql/aggregate/aggregates/test_kahan_sum.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T05:58:48.413362+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 39
- Unknown function TODOs: 165
- Unsupported feature TODOs: 395
- Unknown function unique issues: 1
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `kurtosis_pop` | 1 | sql/aggregate/aggregates/test_kurtosis.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `kurtosis` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_kurtosis.test | `query I<br>select  kurtosis(v2) from aggr group by v ORDER BY ALL;<br>----<br>-3.977599<br>NULL<br>NULL<br>NULL` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:00:31.532441+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 51
- Files with issues: 42
- Unknown function TODOs: 171
- Unsupported feature TODOs: 495
- Unknown function unique issues: 1
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `mad` | 6 | sql/aggregate/aggregates/test_mad.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `mad` | `unsupported feature` | 45 | sql/aggregate/aggregates/test_mad.test | `query I<br>SELECT mad(r::tinyint) FROM tinys<br>----<br>25` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:00:34.675747+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 38
- Files with issues: 43
- Unknown function TODOs: 171
- Unsupported feature TODOs: 534
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `median` | `unsupported feature` | 38 | sql/aggregate/aggregates/test_median.test | `query I<br>SELECT median(42) FROM quantile<br>----<br>42` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:00:35.334184+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 44
- Unknown function TODOs: 171
- Unsupported feature TODOs: 537
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `generate_series` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_minmax.test | `statement ok<br>create table lists as select array[i] l from generate_series(0,5,1) tbl(i);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:01:07.008243+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 11
- Files with issues: 46
- Unknown function TODOs: 171
- Unsupported feature TODOs: 554
- Unknown function unique issues: 0
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `mode` | `unsupported feature` | 10 | sql/aggregate/aggregates/test_mode.test | `query I<br>select mode(v) from hugeints;<br>----<br>5` |  |
| `times` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_mode.test | `statement ok<br>create table times (k int, v time)` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:02:27.424514+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 47
- Unknown function TODOs: 171
- Unsupported feature TODOs: 564
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `list` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_order_by_aggregate.test | `query II<br>SELECT <br>    user_id, <br>    list(DISTINCT cause ORDER BY cause DESC) FILTER(cause IS NOT NULL) AS causes<br>FROM user_causes <br>GROUP BY user_id;<br>----<br>1	[Social, Health, Environmental]` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:02:32.398548+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 10
- Files with issues: 48
- Unknown function TODOs: 179
- Unsupported feature TODOs: 568
- Unknown function unique issues: 2
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `percentile_cont` | 4 | sql/aggregate/aggregates/test_ordered_aggregates.test |  |
| `percentile_disc` | 4 | sql/aggregate/aggregates/test_ordered_aggregates.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `percentile_disc` | `unsupported feature` | 2 | sql/aggregate/aggregates/test_ordered_aggregates.test | `query I<br>SELECT percentile_disc(.5) WITHIN GROUP (order by col desc) <br>FROM VALUES (11000), (3100), (2900), (2800), (2600), (2500) AS tab(col);<br>----<br>2900` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:03:19.622716+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 5
- Files with issues: 49
- Unknown function TODOs: 179
- Unsupported feature TODOs: 573
- Unknown function unique issues: 0
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `extract` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_perfect_ht.test | `query II<br>select extract(year from d), extract(month from d) from dates group by 1, 2 ORDER BY ALL;<br>----<br>1992	1<br>1992	2<br>1992	3<br>1992	4<br>1992	5<br>1992	6<br>1992	7<br>1992	8<br>1992	9<br>1992	10<br>1992	11<br>1992	12<br>1993	1<br>1993	2<br>1993	3<br>1993	4<br>1993	5<br>1993	6<br>1993	7<br>1993	8<br>1993	9<br>1993	10<br>1993	11<br>1993	12<br>1994	1<br>1994	2<br>1994	3<br>1994	4<br>1994	5<br>1994	6<br>1994	7<br>1994	8<br>1994	9<br>1994	10<br>1994	11<br>1994	12<br>1995	1<br>1995	2<br>1995	3<br>1995	4<br>1995	5<br>1995	6<br>1995	7<br>1995	8<br>1995	9<br>1995	10<br>1995	11<br>1995	12<br>1996	1<br>1996	2<br>1996	3<br>1996	4<br>1996	5<br>1996	6<br>1996	7<br>1996	8<br>1996	9<br>1996	10<br>1996	11<br>1996	12<br>1997	1<br>1997	2<br>1997	3<br>1997	4<br>1997	5<br>1997	6<br>1997	7<br>1997	8<br>1997	9<br>1997	10<br>1997	11<br>1997	12<br>1998	1<br>1998	2<br>1998	3<br>1998	4<br>1998	5<br>1998	6<br>1998	7<br>1998	8<br>1998	9<br>1998	10<br>1998	11<br>1998	12<br>1999	1<br>1999	2<br>1999	3<br>1999	4<br>1999	5<br>1999	6<br>1999	7<br>1999	8<br>1999	9<br>1999	10<br>1999	11<br>1999	12<br>2000	1<br>2000	2<br>2000	3<br>2000	4` |  |
| `timeseries` | `unsupported feature` | 4 | sql/aggregate/aggregates/test_perfect_ht.test | `statement ok<br>CREATE OR REPLACE TABLE timeseries(year UBIGINT, val UBIGINT);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:03:23.696430+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 7
- Files with issues: 50
- Unknown function TODOs: 186
- Unsupported feature TODOs: 574
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `product` | 7 | sql/aggregate/aggregates/test_product.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:03:43.262258+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 37
- Files with issues: 51
- Unknown function TODOs: 186
- Unsupported feature TODOs: 614
- Unknown function unique issues: 0
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `percentile_cont` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_quantile_cont.test | `query II<br>SELECT <br>    percentile_cont(0.8) WITHIN GROUP (ORDER BY x DESC),<br>    quantile_cont(x, 0.8 ORDER BY x DESC),<br>FROM <br>    (VALUES (2), (1)) _(x);<br>----<br>1.2	1.2` |  |
| `quantile_cont` | `unsupported feature` | 36 | sql/aggregate/aggregates/test_quantile_cont.test | `query R<br>SELECT quantile_cont(42, 0.5)<br>----<br>42` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:03:52.096909+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 30
- Files with issues: 53
- Unknown function TODOs: 186
- Unsupported feature TODOs: 688
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `quantile_disc` | `unsupported feature` | 30 | sql/aggregate/aggregates/test_quantile_disc.test | `query I<br>SELECT quantile_disc(42, 0.5)<br>----<br>42` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:04:04.526581+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 46
- Files with issues: 55
- Unknown function TODOs: 225
- Unsupported feature TODOs: 725
- Unknown function unique issues: 9
- Unsupported feature unique issues: 7

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `regr_avgx` | 4 | sql/aggregate/aggregates/test_regression.test |  |
| `regr_avgy` | 4 | sql/aggregate/aggregates/test_regression.test |  |
| `regr_count` | 4 | sql/aggregate/aggregates/test_regression.test |  |
| `regr_intercept` | 4 | sql/aggregate/aggregates/test_regression.test |  |
| `regr_r2` | 6 | sql/aggregate/aggregates/test_regression.test |  |
| `regr_slope` | 4 | sql/aggregate/aggregates/test_regression.test |  |
| `regr_sxx` | 5 | sql/aggregate/aggregates/test_regression.test |  |
| `regr_sxy` | 4 | sql/aggregate/aggregates/test_regression.test |  |
| `regr_syy` | 4 | sql/aggregate/aggregates/test_regression.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `regr_avgy` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_regression.test | `query II<br>select k, regr_avgy(v, v2) from aggr group by k ORDER BY ALL;<br>----<br>1	NULL<br>2	20` |  |
| `regr_intercept` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_regression.test | `query II<br>select k, regr_intercept(v, v2) from aggr group by k ORDER BY ALL;<br>----<br>1	NULL<br>2	1.154734` |  |
| `regr_r2` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_regression.test | `query II<br>select k, regr_r2(v, v2) from aggr group by k ORDER BY ALL;<br>----<br>1	NULL<br>2	0.997691` |  |
| `regr_slope` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_regression.test | `query II<br>select k, regr_slope(v, v2) from aggr group by k ORDER BY ALL;<br>----<br>1	NULL<br>2	0.831409` |  |
| `regr_sxx` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_regression.test | `query II<br>select k, regr_sxx(v, v2) from aggr group by k ORDER BY ALL;<br>----<br>1	NULL<br>2	288.666667` |  |
| `regr_sxy` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_regression.test | `query II<br>select k, regr_sxy(v, v2) from aggr group by k ORDER BY ALL;<br>----<br>1	NULL<br>2	240.000000` |  |
| `regr_syy` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_regression.test | `query II<br>select k, regr_syy(v, v2) from aggr group by k ORDER BY ALL;<br>----<br>1	NULL<br>2	200.000000` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:04:11.768555+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 5
- Files with issues: 57
- Unknown function TODOs: 228
- Unsupported feature TODOs: 734
- Unknown function unique issues: 1
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `sem` | 3 | sql/aggregate/aggregates/test_sem.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `sem` | `unsupported feature` | 2 | sql/aggregate/aggregates/test_sem.test | `query III<br>select k, sem(v),sem(v2)  from aggr group by k ORDER BY ALL;<br>----<br>1	0.000000	NULL<br>2	3.697550	5.663398` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:04:13.581786+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 4
- Files with issues: 58
- Unknown function TODOs: 228
- Unsupported feature TODOs: 741
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `sum` | `unsupported feature` | 4 | sql/aggregate/aggregates/test_simple_filter.test | `query III<br>SELECT<br>	SUM(pay) FILTER (gender = 'male'),<br>	SUM(pay),<br>	SUM(pay) FILTER (gender = 'female')<br>FROM issue3105;<br>----<br>600.000000	1000.000000	400.000000` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:04:21.685124+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 59
- Unknown function TODOs: 228
- Unsupported feature TODOs: 742
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `skewness` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_skewness.test | `query I<br>select skewness(v2) from aggr group by v ORDER BY ALL<br>----<br>-0.423273<br>-0.330141<br>NULL<br>NULL` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:04:34.531045+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 6
- Files with issues: 61
- Unknown function TODOs: 230
- Unsupported feature TODOs: 838
- Unknown function unique issues: 0
- Unsupported feature unique issues: 3

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `bool_and` | `unsupported feature` | 2 | sql/aggregate/aggregates/test_state_export_struct.test | `query II nosort res8a<br>SELECT g, bool_and(d > 50) FROM dummy GROUP BY g ORDER BY g;<br>----` |  |
| `bool_or` | `unsupported feature` | 3 | sql/aggregate/aggregates/test_state_export_struct.test | `query II nosort res8a2<br>SELECT g, bool_or(d > 50) FROM dummy GROUP BY g ORDER BY g;<br>----` |  |
| `favg` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_state_export_struct.test | `query IIIIIII nosort res8<br>SELECT g, favg(d) FROM dummy GROUP BY g ORDER BY g;<br>----` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:04:45.379619+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 6
- Files with issues: 62
- Unknown function TODOs: 231
- Unsupported feature TODOs: 847
- Unknown function unique issues: 1
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `round` | 1 | sql/aggregate/aggregates/test_stddev.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `round` | `unsupported feature` | 4 | sql/aggregate/aggregates/test_stddev.test | `query R<br>select round(var_pop(val), 1) from stddev_test<br>----<br>171961.200000` |  |
| `var_samp` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_stddev.test | `query I<br>SELECT var_samp(1)<br>----<br>NULL` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:05:18.910227+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 7
- Files with issues: 66
- Unknown function TODOs: 238
- Unsupported feature TODOs: 867
- Unknown function unique issues: 2
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `wavg` | 1 | sql/aggregate/aggregates/test_weighted_avg.test |  |
| `weighted_avg` | 6 | sql/aggregate/aggregates/test_weighted_avg.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:05:21.954461+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 10
- Files with issues: 67
- Unknown function TODOs: 238
- Unsupported feature TODOs: 877
- Unknown function unique issues: 0
- Unsupported feature unique issues: 3

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `asc` | `unsupported feature` | 1 | sql/aggregate/distinct/distinct_on_nulls.test | `statement ok<br>INSERT INTO distinct_on_test VALUES<br>	(1, 'hello', ARRAY[1], 42), -- ASC<br>	(1, 'hello', ARRAY[1], 42),<br>	(1, 'hello', ARRAY[1], 43), -- DESC<br>	(2, NULL, NULL, 0),     -- ASC<br>	(2, NULL, NULL, 1),<br>	(2, NULL, NULL, NULL),  -- DESC<br>	(3, 'thisisalongstring', NULL, 0),     -- ASC<br>	(3, 'thisisalongstringbutlonger', NULL, 1),<br>	(3, 'thisisalongstringbutevenlonger', ARRAY[1, 2, 3, 4, 5, 6, 7, 8, 9], 2)  -- DESC<br>;` |  |
| `distinct_on_test` | `unsupported feature` | 1 | sql/aggregate/distinct/distinct_on_nulls.test | `statement ok<br>CREATE TABLE distinct_on_test(key INTEGER, v1 VARCHAR, v2 INTEGER[], v3 INTEGER);` |  |
| `on` | `unsupported feature` | 8 | sql/aggregate/distinct/distinct_on_nulls.test | `query II<br>SELECT DISTINCT ON (i) i, j FROM integers ORDER BY j<br>----<br>2	3<br>4	5<br>NULL	NULL` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:05:30.912456+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 9
- Files with issues: 69
- Unknown function TODOs: 238
- Unsupported feature TODOs: 905
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `grouping` | `unsupported feature` | 9 | sql/aggregate/distinct/grouped/combined_with_grouping.test | `query IIII<br>SELECT GROUPING(course), course, sum(distinct value), COUNT(*) FROM students GROUP BY course ORDER BY all;<br>----<br>0	CS	56	5<br>0	Math	12	3` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:06:31.562676+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 81
- Unknown function TODOs: 239
- Unsupported feature TODOs: 980
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `count` | 1 | sql/aggregate/distinct/ungrouped/test_distinct_ungrouped_shared_input.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:06:41.115090+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 83
- Unknown function TODOs: 239
- Unsupported feature TODOs: 984
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `t0` | `unsupported feature` | 1 | sql/aggregate/group/group_by_all_having.test | `query I<br>SELECT c0 FROM (SELECT 1, 1 UNION ALL SELECT 1, 2) t0(c0, c1) GROUP BY ALL HAVING c1>0 ORDER BY c0<br>----<br>1<br>1` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:07:51.470021+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 12
- Files with issues: 88
- Unknown function TODOs: 240
- Unsupported feature TODOs: 1036
- Unknown function unique issues: 1
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `sv` | 1 | sql/aggregate/group/test_group_by_nested.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `least` | `unsupported feature` | 6 | sql/aggregate/group/test_group_by_nested.test | `query III<br>SELECT k, LEAST(v, 21) as c, SUM(v) FROM structs GROUP BY k, c ORDER BY 2, 3<br>----<br>{'x': 13, 'y': Somateria mollissima}	21	NULL<br>{'x': 1, 'y': a}	21	21<br>{'x': 0, 'y': ''}	21	23<br>{'x': NULL, 'y': NULL}	21	54<br>{'x': 2, 'y': c}	21	58` |  |
| `sv` | `unsupported feature` | 5 | sql/aggregate/group/test_group_by_nested.test | `statement ok<br>CREATE VIEW structs AS SELECT * FROM (VALUES<br>	(21, {'x': 1, 'y': 'a'}),<br>	(22, {'x': NULL, 'y': NULL}),<br>	(23, {'x': 0, 'y': ''}),<br>	(24, {'x': 2, 'y': 'c'}),<br>	(NULL::INTEGER, {'x': 13, 'y': 'Somateria mollissima'}),<br>	(32, {'x': NULL, 'y': NULL}),<br>	(34, {'x': 2, 'y': 'c'})<br>	) sv(v, k);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:10:43.869558+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 90
- Unknown function TODOs: 241
- Unsupported feature TODOs: 1041
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `grouping_id` | 1 | sql/aggregate/grouping_sets/grouping.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:10:52.434492+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 91
- Unknown function TODOs: 241
- Unsupported feature TODOs: 1048
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `by` | `unsupported feature` | 1 | sql/aggregate/grouping_sets/grouping_sets.test | `query I<br>select 1 from students group by ();<br>----<br>1` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:13:11.760935+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 4
- Files with issues: 95
- Unknown function TODOs: 242
- Unsupported feature TODOs: 1061
- Unknown function unique issues: 0
- Unsupported feature unique issues: 4

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `cume_dist` | `unsupported feature` | 1 | sql/aggregate/qualify/test_qualify.test | `query I<br>SELECT unique1 FROM tenk1 QUALIFY cast(cume_dist() OVER (PARTITION BY four ORDER BY ten)*10 as integer) = 5 order by four, ten<br>----<br>6701<br>4321<br>1891` |  |
| `first_value` | `unsupported feature` | 1 | sql/aggregate/qualify/test_qualify.test | `query I<br>SELECT unique1 FROM tenk1 QUALIFY first_value(ten) OVER (PARTITION BY four ORDER BY ten) = 1 order by four, ten<br>----<br>6701	<br>4321	<br>5057	<br>8009	<br>1891	<br>3043` |  |
| `lead` | `unsupported feature` | 1 | sql/aggregate/qualify/test_qualify.test | `query I<br>SELECT unique1 FROM tenk1 qualify lead(ten * 2, 1, -1) OVER (PARTITION BY four ORDER BY ten) = -1 order by four, ten<br>----<br>7164	<br>8009	<br>9850	<br>3043` |  |
| `tenk1` | `unsupported feature` | 1 | sql/aggregate/qualify/test_qualify.test | `statement ok<br>CREATE TABLE tenk1 (unique1 int4, unique2 int4, two int4, four int4, ten int4, twenty int4, hundred int4, thousand int4, twothousand int4, fivethous int4, tenthous int4, odd int4, even int4, stringu1 varchar, stringu2 varchar, string4 varchar)` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:13:23.502933+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 6
- Files with issues: 96
- Unknown function TODOs: 245
- Unsupported feature TODOs: 1066
- Unknown function unique issues: 1
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `plus1` | 3 | sql/aggregate/qualify/test_qualify_macro.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `plus1` | `unsupported feature` | 3 | sql/aggregate/qualify/test_qualify_macro.test | `statement ok<br>CREATE MACRO plus1(x) AS (x + (SELECT COUNT(*) FROM (SELECT b, SUM(test.a) FROM test GROUP BY b QUALIFY row_number() OVER (PARTITION BY b) = 1)))` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:13:46.707570+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 97
- Unknown function TODOs: 245
- Unsupported feature TODOs: 1077
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `row_number` | `unsupported feature` | 2 | sql/aggregate/qualify/test_qualify_view.test | `statement ok<br>CREATE VIEW test.v AS SELECT * FROM test.t QUALIFY row_number() OVER (PARTITION BY b) = 1;` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:15:15.200970+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 100
- Unknown function TODOs: 246
- Unsupported feature TODOs: 1107
- Unknown function unique issues: 1
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `currval` | 1 | sql/alter/add_col/test_add_col_default_seq.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `nextval` | `unsupported feature` | 1 | sql/alter/add_col/test_add_col_default_seq.test | `statement ok<br>ALTER TABLE test ADD COLUMN m INTEGER DEFAULT nextval('seq')` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:15:26.980023+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 101
- Unknown function TODOs: 246
- Unsupported feature TODOs: 1108
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `test` | `unsupported feature` | 1 | sql/alter/add_col/test_add_col_index.test | `statement ok<br>CREATE INDEX i_index ON test(k)` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:16:06.927606+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 104
- Unknown function TODOs: 247
- Unsupported feature TODOs: 1114
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `stats` | 1 | sql/alter/add_col/test_add_col_stats.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:16:45.288868+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 107
- Unknown function TODOs: 247
- Unsupported feature TODOs: 1121
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `key` | `unsupported feature` | 1 | sql/alter/add_pk/test_add_multi_column_pk.test | `statement ok<br>ALTER TABLE test ADD PRIMARY KEY (i, j);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:16:55.216807+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 108
- Unknown function TODOs: 247
- Unsupported feature TODOs: 1128
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `duckdb_tables` | `unsupported feature` | 2 | sql/alter/add_pk/test_add_pk.test | `query TTI<br>SELECT table_name, database_name, temporary FROM duckdb_tables() WHERE table_name='temp_pk_test'<br>----<br>temp_pk_test	temp	1` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:17:06.492725+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 111
- Unknown function TODOs: 247
- Unsupported feature TODOs: 1138
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `uniq` | `unsupported feature` | 1 | sql/alter/add_pk/test_add_pk_catalog_error.test | `statement ok<br>CREATE TABLE uniq (i INTEGER UNIQUE, j INTEGER)` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:17:28.377336+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 116
- Unknown function TODOs: 247
- Unsupported feature TODOs: 1145
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `tbl` | `unsupported feature` | 1 | sql/alter/add_pk/test_add_pk_naming_conflict.test | `statement ok<br>CREATE INDEX PRIMARY_tbl_i ON tbl(i);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:17:40.263303+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 121
- Unknown function TODOs: 247
- Unsupported feature TODOs: 1155
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `other` | `unsupported feature` | 1 | sql/alter/add_pk/test_add_same_pk_twice.test | `statement ok<br>CREATE TABLE other (i INTEGER PRIMARY KEY, j INTEGER)` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:19:15.424586+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 126
- Unknown function TODOs: 247
- Unsupported feature TODOs: 1208
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `row` | `unsupported feature` | 2 | sql/alter/alter_type/alter_type_struct.test | `statement ok<br>ALTER TABLE test ALTER t TYPE ROW(t VARCHAR) USING {'t': concat('hello', (t.t + 42)::varchar)}` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:19:20.299163+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 127
- Unknown function TODOs: 248
- Unsupported feature TODOs: 1216
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `struct_insert` | `unsupported feature` | 1 | sql/alter/alter_type/test_alter_type.test | `statement ok<br>ALTER TABLE tbl ALTER col TYPE USING struct_insert(col, a := 42, b := NULL::VARCHAR);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:19:25.693169+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 129
- Unknown function TODOs: 248
- Unsupported feature TODOs: 1225
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `v2` | `unsupported feature` | 1 | sql/alter/alter_type/test_alter_type_dependencies.test | `query I<br>EXECUTE v2(1)<br>----<br>2<br>3` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:23:01.933342+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 140
- Unknown function TODOs: 248
- Unsupported feature TODOs: 1269
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `constrainty` | `unsupported feature` | 2 | sql/alter/default/test_set_default.test | `statement ok<br>INSERT INTO constrainty (i) VALUES (2)` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:23:23.082192+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 142
- Unknown function TODOs: 248
- Unsupported feature TODOs: 1272
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `test2` | `unsupported feature` | 1 | sql/alter/drop_col/test_drop_col_failure.test | `statement ok<br>CREATE TABLE test2 (id INT PRIMARY KEY, name TEXT, surname TEXT, age INT, UNIQUE(surname, age))` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:24:48.849444+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 159
- Unknown function TODOs: 248
- Unsupported feature TODOs: 1383
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `tbl4` | `unsupported feature` | 1 | sql/alter/rename_table/test_rename_table.test | `statement ok<br>CREATE TABLE tbl4(i INTEGER);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:24:57.097570+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 161
- Unknown function TODOs: 248
- Unsupported feature TODOs: 1393
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `entry` | `unsupported feature` | 2 | sql/alter/rename_table/test_rename_table_chain_commit.test | `statement ok<br>CREATE TABLE entry(j INTEGER);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:25:33.102663+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 168
- Unknown function TODOs: 248
- Unsupported feature TODOs: 1418
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `t3` | `unsupported feature` | 1 | sql/alter/rename_table/test_rename_table_with_dependency_check.test | `statement ok<br>CREATE TABLE t3 (c0 INT);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:26:28.848716+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 184
- Unknown function TODOs: 248
- Unsupported feature TODOs: 1520
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `t1` | `unsupported feature` | 2 | sql/attach/attach_create_index.test | `statement ok<br>CREATE TABLE tmp.t1(id int);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:26:33.728374+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 185
- Unknown function TODOs: 248
- Unsupported feature TODOs: 1525
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `integers` | `unsupported feature` | 1 | sql/attach/attach_cross_catalog.test | `statement ok<br>CREATE TABLE db1.integers(i mood)` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:26:36.299012+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 3
- Files with issues: 187
- Unknown function TODOs: 248
- Unsupported feature TODOs: 1531
- Unknown function unique issues: 0
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `duckdb_databases` | `unsupported feature` | 2 | sql/attach/attach_database_options.test | `query I<br>SELECT options['block_size'] from duckdb_databases() where database_name = 'new_database';<br>----<br>262144` |  |
| `new_database` | `unsupported feature` | 1 | sql/attach/attach_database_options.test | `statement ok<br>ATTACH DATABASE ':memory:' AS new_database (BLOCK_SIZE 262144, ROW_GROUP_SIZE 2048);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:26:37.792371+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 188
- Unknown function TODOs: 248
- Unsupported feature TODOs: 1534
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `pragma_database_size` | `unsupported feature` | 2 | sql/attach/attach_database_size.test | `query I<br>SELECT database_name FROM pragma_database_size() WHERE database_name = 'db1';<br>----<br>db1` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:26:49.694099+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 3
- Files with issues: 192
- Unknown function TODOs: 248
- Unsupported feature TODOs: 1565
- Unknown function unique issues: 0
- Unsupported feature unique issues: 3

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `concat` | `unsupported feature` | 1 | sql/attach/attach_dependencies.test | `statement ok<br>ALTER TABLE tbl_alter_column ALTER other SET DATA TYPE VARCHAR USING concat(other, '_', 'yay');` |  |
| `fk_tbl` | `unsupported feature` | 1 | sql/attach/attach_dependencies.test | `statement ok<br>CREATE TABLE fk_tbl (id INTEGER REFERENCES pk_tbl(id));` |  |
| `pk_tbl` | `unsupported feature` | 1 | sql/attach/attach_dependencies.test | `statement ok<br>CREATE TABLE pk_tbl (id INTEGER PRIMARY KEY, name VARCHAR UNIQUE);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:26:53.903694+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 193
- Unknown function TODOs: 248
- Unsupported feature TODOs: 1574
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `blablabla` | `unsupported feature` | 1 | sql/attach/attach_did_you_mean.test | `statement ok<br>CREATE TABLE db1.myschema.blablabla(i INTEGER)` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:27:01.255115+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 199
- Unknown function TODOs: 248
- Unsupported feature TODOs: 1597
- Unknown function unique issues: 0
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `enc1` | `unsupported feature` | 1 | sql/attach/attach_encryption_downgrade_prevention.test | `statement ok<br>ATTACH 'data/attach_test/encrypted_ctr_key=abcde.db' as enc1 (ENCRYPTION_KEY 'abcde', ENCRYPTION_CIPHER 'CTR');` |  |
| `enc2` | `unsupported feature` | 1 | sql/attach/attach_encryption_downgrade_prevention.test | `statement ok<br>ATTACH 'data/attach_test/encrypted_gcm_key=abcde.db' as enc2 (ENCRYPTION_KEY 'abcde');` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:27:03.011285+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 200
- Unknown function TODOs: 248
- Unsupported feature TODOs: 1606
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `enc` | `unsupported feature` | 2 | sql/attach/attach_encryption_fallback_readonly.test | `statement ok<br>ATTACH 'data/attach_test/encrypted_gcm_key=abcde.db' as enc (ENCRYPTION_KEY 'abcde', ENCRYPTION_CIPHER 'GCM');` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:27:05.222571+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 4
- Files with issues: 201
- Unknown function TODOs: 248
- Unsupported feature TODOs: 1622
- Unknown function unique issues: 0
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `enum_range` | `unsupported feature` | 2 | sql/attach/attach_enums.test | `query I<br>SELECT enum_range(NULL::db1.mood) AS my_enum_range;<br>----<br>[sad, ok, happy]` |  |
| `person` | `unsupported feature` | 2 | sql/attach/attach_enums.test | `statement ok<br>CREATE TABLE db1.person (<br>    name text,<br>    current_mood mood<br>);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:27:10.250307+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 202
- Unknown function TODOs: 248
- Unsupported feature TODOs: 1627
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `dont_export_me` | `unsupported feature` | 1 | sql/attach/attach_export_import.test | `statement ok<br>CREATE TABLE other.dont_export_me (i integer);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:27:10.677464+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 203
- Unknown function TODOs: 248
- Unsupported feature TODOs: 1628
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `db1` | `unsupported feature` | 1 | sql/attach/attach_expr.test | `statement ok<br>ATTACH ':memory:' AS db1 (TYPE getvariable('db_type'))` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:27:13.535503+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 3
- Files with issues: 206
- Unknown function TODOs: 248
- Unsupported feature TODOs: 1645
- Unknown function unique issues: 0
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `album` | `unsupported feature` | 2 | sql/attach/attach_foreign_key.test | `statement ok<br>CREATE TABLE album(artistid INTEGER, albumname TEXT, albumcover TEXT, UNIQUE (artistid, albumname));` |  |
| `song` | `unsupported feature` | 1 | sql/attach/attach_foreign_key.test | `statement ok<br>CREATE TABLE song(songid INTEGER, songartist INTEGER, songalbum TEXT, songname TEXT, FOREIGN KEY(songartist, songalbum) REFERENCES album(artistid, albumname));` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:27:18.918155+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 212
- Unknown function TODOs: 248
- Unsupported feature TODOs: 1669
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `tbl_a` | `unsupported feature` | 2 | sql/attach/attach_index.test | `statement ok<br>CREATE INDEX idx_tbl_a ON tbl_a (value)` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:27:22.640861+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 213
- Unknown function TODOs: 248
- Unsupported feature TODOs: 1672
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `mytable` | `unsupported feature` | 1 | sql/attach/attach_issue16122.test | `statement ok<br>create table TOMERGE.mytable (C1 VARCHAR(10));` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:27:36.662364+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 4
- Files with issues: 217
- Unknown function TODOs: 248
- Unsupported feature TODOs: 1694
- Unknown function unique issues: 0
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `list_reduce` | `unsupported feature` | 2 | sql/attach/attach_lambda_view.test | `statement ok<br>CREATE VIEW version_1_2_0.reduced_lists AS<br>	SELECT list_reduce(l, LAMBDA x, y : x + y, initial) AS r FROM version_1_2_0.lists;` |  |
| `lists` | `unsupported feature` | 2 | sql/attach/attach_lambda_view.test | `statement ok<br>CREATE TABLE version_1_2_0.lists(l integer[], initial integer);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:27:38.788479+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 4
- Files with issues: 218
- Unknown function TODOs: 249
- Unsupported feature TODOs: 1698
- Unknown function unique issues: 1
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `two_x_plus_y` | 1 | sql/attach/attach_macros.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `two_x_plus_y` | `unsupported feature` | 3 | sql/attach/attach_macros.test | `query I<br>SELECT db1.two_x_plus_y(x, y) FROM db1.tbl;<br>----<br>87` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:27:43.545934+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 6
- Files with issues: 220
- Unknown function TODOs: 249
- Unsupported feature TODOs: 1727
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `exclude` | `unsupported feature` | 6 | sql/attach/attach_multi_identifiers.test | `query I<br>SELECT * EXCLUDE (db1.s1.t.c) FROM db1.s1.t, db2.s1.t<br>----<br>84` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:27:54.668167+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 226
- Unknown function TODOs: 249
- Unsupported feature TODOs: 1774
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `pragma_storage_info` | `unsupported feature` | 1 | sql/attach/attach_pragma_storage_info.test | `query I<br>SELECT column_name from pragma_storage_info('persistent.T1');<br>----<br>A0<br>A0` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:28:17.652539+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 6
- Files with issues: 235
- Unknown function TODOs: 249
- Unsupported feature TODOs: 1813
- Unknown function unique issues: 0
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `a` | `unsupported feature` | 4 | sql/attach/attach_serialize_dependency.test | `statement ok<br>CREATE INDEX A_index ON A (A2);` |  |
| `b` | `unsupported feature` | 2 | sql/attach/attach_serialize_dependency.test | `statement ok<br>CREATE TABLE B(B1 INTEGER REFERENCES A(A1));` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:28:24.773712+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 237
- Unknown function TODOs: 249
- Unsupported feature TODOs: 1828
- Unknown function unique issues: 0
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `table_in_db2` | `unsupported feature` | 1 | sql/attach/attach_show_table.test | `statement ok<br>CREATE TABLE db2.table_in_db2(i int);` |  |
| `table_in_db2_test_schema` | `unsupported feature` | 1 | sql/attach/attach_show_table.test | `statement ok<br>CREATE TABLE db2.test_schema.table_in_db2_test_schema(i int);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:28:32.720834+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 240
- Unknown function TODOs: 249
- Unsupported feature TODOs: 1893
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `replace` | `unsupported feature` | 2 | sql/attach/attach_table_ddl.test | `statement ok<br>ALTER TABLE new_database.integers ALTER j TYPE INT USING REPLACE(j, 'T', '')::INT` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:31:12.661319+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 244
- Unknown function TODOs: 249
- Unsupported feature TODOs: 1921
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `my_tbl` | `unsupported feature` | 1 | sql/attach/attach_view_search_path.test | `statement ok<br>CREATE TABLE my_tbl(i INTEGER)` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T06:31:24.172963+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 246
- Unknown function TODOs: 249
- Unsupported feature TODOs: 1954
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `t2` | `unsupported feature` | 1 | sql/attach/attach_wal_alter.test | `statement ok<br>CREATE TABLE t2(c1 INT);` |  |
