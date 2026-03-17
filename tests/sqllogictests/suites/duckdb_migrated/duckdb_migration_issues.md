# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T13:08:55.663985+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 58
- Files with issues: 2
- Unknown function TODOs: 36
- Unsupported feature TODOs: 22
- Unknown function unique issues: 2
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `equi_width_bins` | 34 | sql/aggregate/aggregates/binning.test |  |
| `unnest` | 2 | sql/aggregate/aggregates/binning.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `compute_top_k` | `unsupported feature` | 11 | sql/aggregate/aggregates/max_n_all_types_grouped.test | `statement ok<br>CREATE OR REPLACE TABLE window_table AS SELECT * FROM compute_top_k(tbl, grp_col, val_col, 2) as rs(grp, res);` |  |
| `unsupported feature` | `unsupported feature` | 11 | sql/aggregate/aggregates/max_n_all_types_grouped.test | `query II<br>SELECT * FROM (SELECT * FROM window_table ORDER BY rowid) EXCEPT SELECT * FROM (SELECT * FROM agg_table ORDER BY rowid);<br>----` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T13:23:08.278911+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 5
- Files with issues: 3
- Unknown function TODOs: 41
- Unsupported feature TODOs: 22
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

- Generated at (UTC): 2026-03-16T13:23:14.102280+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 10
- Files with issues: 4
- Unknown function TODOs: 51
- Unsupported feature TODOs: 22
- Unknown function unique issues: 2
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `arg_max_nulls_last` | 5 | sql/aggregate/aggregates/arg_min_max_nulls_last.test |  |
| `arg_min_nulls_last` | 5 | sql/aggregate/aggregates/arg_min_max_nulls_last.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T13:23:25.350136+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 5
- Unknown function TODOs: 72
- Unsupported feature TODOs: 22
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

- Generated at (UTC): 2026-03-16T13:33:44.900153+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 31
- Files with issues: 9
- Unknown function TODOs: 74
- Unsupported feature TODOs: 53
- Unknown function unique issues: 1
- Unsupported feature unique issues: 9

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `is_histogram_other_bin` | 2 | sql/aggregate/aggregates/histogram_exact.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `arg_max` | `unsupported feature` | 3 | sql/aggregate/aggregates/arg_min_max_n.test | `query I<br>SELECT arg_max(even_groups, i, 3) FROM t2;<br>----<br>[4, 3, 2]` |  |
| `compute_bottom_k` | `unsupported feature` | 1 | sql/aggregate/aggregates/arg_min_max_n_tpch.test | `statement ok<br>CREATE MACRO compute_bottom_k(table_name, group_col, val_col, k) AS TABLE<br>SELECT rs.grp, array_agg(rs.val ORDER BY rid)<br>FROM (<br>  SELECT group_col AS grp, val_col AS val, row_number() OVER (PARTITION BY group_col ORDER BY val_col ASC) as rid<br>  FROM query_table(table_name::VARCHAR) ORDER BY group_col ASC<br>) as rs<br>WHERE rid <= k<br>GROUP BY ALL<br>ORDER BY ALL;` |  |
| `count` | `unsupported feature` | 3 | sql/aggregate/aggregates/histogram_table_function.test | `query II<br>SELECT COUNT(*), AVG(count) FROM histogram_values(integers, i, technique := 'equi-height')<br>----<br>10	13` |  |
| `dbgen` | `unsupported feature` | 1 | sql/aggregate/aggregates/arg_min_max_n_tpch.test | `statement ok<br>CALL dbgen(sf=0.001);` |  |
| `histogram` | `unsupported feature` | 1 | sql/aggregate/aggregates/histogram_table_function.test | `query II<br>SELECT bin, count FROM histogram(integers, i, bin_count := 10, technique := 'equi-width')<br>----<br>x <= 12	13<br>12 < x <= 25	13<br>25 < x <= 37	12<br>37 < x <= 50	14<br>50 < x <= 63	13<br>63 < x <= 75	12<br>75 < x <= 88	14<br>88 < x <= 100	12<br>100 < x <= 113	13<br>113 < x <= 126	13` |  |
| `histogram_exact` | `unsupported feature` | 2 | sql/aggregate/aggregates/histogram_exact.test | `query I<br>SELECT histogram_exact([n], [[x] for x in [10, 20, 30, 40, 50]]) FROM obs<br>----<br>{[10]=0, [20]=1, [30]=0, [40]=0, [50]=0, []=14}` |  |
| `histogram_values` | `unsupported feature` | 2 | sql/aggregate/aggregates/histogram_table_function.test | `query II<br>SELECT * FROM histogram_values(integers, i, bin_count := 2)<br>----<br>60	1<br>80	0<br>100	1` |  |
| `max` | `unsupported feature` | 15 | sql/aggregate/aggregates/arg_min_max_n_tpch.test<br>sql/aggregate/aggregates/max_n_all_types_grouped.test | `query II nosort top_resultset<br>SELECT l_returnflag, max(l_orderkey, 3) FROM lineitem GROUP BY ALL ORDER BY ALL;` |  |
| `min` | `unsupported feature` | 1 | sql/aggregate/aggregates/arg_min_max_n_tpch.test | `query II nosort bottom_resultset<br>SELECT l_returnflag, min(l_orderkey, 3) FROM lineitem GROUP BY ALL ORDER BY ALL;` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T13:33:51.142573+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 5
- Files with issues: 9
- Unknown function TODOs: 81
- Unsupported feature TODOs: 58
- Unknown function unique issues: 1
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `histogram_exact` | 4 | sql/aggregate/aggregates/histogram_exact.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `is_histogram_other_bin` | `unsupported feature` | 1 | sql/aggregate/aggregates/histogram_exact.test | `query II<br>SELECT case when is_histogram_other_bin(bin) then '(other values)' else bin::varchar end as bin,<br>       count<br>FROM (<br>	SELECT UNNEST(map_keys(hist)) AS bin, UNNEST(map_values(hist)) AS count<br>	FROM (SELECT histogram(n, [10, 20, 30, 40]) AS hist FROM obs)<br>)<br>----<br>10	3<br>20	2<br>30	5<br>40	3<br>(other values)	2` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T13:34:06.305729+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 14
- Files with issues: 9
- Unknown function TODOs: 81
- Unsupported feature TODOs: 79
- Unknown function unique issues: 0
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `range` | `unsupported feature` | 13 | sql/aggregate/aggregates/max_n_all_types_grouped.test | `statement ok<br>CREATE OR REPLACE TABLE tbl AS SELECT * FROM<br>		(SELECT int as val_col FROM all_types)<br>	CROSS JOIN (SELECT i % 2 as grp_col FROM range(0, 5) as r(i));` |  |
| `test_all_types` | `unsupported feature` | 1 | sql/aggregate/aggregates/max_n_all_types_grouped.test | `statement ok<br>create table all_types as from test_all_types()` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T14:21:30.816446+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1003
- Files with issues: 89
- Unknown function TODOs: 279
- Unsupported feature TODOs: 1111
- Unknown function unique issues: 46
- Unsupported feature unique issues: 86

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `_unclassified` | 6 | sql/aggregate/aggregates/test_arg_min_max.test<br>sql/aggregate/aggregates/test_bitstring_agg.test<br>sql/aggregate/aggregates/test_weighted_avg.test |  |
| `approx_quantile` | 8 | sql/aggregate/aggregates/test_approx_quantile.test |  |
| `arbitrary` | 1 | sql/aggregate/aggregates/test_first_noninlined.test |  |
| `arg_max_null` | 5 | sql/aggregate/aggregates/test_arg_min_max_null.test |  |
| `arg_min_null` | 11 | sql/aggregate/aggregates/test_arg_min_max_null.test |  |
| `argmax` | 6 | sql/aggregate/aggregates/test_arg_min_max.test |  |
| `argmin` | 8 | sql/aggregate/aggregates/test_arg_min_max.test |  |
| `args` | 1 | sql/aggregate/aggregates/test_arg_min_max.test |  |
| `bit_count` | 6 | sql/aggregate/aggregates/test_bitstring_agg.test |  |
| `bit_length` | 1 | sql/aggregate/aggregates/test_bitstring_agg.test |  |
| `corr` | 4 | sql/aggregate/aggregates/test_corr.test |  |
| `count` | 4 | sql/aggregate/aggregates/test_bitstring_agg.test<br>sql/aggregate/distinct/ungrouped/test_distinct_ungrouped_shared_input.test |  |
| `employees` | 1 | sql/aggregate/aggregates/test_arg_min_max.test |  |
| `entropy` | 9 | sql/aggregate/aggregates/test_entropy.test |  |
| `favg` | 1 | sql/aggregate/aggregates/test_kahan_avg.test |  |
| `finalize` | 3 | sql/aggregate/aggregates/test_aggregate_state_type.test<br>sql/aggregate/aggregates/test_kahan_avg.test<br>sql/aggregate/aggregates/test_state_export_struct.test |  |
| `first` | 1 | sql/aggregate/aggregates/test_aggregate_state_type.test |  |
| `fsum` | 1 | sql/aggregate/aggregates/test_kahan_sum.test |  |
| `kahan_sum` | 1 | sql/aggregate/aggregates/test_kahan_sum.test |  |
| `kurtosis_pop` | 1 | sql/aggregate/aggregates/test_kurtosis.test |  |
| `mad` | 9 | sql/aggregate/aggregates/test_mad.test |  |
| `max_by` | 3 | sql/aggregate/aggregates/test_arg_min_max.test |  |
| `min` | 1 | sql/aggregate/aggregates/test_state_export_opaque.test |  |
| `min_by` | 2 | sql/aggregate/aggregates/test_arg_min_max.test |  |
| `percentile_cont` | 4 | sql/aggregate/aggregates/test_ordered_aggregates.test |  |
| `percentile_disc` | 6 | sql/aggregate/aggregates/test_ordered_aggregates.test |  |
| `product` | 7 | sql/aggregate/aggregates/test_product.test |  |
| `quantile_disc` | 2 | sql/aggregate/aggregates/test_quantile_disc_list.test |  |
| `random` | 1 | sql/aggregate/aggregates/test_approx_quantile.test |  |
| `regr_avgx` | 4 | sql/aggregate/aggregates/test_regression.test |  |
| `regr_avgy` | 4 | sql/aggregate/aggregates/test_regression.test |  |
| `regr_count` | 4 | sql/aggregate/aggregates/test_regression.test |  |
| `regr_intercept` | 4 | sql/aggregate/aggregates/test_regression.test |  |
| `regr_r2` | 6 | sql/aggregate/aggregates/test_regression.test |  |
| `regr_slope` | 4 | sql/aggregate/aggregates/test_regression.test |  |
| `regr_sxx` | 6 | sql/aggregate/aggregates/test_regression.test |  |
| `regr_sxy` | 4 | sql/aggregate/aggregates/test_regression.test |  |
| `regr_syy` | 3 | sql/aggregate/aggregates/test_regression.test |  |
| `reservoir_quantile` | 6 | sql/aggregate/aggregates/test_approx_quantile.test |  |
| `round` | 1 | sql/aggregate/aggregates/test_stddev.test |  |
| `sem` | 3 | sql/aggregate/aggregates/test_sem.test |  |
| `sumkahan` | 1 | sql/aggregate/aggregates/test_kahan_sum.test |  |
| `sv` | 1 | sql/aggregate/group/test_group_by_nested.test |  |
| `typeof` | 9 | sql/aggregate/aggregates/test_aggregate_state_type.test |  |
| `wavg` | 1 | sql/aggregate/aggregates/test_weighted_avg.test |  |
| `weighted_avg` | 6 | sql/aggregate/aggregates/test_weighted_avg.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `aggr` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_regression.test | `statement ok<br>create  table aggr(k int, v decimal(10,2), v2 decimal(10, 2));` |  |
| `any_value` | `unsupported feature` | 38 | sql/aggregate/aggregates/test_aggregate_state_type.test<br>sql/aggregate/aggregates/test_any_value.test<br>sql/aggregate/aggregates/test_first_last_any_ordered.test | `query I<br>SELECT ANY_VALUE(i ORDER BY 5-i) FROM five<br>----<br>5` |  |
| `approx_count_distinct` | `unsupported feature` | 3 | sql/aggregate/aggregates/test_approximate_distinct_count.test | `statement ok<br>SELECT approx_count_distinct(c0 ORDER BY (c0, 1)) FROM issue5259;` |  |
| `approx_quantile` | `unsupported feature` | 8 | sql/aggregate/aggregates/test_approx_quantile.test | `statement error<br>SELECT approx_quantile(r) FROM quantile` |  |
| `arg_min` | `unsupported feature` | 2 | sql/aggregate/aggregates/test_arg_min_max.test | `query TT<br>select arg_min(name,salary),arg_max(name,salary)  from names;<br>----<br>NULL NULL` |  |
| `arg_min_null` | `unsupported feature` | 2 | sql/aggregate/aggregates/test_arg_min_max_null.test | `query II<br>select arg_min_null(a,b), arg_max_null(a,b)  from blobs;<br>----<br>5	20` |  |
| `argmin` | `unsupported feature` | 3 | sql/aggregate/aggregates/test_arg_min_max.test | `query II<br>select argmin(a,b), argmax(a,b) from args;<br>----<br>1.000000	10.000000` |  |
| `as` | `unsupported feature` | 27 | sql/aggregate/aggregates/test_combine_aggr.test<br>sql/aggregate/aggregates/test_kurtosis.test<br>sql/aggregate/aggregates/test_quantile_disc.test<br>sql/aggregate/aggregates/test_state_export_struct.test<br>sql/aggregate/distinct/grouped/issue_5070.test | `statement ok<br>CREATE TABLE legacy_states AS (SELECT g, (min(d) export_state) state FROM dummy GROUP BY g);` |  |
| `asc` | `unsupported feature` | 1 | sql/aggregate/distinct/distinct_on_nulls.test | `statement ok<br>INSERT INTO distinct_on_test VALUES<br>	(1, 'hello', ARRAY[1], 42), -- ASC<br>	(1, 'hello', ARRAY[1], 42),<br>	(1, 'hello', ARRAY[1], 43), -- DESC<br>	(2, NULL, NULL, 0),     -- ASC<br>	(2, NULL, NULL, 1),<br>	(2, NULL, NULL, NULL),  -- DESC<br>	(3, 'thisisalongstring', NULL, 0),     -- ASC<br>	(3, 'thisisalongstringbutlonger', NULL, 1),<br>	(3, 'thisisalongstringbutevenlonger', ARRAY[1, 2, 3, 4, 5, 6, 7, 8, 9], 2)  -- DESC<br>;` |  |
| `avg` | `unsupported feature` | 37 | sql/aggregate/aggregates/test_aggregate_state_type.test<br>sql/aggregate/aggregates/test_avg.test<br>sql/aggregate/aggregates/test_bigint_avg.test<br>sql/aggregate/aggregates/test_combine_aggr.test<br>sql/aggregate/aggregates/test_state_export_struct.test | `query R<br>SELECT AVG(nextval('seq'))<br>----<br>1` |  |
| `between` | `unsupported feature` | 7 | sql/aggregate/aggregates/test_approx_quantile.test | `query I<br>SELECT CASE<br>	  WHEN ( approx_quantile between (true_quantile - 100) and (true_quantile + 100) )<br>		  THEN TRUE<br>		  ELSE FALSE<br>	  END<br>	  FROM (SELECT approx_quantile(42, 0.5)  as approx_quantile ,quantile(42, 0.5)  as true_quantile) AS T<br>----<br>1` |  |
| `bigints` | `unsupported feature` | 3 | sql/aggregate/aggregates/test_bigint_avg.test<br>sql/aggregate/aggregates/test_bitstring_agg.test | `statement ok<br>CREATE TABLE bigints(i BIGINT);` |  |
| `bit_and` | `unsupported feature` | 10 | sql/aggregate/aggregates/test_bit_and.test<br>sql/aggregate/aggregates/test_state_export_struct.test | `query I<br>SELECT BIT_AND(nextval('seq'))<br>----<br>1` |  |
| `bit_count` | `unsupported feature` | 2 | sql/aggregate/aggregates/test_bitstring_agg.test | `query I nosort distinct_bigints<br>SELECT bit_count(BITSTRING_AGG(i)) FROM bigints<br>----` |  |
| `bit_length` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_bitstring_agg.test | `query I<br>SELECT bit_length(BITSTRING_AGG(i)) FROM hugeints<br>----<br>56068` |  |
| `bit_or` | `unsupported feature` | 8 | sql/aggregate/aggregates/test_bit_or.test | `query I<br>SELECT BIT_OR(nextval('seq'))<br>----<br>1` |  |
| `bit_xor` | `unsupported feature` | 10 | sql/aggregate/aggregates/test_bit_xor.test<br>sql/aggregate/aggregates/test_state_export_struct.test | `query I<br>SELECT BIT_XOR(nextval('seq'))<br>----<br>1` |  |
| `bits` | `unsupported feature` | 3 | sql/aggregate/aggregates/test_bit_and.test<br>sql/aggregate/aggregates/test_bit_or.test<br>sql/aggregate/aggregates/test_bit_xor.test | `statement ok<br>CREATE TABLE bits(b BIT);` |  |
| `blobs` | `unsupported feature` | 2 | sql/aggregate/aggregates/test_arg_min_max.test<br>sql/aggregate/aggregates/test_arg_min_max_null.test | `statement ok<br>CREATE TABLE blobs (b BYTEA, a BIGINT);` |  |
| `bool_and` | `unsupported feature` | 2 | sql/aggregate/aggregates/test_state_export_struct.test | `query II nosort res8a<br>SELECT g, bool_and(d > 50) FROM dummy GROUP BY g ORDER BY g;<br>----` |  |
| `bool_or` | `unsupported feature` | 3 | sql/aggregate/aggregates/test_state_export_struct.test | `query II nosort res8a2<br>SELECT g, bool_or(d > 50) FROM dummy GROUP BY g ORDER BY g;<br>----` |  |
| `combine` | `unsupported feature` | 2 | sql/aggregate/aggregates/test_state_export_struct.test | `statement error<br>SELECT combine(42, 42);` |  |
| `combine_aggr` | `unsupported feature` | 2 | sql/aggregate/aggregates/test_combine_aggr.test | `statement ok<br>create view combined_view as select combine_aggr(state) as global_state from states;` |  |
| `corr` | `unsupported feature` | 6 | sql/aggregate/aggregates/test_corr.test<br>sql/aggregate/aggregates/test_corr_state_export.test<br>sql/aggregate/aggregates/test_state_export_struct.test | `query I nosort res_corr0<br>SELECT corr(d, d+1) FROM dummy;<br>----` |  |
| `covar_pop` | `unsupported feature` | 2 | sql/aggregate/aggregates/test_covar.test | `query R<br>SELECT COVAR_POP(nextval('seqx'),nextval('seqy'))<br>----<br>0.000000` |  |
| `distinct_on_test` | `unsupported feature` | 1 | sql/aggregate/distinct/distinct_on_nulls.test | `statement ok<br>CREATE TABLE distinct_on_test(key INTEGER, v1 VARCHAR, v2 INTEGER[], v3 INTEGER);` |  |
| `entropy` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_entropy.test | `query I<br>select entropy(k) from aggr group by k%2 order by all<br>----<br>1.000000<br>1.584963` |  |
| `enum` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_histogram.test | `statement ok<br>CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')` |  |
| `enums` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_histogram.test | `statement ok<br>CREATE TABLE enums (e mood)` |  |
| `extract` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_perfect_ht.test | `query II<br>select extract(year from d), extract(month from d) from dates group by 1, 2 ORDER BY ALL;<br>----<br>1992	1<br>1992	2<br>1992	3<br>1992	4<br>1992	5<br>1992	6<br>1992	7<br>1992	8<br>1992	9<br>1992	10<br>1992	11<br>1992	12<br>1993	1<br>1993	2<br>1993	3<br>1993	4<br>1993	5<br>1993	6<br>1993	7<br>1993	8<br>1993	9<br>1993	10<br>1993	11<br>1993	12<br>1994	1<br>1994	2<br>1994	3<br>1994	4<br>1994	5<br>1994	6<br>1994	7<br>1994	8<br>1994	9<br>1994	10<br>1994	11<br>1994	12<br>1995	1<br>1995	2<br>1995	3<br>1995	4<br>1995	5<br>1995	6<br>1995	7<br>1995	8<br>1995	9<br>1995	10<br>1995	11<br>1995	12<br>1996	1<br>1996	2<br>1996	3<br>1996	4<br>1996	5<br>1996	6<br>1996	7<br>1996	8<br>1996	9<br>1996	10<br>1996	11<br>1996	12<br>1997	1<br>1997	2<br>1997	3<br>1997	4<br>1997	5<br>1997	6<br>1997	7<br>1997	8<br>1997	9<br>1997	10<br>1997	11<br>1997	12<br>1998	1<br>1998	2<br>1998	3<br>1998	4<br>1998	5<br>1998	6<br>1998	7<br>1998	8<br>1998	9<br>1998	10<br>1998	11<br>1998	12<br>1999	1<br>1999	2<br>1999	3<br>1999	4<br>1999	5<br>1999	6<br>1999	7<br>1999	8<br>1999	9<br>1999	10<br>1999	11<br>1999	12<br>2000	1<br>2000	2<br>2000	3<br>2000	4` |  |
| `favg` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_state_export_struct.test | `query IIIIIII nosort res8<br>SELECT g, favg(d) FROM dummy GROUP BY g ORDER BY g;<br>----` |  |
| `finalize` | `unsupported feature` | 68 | sql/aggregate/aggregates/test_aggregate_state_type.test<br>sql/aggregate/aggregates/test_combine_aggr.test<br>sql/aggregate/aggregates/test_corr_state_export.test<br>sql/aggregate/aggregates/test_state_export_opaque.test<br>sql/aggregate/aggregates/test_state_export_struct.test | `statement error<br>SELECT finalize(42);` |  |
| `first` | `unsupported feature` | 24 | sql/aggregate/aggregates/test_aggregate_types_scalar.test<br>sql/aggregate/aggregates/test_first_last_any_ordered.test<br>sql/aggregate/aggregates/test_first_noninlined.test<br>sql/aggregate/aggregates/test_order_by_aggregate.test<br>sql/aggregate/aggregates/test_scalar_aggr.test<br>sql/aggregate/aggregates/test_state_export_struct.test | `query I<br>SELECT FIRST(NULL)<br>----<br>NULL` |  |
| `foo` | `unsupported feature` | 2 | sql/aggregate/distinct/distinct_on_order_by.test<br>sql/aggregate/distinct/issue9241.test | `statement ok<br>create table foo (a int, b int);` |  |
| `generate_series` | `unsupported feature` | 2 | sql/aggregate/aggregates/test_minmax.test<br>sql/aggregate/aggregates/test_mode.test | `statement ok<br>create table lists as select array[i] l from generate_series(0,5,1) tbl(i);` |  |
| `grouping` | `unsupported feature` | 9 | sql/aggregate/distinct/grouped/combined_with_grouping.test | `query IIII<br>SELECT GROUPING(course), course, sum(distinct value), COUNT(*) FROM students GROUP BY course ORDER BY all;<br>----<br>0	CS	56	5<br>0	Math	12	3` |  |
| `hugeints` | `unsupported feature` | 3 | sql/aggregate/aggregates/test_arg_min_max.test<br>sql/aggregate/aggregates/test_bitstring_agg.test<br>sql/aggregate/aggregates/test_mode.test | `statement ok<br>CREATE TABLE hugeints(i HUGEINT);` |  |
| `integers` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_quantile_cont.test | `statement ok<br>CREATE TABLE integers(t INTEGER);` |  |
| `interval` | `unsupported feature` | 2 | sql/aggregate/aggregates/test_any_value.test<br>sql/aggregate/aggregates/test_last.test | `statement ok<br>CREATE TABLE five_dates AS<br>	SELECT<br>		i::integer AS i,<br>		'2021-08-20'::DATE + i::INTEGER AS d,<br>		'2021-08-20'::TIMESTAMP + INTERVAL (i) HOUR AS dt,<br>		'14:59:37'::TIME + INTERVAL (i) MINUTE AS t,<br>		INTERVAL (i) SECOND AS s<br>	FROM range(1, 6, 1) t1(i)` |  |
| `kurtosis` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_kurtosis.test | `query I<br>select  kurtosis(v2) from aggr group by v ORDER BY ALL;<br>----<br>-3.977599<br>NULL<br>NULL<br>NULL` |  |
| `last` | `unsupported feature` | 56 | sql/aggregate/aggregates/test_aggregate_state_type.test<br>sql/aggregate/aggregates/test_aggregate_types_scalar.test<br>sql/aggregate/aggregates/test_first_last_any_ordered.test<br>sql/aggregate/aggregates/test_last.test<br>sql/aggregate/aggregates/test_last_noninlined.test<br>sql/aggregate/aggregates/test_scalar_aggr.test | `query I<br>SELECT LAST(NULL)<br>----<br>NULL` |  |
| `least` | `unsupported feature` | 9 | sql/aggregate/group/test_group_by_nested.test | `query III<br>SELECT k, LEAST(v, 21) as c, SUM(v) FROM longlists<br>GROUP BY k, c<br>ORDER BY 2, 3<br>----` |  |
| `list` | `unsupported feature` | 2 | sql/aggregate/aggregates/test_order_by_aggregate.test<br>sql/aggregate/group/test_group_by_nested.test | `query II<br>SELECT <br>    user_id, <br>    list(DISTINCT cause ORDER BY cause DESC) FILTER(cause IS NOT NULL) AS causes<br>FROM user_causes <br>GROUP BY user_id;<br>----<br>1	[Social, Health, Environmental]` |  |
| `list_aggr` | `unsupported feature` | 2 | sql/aggregate/aggregates/test_count_all_types.test | `query I nosort count_int_list<br>SELECT list_aggr(n, 'count') FROM test_vector_types(NULL::INT[], all_flat=true) t(n);<br>----` |  |
| `lv` | `unsupported feature` | 2 | sql/aggregate/group/test_group_by_nested.test | `statement ok<br>CREATE TABLE intlists AS SELECT * FROM (VALUES<br>	(21, [1]),<br>	(22, [NULL]),<br>	(23, []),<br>	(24, [2, 3]),<br>	(NULL::INTEGER, [13]),<br>	(32, [NULL]),<br>	(34, [2, 3])<br>	) lv(v, k);` |  |
| `mad` | `unsupported feature` | 42 | sql/aggregate/aggregates/test_mad.test | `query I<br>SELECT mad(r::tinyint) FROM tinys<br>----<br>25` |  |
| `median` | `unsupported feature` | 40 | sql/aggregate/aggregates/test_median.test<br>sql/aggregate/aggregates/test_quantile_disc.test | `query I<br>SELECT median(42) FROM quantile<br>----<br>42` |  |
| `min_by` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_arg_min_max.test | `query I<br>SELECT MIN_BY(employee_id, salary) as employee_with_least_salary<br>FROM employees;<br>----<br>1030` |  |
| `mod` | `unsupported feature` | 9 | sql/aggregate/aggregates/test_approximate_distinct_count.test<br>sql/aggregate/aggregates/test_quantile_cont.test<br>sql/aggregate/aggregates/test_quantile_cont_list.test<br>sql/aggregate/aggregates/test_quantile_disc.test<br>sql/aggregate/aggregates/test_quantile_disc_list.test | `statement ok<br>create  table t as select range a, mod(range,10) b from range(0, 2000);` |  |
| `mode` | `unsupported feature` | 12 | sql/aggregate/aggregates/test_mode.test<br>sql/aggregate/aggregates/test_ordered_aggregates.test | `query I<br>select mode(v) from hugeints;<br>----<br>5` |  |
| `names` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_arg_min_max.test | `statement ok<br>create table names (first_name string, last_name string)` |  |
| `on` | `unsupported feature` | 57 | sql/aggregate/distinct/distinct_on_nulls.test<br>sql/aggregate/distinct/distinct_on_order_by.test<br>sql/aggregate/distinct/issue2656.test<br>sql/aggregate/distinct/issue8505.test<br>sql/aggregate/distinct/issue9241.test<br>sql/aggregate/distinct/test_distinct_on.test<br>sql/aggregate/distinct/test_distinct_on_columns.test<br>sql/aggregate/distinct/test_distinct_order_by.test | `query II<br>SELECT DISTINCT ON (1) t1, t2<br>FROM T<br>ORDER BY t1, t2;<br>----<br>1	1` |  |
| `percentile_cont` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_quantile_cont.test | `query II<br>SELECT <br>    percentile_cont(0.8) WITHIN GROUP (ORDER BY x DESC),<br>    quantile_cont(x, 0.8 ORDER BY x DESC),<br>FROM <br>    (VALUES (2), (1)) _(x);<br>----<br>1.2	1.2` |  |
| `product` | `unsupported feature` | 5 | sql/aggregate/aggregates/test_aggregate_state_type.test<br>sql/aggregate/aggregates/test_product.test | `query I<br>select product(i) from integers group by i%2 order by all<br>----<br>1.000000<br>8.000000<br>NULL` |  |
| `quantile_cont` | `unsupported feature` | 59 | sql/aggregate/aggregates/test_quantile_cont.test<br>sql/aggregate/aggregates/test_quantile_cont_list.test | `query R<br>SELECT quantile_cont(42, 0.5)<br>----<br>42` |  |
| `quantile_disc` | `unsupported feature` | 55 | sql/aggregate/aggregates/test_quantile_disc.test<br>sql/aggregate/aggregates/test_quantile_disc_list.test | `query I<br>SELECT quantile_disc(42, 0.5)<br>----<br>42` |  |
| `random` | `unsupported feature` | 6 | sql/aggregate/aggregates/test_mad.test<br>sql/aggregate/aggregates/test_median.test<br>sql/aggregate/aggregates/test_quantile_cont.test<br>sql/aggregate/aggregates/test_quantile_cont_list.test<br>sql/aggregate/aggregates/test_quantile_disc_list.test | `statement ok<br>create table tinys as<br>	select range r, random()<br>	from range(0, 100)<br>	union all values (NULL, 0.1), (NULL, 0.5), (NULL, 0.9)<br>	order by 2;` |  |
| `regr_avgx` | `unsupported feature` | 4 | sql/aggregate/aggregates/test_aggregate_state_type.test<br>sql/aggregate/aggregates/test_regression.test<br>sql/aggregate/aggregates/test_state_export_struct.test | `query I<br>SELECT regr_avgx(d, d+1) FROM dummy;<br>----<br>50.5` |  |
| `regr_avgy` | `unsupported feature` | 2 | sql/aggregate/aggregates/test_regression.test<br>sql/aggregate/aggregates/test_state_export_struct.test | `query I<br>SELECT regr_avgy(d, d+1) FROM dummy;<br>----<br>49.5` |  |
| `regr_count` | `unsupported feature` | 6 | sql/aggregate/aggregates/test_aggregate_state_type.test<br>sql/aggregate/aggregates/test_state_export_struct.test | `query I<br>SELECT regr_count(d, d+1) FROM dummy;<br>----<br>100` |  |
| `regr_intercept` | `unsupported feature` | 4 | sql/aggregate/aggregates/test_regression.test<br>sql/aggregate/aggregates/test_state_export_struct.test | `statement error<br>select regr_intercept()` |  |
| `regr_r2` | `unsupported feature` | 2 | sql/aggregate/aggregates/test_regression.test<br>sql/aggregate/aggregates/test_state_export_struct.test | `query I<br>SELECT regr_r2(d, d+1) FROM dummy;<br>----<br>1.0` |  |
| `regr_slope` | `unsupported feature` | 4 | sql/aggregate/aggregates/test_regression.test<br>sql/aggregate/aggregates/test_state_export_struct.test | `query I<br>SELECT regr_slope(d, d+1) FROM dummy;<br>----<br>1.0` |  |
| `regr_sxx` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_state_export_struct.test | `query I<br>SELECT regr_sxx(d, d+1) FROM dummy;<br>----<br>83325.0` |  |
| `regr_sxy` | `unsupported feature` | 2 | sql/aggregate/aggregates/test_regression.test<br>sql/aggregate/aggregates/test_state_export_struct.test | `query I<br>SELECT regr_sxy(d, d+1) FROM dummy;<br>----<br>83325.0` |  |
| `regr_syy` | `unsupported feature` | 4 | sql/aggregate/aggregates/test_regression.test<br>sql/aggregate/aggregates/test_state_export_struct.test | `statement error<br>select regr_syy(*)` |  |
| `repro` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_approx_quantile.test | `statement ok<br>CREATE TABLE repro (i DECIMAL(15,2));` |  |
| `reservoir_quantile` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_approx_quantile.test | `statement ok<br>SELECT reservoir_quantile(r, 0.9)  from quantile` |  |
| `round` | `unsupported feature` | 4 | sql/aggregate/aggregates/test_stddev.test | `query R<br>select round(var_pop(val), 1) from stddev_test<br>----<br>171961.200000` |  |
| `sem` | `unsupported feature` | 2 | sql/aggregate/aggregates/test_sem.test | `query III<br>select k, sem(v),sem(v2)  from aggr group by k ORDER BY ALL;<br>----<br>1	0.000000	NULL<br>2	3.697550	5.663398` |  |
| `skewness` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_skewness.test | `query I<br>select skewness(v2) from aggr group by v ORDER BY ALL<br>----<br>-0.423273<br>-0.330141<br>NULL<br>NULL` |  |
| `smallints` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_quantile_cont.test | `statement ok<br>CREATE TABLE smallints(t SMALLINT);` |  |
| `string_agg` | `unsupported feature` | 16 | sql/aggregate/aggregates/test_aggregate_types_scalar.test<br>sql/aggregate/aggregates/test_string_agg.test<br>sql/aggregate/aggregates/test_string_agg_big.test<br>sql/aggregate/distinct/grouped/string_agg.test | `query I<br>SELECT g, STRING_AGG(x ORDER BY x DESC) FROM strings GROUP BY g ORDER BY 1, 2<br>----` |  |
| `sum` | `unsupported feature` | 50 | sql/aggregate/aggregates/test_simple_filter.test<br>sql/aggregate/aggregates/test_state_export_struct.test<br>sql/aggregate/aggregates/test_stddev.test<br>sql/aggregate/aggregates/test_sum.test<br>sql/aggregate/distinct/grouped/combined_with_grouping.test<br>sql/aggregate/distinct/grouped/multiple_grouping_sets.test<br>sql/aggregate/distinct/ungrouped/test_distinct_ungrouped.test<br>sql/aggregate/group/group_by_all.test<br>sql/aggregate/group/group_by_all_order.test<br>sql/aggregate/group/test_group_by.test<br>sql/aggregate/group/test_group_by_alias.test<br>sql/aggregate/group/test_group_by_nested.test | `query I<br>SELECT SUM(b) FROM bigints<br>----<br>4611686018427388403500` |  |
| `sv` | `unsupported feature` | 5 | sql/aggregate/group/test_group_by_nested.test | `statement ok<br>CREATE VIEW structs AS SELECT * FROM (VALUES<br>	(21, {'x': 1, 'y': 'a'}),<br>	(22, {'x': NULL, 'y': NULL}),<br>	(23, {'x': 0, 'y': ''}),<br>	(24, {'x': 2, 'y': 'c'}),<br>	(NULL::INTEGER, {'x': 13, 'y': 'Somateria mollissima'}),<br>	(32, {'x': NULL, 'y': NULL}),<br>	(34, {'x': 2, 'y': 'c'})<br>	) sv(v, k);` |  |
| `t` | `unsupported feature` | 15 | sql/aggregate/distinct/test_distinct_on_columns.test<br>sql/aggregate/group/group_by_limits.test | `statement ok<br>CREATE TABLE t(t_k0 HUGEINT);` |  |
| `t0` | `unsupported feature` | 1 | sql/aggregate/group/group_by_all_having.test | `query I<br>SELECT c0 FROM (SELECT 1, 1 UNION ALL SELECT 1, 2) t0(c0, c1) GROUP BY ALL HAVING c1>0 ORDER BY c0<br>----<br>1<br>1` |  |
| `test_strings` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_aggr_string.test | `statement ok<br>CREATE TABLE test_strings(s VARCHAR);<br>INSERT INTO test_strings VALUES ('aaaaaaaahello'), ('bbbbbbbbbbbbbbbbbbbbhello'), ('ccccccccccccccchello'), ('aaaaaaaaaaaaaaaaaaaaaaaahello');;` |  |
| `times` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_mode.test | `statement ok<br>create table times (k int, v time)` |  |
| `timeseries` | `unsupported feature` | 4 | sql/aggregate/aggregates/test_perfect_ht.test | `statement ok<br>CREATE OR REPLACE TABLE timeseries(year UBIGINT, val UBIGINT);` |  |
| `timetzs` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_avg.test | `statement ok<br>CREATE TABLE timetzs (ttz TIMETZ);` |  |
| `tinyints` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_quantile_cont.test | `statement ok<br>CREATE TABLE tinyints(t TINYINT);` |  |
| `typeof` | `unsupported feature` | 23 | sql/aggregate/aggregates/test_aggregate_state_type.test<br>sql/aggregate/aggregates/test_sum.test | `query T<br>SELECT typeof(min(1::BIGINT) EXPORT_STATE);<br>----<br>LEGACY_AGGREGATE_STATE<min(BIGINT)::BIGINT>` |  |
| `uhugeints` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_bitstring_agg.test | `statement ok<br>CREATE TABLE uhugeints(i UHUGEINT);` |  |
| `vals` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_avg.test | `statement ok<br>CREATE TABLE vals(i INTEGER, j DOUBLE, k HUGEINT);` |  |
| `var_samp` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_stddev.test | `query I<br>SELECT var_samp(1)<br>----<br>NULL` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:10:40.396276+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 91
- Unknown function TODOs: 280
- Unsupported feature TODOs: 1124
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

- Generated at (UTC): 2026-03-16T15:12:06.532482+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 92
- Unknown function TODOs: 280
- Unsupported feature TODOs: 1131
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

- Generated at (UTC): 2026-03-16T15:13:05.825793+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 4
- Files with issues: 96
- Unknown function TODOs: 281
- Unsupported feature TODOs: 1144
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

- Generated at (UTC): 2026-03-16T15:13:09.277270+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 6
- Files with issues: 97
- Unknown function TODOs: 284
- Unsupported feature TODOs: 1149
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

- Generated at (UTC): 2026-03-16T15:13:18.939436+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 98
- Unknown function TODOs: 284
- Unsupported feature TODOs: 1160
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

- Generated at (UTC): 2026-03-16T15:14:00.194611+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 101
- Unknown function TODOs: 287
- Unsupported feature TODOs: 1190
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

- Generated at (UTC): 2026-03-16T15:14:05.377610+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 102
- Unknown function TODOs: 287
- Unsupported feature TODOs: 1191
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

- Generated at (UTC): 2026-03-16T15:15:20.451956+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 105
- Unknown function TODOs: 288
- Unsupported feature TODOs: 1198
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

- Generated at (UTC): 2026-03-16T15:16:06.419300+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 108
- Unknown function TODOs: 288
- Unsupported feature TODOs: 1205
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

- Generated at (UTC): 2026-03-16T15:16:17.401016+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 109
- Unknown function TODOs: 288
- Unsupported feature TODOs: 1212
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

- Generated at (UTC): 2026-03-16T15:16:29.522983+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 112
- Unknown function TODOs: 288
- Unsupported feature TODOs: 1222
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

- Generated at (UTC): 2026-03-16T15:16:54.193597+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 117
- Unknown function TODOs: 288
- Unsupported feature TODOs: 1229
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

- Generated at (UTC): 2026-03-16T15:17:11.213803+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 122
- Unknown function TODOs: 288
- Unsupported feature TODOs: 1239
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

- Generated at (UTC): 2026-03-16T15:19:12.312679+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 126
- Unknown function TODOs: 288
- Unsupported feature TODOs: 1306
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `temp_not_null_test` | `unsupported feature` | 1 | sql/alter/alter_col/test_set_not_null.test | `statement ok<br>CREATE TEMPORARY TABLE temp_not_null_test(x INTEGER)` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:19:22.596934+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 127
- Unknown function TODOs: 288
- Unsupported feature TODOs: 1308
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

- Generated at (UTC): 2026-03-16T15:19:27.487317+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 128
- Unknown function TODOs: 289
- Unsupported feature TODOs: 1316
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

- Generated at (UTC): 2026-03-16T15:19:33.756990+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 130
- Unknown function TODOs: 289
- Unsupported feature TODOs: 1325
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

- Generated at (UTC): 2026-03-16T15:21:23.272764+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 3
- Files with issues: 141
- Unknown function TODOs: 289
- Unsupported feature TODOs: 1375
- Unknown function unique issues: 0
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `constrainty` | `unsupported feature` | 2 | sql/alter/default/test_set_default.test | `statement ok<br>INSERT INTO constrainty (i) VALUES (2)` |  |
| `temp_default_test` | `unsupported feature` | 1 | sql/alter/default/test_set_default.test | `statement ok<br>CREATE TEMPORARY TABLE temp_default_test(x INTEGER DEFAULT 1)` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:21:50.625924+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 143
- Unknown function TODOs: 289
- Unsupported feature TODOs: 1378
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

- Generated at (UTC): 2026-03-16T15:23:35.598627+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 160
- Unknown function TODOs: 289
- Unsupported feature TODOs: 1489
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

- Generated at (UTC): 2026-03-16T15:23:49.930066+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 162
- Unknown function TODOs: 289
- Unsupported feature TODOs: 1500
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `entry` | `unsupported feature` | 1 | sql/alter/rename_table/test_rename_table_chain_commit.test | `statement ok<br>CREATE TABLE entry(k INTEGER);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:24:44.022516+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 168
- Unknown function TODOs: 289
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
| `t3` | `unsupported feature` | 1 | sql/alter/rename_table/test_rename_table_with_dependency_check.test | `statement ok<br>CREATE TABLE t3 (c0 INT);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:25:41.634490+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 183
- Unknown function TODOs: 289
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
| `t1` | `unsupported feature` | 2 | sql/attach/attach_create_index.test | `statement ok<br>CREATE TABLE tmp.t1(id int);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:25:46.299678+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 3
- Files with issues: 186
- Unknown function TODOs: 289
- Unsupported feature TODOs: 1639
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

- Generated at (UTC): 2026-03-16T15:25:46.763154+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 187
- Unknown function TODOs: 289
- Unsupported feature TODOs: 1642
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

- Generated at (UTC): 2026-03-16T15:26:01.333590+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 3
- Files with issues: 191
- Unknown function TODOs: 289
- Unsupported feature TODOs: 1675
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

- Generated at (UTC): 2026-03-16T15:26:07.090502+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 192
- Unknown function TODOs: 289
- Unsupported feature TODOs: 1684
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

- Generated at (UTC): 2026-03-16T15:26:11.404155+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 198
- Unknown function TODOs: 289
- Unsupported feature TODOs: 1707
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

- Generated at (UTC): 2026-03-16T15:26:12.629916+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 199
- Unknown function TODOs: 289
- Unsupported feature TODOs: 1716
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

- Generated at (UTC): 2026-03-16T15:26:14.739591+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 5
- Files with issues: 200
- Unknown function TODOs: 289
- Unsupported feature TODOs: 1733
- Unknown function unique issues: 0
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `enum_range` | `unsupported feature` | 3 | sql/attach/attach_enums.test | `statement error<br>SELECT enum_range(NULL::xx.db1.main.mood) AS my_enum_range;` |  |
| `person` | `unsupported feature` | 2 | sql/attach/attach_enums.test | `statement ok<br>CREATE TABLE db1.person (<br>    name text,<br>    current_mood mood<br>);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:26:21.824908+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 201
- Unknown function TODOs: 289
- Unsupported feature TODOs: 1739
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

- Generated at (UTC): 2026-03-16T15:26:22.357216+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 202
- Unknown function TODOs: 289
- Unsupported feature TODOs: 1740
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

- Generated at (UTC): 2026-03-16T15:26:25.930574+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 3
- Files with issues: 205
- Unknown function TODOs: 289
- Unsupported feature TODOs: 1757
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

- Generated at (UTC): 2026-03-16T15:26:36.358504+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 211
- Unknown function TODOs: 289
- Unsupported feature TODOs: 1778
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

- Generated at (UTC): 2026-03-16T15:26:39.141060+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 212
- Unknown function TODOs: 289
- Unsupported feature TODOs: 1781
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

- Generated at (UTC): 2026-03-16T15:26:48.304676+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 4
- Files with issues: 216
- Unknown function TODOs: 289
- Unsupported feature TODOs: 1803
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

- Generated at (UTC): 2026-03-16T15:26:51.816336+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 4
- Files with issues: 217
- Unknown function TODOs: 290
- Unsupported feature TODOs: 1807
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

- Generated at (UTC): 2026-03-16T15:26:58.342520+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 6
- Files with issues: 219
- Unknown function TODOs: 290
- Unsupported feature TODOs: 1838
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

- Generated at (UTC): 2026-03-16T15:27:14.794458+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 225
- Unknown function TODOs: 290
- Unsupported feature TODOs: 1885
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

- Generated at (UTC): 2026-03-16T15:27:37.563181+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 6
- Files with issues: 234
- Unknown function TODOs: 290
- Unsupported feature TODOs: 1924
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

- Generated at (UTC): 2026-03-16T15:27:45.944233+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 3
- Files with issues: 236
- Unknown function TODOs: 290
- Unsupported feature TODOs: 1944
- Unknown function unique issues: 0
- Unsupported feature unique issues: 3

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `table_in_db1` | `unsupported feature` | 1 | sql/attach/attach_show_table.test | `statement ok<br>CREATE TABLE db1.table_in_db1(i int);` |  |
| `table_in_db2` | `unsupported feature` | 1 | sql/attach/attach_show_table.test | `statement ok<br>CREATE TABLE db2.table_in_db2(i int);` |  |
| `table_in_db2_test_schema` | `unsupported feature` | 1 | sql/attach/attach_show_table.test | `statement ok<br>CREATE TABLE db2.test_schema.table_in_db2_test_schema(i int);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:27:53.779156+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 239
- Unknown function TODOs: 290
- Unsupported feature TODOs: 2013
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

- Generated at (UTC): 2026-03-16T15:28:07.503949+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 243
- Unknown function TODOs: 290
- Unsupported feature TODOs: 2048
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `my_tbl` | `unsupported feature` | 2 | sql/attach/attach_view_search_path.test | `statement ok<br>CREATE TABLE my_tbl(i INTEGER)` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:28:13.508807+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 245
- Unknown function TODOs: 290
- Unsupported feature TODOs: 2083
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

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:28:14.630677+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 246
- Unknown function TODOs: 290
- Unsupported feature TODOs: 2092
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `blubb` | `unsupported feature` | 2 | sql/attach/attach_wal_alter_sequence.test | `statement ok<br>INSERT INTO db1.blubb (b) VALUES (10);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:28:18.768252+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 5
- Files with issues: 249
- Unknown function TODOs: 291
- Unsupported feature TODOs: 2120
- Unknown function unique issues: 1
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `one` | 1 | sql/attach/reattach_schema.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `my_table` | `unsupported feature` | 1 | sql/attach/reattach_schema.test | `statement ok<br>CREATE TABLE new_db.my_schema.my_table(col INTEGER);` |  |
| `one` | `unsupported feature` | 3 | sql/attach/reattach_schema.test | `query I<br>SELECT new_db.my_schema.one()<br>----<br>1` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:28:56.714088+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 257
- Unknown function TODOs: 291
- Unsupported feature TODOs: 2166
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `lower` | `unsupported feature` | 1 | sql/binder/alias_qualification_select_projection.test | `query II<br>SELECT 'AbC' AS s, alias.s.lower();<br>----<br>AbC	abc` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:29:11.952193+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 6
- Files with issues: 260
- Unknown function TODOs: 291
- Unsupported feature TODOs: 2181
- Unknown function unique issues: 0
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `list_contains_macro` | `unsupported feature` | 1 | sql/binder/function_chaining_19035.test | `statement ok<br>CREATE MACRO list_contains_macro(x, y) AS (SELECT list_contains(x, y))` |  |
| `list_transform` | `unsupported feature` | 5 | sql/binder/function_chaining_19035.test | `query I<br>select ['a a ', ' b ', ' cc'].list_transform(lambda x: nullif(trim(x), '')) as trimmed_and_nulled<br>----<br>[a a, b, cc]` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:29:29.840196+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 262
- Unknown function TODOs: 293
- Unsupported feature TODOs: 2195
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `regexp_matches` | 2 | sql/binder/not_similar_to.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:29:37.914412+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 264
- Unknown function TODOs: 295
- Unsupported feature TODOs: 2196
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `list_extract` | 2 | sql/binder/old_implicit_cast_template.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:30:13.334390+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 5
- Files with issues: 266
- Unknown function TODOs: 295
- Unsupported feature TODOs: 2210
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `using` | `unsupported feature` | 5 | sql/binder/separate_schema_tables.test | `query I<br>SELECT id FROM s1.t JOIN s2.t USING (id) JOIN s3.t USING (id)<br>----<br>1` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:30:44.789872+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 5
- Files with issues: 271
- Unknown function TODOs: 300
- Unsupported feature TODOs: 2220
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `alias` | 5 | sql/binder/test_alias.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:30:52.649888+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 272
- Unknown function TODOs: 301
- Unsupported feature TODOs: 2224
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `strip_null_value` | `unsupported feature` | 2 | sql/binder/test_alias_map_in_subquery.test | `query I<br>SELECT strip_null_value('{ "location" : { "address" : "123 Main St" }, "sampleField" : null, "anotherField" : 123, "yetAnotherField" : "abc" }')<br>AS example;<br>----<br>{"location":{"address":"123 Main St"},"anotherField":123,"yetAnotherField":"abc"}` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:31:10.901550+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 273
- Unknown function TODOs: 303
- Unsupported feature TODOs: 2251
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `alias` | `unsupported feature` | 1 | sql/binder/test_case_insensitive_binding.test | `query I<br>SELECT alias(x) FROM (SELECT HeLlO as x FROM test) tbl;<br>----<br>x` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:31:13.016745+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 274
- Unknown function TODOs: 303
- Unsupported feature TODOs: 2257
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `upper` | `unsupported feature` | 2 | sql/binder/test_duplicate_in_set_operation.test | `query I<br>select upper(col1)<br>from (<br>	select *<br>	from t1<br>	union<br>	select *<br>	from t2<br>) d;<br>----<br>A` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:31:19.162097+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 9
- Files with issues: 275
- Unknown function TODOs: 303
- Unsupported feature TODOs: 2268
- Unknown function unique issues: 0
- Unsupported feature unique issues: 4

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `split` | `unsupported feature` | 5 | sql/binder/test_function_chaining_alias.test | `statement ok<br>PREPARE v1 AS <br>SELECT<br>	(?.split(' ')::VARCHAR).lower() lstrings,<br>	(?.split(' ')::VARCHAR).upper() ustrings,<br>	list_concat(lstrings::VARCHAR[], ustrings::VARCHAR[]) AS mix_case_srings` |  |
| `substr` | `unsupported feature` | 1 | sql/binder/test_function_chaining_alias.test | `query II<br>SELECT 'test' \|\| ' more testing' AS added, added.substr(5) AS my_substr<br>----<br>test more testing	 more testing` |  |
| `trim` | `unsupported feature` | 2 | sql/binder/test_function_chaining_alias.test | `query III<br>SELECT<br>	v.trim('><') AS trim_inequality,<br>	trim_inequality.replace('%', '') AS only_alphabet,<br>	only_alphabet.lower() AS lower<br>FROM varchars<br>----<br>%Test	Test	test<br>%FUNCTION%	FUNCTION	function<br>Chaining	Chaining	chaining` |  |
| `v1` | `unsupported feature` | 1 | sql/binder/test_function_chaining_alias.test | `query III<br>EXECUTE v1('Hello World', 'test function chaining')<br>----<br>[hello, world]	[TEST, FUNCTION, CHAINING]	[hello, world, TEST, FUNCTION, CHAINING]` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:32:01.981677+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 6
- Files with issues: 278
- Unknown function TODOs: 304
- Unsupported feature TODOs: 2285
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `in` | `unsupported feature` | 6 | sql/binder/test_in_with_collate.test | `query I<br>select * from tbl where str collate noaccent in ('abcde');<br>----<br>àbcdë` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:32:53.065100+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 284
- Unknown function TODOs: 304
- Unsupported feature TODOs: 2317
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `cast_table` | `unsupported feature` | 1 | sql/cast/cast_error_location.test | `statement ok<br>CREATE TABLE cast_table(i INTEGER, s VARCHAR, d DECIMAL(5,1), l INT[], int_struct ROW(i INTEGER), dbl DOUBLE, hge HUGEINT, invalid_blob_str VARCHAR);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:34:03.661749+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 31
- Files with issues: 289
- Unknown function TODOs: 304
- Unsupported feature TODOs: 2490
- Unknown function unique issues: 0
- Unsupported feature unique issues: 16

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `assorted_lists` | `unsupported feature` | 1 | sql/cast/string_to_list_cast.test | `statement ok<br>CREATE TABLE assorted_lists(col1 INT[], col2 VARCHAR[], col3 DATE[]);` |  |
| `crazynested` | `unsupported feature` | 1 | sql/cast/string_to_list_cast.test | `statement ok<br>CREATE TABLE crazyNested (col1 VARCHAR)` |  |
| `doublenested` | `unsupported feature` | 1 | sql/cast/string_to_list_cast.test | `statement ok<br>CREATE TABLE doubleNested (col1 VARCHAR);` |  |
| `int_list` | `unsupported feature` | 1 | sql/cast/string_to_list_cast.test | `statement ok<br>CREATE TABLE int_list(col INT[]);` |  |
| `len` | `unsupported feature` | 4 | sql/cast/string_to_list_cast.test | `query I<br>SELECT col1 FROM tbl WHERE LEN(cast(col1 as int[])) < 4;<br>----<br>[1,2,2]<br>[5,6,7]` |  |
| `nestedstrings` | `unsupported feature` | 1 | sql/cast/string_to_list_cast.test | `statement ok<br>CREATE TABLE nestedStrings (col1 VARCHAR)` |  |
| `null_tbl` | `unsupported feature` | 1 | sql/cast/string_to_list_cast.test | `statement ok<br>CREATE TABLE null_tbl(col1 VARCHAR);` |  |
| `stringlist` | `unsupported feature` | 1 | sql/cast/string_to_list_cast.test | `statement ok<br>CREATE TABLE stringList (col1 VARCHAR)` |  |
| `struct` | `unsupported feature` | 8 | sql/cast/string_to_list_cast.test | `statement error<br>select '[{"bar":"\\""}]'::STRUCT(bar VARCHAR)[];` |  |
| `struct_tbl1` | `unsupported feature` | 1 | sql/cast/string_to_list_cast.test | `statement ok<br>CREATE TABLE struct_tbl1(col VARCHAR);` |  |
| `struct_tbl2` | `unsupported feature` | 1 | sql/cast/string_to_list_cast.test | `statement ok<br>CREATE TABLE struct_tbl2(col VARCHAR);` |  |
| `supernestedstrings` | `unsupported feature` | 1 | sql/cast/string_to_list_cast.test | `statement ok<br>CREATE TABLE superNestedStrings (col1 VARCHAR)` |  |
| `triplenested` | `unsupported feature` | 1 | sql/cast/string_to_list_cast.test | `statement ok<br>CREATE TABLE tripleNested (col1 VARCHAR)` |  |
| `try_cast` | `unsupported feature` | 2 | sql/cast/string_to_list_cast.test | `query I<br>SELECT TRY_CAST('Hello World' AS INT[]);<br>----<br>NULL` |  |
| `try_cast_tbl` | `unsupported feature` | 1 | sql/cast/string_to_list_cast.test | `statement ok<br>CREATE TABLE try_cast_tbl (col1 VARCHAR);` |  |
| `unnest` | `unsupported feature` | 5 | sql/cast/string_to_list_cast.test | `query I<br>select UNNEST('[NULL,, NULL]'::varchar[]);<br>----<br>NULL<br>(empty)<br>NULL` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:34:07.630793+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 290
- Unknown function TODOs: 304
- Unsupported feature TODOs: 2536
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `comma` | `unsupported feature` | 1 | sql/cast/string_to_list_escapes.test | `query I<br>SELECT $$[\,]$$::VARCHAR[]; -- List with only a comma (not quoted)<br>----<br>[\, '']` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:34:10.974080+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 25
- Files with issues: 292
- Unknown function TODOs: 304
- Unsupported feature TODOs: 2566
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `map` | `unsupported feature` | 25 | sql/cast/string_to_map_escapes.test | `query I<br>SELECT $${}$$::MAP(VARCHAR, VARCHAR);<br>----<br>{}` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:34:33.610169+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 293
- Unknown function TODOs: 304
- Unsupported feature TODOs: 2616
- Unknown function unique issues: 0
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `assorted_structs` | `unsupported feature` | 1 | sql/cast/string_to_struct_cast.test | `statement ok<br>CREATE TABLE assorted_structs(col1 STRUCT(a INT, b VARCHAR));` |  |
| `struct_pack` | `unsupported feature` | 1 | sql/cast/string_to_struct_cast.test | `query I<br>select (struct_pack(key_A=>'42'::double, key_B=>'DuckDB'::string)::VARCHAR)::STRUCT(key_A INT, key_B VARCHAR);<br>----<br>{'key_A': 42, 'key_B': DuckDB}` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:34:37.731770+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 294
- Unknown function TODOs: 304
- Unsupported feature TODOs: 2648
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `user` | `unsupported feature` | 2 | sql/cast/string_to_struct_escapes.test | `query I<br>SELECT $${user(name: "Alice", age: 25}$$::STRUCT("user(name" VARCHAR, age INT);<br>----<br>{'user(name': Alice, 'age': 25}` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:34:43.105330+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 297
- Unknown function TODOs: 304
- Unsupported feature TODOs: 2703
- Unknown function unique issues: 0
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `decimal` | `unsupported feature` | 1 | sql/cast/struct_to_map.test | `query I<br>SELECT {"price": 19.99::DECIMAL(10,2)}::MAP(VARCHAR, VARCHAR);<br>----<br>{price=19.99}` |  |
| `tab` | `unsupported feature` | 1 | sql/cast/struct_to_map.test | `query I<br>SELECT *<br>FROM (VALUES ({"status": 'active'}), ({"status": 'inactive'}))<br>AS tab(col)<br>WHERE (col::MAP(VARCHAR, VARCHAR))['status'] = 'active';<br>----<br>{'status': active}` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:34:49.537100+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 5
- Files with issues: 298
- Unknown function TODOs: 306
- Unsupported feature TODOs: 2777
- Unknown function unique issues: 1
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `bitstring` | 2 | sql/cast/test_bit_cast.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `bitstring` | `unsupported feature` | 3 | sql/cast/test_bit_cast.test | `statement error<br>SELECT bitstring('1', 9)::BOOL;` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:36:43.131429+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 308
- Unknown function TODOs: 306
- Unsupported feature TODOs: 2952
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `structs` | `unsupported feature` | 2 | sql/catalog/case_insensitive_operations.test | `statement ok<br>CREATE TABLE STRUCTS(S ROW(I ROW(K INTEGER)));` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:37:38.286649+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 34
- Files with issues: 310
- Unknown function TODOs: 307
- Unsupported feature TODOs: 3057
- Unknown function unique issues: 1
- Unsupported feature unique issues: 10

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `duckdb_columns` | 1 | sql/catalog/comment_on.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `art` | `unsupported feature` | 1 | sql/catalog/comment_on.test | `statement ok<br>CREATE INDEX test_index ON test_table using art(test_table_column)` |  |
| `duckdb_columns` | `unsupported feature` | 6 | sql/catalog/comment_on.test | `query I<br>select comment from duckdb_columns() where column_name='test_view_column';<br>----<br>NULL` |  |
| `duckdb_functions` | `unsupported feature` | 9 | sql/catalog/comment_on.test | `query I<br>select comment from duckdb_functions() where function_name='test_macro';<br>----<br>NULL` |  |
| `duckdb_indexes` | `unsupported feature` | 3 | sql/catalog/comment_on.test | `query I<br>select comment from duckdb_indexes() where index_name='test_index';<br>----<br>NULL` |  |
| `duckdb_sequences` | `unsupported feature` | 3 | sql/catalog/comment_on.test | `query I<br>select comment from duckdb_sequences() where sequence_name='test_sequence';<br>----<br>NULL` |  |
| `duckdb_types` | `unsupported feature` | 3 | sql/catalog/comment_on.test | `query I<br>select comment from duckdb_types() where type_name='test_type';<br>----<br>NULL` |  |
| `duckdb_views` | `unsupported feature` | 3 | sql/catalog/comment_on.test | `query I<br>select comment from duckdb_views() where view_name='test_view';<br>----<br>NULL` |  |
| `test_function` | `unsupported feature` | 1 | sql/catalog/comment_on.test | `statement ok<br>CREATE FUNCTION test_function(a, b) AS a + b` |  |
| `test_macro` | `unsupported feature` | 2 | sql/catalog/comment_on.test | `query I<br>SELECT test_macro(1,2);<br>----<br>3` |  |
| `test_table_macro` | `unsupported feature` | 2 | sql/catalog/comment_on.test | `query II<br>from test_table_macro(1,2);<br>----<br>1	2` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:38:18.280860+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 315
- Unknown function TODOs: 308
- Unsupported feature TODOs: 3191
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `test_macro` | 1 | sql/catalog/comment_on_wal.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:38:20.443468+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 316
- Unknown function TODOs: 308
- Unsupported feature TODOs: 3198
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `tbl2` | `unsupported feature` | 1 | sql/catalog/dependencies/add_column_to_table_referenced_by_fk.test | `statement ok<br>create table tbl2(<br>	a varchar,<br>	foreign key (a) references tbl(a)<br>)` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:38:23.554956+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 3
- Files with issues: 317
- Unknown function TODOs: 308
- Unsupported feature TODOs: 3201
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `mcr` | `unsupported feature` | 3 | sql/catalog/dependencies/add_column_to_table_referenced_by_macro.test | `query I<br>select * from mcr();<br>----<br>abc` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:38:40.330492+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 321
- Unknown function TODOs: 308
- Unsupported feature TODOs: 3206
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `static_table` | `unsupported feature` | 1 | sql/catalog/dependencies/rename_view_referenced_by_table_macro.test | `statement ok<br>create macro static_table() as table select * from vw;` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:39:17.508091+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 14
- Files with issues: 323
- Unknown function TODOs: 308
- Unsupported feature TODOs: 3308
- Unknown function unique issues: 0
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `tablename` | `unsupported feature` | 13 | sql/catalog/dependencies/test_alter_dependency_ownership.test | `statement ok<br>CREATE TABLE tablename (<br>    colname integer<br>);` |  |
| `tablename2` | `unsupported feature` | 1 | sql/catalog/dependencies/test_alter_dependency_ownership.test | `statement ok<br>CREATE TABLE tablename2 (<br>    colname integer<br>);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:40:18.333005+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 3
- Files with issues: 334
- Unknown function TODOs: 308
- Unsupported feature TODOs: 3375
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `checksum` | `unsupported feature` | 3 | sql/catalog/function/attached_macro.test | `statement ok<br>SELECT * FROM checksum_macro.checksum('tbl');` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:40:22.151209+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 335
- Unknown function TODOs: 309
- Unsupported feature TODOs: 3376
- Unknown function unique issues: 1
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `min_from_tbl` | 1 | sql/catalog/function/macro_query_table.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `min_from_tbl` | `unsupported feature` | 1 | sql/catalog/function/macro_query_table.test | `statement ok<br>create macro min_from_tbl(tbl, col) as (select min(col) from query_table(tbl::VARCHAR));` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:40:25.347421+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 30
- Files with issues: 336
- Unknown function TODOs: 313
- Unsupported feature TODOs: 3402
- Unknown function unique issues: 1
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `m` | 4 | sql/catalog/function/python_style_macro_parameters.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `m` | `unsupported feature` | 26 | sql/catalog/function/python_style_macro_parameters.test | `statement error<br>select m()` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:40:51.221414+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 13
- Files with issues: 337
- Unknown function TODOs: 313
- Unsupported feature TODOs: 3434
- Unknown function unique issues: 0
- Unsupported feature unique issues: 3

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `abs` | `unsupported feature` | 1 | sql/catalog/function/query_function.test | `query I<br>FROM query('SELECT abs(-42)');<br>----<br>42` |  |
| `pivot` | `unsupported feature` | 1 | sql/catalog/function/query_function.test | `query I<br>SELECT * FROM query('SELECT * FROM (PIVOT (SELECT 1 AS col) ON col IN (1) using first(col))');<br>----<br>1` |  |
| `query_table` | `unsupported feature` | 11 | sql/catalog/function/query_function.test | `query I<br>FROM query_table('tbl_int');<br>----<br>42` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:40:51.696142+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 338
- Unknown function TODOs: 314
- Unsupported feature TODOs: 3435
- Unknown function unique issues: 1
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `my_extract` | 1 | sql/catalog/function/struct_extract_macro.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `my_extract` | `unsupported feature` | 1 | sql/catalog/function/struct_extract_macro.test | `statement ok<br>CREATE MACRO my_extract(x) AS x.a.b` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:41:04.566013+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 29
- Files with issues: 339
- Unknown function TODOs: 326
- Unsupported feature TODOs: 3471
- Unknown function unique issues: 8
- Unsupported feature unique issues: 14

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `cte_sq` | 1 | sql/catalog/function/test_complex_macro.test |  |
| `double_add` | 1 | sql/catalog/function/test_complex_macro.test |  |
| `f1` | 1 | sql/catalog/function/test_complex_macro.test |  |
| `ifelse` | 4 | sql/catalog/function/test_complex_macro.test |  |
| `mod_two` | 1 | sql/catalog/function/test_complex_macro.test |  |
| `nested_cte` | 1 | sql/catalog/function/test_complex_macro.test |  |
| `triple_add1` | 1 | sql/catalog/function/test_complex_macro.test |  |
| `triple_add2` | 1 | sql/catalog/function/test_complex_macro.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `add` | `unsupported feature` | 1 | sql/catalog/function/test_complex_macro.test | `query I<br>SELECT add((SELECT MIN(a) FROM integers), (SELECT MAX(a) FROM integers))<br>----<br>42` |  |
| `add_mac` | `unsupported feature` | 1 | sql/catalog/function/test_complex_macro.test | `statement ok<br>CREATE MACRO add_mac(a, b) AS a + b` |  |
| `add_one` | `unsupported feature` | 2 | sql/catalog/function/test_complex_macro.test | `statement ok<br>CREATE MACRO add_one(a) AS a + 1` |  |
| `cte_sq` | `unsupported feature` | 1 | sql/catalog/function/test_complex_macro.test | `statement ok<br>CREATE MACRO cte_sq(a,b) AS (WITH cte AS (SELECT a * 2 AS c) SELECT cte.c + sq.d FROM cte, (SELECT b * 3 AS d) AS sq)` |  |
| `double_add` | `unsupported feature` | 1 | sql/catalog/function/test_complex_macro.test | `statement ok<br>CREATE MACRO double_add(a, b, c) AS add_mac(add_mac(a, b), c)` |  |
| `f1` | `unsupported feature` | 1 | sql/catalog/function/test_complex_macro.test | `statement ok<br>CREATE MACRO f1(x) AS (SELECT MIN(a) + x FROM integers)` |  |
| `fts_match` | `unsupported feature` | 2 | sql/catalog/function/test_complex_macro.test | `query II<br>SELECT * FROM documents WHERE fts_match(id, 'QUÁCK BÁRK')<br>----<br>doc1	 QUÁCK+QUÁCK+QUÁCK<br>doc2	 BÁRK+BÁRK+BÁRK+BÁRK` |  |
| `ifelse` | `unsupported feature` | 2 | sql/catalog/function/test_complex_macro.test | `statement error<br>SELECT IFELSE(1,IFELSE(1,b,1),a) FROM integers` |  |
| `mod_two` | `unsupported feature` | 1 | sql/catalog/function/test_complex_macro.test | `statement ok<br>CREATE MACRO mod_two(k) AS k%2` |  |
| `my_square` | `unsupported feature` | 2 | sql/catalog/function/test_complex_macro.test | `statement ok<br>CREATE MACRO my_square(a) AS a * a` |  |
| `mywindow` | `unsupported feature` | 1 | sql/catalog/function/test_complex_macro.test | `statement ok<br>CREATE MACRO mywindow(k,v) AS SUM(v) OVER (PARTITION BY k)` |  |
| `nested_cte` | `unsupported feature` | 1 | sql/catalog/function/test_complex_macro.test | `statement ok<br>CREATE MACRO nested_cte(needle, haystack) AS needle IN (<br>    SELECT i FROM (<br>        WITH ints AS (<br>            SELECT CAST(UNNEST(string_split(haystack,',')) AS INT) AS i<br>        )<br>        SELECT i FROM ints<br>    ) AS sq<br>)` |  |
| `triple_add1` | `unsupported feature` | 1 | sql/catalog/function/test_complex_macro.test | `statement ok<br>CREATE MACRO triple_add1(a, b, c, d) AS add_mac(add_mac(a, b), add_mac(c, d))` |  |
| `triple_add2` | `unsupported feature` | 1 | sql/catalog/function/test_complex_macro.test | `statement ok<br>CREATE MACRO triple_add2(a, b, c, d) as add_mac(add_mac(add_mac(a, b), c), d)` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:41:05.191306+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 3
- Files with issues: 340
- Unknown function TODOs: 327
- Unsupported feature TODOs: 3473
- Unknown function unique issues: 1
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `my_second_macro` | 1 | sql/catalog/function/test_cross_catalog_macros.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `my_first_macro` | `unsupported feature` | 1 | sql/catalog/function/test_cross_catalog_macros.test | `statement ok<br>CREATE MACRO my_first_macro() AS (84)` |  |
| `my_second_macro` | `unsupported feature` | 1 | sql/catalog/function/test_cross_catalog_macros.test | `statement ok<br>CREATE TEMPORARY MACRO my_second_macro() AS my_first_macro() + 42;` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:41:09.357396+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 12
- Files with issues: 341
- Unknown function TODOs: 336
- Unsupported feature TODOs: 3478
- Unknown function unique issues: 4
- Unsupported feature unique issues: 4

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `deep_cte` | 1 | sql/catalog/function/test_cte_macro.test |  |
| `in_with_cte` | 2 | sql/catalog/function/test_cte_macro.test |  |
| `parameterized_cte` | 1 | sql/catalog/function/test_cte_macro.test |  |
| `plus42` | 4 | sql/catalog/function/test_cte_macro.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `deep_cte` | `unsupported feature` | 1 | sql/catalog/function/test_cte_macro.test | `statement ok<br>CREATE MACRO deep_cte(param) AS (<br>    WITH cte1 AS (<br>        WITH cte2 AS (<br>            WITH cte3 AS (<br>                WITH cte4 AS (<br>                    SELECT param AS d<br>                )<br>                SELECT d AS c FROM cte4<br>            )<br>            SELECT c AS b FROM cte3<br>        )<br>        SELECT b AS a FROM cte2<br>    )<br>    SELECT a FROM cte1<br>)` |  |
| `in_with_cte` | `unsupported feature` | 1 | sql/catalog/function/test_cte_macro.test | `statement ok<br>CREATE MACRO in_with_cte(i) AS i IN (WITH cte AS (SELECT a AS answer FROM integers) SELECT answer FROM cte)` |  |
| `parameterized_cte` | `unsupported feature` | 1 | sql/catalog/function/test_cte_macro.test | `statement ok<br>CREATE MACRO parameterized_cte(a) AS (WITH cte AS (SELECT a AS answer) SELECT answer FROM cte)` |  |
| `plus42` | `unsupported feature` | 1 | sql/catalog/function/test_cte_macro.test | `statement ok<br>CREATE MACRO plus42(a) AS (WITH cte AS (SELECT 42 AS answer) SELECT answer + a FROM cte)` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:41:21.105639+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 8
- Files with issues: 343
- Unknown function TODOs: 337
- Unsupported feature TODOs: 3491
- Unknown function unique issues: 1
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `f` | 1 | sql/catalog/function/test_macro_default_arg.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `eq` | `unsupported feature` | 1 | sql/catalog/function/test_macro_default_arg.test | `statement ok<br>CREATE OR REPLACE MACRO eq(x := NULL, y := NULL) AS x = y` |  |
| `f` | `unsupported feature` | 6 | sql/catalog/function/test_macro_default_arg.test | `query I<br>SELECT f(x := 41)<br>----<br>42` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:41:32.035355+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 344
- Unknown function TODOs: 339
- Unsupported feature TODOs: 3502
- Unknown function unique issues: 1
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `my_macro2` | 1 | sql/catalog/function/test_macro_default_arg_with_dependencies.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `my_macro2` | `unsupported feature` | 1 | sql/catalog/function/test_macro_default_arg_with_dependencies.test | `statement ok<br>create macro my_macro2(i := 42) as (<br>    select min(a) + i from integers<br>);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:41:32.872744+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 8
- Files with issues: 345
- Unknown function TODOs: 343
- Unsupported feature TODOs: 3506
- Unknown function unique issues: 1
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `my_macro` | 4 | sql/catalog/function/test_macro_issue_13104.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `my_macro` | `unsupported feature` | 4 | sql/catalog/function/test_macro_issue_13104.test | `statement ok<br>create or replace macro my_macro(a:=true) as a;` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:41:33.381074+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 346
- Unknown function TODOs: 344
- Unsupported feature TODOs: 3507
- Unknown function unique issues: 1
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `extract_many` | 1 | sql/catalog/function/test_macro_issue_14276.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `extract_many` | `unsupported feature` | 1 | sql/catalog/function/test_macro_issue_14276.test | `statement ok<br>CREATE OR REPLACE MACRO extract_many(x, y) AS (SELECT struct_pack(*COLUMNS(z -> z in y)) FROM (SELECT unnest(x)));` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:41:33.827631+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 347
- Unknown function TODOs: 345
- Unsupported feature TODOs: 3508
- Unknown function unique issues: 1
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `my` | 1 | sql/catalog/function/test_macro_issue_18927.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `my` | `unsupported feature` | 1 | sql/catalog/function/test_macro_issue_18927.test | `statement ok<br>CREATE OR REPLACE MACRO my(s) AS s.lower().split('').aggregate('count');` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:41:36.931480+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 348
- Unknown function TODOs: 346
- Unsupported feature TODOs: 3508
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `round_even` | 1 | sql/catalog/function/test_macro_issue_19119.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:41:38.311468+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 5
- Files with issues: 349
- Unknown function TODOs: 347
- Unsupported feature TODOs: 3515
- Unknown function unique issues: 1
- Unsupported feature unique issues: 3

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `multi_add` | 1 | sql/catalog/function/test_macro_overloads.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `arithmetic` | `unsupported feature` | 2 | sql/catalog/function/test_macro_overloads.test | `statement ok<br>CREATE MACRO arithmetic<br>	(a, b, mult := 1) AS (a + b) * mult,<br>	(a, b, c, division := 1) AS  (a + b + c) / division` |  |
| `generate_numbers` | `unsupported feature` | 1 | sql/catalog/function/test_macro_overloads.test | `statement ok<br>CREATE MACRO generate_numbers<br>	(a, b) AS TABLE (SELECT * FROM range(a + b) t(i)),<br>	(a, b, c, mult := 1) AS TABLE (SELECT * FROM range((a + b + c) * mult) t(i))` |  |
| `multi_add` | `unsupported feature` | 1 | sql/catalog/function/test_macro_overloads.test | `statement ok<br>CREATE MACRO multi_add<br>	() AS 0,<br>	(a) AS a,<br>	(a, b) AS a + b,<br>	(a, b, c) AS  a + b + c,<br>	(a, b, c, d) AS  a + b + c + d,<br>	(a, b, c, d, e) AS  a + b + c + d + e` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:41:39.944837+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 7
- Files with issues: 350
- Unknown function TODOs: 354
- Unsupported feature TODOs: 3521
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `test` | 7 | sql/catalog/function/test_macro_relpersistence_conflict.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:41:46.668647+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 352
- Unknown function TODOs: 361
- Unsupported feature TODOs: 3574
- Unknown function unique issues: 1
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `m1` | 1 | sql/catalog/function/test_macro_with_unknown_types.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `m1` | `unsupported feature` | 1 | sql/catalog/function/test_macro_with_unknown_types.test | `statement ok<br>CREATE TEMP MACRO m1(x, y) AS (<br>    SELECT list_has_all(x, y) AND list_has_all(y, x)<br>);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:41:48.110279+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 3
- Files with issues: 353
- Unknown function TODOs: 361
- Unsupported feature TODOs: 3581
- Unknown function unique issues: 0
- Unsupported feature unique issues: 3

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `m2` | `unsupported feature` | 1 | sql/catalog/function/test_recursive_macro.test | `statement ok<br>create macro m2(a) as m1(a)+1;` |  |
| `m3` | `unsupported feature` | 1 | sql/catalog/function/test_recursive_macro.test | `statement ok<br>create macro m3(a) as a+1;` |  |
| `m4` | `unsupported feature` | 1 | sql/catalog/function/test_recursive_macro.test | `statement ok<br>create macro m4(a) as table select m3(a);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:41:54.894703+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 7
- Files with issues: 355
- Unknown function TODOs: 366
- Unsupported feature TODOs: 3601
- Unknown function unique issues: 2
- Unsupported feature unique issues: 3

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `in_next_n` | 3 | sql/catalog/function/test_sequence_macro.test |  |
| `in_next_n2` | 1 | sql/catalog/function/test_sequence_macro.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `add_macro` | `unsupported feature` | 1 | sql/catalog/function/test_sequence_macro.test | `statement ok<br>CREATE MACRO add_macro(a, b) AS a + b` |  |
| `in_next_n` | `unsupported feature` | 1 | sql/catalog/function/test_sequence_macro.test | `statement ok<br>CREATE MACRO in_next_n(x, s, n) AS x IN (<br>    WITH RECURSIVE cte AS (<br>            SELECT nextval(s) AS nxt, 1 AS iter<br>        UNION ALL<br>            SELECT nextval(s), iter + 1<br>            FROM cte<br>            WHERE iter < n<br>    )<br>    SELECT nxt<br>    FROM cte<br>)` |  |
| `in_next_n2` | `unsupported feature` | 1 | sql/catalog/function/test_sequence_macro.test | `statement ok<br>CREATE MACRO in_next_n2(x, s, n) AS x IN (<br>    WITH RECURSIVE cte AS (<br>            SELECT nextval(s) AS nxt, n AS n<br>        UNION ALL<br>            SELECT nextval(s), cte.n - 1<br>            FROM cte<br>            WHERE cte.n > 1<br>    )<br>    SELECT nxt<br>    FROM cte<br>)` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:42:08.579670+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 39
- Files with issues: 356
- Unknown function TODOs: 395
- Unsupported feature TODOs: 3636
- Unknown function unique issues: 12
- Unsupported feature unique issues: 14

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `add_default5` | 2 | sql/catalog/function/test_simple_macro.test |  |
| `add_macro` | 3 | sql/catalog/function/test_simple_macro.test |  |
| `having_macro` | 2 | sql/catalog/function/test_simple_macro.test |  |
| `in_expression_list` | 3 | sql/catalog/function/test_simple_macro.test |  |
| `myavg` | 1 | sql/catalog/function/test_simple_macro.test |  |
| `select_plus_floats` | 1 | sql/catalog/function/test_simple_macro.test |  |
| `string_split` | 1 | sql/catalog/function/test_simple_macro.test |  |
| `two` | 1 | sql/catalog/function/test_simple_macro.test |  |
| `two_default_params` | 1 | sql/catalog/function/test_simple_macro.test |  |
| `union_macro` | 3 | sql/catalog/function/test_simple_macro.test |  |
| `weird_avg` | 1 | sql/catalog/function/test_simple_macro.test |  |
| `zz1` | 1 | sql/catalog/function/test_simple_macro.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `add_default5` | `unsupported feature` | 2 | sql/catalog/function/test_simple_macro.test | `query T<br>SELECT add_default5(3)<br>----<br>8` |  |
| `floats` | `unsupported feature` | 1 | sql/catalog/function/test_simple_macro.test | `statement ok<br>CREATE TABLE floats (b FLOAT)` |  |
| `having_macro` | `unsupported feature` | 1 | sql/catalog/function/test_simple_macro.test | `statement ok<br>CREATE MACRO having_macro(x) AS (SELECT * FROM integers GROUP BY a HAVING a = x)` |  |
| `in_expression_list` | `unsupported feature` | 1 | sql/catalog/function/test_simple_macro.test | `statement ok<br>CREATE MACRO in_expression_list(x, y, z) AS (SELECT x IN (VALUES (y), (z)))` |  |
| `myavg` | `unsupported feature` | 1 | sql/catalog/function/test_simple_macro.test | `statement ok<br>CREATE MACRO myavg(x) AS SUM(x) / COUNT(x)` |  |
| `string_split` | `unsupported feature` | 1 | sql/catalog/function/test_simple_macro.test | `statement ok<br>CREATE MACRO string_split(a,b) AS a + b` |  |
| `two` | `unsupported feature` | 1 | sql/catalog/function/test_simple_macro.test | `statement ok<br>CREATE FUNCTION two() AS (SELECT 2);` |  |
| `two_default_params` | `unsupported feature` | 4 | sql/catalog/function/test_simple_macro.test | `query T<br>SELECT two_default_params(a := 5)<br>----<br>7` |  |
| `union_macro` | `unsupported feature` | 1 | sql/catalog/function/test_simple_macro.test | `statement ok<br>CREATE MACRO union_macro(x, y, z) AS (SELECT x IN (SELECT y UNION ALL SELECT z))` |  |
| `weird_avg` | `unsupported feature` | 1 | sql/catalog/function/test_simple_macro.test | `query T<br>SELECT weird_avg(a) FROM integers<br>----<br>14` |  |
| `wrong_order` | `unsupported feature` | 1 | sql/catalog/function/test_simple_macro.test | `statement error<br>CREATE MACRO wrong_order(a := 3, b) AS a + b` |  |
| `wrong_type` | `unsupported feature` | 2 | sql/catalog/function/test_simple_macro.test | `query I<br>select wrong_type(42)<br>----<br>42.5` |  |
| `zz1` | `unsupported feature` | 1 | sql/catalog/function/test_simple_macro.test | `statement ok<br>create macro zz1(x) as (select 10+x);` |  |
| `zz2` | `unsupported feature` | 1 | sql/catalog/function/test_simple_macro.test | `statement ok<br>create macro zz2(x) as 20+x;` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:42:12.921751+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 10
- Files with issues: 357
- Unknown function TODOs: 402
- Unsupported feature TODOs: 3639
- Unknown function unique issues: 3
- Unsupported feature unique issues: 3

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `a1` | 2 | sql/catalog/function/test_subquery_macro.test |  |
| `a2` | 1 | sql/catalog/function/test_subquery_macro.test |  |
| `subquery` | 4 | sql/catalog/function/test_subquery_macro.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `a1` | `unsupported feature` | 1 | sql/catalog/function/test_subquery_macro.test | `statement ok<br>CREATE MACRO a1(b) AS (SELECT a + a FROM integers)` |  |
| `a2` | `unsupported feature` | 1 | sql/catalog/function/test_subquery_macro.test | `statement ok<br>CREATE MACRO a2(b) AS (SELECT i.a + b FROM integers i)` |  |
| `subquery` | `unsupported feature` | 1 | sql/catalog/function/test_subquery_macro.test | `statement ok<br>CREATE MACRO subquery(a) AS (SELECT a)` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:42:22.025188+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 17
- Files with issues: 358
- Unknown function TODOs: 402
- Unsupported feature TODOs: 3661
- Unknown function unique issues: 0
- Unsupported feature unique issues: 8

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `cmp` | `unsupported feature` | 1 | sql/catalog/function/test_table_macro.test | `statement ok<br>CREATE MACRO   cmp(a,m) as regexp_matches(a,m) or a similar to m;` |  |
| `gm` | `unsupported feature` | 2 | sql/catalog/function/test_table_macro.test | `statement ok<br>SELECT * FROM  gm('^m');` |  |
| `ii` | `unsupported feature` | 2 | sql/catalog/function/test_table_macro.test | `query II<br>(SELECT* FROM xt(1, 'tom') EXCEPT SELECT* FROM xt(20,'tom' )) INTERSECT SELECT* FROM xt(100,'harry');<br>----<br>3	harry` |  |
| `sgreek` | `unsupported feature` | 1 | sql/catalog/function/test_table_macro.test | `statement ok<br>CREATE  MACRO sgreek(a,b,c) as TABLE SELECT a,b FROM greek_tbl WHERE(id >= c);` |  |
| `xt` | `unsupported feature` | 5 | sql/catalog/function/test_table_macro.test | `statement ok<br>CREATE MACRO xt(a,b) as a+b;` |  |
| `xt2` | `unsupported feature` | 2 | sql/catalog/function/test_table_macro.test | `query II<br>SELECT * FROM xt2(100,'m%');<br>----<br>4	mary<br>5	mungo<br>6	midge` |  |
| `xt_reg` | `unsupported feature` | 2 | sql/catalog/function/test_table_macro.test | `statement ok<br>SELECT * FROM xt_reg('^m');` |  |
| `xtm` | `unsupported feature` | 2 | sql/catalog/function/test_table_macro.test | `statement ok<br>SELECT * FROM xtm('m.*');` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:42:27.996268+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 16
- Files with issues: 359
- Unknown function TODOs: 402
- Unsupported feature TODOs: 3678
- Unknown function unique issues: 0
- Unsupported feature unique issues: 6

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `card_dfl` | `unsupported feature` | 2 | sql/catalog/function/test_table_macro_args.test | `query I<br>SELECT * FROM card_dfl();<br>----<br>hearts` |  |
| `card_select` | `unsupported feature` | 5 | sql/catalog/function/test_table_macro_args.test | `query I<br>SELECT DISTINCT val from card_select();<br>----<br>1` |  |
| `card_select_args` | `unsupported feature` | 2 | sql/catalog/function/test_table_macro_args.test | `query I<br>SELECT suit FROM card_select_args(1, 13, _name:='king' ) ORDER BY suit;<br>----<br>clubs<br>diamonds<br>hearts<br>spades` |  |
| `sc` | `unsupported feature` | 2 | sql/catalog/function/test_table_macro_args.test | `query III<br>SELECT * FROM sc(name, suit, 4);<br>----<br>1	ace	clubs<br>1	ace	diamonds<br>1	ace	hearts<br>1	ace	spades` |  |
| `sc2` | `unsupported feature` | 2 | sql/catalog/function/test_table_macro_args.test | `query I<br>SELECT * FROM sc2(50.0, 2);<br>----<br>hearts<br>spades` |  |
| `sc3` | `unsupported feature` | 3 | sql/catalog/function/test_table_macro_args.test | `query I<br>SELECT * FROM sc3(name);<br>----<br>ace<br>jack<br>king<br>queen` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:42:28.836611+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 6
- Files with issues: 360
- Unknown function TODOs: 402
- Unsupported feature TODOs: 3686
- Unknown function unique issues: 0
- Unsupported feature unique issues: 4

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `dates_between` | `unsupported feature` | 2 | sql/catalog/function/test_table_macro_complex.test | `query T<br>select * from dates_between('2021-01-01', '2021-02-04');<br>----<br>2021-01-01 00:00:00<br>2021-02-01 00:00:00` |  |
| `fibonacci` | `unsupported feature` | 2 | sql/catalog/function/test_table_macro_complex.test | `query II<br>SELECT * FROM fibonacci(1, 2, 5, 10);<br>----<br>11	144<br>12	233<br>13	377  <br>14	610<br>15	987` |  |
| `my_values` | `unsupported feature` | 1 | sql/catalog/function/test_table_macro_complex.test | `statement ok<br>CREATE MACRO my_values(m,s) as TABLE select * from (values  (1.0*m+s,'adam'), (2.0*m+s,'ben'),<br>(3.0*m+s,'cris'), (4.0*m+s,'desmond'),(5.0*m+s, 'eric'));` |  |
| `my_values_union` | `unsupported feature` | 1 | sql/catalog/function/test_table_macro_complex.test | `statement ok<br>CREATE MACRO my_values_union(m1,s1,m2,s2) as TABLE select * from my_values(m1,s1) UNION select * from my_values(m2,s2);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:42:41.513303+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 7
- Files with issues: 362
- Unknown function TODOs: 402
- Unsupported feature TODOs: 3719
- Unknown function unique issues: 0
- Unsupported feature unique issues: 4

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `car_pool` | `unsupported feature` | 1 | sql/catalog/function/test_table_macro_groups.test | `statement ok<br>CREATE TABLE car_pool (<br>  -- define columns (name / type / default value / nullable)<br>  id           DECIMAL      ,<br>  producer     VARCHAR(50)  ,<br>  model        VARCHAR(50)  ,<br>  yyyy         DECIMAL       CHECK (yyyy BETWEEN 1970 AND 2020),<br>  counter      DECIMAL       CHECK (counter >= 0),<br>  CONSTRAINT   car_pool_pk PRIMARY KEY (id)<br>);` |  |
| `car_pool_cube` | `unsupported feature` | 2 | sql/catalog/function/test_table_macro_groups.test | `statement ok<br>CREATE MACRO car_pool_cube(g1, g2, hcnt:=1) AS<br>TABLE SELECT g1, g2, sum(counter) AS cnt  FROM car_pool<br>GROUP BY CUBE(g1, g2) HAVING cnt >= hcnt order by g1 NULLS LAST, g2 NULLS LAST;` |  |
| `car_pool_groups` | `unsupported feature` | 2 | sql/catalog/function/test_table_macro_groups.test | `statement ok<br>CREATE MACRO car_pool_groups(g1, g2, hcnt:=1) AS<br>TABLE SELECT g1, g2, sum(counter) AS cnt  FROM car_pool  <br>GROUP BY  (g1, g2) HAVING cnt >= hcnt order by g1, g2;` |  |
| `car_pool_rollup` | `unsupported feature` | 2 | sql/catalog/function/test_table_macro_groups.test | `statement ok<br>CREATE MACRO car_pool_rollup(g1, g2, hcnt:=1) AS<br>TABLE SELECT g1, g2, sum(counter) AS cnt  FROM car_pool<br>GROUP BY ROLLUP(g1, g2) HAVING cnt >= hcnt order by g1, g2;` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:42:42.586132+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 7
- Files with issues: 363
- Unknown function TODOs: 402
- Unsupported feature TODOs: 3726
- Unknown function unique issues: 0
- Unsupported feature unique issues: 3

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `my_agg` | `unsupported feature` | 3 | sql/catalog/function/test_window_macro.test | `statement ok<br>select my_agg(range) <br>from range(0, 2);` |  |
| `my_case` | `unsupported feature` | 2 | sql/catalog/function/test_window_macro.test | `statement ok<br>select my_case(range) <br>from range(0, 2);` |  |
| `my_func` | `unsupported feature` | 2 | sql/catalog/function/test_window_macro.test | `statement ok<br>select my_func(range) <br>from range(0, 2);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:43:11.132611+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 5
- Files with issues: 369
- Unknown function TODOs: 423
- Unsupported feature TODOs: 3822
- Unknown function unique issues: 1
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `nextval` | 3 | sql/catalog/sequence/test_sequence.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `currval` | `unsupported feature` | 2 | sql/catalog/sequence/test_sequence.test | `query II<br>SELECT currval('a.seq'), currval('b.seq');<br>----<br>1	1` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:43:45.128451+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 4
- Files with issues: 372
- Unknown function TODOs: 424
- Unsupported feature TODOs: 3851
- Unknown function unique issues: 0
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `returning` | `unsupported feature` | 2 | sql/catalog/table/test_default_values.test | `query I<br>insert into x default values returning (i);<br>----<br>1` |  |
| `x` | `unsupported feature` | 2 | sql/catalog/table/test_default_values.test | `statement ok<br>create table x (i int default 1, j int default 2)` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:44:05.169172+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 375
- Unknown function TODOs: 426
- Unsupported feature TODOs: 3870
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `current_setting` | 2 | sql/catalog/test_querying_from_detached_catalog.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:44:15.569788+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 376
- Unknown function TODOs: 426
- Unsupported feature TODOs: 3878
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `existing_table` | `unsupported feature` | 1 | sql/catalog/test_schema.test | `statement ok<br>CREATE TABLE test_catalog.main.existing_table(i INTEGER);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:45:01.642885+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 13
- Files with issues: 378
- Unknown function TODOs: 428
- Unsupported feature TODOs: 3943
- Unknown function unique issues: 1
- Unsupported feature unique issues: 7

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `main_macro` | 1 | sql/catalog/test_set_schema.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `main_macro` | `unsupported feature` | 2 | sql/catalog/test_set_schema.test | `statement ok<br>SELECT main.main_macro(1, 2);` |  |
| `main_table` | `unsupported feature` | 3 | sql/catalog/test_set_schema.test | `statement ok<br>INSERT INTO main_table (j) VALUES (4);` |  |
| `main_table2` | `unsupported feature` | 1 | sql/catalog/test_set_schema.test | `statement ok<br>CREATE TABLE main.main_table2(i INTEGER);` |  |
| `oop_macro` | `unsupported feature` | 3 | sql/catalog/test_set_schema.test | `statement error<br>SELECT oop_macro(1, 2);` |  |
| `test_macro2` | `unsupported feature` | 1 | sql/catalog/test_set_schema.test | `statement ok<br>CREATE MACRO test_macro2(c, d) AS c * d;` |  |
| `test_table` | `unsupported feature` | 1 | sql/catalog/test_set_schema.test | `statement ok<br>INSERT INTO test.test_table (i) VALUES (2), (3);` |  |
| `test_table2` | `unsupported feature` | 1 | sql/catalog/test_set_schema.test | `statement ok<br>CREATE TABLE test_table2(i INTEGER);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:45:30.873592+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 19
- Files with issues: 380
- Unknown function TODOs: 443
- Unsupported feature TODOs: 3971
- Unknown function unique issues: 5
- Unsupported feature unique issues: 5

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `current_catalog` | 1 | sql/catalog/test_set_search_path.test |  |
| `current_database` | 1 | sql/catalog/test_set_search_path.test |  |
| `current_query` | 1 | sql/catalog/test_set_search_path.test |  |
| `current_schema` | 4 | sql/catalog/test_set_search_path.test |  |
| `current_schemas` | 3 | sql/catalog/test_set_search_path.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `current_schemas` | `unsupported feature` | 2 | sql/catalog/test_set_search_path.test | `query I<br>SELECT MAIN.CURRENT_SCHEMAS(false);<br>----<br>[test, test2]` |  |
| `current_setting` | `unsupported feature` | 3 | sql/catalog/test_set_search_path.test | `query I<br>SELECT CURRENT_SETTING('search_path');<br>----<br>test3` |  |
| `table_with_same_name` | `unsupported feature` | 2 | sql/catalog/test_set_search_path.test | `statement ok<br>CREATE TABLE main.table_with_same_name(in_main INTEGER);` |  |
| `test2_table` | `unsupported feature` | 1 | sql/catalog/test_set_search_path.test | `statement ok<br>CREATE TABLE test2.test2_table(i INTEGER);` |  |
| `test5_table` | `unsupported feature` | 1 | sql/catalog/test_set_search_path.test | `statement ok<br>CREATE TABLE test5.test5_table(i INTEGER);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:45:40.263949+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 381
- Unknown function TODOs: 443
- Unsupported feature TODOs: 3984
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `integersy` | `unsupported feature` | 2 | sql/catalog/test_temporary.test | `statement ok<br>CREATE TABLE memory.temp.integersy(i INTEGER)` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-16T15:45:41.271045+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 3
- Files with issues: 382
- Unknown function TODOs: 443
- Unsupported feature TODOs: 3995
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `varchar` | `unsupported feature` | 3 | sql/catalog/test_unicode_schema.test | `statement ok<br>CREATE TABLE ✍(🔑 INTEGER PRIMARY KEY, 🗣 varchar(64));` |  |
