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

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T00:48:05.805092+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 699
- Files with issues: 635
- Unknown function TODOs: 598
- Unsupported feature TODOs: 5553
- Unknown function unique issues: 13
- Unsupported feature unique issues: 43

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `duckdb_databases` | 9 | sql/copy/encryption/different_aes_ciphers.test<br>sql/copy/encryption/different_encryption_versions_and_ciphers.test<br>sql/copy/encryption/test_different_encryption_versions.test |  |
| `exclude` | 1 | sql/copy/csv/duck_fuzz/test_internal_4048.test |  |
| `glob` | 1 | sql/copy/csv/read_csv_variable.test |  |
| `make_timestamp` | 1 | sql/copy/parquet/parquet_6580.test |  |
| `parquet_file_metadata` | 2 | sql/copy/parquet/file_metadata.test |  |
| `parquet_full_metadata` | 4 | sql/copy/parquet/metadata_full.test |  |
| `parquet_scan` | 17 | sql/copy/parquet/fixed.test<br>sql/copy/parquet/parquet_1588.test<br>sql/copy/parquet/parquet_1589.test<br>sql/copy/parquet/parquet_1618_struct_strings.test<br>sql/copy/parquet/parquet_1619.test<br>sql/copy/parquet/parquet_2267.test<br>sql/copy/parquet/parquet_arrow_timestamp.test<br>sql/copy/parquet/parquet_blob.test<br>sql/copy/parquet/parquet_blob_string.test<br>sql/copy/parquet/parquet_enum_test.test<br>sql/copy/parquet/parquet_filter_bug1391.test |  |
| `parquet_schema` | 3 | sql/copy/parquet/float16.test<br>sql/copy/parquet/parquet_13053_duplicate_column_names.test |  |
| `read_csv` | 22 | sql/copy/csv/auto/test_sniffer_empty_start_value.test<br>sql/copy/csv/code_cov/csv_sniffer_header.test<br>sql/copy/csv/glob/read_csv_glob.test<br>sql/copy/csv/glob/test_unmatch_globs.test<br>sql/copy/csv/read_csv_variable.test<br>sql/copy/csv/test_column_inconsistencies.test<br>sql/copy/csv/test_read_csv.test<br>sql/copy/csv/test_sniff_csv.test<br>sql/copy/csv/test_sniffer_tab_delimiter.test<br>sql/copy/csv/test_validator.test<br>sql/copy/csv/timestamp_with_tz.test |  |
| `read_csv_auto` | 20 | sql/copy/csv/auto/test_type_detection.test<br>sql/copy/csv/code_cov/csv_type_detection.test<br>sql/copy/csv/glob/read_csv_glob.test<br>sql/copy/csv/leading_zeros_autodetect.test<br>sql/copy/csv/parallel/test_5566.test<br>sql/copy/csv/parallel/test_multiple_files.test<br>sql/copy/csv/parallel/test_parallel_csv.test<br>sql/copy/csv/plus_autodetect.test<br>sql/copy/csv/recursive_read_csv.test<br>sql/copy/csv/test_allow_quoted_nulls_option.test<br>sql/copy/csv/test_bgzf_read.test<br>sql/copy/csv/test_many_columns.test<br>sql/copy/csv/test_read_csv.test<br>sql/copy/csv/test_skip_bom.test<br>sql/copy/csv/test_union_by_name.test |  |
| `read_parquet` | 3 | sql/copy/parquet/float16.test<br>sql/copy/parquet/parquet_12621.test |  |
| `sniff_csv` | 31 | sql/copy/csv/test_11403.test<br>sql/copy/csv/test_15473.test<br>sql/copy/csv/test_comment_option.test<br>sql/copy/csv/test_csv_timestamp_tz.test<br>sql/copy/csv/test_date_sniffer.test<br>sql/copy/csv/test_empty_header.test<br>sql/copy/csv/test_headers_12089.test<br>sql/copy/csv/test_read_csv.test<br>sql/copy/csv/test_sniff_csv.test<br>sql/copy/csv/test_sniff_csv_options.test<br>sql/copy/csv/test_sniffer_tab_delimiter.test<br>sql/copy/csv/test_time.test |  |
| `struct_extract` | 1 | sql/copy/parquet/parquet_1619.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `array_to_string` | `unsupported feature` | 1 | sql/copy/parquet/parquet_filename.test | `query III<br>SELECT i, j, filename_tail3: array_to_string(parse_path(filename)[-3:], '/')<br>FROM parquet_scan('{DATA_DIR}/parquet-testing/glob*/t?.parquet', FILENAME=1)<br>WHERE filename_tail3 = 'parquet-testing/glob2/t1.parquet'<br>----<br>3	c	parquet-testing/glob2/t1.parquet` |  |
| `assert_bloom_filter_hit` | `unsupported feature` | 1 | sql/copy/parquet/bloom_filters.test | `statement ok<br>CREATE MACRO assert_bloom_filter_hit(file, col, val) AS TABLE<br>    SELECT COUNT(*) > 0 AND COUNT(*) < MAX(row_group_id+1) FROM parquet_bloom_probe(file, col, val) WHERE NOT bloom_filter_excludes;` |  |
| `copy` | `unsupported feature` | 7 | sql/copy/csv/copy_expression.test<br>sql/copy/csv/csv_error_message.test<br>sql/copy/csv/overwrite/test_overwrite_pipe_windows.test<br>sql/copy/csv/test_copy.test<br>sql/copy/hive_partition_null_overwrite.test | `statement ok<br>copy (select 42) to 'con:'` |  |
| `force_not_null` | `unsupported feature` | 2 | sql/copy/csv/test_force_not_null.test | `query I<br>COPY test FROM '{DATA_DIR}/csv/test/force_not_null.csv' (FORCE_NOT_NULL (col_a), HEADER 0);<br>----<br>3` |  |
| `force_quote` | `unsupported feature` | 1 | sql/copy/csv/test_force_quote.test | `query I<br>COPY test TO '{TEMP_DIR}/test_chosen_columns.csv' (FORCE_QUOTE (col_a, col_c), QUOTE 't', NULL 'ea');<br>----<br>3` |  |
| `getvariable` | `unsupported feature` | 1 | sql/copy/csv/copy_expression.test | `query I<br>COPY tbl FROM (getvariable('copy_target'));<br>----<br>5` |  |
| `hamming` | `unsupported feature` | 1 | sql/copy/csv/unquoted_escape/mixed.test | `query III<br>SELECT<br>    hamming(replace(string_agg(w, '\|' ORDER BY y), E'\r\n', E'\n'), E'\\\|,\|"\|\n'),<br>    hamming(string_agg(z, '\|' ORDER BY y), '"\|"a"\|"b\|c"'),<br>    bool_and(x = concat(w, '"', w))::int<br>FROM read_csv('{DATA_DIR}/csv/unquoted_escape/mixed.csv', quote = '"', escape = '\', sep = ',', strict_mode = false);<br>----<br>0	0	1` |  |
| `human_eval_csv` | `unsupported feature` | 1 | sql/copy/csv/unquoted_escape/human_eval.test | `statement ok<br>CREATE TABLE human_eval_csv(task_id TEXT, prompt TEXT, entry_point TEXT, canonical_solution TEXT, test TEXT);` |  |
| `human_eval_tsv` | `unsupported feature` | 1 | sql/copy/csv/unquoted_escape/human_eval.test | `statement ok<br>CREATE TABLE human_eval_tsv(task_id TEXT, prompt TEXT, entry_point TEXT, canonical_solution TEXT, test TEXT);` |  |
| `json_extract` | `unsupported feature` | 1 | sql/copy/parquet/json_parquet.test | `query I<br>SELECT json_extract(TX_JSON[1], 'block_hash') FROM json_tbl<br>----<br>"0x95cc694a09424ba463e4b1b704b86a56a41521473b3b4875691383c3d5c799b3"<br>"0x5aa34b59d13fc0c6c199c67451c5643ecfd905ee1ac940478b1e700203c707be"<br>"0x987d9d3a51a630337cdbd78858676ca6237ea692306ebd3586b4c3cb79e3762c"<br>"0x5f90325321b570ba4ade766478df3c73a5b89336c2b57b8fa8e64d2f937639d9"<br>"0x48cb55cb6814a86cbba8e5e859df11bc8f01f772a3972fb5c22a31d1339c24e4"<br>"0x059f581a8c9b196e0d11b462be0e194eb837922e17409c94c6888b72d0001b49"<br>"0xa3855d62087826f46f97d6c90854c223f19bf95b2df0d2a446e5f00efbae8b80"<br>"0x5193e56b142dfca5873d7272ea4892a83a74b3d4faa99f8e49ff83d7d4b53e0d"<br>"0x099041d5e2b624f8be592db1d624998e06495f680c6134bf1d7401d919bd0af0"<br>"0x2e5b481bba4f1596484c65ac5c36075d83eb0f85d10ee509d808d23f2f2af8e0"` |  |
| `length` | `unsupported feature` | 1 | sql/copy/csv/test_long_line.test | `query I<br>SELECT LENGTH(b) FROM test ORDER BY a;<br>----<br>10000<br>20000` |  |
| `lineitem` | `unsupported feature` | 1 | sql/copy/csv/test_compression_flag.test | `statement ok<br>CREATE TABLE lineitem(a INT NOT NULL,<br>                      b INT NOT NULL,<br>                      c INT NOT NULL);` |  |
| `movie_info` | `unsupported feature` | 1 | sql/copy/csv/test_imdb.test | `statement ok<br>CREATE TABLE movie_info (id integer NOT NULL PRIMARY KEY, movie_id integer NOT NULL, info_type_id integer NOT NULL, info text NOT NULL, note text);` |  |
| `mytbl` | `unsupported feature` | 1 | sql/copy/csv/test_replacement_scan_alias.test | `statement ok<br>select mytbl.mycol from '{DATA_DIR}/csv/test/dateformat.csv' mytbl(mycol);` |  |
| `nfc_normalize` | `unsupported feature` | 1 | sql/copy/csv/test_greek_utf8.test | `statement ok<br>CREATE TABLE greek_utf8 AS SELECT i, nfc_normalize(j) j, k FROM read_csv('{DATA_DIR}/csv/real/greek_utf8.csv', columns=STRUCT_PACK(i := 'INTEGER', j := 'VARCHAR', k := 'INTEGER'), delim='\|')` |  |
| `parquet_scan` | `unsupported feature` | 21 | sql/copy/parquet/parquet_blob_string.test<br>sql/copy/parquet/parquet_filename_filter.test | `query I<br>SELECT * FROM parquet_scan('{DATA_DIR}/parquet-testing/binary_string.parquet')<br>----<br>foo<br>bar<br>baz` |  |
| `parquet_schema` | `unsupported feature` | 1 | sql/copy/parquet/parquet_blob_string.test | `query I<br>SELECT converted_type FROM parquet_schema('{DATA_DIR}/parquet-testing/binary_string.parquet')<br>----<br>NULL<br>NULL` |  |
| `parse_filename` | `unsupported feature` | 7 | sql/copy/csv/parallel/parallel_csv_union_by_name.test<br>sql/copy/csv/test_filename_filter.test<br>sql/copy/csv/test_union_by_name.test | `query IIII<br>SELECT column1, column2, column3, parse_filename(filename) FROM read_csv('{DATA_DIR}/csv/filename_filter/*.csv', filename=true);<br>----<br>1	2	3	a.csv<br>4	5	6	b.csv<br>1	NULL	3	c.csv<br>1	1	3	d.csv<br>2	NULL	2	d.csv<br>3	3	100	d.csv` |  |
| `parse_path` | `unsupported feature` | 8 | sql/copy/csv/csv_hive.test<br>sql/copy/csv/csv_windows_mixed_separators.test<br>sql/copy/csv/read_csv_variable.test<br>sql/copy/parquet/parquet_filename.test | `query III<br>SELECT i, j, parse_path(filename)[-2:] FROM test_copy<br>----<br>1	a	[glob, t1.parquet]` |  |
| `proj` | `unsupported feature` | 6 | sql/copy/csv/test_insert_into_types.test | `statement ok<br>CREATE TABLE proj (<br>        id INTEGER NOT NULL,  /*primary key*/<br>);` |  |
| `raw_data` | `unsupported feature` | 1 | sql/copy/parquet/hive_timestamps.test | `statement ok<br>CREATE TABLE raw_data (<br>  ts TIMESTAMP_S NOT NULL,<br>  hits INTEGER NOT NULL<br>);` |  |
| `read_csv` | `unsupported feature` | 326 | sql/copy/csv/code_cov/csv_exact_buffer_size.test<br>sql/copy/csv/code_cov/csv_state_machine_invalid_utf.test<br>sql/copy/csv/csv_enum.test<br>sql/copy/csv/csv_enum_storage.test<br>sql/copy/csv/csv_names.test<br>sql/copy/csv/csv_null_byte.test<br>sql/copy/csv/csv_nullstr_list.test<br>sql/copy/csv/glob/read_csv_glob.test<br>sql/copy/csv/multidelimiter/test_2_byte_delimiter.test<br>sql/copy/csv/multidelimiter/test_3_4_byte_delimiter.test<br>sql/copy/csv/multidelimiter/test_abac.test<br>sql/copy/csv/null_terminator.test<br>sql/copy/csv/parallel/csv_parallel_buffer_size.test<br>sql/copy/csv/parallel/test_7578.test<br>sql/copy/csv/parallel/test_multiple_files.test<br>sql/copy/csv/parallel/test_parallel_csv.test<br>sql/copy/csv/pollock/test_field_delimiter.test<br>sql/copy/csv/pollock/test_quotation_char.test<br>sql/copy/csv/recursive_read_csv.test<br>sql/copy/csv/rejects/csv_incorrect_columns_amount_rejects.test<br>sql/copy/csv/rejects/csv_rejects_flush_message.test<br>sql/copy/csv/rejects/csv_rejects_maximum_line.test<br>sql/copy/csv/rejects/csv_rejects_read.test<br>sql/copy/csv/rejects/csv_unquoted_rejects.test<br>sql/copy/csv/rejects/test_invalid_utf_rejects.test<br>sql/copy/csv/rejects/test_mixed.test<br>sql/copy/csv/rejects/test_multiple_errors_same_line.test<br>sql/copy/csv/relaxed_quotes.test<br>sql/copy/csv/struct_padding.test<br>sql/copy/csv/test_12596.test<br>sql/copy/csv/test_auto_detection_headers.test<br>sql/copy/csv/test_big_header.test<br>sql/copy/csv/test_bug_10273.test<br>sql/copy/csv/test_column_inconsistencies.test<br>sql/copy/csv/test_comment_midline.test<br>sql/copy/csv/test_comment_option.test<br>sql/copy/csv/test_compression_flag.test<br>sql/copy/csv/test_csv_error_message_type.test<br>sql/copy/csv/test_csv_json.test<br>sql/copy/csv/test_csv_mixed_casts.test<br>sql/copy/csv/test_csv_no_trailing_newline.test<br>sql/copy/csv/test_csv_projection_pushdown.test<br>sql/copy/csv/test_csv_timestamp_tz.test<br>sql/copy/csv/test_date.test<br>sql/copy/csv/test_decimal.test<br>sql/copy/csv/test_empty_quote.test<br>sql/copy/csv/test_enum_csv.test<br>sql/copy/csv/test_extra_delimiters_rfc.test<br>sql/copy/csv/test_header_only.test<br>sql/copy/csv/test_hits_problematic.test<br>sql/copy/csv/test_ignore_errors.test<br>sql/copy/csv/test_ignore_mid_null_line.test<br>sql/copy/csv/test_insert_into_types.test<br>sql/copy/csv/test_issue3562_assertion.test<br>sql/copy/csv/test_issue5077.test<br>sql/copy/csv/test_mismatch_schemas.test<br>sql/copy/csv/test_missing_row.test<br>sql/copy/csv/test_mixed_new_line.test<br>sql/copy/csv/test_non_unicode_header.test<br>sql/copy/csv/test_null_padding_projection.test<br>sql/copy/csv/test_null_padding_union.test<br>sql/copy/csv/test_quote_default.test<br>sql/copy/csv/test_read_csv.test<br>sql/copy/csv/test_skip_bom.test<br>sql/copy/csv/test_sniff_csv.test<br>sql/copy/csv/test_sniff_csv_options.test<br>sql/copy/csv/test_sniffer_tab_delimiter.test<br>sql/copy/csv/test_thijs_unquoted_file.test<br>sql/copy/csv/test_thousands_separator.test<br>sql/copy/csv/test_time.test<br>sql/copy/csv/test_timestamptz_12926.test<br>sql/copy/csv/test_union_by_name.test<br>sql/copy/csv/test_union_by_name_types.test<br>sql/copy/csv/test_validator.test<br>sql/copy/csv/unquoted_escape/basic.test | `query I<br>select * FROM read_csv('{TEMP_DIR}/csv2tsv.tsv', header = 0)<br>----<br>a\0b` |  |
| `read_csv_auto` | `unsupported feature` | 93 | sql/copy/csv/auto/test_type_detection.test<br>sql/copy/csv/code_cov/csv_dialect_detection.test<br>sql/copy/csv/code_cov/csv_sniffer_header.test<br>sql/copy/csv/code_cov/csv_type_detection.test<br>sql/copy/csv/column_names.test<br>sql/copy/csv/csv_decimal_separator.test<br>sql/copy/csv/csv_enum.test<br>sql/copy/csv/csv_enum_storage.test<br>sql/copy/csv/csv_hive.test<br>sql/copy/csv/csv_hive_filename_union.test<br>sql/copy/csv/csv_names.test<br>sql/copy/csv/csv_null_padding.test<br>sql/copy/csv/empty_first_line.test<br>sql/copy/csv/empty_string_quote.test<br>sql/copy/csv/issue_6690.test<br>sql/copy/csv/issue_6764.test<br>sql/copy/csv/null_padding_big.test<br>sql/copy/csv/parallel/parallel_csv_hive_partitioning.test<br>sql/copy/csv/parallel/parallel_csv_union_by_name.test<br>sql/copy/csv/parallel/test_5438.test<br>sql/copy/csv/parallel/test_multiple_files.test<br>sql/copy/csv/parallel/test_parallel_csv.test<br>sql/copy/csv/recursive_csv_union_by_name.test<br>sql/copy/csv/test_9005.test<br>sql/copy/csv/test_allow_quoted_nulls_option.test<br>sql/copy/csv/test_big_header.test<br>sql/copy/csv/test_compression_flag.test<br>sql/copy/csv/test_csv_projection_pushdown.test<br>sql/copy/csv/test_mixed_line_endings.test<br>sql/copy/csv/test_read_csv.test<br>sql/copy/csv/test_skip_header.test<br>sql/copy/csv/test_union_by_name.test | `query I<br>SELECT * from read_csv_auto('{DATA_DIR}/csv/escape.csv', header = 0)<br>----<br>"]"bla]""` |  |
| `read_parquet` | `unsupported feature` | 8 | sql/copy/parquet/describe_parquet.test<br>sql/copy/parquet/encryption/arrow_compatibility.test<br>sql/copy/parquet/parquet_6630_union_by_name.test<br>sql/copy/parquet/parquet_filename.test | `statement ok<br>INSERT INTO test_copy FROM read_parquet('{DATA_DIR}/parquet-testing/glob/t1.parquet', filename=1);` |  |
| `regexp_matches` | `unsupported feature` | 2 | sql/copy/csv/test_header_only.test | `query I<br>SELECT REGEXP_MATCHES(abs_file_name, 'foo')<br>  FROM ( SELECT abs_file_name FROM read_csv('{DATA_DIR}/csv/header_only.csv', header=True, ignore_errors=True))<br>----` |  |
| `regexp_replace` | `unsupported feature` | 8 | sql/copy/csv/rejects/csv_unquoted_rejects.test | `query IIIIIII rowsort<br>SELECT regexp_replace(file_path, '\\', '/', 'g'), line, column_idx, column_name, error_type, line_byte_position,byte_position<br>FROM reject_scans inner join reject_errors on (reject_scans.scan_id = reject_errors.scan_id and reject_scans.file_id = reject_errors.file_id);<br>----<br>{DATA_DIR}/csv/rejects/unquoted/unquoted_new_line.csv	5	1	a	UNQUOTED VALUE	29	29` |  |
| `rtrim` | `unsupported feature` | 1 | sql/copy/csv/test_read_csv.test | `query IT<br>SELECT l_partkey, RTRIM(l_comment) FROM lineitem WHERE l_orderkey=1 ORDER BY l_linenumber;<br>----<br>15519	egular courts above the<br>6731	ly final dependencies: slyly bold<br>6370	riously. regular, express dep<br>214	lites. fluffily even de<br>2403	 pending foxes. slyly re<br>1564	arefully slyly ex` |  |
| `sales` | `unsupported feature` | 1 | sql/copy/csv/csv_copy_sniffer.test | `statement ok<br>CREATE TABLE sales (<br>    salesid INTEGER  NOT NULL PRIMARY KEY,<br>    listid INTEGER NOT NULL,<br>    sellerid INTEGER NOT NULL,<br>    buyerid  INTEGER NOT NULL,<br>    eventid INTEGER NOT NULL,<br>    dateid  SMALLINT  NOT NULL,<br>    qtysold   SMALLINT  NOT NULL,<br>    pricepaid DECIMAL (8,2),<br>    commission DECIMAL (8,2),<br>    saletime TIMESTAMP);` |  |
| `sniff_csv` | `unsupported feature` | 58 | sql/copy/csv/csv_names.test<br>sql/copy/csv/test_all_quotes.test<br>sql/copy/csv/test_comment_option.test<br>sql/copy/csv/test_dateformat.test<br>sql/copy/csv/test_header_only.test<br>sql/copy/csv/test_sniff_csv.test<br>sql/copy/csv/test_sniff_csv_options.test<br>sql/copy/csv/test_sniffer_tab_delimiter.test<br>sql/copy/csv/test_thijs_unquoted_file.test<br>sql/copy/csv/test_thousands_separator.test | `statement error<br>FROM sniff_csv('{DATA_DIR}/csv/real/non_ecziste.csv');` |  |
| `special_char` | `unsupported feature` | 1 | sql/copy/csv/unquoted_escape/basic.test | `statement ok<br>CREATE TABLE special_char(a INT, b STRING);` |  |
| `split_part` | `unsupported feature` | 1 | sql/copy/csv/unquoted_escape/human_eval.test | `statement ok<br>DELETE FROM human_eval_jsonl WHERE split_part(task_id, '/', 2)::int >= 10;` |  |
| `string_split_regex` | `unsupported feature` | 1 | sql/copy/csv/test_quoted_newline.test | `query T<br>SELECT string_split_regex(a, '[\r\n]+') FROM test ORDER BY a;<br>----<br>[hello, world]<br>['what,', ' brings, you here', ' , today']` |  |
| `table1` | `unsupported feature` | 1 | sql/copy/parquet/parquet_6933.test | `statement ok<br>CREATE TABLE table1 (<br>    name VARCHAR,<br>);` |  |
| `table2` | `unsupported feature` | 1 | sql/copy/parquet/parquet_6933.test | `statement ok<br>CREATE TABLE table2 (<br>    name VARCHAR,<br>    number INTEGER,<br>);` |  |
| `tbl_tz` | `unsupported feature` | 1 | sql/copy/csv/timestamp_with_tz.test | `statement ok<br>CREATE TABLE tbl_tz(id int, ts timestamptz);` |  |
| `test3` | `unsupported feature` | 2 | sql/copy/csv/test_copy.test<br>sql/copy/csv/test_force_quote.test | `statement ok<br>CREATE TABLE test3 (a INTEGER, b INTEGER);` |  |
| `test4` | `unsupported feature` | 3 | sql/copy/csv/test_copy.test | `statement ok<br>CREATE TABLE test4 (a INTEGER, b INTEGER, c VARCHAR(10));` |  |
| `timestamps` | `unsupported feature` | 1 | sql/copy/csv/test_insert_into_types.test | `statement ok<br>create table timestamps(ts timestamp, dt date);` |  |
| `users` | `unsupported feature` | 3 | sql/copy/csv/test_insert_into_types.test | `statement ok<br>CREATE TABLE users (<br>        id INTEGER NOT NULL,  /*primary key*/<br>        name VARCHAR(10) NOT NULL,<br>        email VARCHAR<br>);` |  |
| `users_age` | `unsupported feature` | 1 | sql/copy/csv/test_insert_into_types.test | `statement ok<br>CREATE TABLE users_age (<br>        id INTEGER NOT NULL,<br>        name VARCHAR(10) NOT NULL,<br>        email VARCHAR,<br>        age integer<br>);` |  |
| `vsize` | `unsupported feature` | 1 | sql/copy/csv/test_copy.test | `statement ok<br>CREATE TABLE vsize (a INTEGER, b INTEGER, c VARCHAR(10));` |  |
| `web_page` | `unsupported feature` | 1 | sql/copy/csv/test_web_page.test | `statement ok<br>CREATE TABLE web_page(wp_web_page_sk integer not null, wp_web_page_id char(16) not null, wp_rec_start_date date, wp_rec_end_date date, wp_creation_date_sk integer, wp_access_date_sk integer, wp_autogen_flag char(1), wp_customer_sk integer, wp_url varchar(100), wp_type char(50), wp_char_count integer, wp_link_count integer, wp_image_count integer, wp_max_ad_count integer, primary key (wp_web_page_sk));` |  |
| `x27` | `unsupported feature` | 3 | sql/copy/csv/test_blob.test | `query T<br>SELECT b FROM blobs<br>----<br>\x00\x01\x02\x03\x04\x05\x06\x07\x08\x0B\x0C\x0E\x0F\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1A\x1B\x1C\x1D\x1E\x1F !\x22#$%&\x27()*+-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[]^_`abcdefghijklmnopqrstuvwxyz{	}~\x80\x81\x82\x83\x84\x85\x86\x87\x88\x89\x8A\x8B\x8C\x8D\x8E\x8F\x90\x91\x92\x93\x94\x95\x96\x97\x98\x99\x9A\x9B\x9C\x9D\x9E\x9F\xA0\xA1\xA2\xA3\xA4\xA5\xA6\xA7\xA8\xA9\xAA\xAB\xAC\xAD\xAE\xAF\xB0\xB1\xB2\xB3\xB4\xB5\xB6\xB7\xB8\xB9\xBA\xBB\xBC\xBD\xBE\xBF\xC0\xC1\xC2\xC3\xC4\xC5\xC6\xC7\xC8\xC9\xCA\xCB\xCC\xCD\xCE\xCF\xD0\xD1\xD2\xD3\xD4\xD5\xD6\xD7\xD8\xD9\xDA\xDB\xDC\xDD\xDE\xDF\xE0\xE1\xE2\xE3\xE4\xE5\xE6\xE7\xE8\xE9\xEA\xEB\xEC\xED\xEE\xEF\xF0\xF1\xF2\xF3\xF4\xF5\xF6\xF7\xF8\xF9\xFA\xFB\xFC\xFD\xFE` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T01:31:23.248923+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 300
- Files with issues: 747
- Unknown function TODOs: 685
- Unsupported feature TODOs: 6038
- Unknown function unique issues: 17
- Unsupported feature unique issues: 16

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `array_cosine_distance` | 2 | sql/function/array/array_cosine_distance.test |  |
| `array_cosine_similarity` | 1 | sql/function/array/array_cosine_similarity.test |  |
| `array_cross_product` | 6 | sql/function/array/array_cross_product.test |  |
| `array_distance` | 1 | sql/function/array/array_distance.test |  |
| `array_inner_product` | 1 | sql/function/array/array_inner_product.test |  |
| `base64` | 7 | sql/function/blob/base64.test |  |
| `create_sort_key` | 8 | sql/function/blob/create_sort_key.test |  |
| `encode` | 1 | sql/function/blob/encode.test |  |
| `from_base64` | 6 | sql/function/blob/base64.test |  |
| `json_structure` | 1 | sql/export/parquet/export_parquet_json.test |  |
| `length` | 1 | sql/function/array/array_length.test |  |
| `list_distinct` | 1 | sql/function/array/array_list_functions.test |  |
| `list_slice` | 1 | sql/function/array/array_list_functions.test |  |
| `map` | 3 | sql/function/array/array_and_map.test |  |
| `time_bucket` | 37 | sql/function/date/test_time_bucket_date.test |  |
| `to_base64` | 1 | sql/function/blob/base64.test |  |
| `try` | 7 | sql/filter/test_try_filter_doesnt_mutate_columns.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `array_cross_product` | `unsupported feature` | 2 | sql/function/array/array_cross_product.test | `query I rowsort<br>SELECT array_cross_product(l, r) FROM (VALUES<br>	([-1, -2, 3]::FLOAT[3], [4, 0, -8]::FLOAT[3]),<br>	([1,2,3]::FLOAT[3], [1,5,7]::FLOAT[3]),<br>	([1,2,3]::FLOAT[3], NULL::FLOAT[3]),<br>	(NULL::FLOAT[3], [1,5,7]::FLOAT[3]),<br>	(NULL::FLOAT[3], NULL::FLOAT[3])<br>) as t(l,r);<br>----<br>NULL<br>NULL<br>NULL<br>[-1.0, -4.0, 3.0]<br>[16.0, 4.0, 8.0]` |  |
| `array_slice` | `unsupported feature` | 2 | sql/function/blob/test_blob_array_slice.test | `query I<br>select array_slice(blob '\x00\x01\x02\x03\x04\x05', 0, 2)<br>----<br>\x00\x01` |  |
| `collatz` | `unsupported feature` | 1 | sql/cte/materialized/test_nested_recursive_cte_materialized.test | `query III<br>WITH RECURSIVE collatz(x, t, steps) AS MATERIALIZED<br>(<br>  SELECT x, x, 0<br>  FROM   (WITH RECURSIVE n(t) AS (SELECT 1 UNION ALL SELECT t+1 FROM n WHERE t < 10) SELECT * FROM n) AS _(x)<br>    UNION ALL<br>  (SELECT x, CASE WHEN t%2 = 1 THEN t * 3 + p ELSE t / 2 END, steps + p<br>   FROM   collatz, (WITH RECURSIVE n(t) AS (SELECT 1 UNION ALL SELECT t+1 FROM n WHERE t < 1) SELECT * FROM n) AS _(p)<br>   WHERE  t <> 1)<br>)<br>SELECT * FROM collatz WHERE t = 1<br>ORDER BY x;<br>----<br>1	1	0<br>2	1	1<br>3	1	7<br>4	1	 2<br>5	1	 5<br>6	1	 8<br>7	1	16<br>8	1	 3<br>9	1	19<br>10	1	6` |  |
| `cte` | `unsupported feature` | 2 | sql/cte/materialized/test_cte_materialized.test<br>sql/cte/test_cte.test | `query I<br>WITH RECURSIVE cte(d) AS (<br>		SELECT 1<br>	UNION ALL<br>		(WITH c(d) AS (SELECT * FROM cte)<br>			SELECT d + 1<br>			FROM c<br>			WHERE FALSE<br>		)<br>)<br>SELECT max(d) FROM cte;<br>----<br>1` |  |
| `cte1` | `unsupported feature` | 1 | sql/cte/materialized/test_cte_in_cte_materialized.test | `query I<br>with cte1(xxx) as MATERIALIZED (with ncte(yyy) as MATERIALIZED (Select i as j from a) Select yyy from ncte) select xxx from cte1;<br>----<br>42` |  |
| `decode` | `unsupported feature` | 4 | sql/function/blob/encode.test | `query I<br>SELECT decode(encode('ü'))<br>----<br>ü` |  |
| `enable_logging` | `unsupported feature` | 1 | sql/cte/warn_deprecated_union_in_using_key.test | `statement ok<br>CALL enable_logging(level='error');` |  |
| `flatten` | `unsupported feature` | 1 | sql/function/array/array_flatten.test | `query I<br>select flatten([['a'], ['b'], ['c']]::varchar[1][3]);<br>----<br>[a, b, c]` |  |
| `materialized` | `unsupported feature` | 30 | sql/cte/materialized/recursive_cte_complex_pipelines.test<br>sql/cte/materialized/test_cte_in_cte_materialized.test<br>sql/cte/materialized/test_cte_materialized.test<br>sql/cte/materialized/test_cte_overflow_materialized.test<br>sql/cte/materialized/test_issue_10260.test<br>sql/cte/materialized/test_recursive_cte_union_all_materialized.test<br>sql/cte/materialized/test_recursive_cte_union_materialized.test | `query I<br>with a as MATERIALIZED (select * from va) select * from a<br>----<br>1729` |  |
| `parents_tab` | `unsupported feature` | 2 | sql/cte/materialized/recursive_hang_2745_materialized.test<br>sql/cte/recursive_hang_2745.test | `statement ok<br>create view vparents as<br>with RECURSIVE parents_tab (id , value , parent )<br>as MATERIALIZED (values (1, 1, 2), (2, 2, 4), (3, 1, 4), (4, 2, -1), (5, 1, 2), (6, 2, 7), (7, 1, -1)<br>),<br>parents_tab2 (id , value , parent )<br>as MATERIALIZED (values (1, 1, 2), (2, 2, 4), (3, 1, 4), (4, 2, -1), (5, 1, 2), (6, 2, 7), (7, 1, -1)<br>)<br>select * from parents_tab<br>union all<br>select id, value+2, parent from parents_tab2;` |  |
| `parquet_metadata` | `unsupported feature` | 3 | sql/copy/parquet/parquet_metadata.test<br>sql/copy/parquet/parquet_row_number.test | `statement ok<br>select * from parquet_metadata('{DATA_DIR}/parquet-testing/glob/*.parquet');` |  |
| `sql_auto_complete` | `unsupported feature` | 160 | sql/function/autocomplete/alter_table.test<br>sql/function/autocomplete/copy.test<br>sql/function/autocomplete/create_function.test<br>sql/function/autocomplete/create_schema.test<br>sql/function/autocomplete/create_sequence.test<br>sql/function/autocomplete/create_table.test<br>sql/function/autocomplete/create_type.test<br>sql/function/autocomplete/drop.test<br>sql/function/autocomplete/expressions.test<br>sql/function/autocomplete/identical_schema_table.test<br>sql/function/autocomplete/insert_into.test<br>sql/function/autocomplete/pragma.test<br>sql/function/autocomplete/scalar_functions.test<br>sql/function/autocomplete/select.test<br>sql/function/autocomplete/setting.test<br>sql/function/autocomplete/show.test<br>sql/function/autocomplete/suggest_file.test<br>sql/function/autocomplete/table_functions.test<br>sql/function/autocomplete/window.test | `query II<br>SELECT suggestion, suggestion_start FROM sql_auto_complete('WI') LIMIT 1;<br>----<br>WITH 	0` |  |
| `stats` | `unsupported feature` | 1 | sql/copy/parquet/parquet_row_number.test | `query IIII<br>select s.min, s.max, s.has_null, s.has_no_null from (select stats(seq) s from parquet_scan('{DATA_DIR}/parquet-testing/file_row_number.parquet') limit 1)<br>----<br>0	9999	0	1` |  |
| `strftime` | `unsupported feature` | 2 | sql/function/date/test_strftime.test | `query I<br>SELECT strftime('%Y', DATE '1992-01-01');<br>----<br>1992` |  |
| `struct_extract` | `unsupported feature` | 1 | sql/copy/parquet/test_parquet_nested.test | `query III<br>SELECT id, struct_extract(unnest(authors), 'name'), struct_extract(unnest(authors), 'id') FROM parquet_scan('{DATA_DIR}/parquet-testing/apkwan.parquet') limit 20<br>----<br>53e997b9b7602d9701f9f044	M. Stoll	56018d9645cedb3395e77641<br>53e997b9b7602d9701f9f044	H. Heiken	53f4d53adabfaef34ff814c8<br>53e997b9b7602d9701f9f044	G. M. N. Behrens	53f42afbdabfaec09f0ed4e0<br>53e997b9b7602d9701f9f044	R. E. Schmidt	56018d9645cedb3395e77644<br>53e997b2b7602d9701f8fea5	D. Barr	5440d4cfdabfae805a6fd46c<br>53e997aeb7602d9701f8856e	B Sharf	54059f34dabfae44f081a626<br>53e997aeb7602d9701f8856e	E Bental	5434518edabfaebba5856df4<br>53e997bab7602d9701fa1e34	R. A. Kyle	53f45704dabfaedd74e30781<br>53e997abb7602d9701f846c0	J. Mitchell	5405942bdabfae44f08177f9<br>53e9978db7602d9701f4d7e8	&NA;	NULL<br>53e9984bb7602d970207c61d	Olaf Th. Buck	53f4cef7dabfaeedd477c91f<br>53e9984bb7602d970207c61d	Volker Linnemann	544837ccdabfae87b7dea930<br>53e99796b7602d9701f5cd36	D. P. McKenzie	53f4384cdabfaeb22f48309c<br>53e99796b7602d9701f5cd36	J. G. Gluyas	53f42c87dabfaec09f108097<br>53e99796b7602d9701f5cd36	G. Eglinton	56017dd445cedb3395e642dd<br>53e99796b7602d9701f5cd36	M. L. Coleman	53f44fc6dabfaedd74e13c0e<br>53e99809b7602d970201f551	A Moncrieff	53f42dcfdabfaee43ebca730<br>53e99809b7602d970201f551	L E Whitby	53f4508fdabfaeb22f4e9af6<br>53e997a6b7602d9701f7ffb0	R R Walters	53f43b0edabfaee0d9b91d40<br>53e99813b7602d970202f0a1	Sean Milmo	53f45f64dabfaee4dc832b5f` |  |
| `weird_tbl` | `unsupported feature` | 2 | sql/copy/partitioned/hive_partition_escape.test | `statement ok<br>CREATE TABLE weird_tbl(id INT DEFAULT nextval('seq'), key VARCHAR)` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T02:44:34.441037+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 761
- Unknown function TODOs: 685
- Unsupported feature TODOs: 6144
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `list_map` | `unsupported feature` | 1 | sql/copy/parquet/writer/write_map.test | `statement ok<br>CREATE TABLE list_map(m MAP(INT[],INT[]));` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T02:56:04.015882+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 768
- Unknown function TODOs: 685
- Unsupported feature TODOs: 6164
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `uuid` | `unsupported feature` | 1 | sql/copy/parquet/writer/parquet_write_uuid.test | `statement ok<br>CREATE TABLE IF NOT EXISTS uuid (u uuid);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T03:23:16.010839+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 10
- Files with issues: 785
- Unknown function TODOs: 685
- Unsupported feature TODOs: 6269
- Unknown function unique issues: 0
- Unsupported feature unique issues: 7

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `c` | `unsupported feature` | 1 | sql/create/create_table_with_arraybounds.test | `statement ok<br>create table C (<br>	vis db2.schema3.bar[]<br>);` |  |
| `data` | `unsupported feature` | 1 | sql/copy_database/copy_database_with_index.test | `statement ok<br>CREATE INDEX data_value ON data(value);` |  |
| `items` | `unsupported feature` | 2 | sql/copy_database/copy_database_with_unique_index.test | `statement ok<br>CREATE UNIQUE INDEX idx_id ON old.items(id);` |  |
| `my_add` | `unsupported feature` | 1 | sql/copy_database/copy_database_different_types.test | `statement ok<br>CREATE FUNCTION my_add(a, b) AS a + b` |  |
| `new1` | `unsupported feature` | 2 | sql/copy_database/copy_database_with_unique_index.test | `statement ok<br>COPY FROM DATABASE old TO new1 (DATA);` |  |
| `tbl5` | `unsupported feature` | 2 | sql/create/create_as.test | `statement ok<br>CREATE TABLE IF NOT EXISTS tbl5(col1, col2) AS SELECT 3, 'database';` |  |
| `tbl6` | `unsupported feature` | 1 | sql/create/create_as.test | `statement ok<br>CREATE TABLE tbl6(col1) AS SELECT 4 ,'mismatch';` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T03:32:15.808710+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 793
- Unknown function TODOs: 685
- Unsupported feature TODOs: 6297
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `explain` | `unsupported feature` | 2 | sql/explain/test_explain.test<br>sql/explain/test_explain_limit.test | `query I<br>EXPLAIN (FORMAT JSON) SELECT * FROM integers LIMIT 10<br>----` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T03:57:12.093229+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 9
- Files with issues: 805
- Unknown function TODOs: 685
- Unsupported feature TODOs: 6416
- Unknown function unique issues: 0
- Unsupported feature unique issues: 8

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `alternating_sequences` | `unsupported feature` | 1 | sql/cte/recursive_cte_key_aggregation.test | `statement ok<br>CREATE TABLE alternating_sequences(it, i) AS SELECT 1, 0 FROM range(0, 10000) UNION ALL SELECT 2, 1 FROM range(0, 100000) UNION ALL SELECT 3, 2 FROM range(0, 10);` |  |
| `dsdgen` | `unsupported feature` | 1 | sql/cte/materialized/materialized_cte_modifiers.test | `statement ok<br>call dsdgen(sf=0)` |  |
| `dvr` | `unsupported feature` | 1 | sql/cte/recursive_cte_key_aggregation.test | `query IIII<br>WITH RECURSIVE dvr(here, there, via, len) USING KEY (here, there , arg_min(via, len), min(len)) AS (<br>	SELECT n.person1id AS here, n.person2id AS there, n.person2id AS via, 1 AS len<br>	FROM knows AS n<br>		UNION<br>	SELECT n.person1id AS here, dvr.there, dvr.here AS via, 1 + dvr.len AS len<br>	FROM dvr<br>	JOIN knows AS n ON<br>	(n.person2id = dvr.here AND n.person1id <> dvr.there))<br>TABLE dvr<br>ORDER BY here, there;<br>----<br>1	2	2	1<br>1	3	3	1<br>2	3	3	1` |  |
| `emp` | `unsupported feature` | 1 | sql/cte/test_recursive_cte_tutorial.test | `statement ok<br>CREATE TABLE emp (empno INTEGER,<br>                  ename VARCHAR,<br>				  job VARCHAR,<br>				  mgr INTEGER,<br>				  hiredate DATE,<br>				  sal DOUBLE,<br>				  comm DOUBLE,<br>				  deptno INTEGER);` |  |
| `p` | `unsupported feature` | 1 | sql/cte/recursive_array_slice.test | `statement ok<br>CREATE TABLE p(loc int8);` |  |
| `parent` | `unsupported feature` | 1 | sql/cte/test_recursive_cte_recurring.test | `query II<br>WITH RECURSIVE<br>parent(p,c) AS (<br>  VALUES ('c1','c2'),<br>         ('c1','c3'),<br>         ('c3','c4'),<br>         ('c3','c5'),<br>         ('c4','c6'),<br>         ('c4','c7')<br>),<br>ancestor(a,c) AS (<br>  FROM parent<br>    UNION<br>  SELECT a1.x, a2.y<br>  FROM   recurring.ancestor AS a1(x,z) NATURAL JOIN recurring.ancestor AS a2(z,y)<br>)<br>FROM ancestor ORDER BY ALL;<br>----<br>c1	c2<br>c1	c3<br>c1	c4<br>c1	c5<br>c1	c6<br>c1	c7<br>c3	c4<br>c3	c5<br>c3	c6<br>c3	c7<br>c4	c6<br>c4	c7` |  |
| `rec` | `unsupported feature` | 1 | sql/cte/test_cte.test | `query III<br>WITH RECURSIVE rec(a, b, c) AS (select a,b,c from (values(1,2,3),(1,2,3)) s(a,b,c) union select 1,2,3) select * from rec;<br>----<br>1	2	3` |  |
| `truncate_duckdb_logs` | `unsupported feature` | 2 | sql/cte/warn_deprecated_union_in_using_key.test | `statement ok<br>CALL truncate_duckdb_logs();` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T04:09:30.668324+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 9
- Files with issues: 815
- Unknown function TODOs: 685
- Unsupported feature TODOs: 6510
- Unknown function unique issues: 0
- Unsupported feature unique issues: 6

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `elaborate_macro` | `unsupported feature` | 4 | sql/export/export_indexes.test<br>sql/export/export_macros.test | `query T<br>SELECT elaborate_macro(28, y := 5)<br>----<br>33` |  |
| `my_other_range` | `unsupported feature` | 1 | sql/export/export_macros.test | `statement ok<br>CREATE MACRO my_schema.my_other_range(x) AS TABLE SELECT * FROM my_schema.my_range(x, y := 3)` |  |
| `my_range` | `unsupported feature` | 1 | sql/export/export_macros.test | `statement ok<br>CREATE MACRO my_schema.my_range(x, y := 7) AS TABLE SELECT range + x i FROM range(y)` |  |
| `table01` | `unsupported feature` | 1 | sql/export/export_database.test | `statement ok<br>CREATE TABLE table01(i INTEGER, j INTEGER);<br>CREATE TABLE s1.table01(i INTEGER, j INTEGER);<br>CREATE TABLE s2.table01(i INTEGER, j INTEGER);` |  |
| `union_tag` | `unsupported feature` | 1 | sql/export/export_quoted_union.test | `query I<br>select union_tag(COLUMNS(*)) from a;<br>----<br>member name 1` |  |
| `union_value` | `unsupported feature` | 1 | sql/export/export_quoted_union.test | `statement ok<br>insert into a values (<br>	union_value("member name 1" := 'hello')<br>);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T04:15:33.767066+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 57
- Files with issues: 815
- Unknown function TODOs: 692
- Unsupported feature TODOs: 6563
- Unknown function unique issues: 6
- Unsupported feature unique issues: 6

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `list_concat` | 2 | sql/function/array/array_list_functions.test |  |
| `list_contains` | 2 | sql/function/array/array_list_functions.test |  |
| `list_filter` | 1 | sql/function/array/array_list_functions.test |  |
| `list_position` | 2 | sql/function/array/array_list_functions.test |  |
| `list_resize` | 3 | sql/function/array/array_list_functions.test |  |
| `list_transform` | 1 | sql/function/array/array_list_functions.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `array_cosine_distance` | `unsupported feature` | 12 | sql/function/array/array_cosine_distance.test | `query I<br>SELECT array_cosine_distance([1, 2, 3]::FLOAT[3], [1, 2, 3]::FLOAT[3]);<br>----<br>0.0` |  |
| `array_cosine_similarity` | `unsupported feature` | 10 | sql/function/array/array_cosine_similarity.test | `query I<br>SELECT array_cosine_similarity([1, 2, 3]::FLOAT[3], [1, 2, 3]::FLOAT[3]);<br>----<br>1.0` |  |
| `array_distance` | `unsupported feature` | 5 | sql/function/array/array_distance.test | `query I<br>SELECT array_distance([1, 2, 3]::FLOAT[3], [1, 2, 3]::FLOAT[3]);<br>----<br>0.0` |  |
| `array_inner_product` | `unsupported feature` | 10 | sql/function/array/array_inner_product.test | `query I<br>SELECT array_inner_product([1, 1, 1]::FLOAT[3], [1, 1, 1]::FLOAT[3]);<br>----<br>3.0` |  |
| `array_length` | `unsupported feature` | 2 | sql/function/array/array_length.test | `query I<br>SELECT array_length([[1, 2, 2], [3, 4, 3]], 1);<br>----<br>2` |  |
| `arrays` | `unsupported feature` | 7 | sql/function/array/array_cosine_distance.test<br>sql/function/array/array_cosine_similarity.test<br>sql/function/array/array_distance.test<br>sql/function/array/array_inner_product.test | `statement ok<br>CREATE OR REPLACE TABLE arrays (l FLOAT[3]);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T07:19:04.546348+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 31
- Files with issues: 824
- Unknown function TODOs: 710
- Unsupported feature TODOs: 6703
- Unknown function unique issues: 2
- Unsupported feature unique issues: 5

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `strftime` | 1 | sql/function/date/test_strftime.test |  |
| `weekday` | 1 | sql/function/date/date_part_stats.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `create_sort_key` | `unsupported feature` | 22 | sql/function/blob/create_sort_key.test | `query I<br>SELECT * FROM integers ORDER BY create_sort_key(i, 'ASC NULLS LAST')<br>----<br>1<br>2<br>3<br>NULL` |  |
| `enum_code` | `unsupported feature` | 4 | sql/function/enum/test_enum_code.test | `statement error<br>SELECT enum_code('bla')` |  |
| `octet_length` | `unsupported feature` | 1 | sql/function/blob/test_blob_array_slice.test | `query I<br>select octet_length(array_slice(blob '\x00\x01\x02\x03\x04\x05', 4, 3))<br>----<br>0` |  |
| `p2` | `unsupported feature` | 1 | sql/function/enum/test_enum_code.test | `query I<br>EXECUTE p2('happy'::mood)<br>----<br>2` |  |
| `read_blob` | `unsupported feature` | 1 | sql/extensions/permissions_duckdb_extension.test | `statement ok<br>FROM read_blob('build/*/repository/*/*/parquet.duckdb_extension');` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T11:28:08.810908+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 5
- Files with issues: 837
- Unknown function TODOs: 710
- Unsupported feature TODOs: 6729
- Unknown function unique issues: 0
- Unsupported feature unique issues: 4

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `enum_first` | `unsupported feature` | 2 | sql/function/enum/test_enum_first.test | `statement error<br>SELECT enum_first('bla')` |  |
| `test_bit_and_bits` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_bit_and.test | `statement ok<br>CREATE TABLE test_bit_and_bits(b BIT);` |  |
| `test_bit_or_bits` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_bit_or.test | `statement ok<br>CREATE TABLE test_bit_or_bits(b BIT);` |  |
| `test_bit_xor_bits` | `unsupported feature` | 1 | sql/aggregate/aggregates/test_bit_xor.test | `statement ok<br>CREATE TABLE test_bit_xor_bits(b BIT);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T11:32:16.506370+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 11
- Files with issues: 839
- Unknown function TODOs: 710
- Unsupported feature TODOs: 6744
- Unknown function unique issues: 0
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `enum_last` | `unsupported feature` | 3 | sql/function/enum/test_enum_last.test<br>sql/function/enum/test_enum_range.test | `statement error<br>SELECT enum_last('bla')` |  |
| `enum_range_boundary` | `unsupported feature` | 8 | sql/function/enum/test_enum_range.test | `statement error<br>SELECT enum_range_boundary(NULL, NULL)` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T11:35:15.185537+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 4
- Files with issues: 840
- Unknown function TODOs: 714
- Unsupported feature TODOs: 6744
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `can_cast_implicitly` | 4 | sql/function/generic/can_cast_implicitly.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T11:43:59.459365+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 9
- Files with issues: 842
- Unknown function TODOs: 720
- Unsupported feature TODOs: 6748
- Unknown function unique issues: 2
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `cast_to_type` | 4 | sql/function/generic/cast_to_type.test |  |
| `v1` | 2 | sql/function/generic/cast_to_type.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `try_trim_null` | `unsupported feature` | 3 | sql/function/generic/cast_to_type.test | `query II<br>SELECT try_trim_null(COLUMNS(*)) FROM tbl<br>----<br>42	hello<br>100	NULL` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T11:46:31.987762+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 843
- Unknown function TODOs: 722
- Unsupported feature TODOs: 6748
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `constant_or_null` | 2 | sql/function/generic/constant_or_null.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T11:47:11.568099+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 844
- Unknown function TODOs: 722
- Unsupported feature TODOs: 6749
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `if` | `unsupported feature` | 1 | sql/function/generic/error.test | `query I<br>SELECT *<br>FROM (SELECT 4 AS x)<br>WHERE IF(x % 2 = 0, true, ERROR(FORMAT('x must be even number but is {}', x)));<br>----<br>4` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T11:59:00.353267+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 26
- Files with issues: 845
- Unknown function TODOs: 748
- Unsupported feature TODOs: 6758
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `hash` | 26 | sql/function/generic/hash_func.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:04:54.512574+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 847
- Unknown function TODOs: 749
- Unsupported feature TODOs: 6759
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `replace_type` | 1 | sql/function/generic/replace_type.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:04:55.351747+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 848
- Unknown function TODOs: 749
- Unsupported feature TODOs: 6760
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `repeat_row` | `unsupported feature` | 1 | sql/function/generic/table_func_varargs.test | `query III<br>SELECT * FROM repeat_row(1, 2, 'foo', num_rows=3)<br>----<br>1	2	foo<br>1	2	foo<br>1	2	foo` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:04:56.199655+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 849
- Unknown function TODOs: 749
- Unsupported feature TODOs: 6761
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `duckdb_approx_database_count` | `unsupported feature` | 1 | sql/function/generic/test_approx_database_count.test | `query I<br>SELECT approx_count >= 3 FROM duckdb_approx_database_count();<br>----<br>True` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:07:00.146433+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 850
- Unknown function TODOs: 749
- Unsupported feature TODOs: 6762
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `duckdb_connection_count` | `unsupported feature` | 1 | sql/function/generic/test_connection_count.test | `query I<br>SELECT count FROM duckdb_connection_count();<br>----<br>3` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:08:14.674289+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 853
- Unknown function TODOs: 751
- Unsupported feature TODOs: 6763
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `sleep_ms` | 1 | sql/function/generic/test_sleep.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:08:23.021881+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 855
- Unknown function TODOs: 752
- Unsupported feature TODOs: 6764
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `summary` | `unsupported feature` | 1 | sql/function/generic/test_table_param.test | `statement ok<br>EXPLAIN SELECT * FROM summary((SELECT * FROM a))` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:08:50.351741+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 856
- Unknown function TODOs: 753
- Unsupported feature TODOs: 6764
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `list_aggr` | 1 | sql/function/list/aggregates/any_value.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:09:07.035012+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 861
- Unknown function TODOs: 754
- Unsupported feature TODOs: 6768
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `list_count` | 1 | sql/function/list/aggregates/count.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:09:10.180398+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 862
- Unknown function TODOs: 755
- Unsupported feature TODOs: 6768
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `list_entropy` | 1 | sql/function/list/aggregates/entropy.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:09:20.622016+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 864
- Unknown function TODOs: 757
- Unsupported feature TODOs: 6768
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `list_histogram` | 1 | sql/function/list/aggregates/histogram.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:09:24.333888+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 866
- Unknown function TODOs: 758
- Unsupported feature TODOs: 6769
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `list_kurtosis` | 1 | sql/function/list/aggregates/kurtosis.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:09:36.848295+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 868
- Unknown function TODOs: 759
- Unsupported feature TODOs: 6770
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `list_mad` | 1 | sql/function/list/aggregates/mad.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:11:12.583476+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 872
- Unknown function TODOs: 760
- Unsupported feature TODOs: 6773
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `list_min` | 1 | sql/function/list/aggregates/nested.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:11:16.486253+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 873
- Unknown function TODOs: 761
- Unsupported feature TODOs: 6773
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `list_sem` | 1 | sql/function/list/aggregates/sem.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:11:19.660031+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 874
- Unknown function TODOs: 762
- Unsupported feature TODOs: 6773
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `list_skewness` | 1 | sql/function/list/aggregates/skewness.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:11:22.543034+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 875
- Unknown function TODOs: 763
- Unsupported feature TODOs: 6773
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `list_string_agg` | 1 | sql/function/list/aggregates/string_agg.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:14:04.576216+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 876
- Unknown function TODOs: 764
- Unsupported feature TODOs: 6773
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `list_sum` | 1 | sql/function/list/aggregates/sum.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:14:15.821962+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 877
- Unknown function TODOs: 765
- Unsupported feature TODOs: 6773
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `len` | 1 | sql/function/list/array_length.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:14:17.886375+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 879
- Unknown function TODOs: 766
- Unsupported feature TODOs: 6774
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `array_to_string_comma_default` | 1 | sql/function/list/array_to_string_comma_default.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:14:25.108942+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 883
- Unknown function TODOs: 767
- Unsupported feature TODOs: 6777
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `generate_subscripts` | 1 | sql/function/list/generate_subscripts.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:14:39.587275+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 886
- Unknown function TODOs: 770
- Unsupported feature TODOs: 6777
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `list_apply` | 1 | sql/function/list/lambdas/arrow/lambda_scope_deprecated.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:14:51.051496+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 888
- Unknown function TODOs: 772
- Unsupported feature TODOs: 6777
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `list_reduce` | 1 | sql/function/list/lambdas/arrow/reduce_deprecated.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:16:22.300604+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 895
- Unknown function TODOs: 777
- Unsupported feature TODOs: 6779
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `list_has_any` | 1 | sql/function/list/list_has_any_and_has_all.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:16:23.755171+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 896
- Unknown function TODOs: 778
- Unsupported feature TODOs: 6779
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `list_inner_product` | 1 | sql/function/list/list_inner_product.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:16:35.311915+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 899
- Unknown function TODOs: 781
- Unsupported feature TODOs: 6779
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `list_reverse` | 1 | sql/function/list/list_reverse.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:16:44.010727+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 901
- Unknown function TODOs: 783
- Unsupported feature TODOs: 6779
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `list_unique` | 1 | sql/function/list/list_unique.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:16:46.275076+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 902
- Unknown function TODOs: 784
- Unsupported feature TODOs: 6779
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `list_value` | 1 | sql/function/list/list_value_arrays.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:16:56.494510+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 906
- Unknown function TODOs: 786
- Unsupported feature TODOs: 6781
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `repeat` | `unsupported feature` | 1 | sql/function/list/repeat_list.test | `query I<br>SELECT repeat([1], 10);<br>----<br>[1, 1, 1, 1, 1, 1, 1, 1, 1, 1]` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:17:29.952124+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 908
- Unknown function TODOs: 787
- Unsupported feature TODOs: 6782
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `even` | 1 | sql/function/numeric/test_even.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:17:33.378025+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 909
- Unknown function TODOs: 787
- Unsupported feature TODOs: 6783
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `factorial` | `unsupported feature` | 1 | sql/function/numeric/test_factorial.test | `query I<br>SELECT factorial(30);<br>----<br>265252859812191058636308480000000` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:17:38.757474+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 910
- Unknown function TODOs: 788
- Unsupported feature TODOs: 6783
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `fmod` | 1 | sql/function/numeric/test_fdiv_fmod.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:19:22.339323+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 911
- Unknown function TODOs: 789
- Unsupported feature TODOs: 6783
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `gamma` | 1 | sql/function/numeric/test_gamma.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:19:23.729481+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 912
- Unknown function TODOs: 789
- Unsupported feature TODOs: 6784
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `gcd` | `unsupported feature` | 1 | sql/function/numeric/test_gcd_lcm.test | `query IIIIII<br>SELECT a, b, gcd(a, b), gcd(a, -b), gcd(b, a), gcd(-b, a)<br>FROM (VALUES (0::int8, 0::int8),<br>             (0::int8, 29893644334::int8),<br>             (288484263558::int8, 29893644334::int8),<br>             (-288484263558::int8, 29893644334::int8),<br>             ((-9223372036854775808)::int8, 1::int8),<br>             ((-9223372036854775808)::int8, 9223372036854775807::int8),<br>             ((-9223372036854775808)::int8, 4611686018427387904::int8)) AS v(a, b);<br>----<br>0	0	0	0	0	0<br>0	29893644334	29893644334	29893644334	29893644334	29893644334<br>288484263558	29893644334	6835958	6835958	6835958	6835958<br>-288484263558	29893644334	6835958	6835958	6835958	6835958<br>-9223372036854775808	1	1	1	1	1<br>-9223372036854775808	9223372036854775807	1	1	1	1<br>-9223372036854775808	4611686018427387904	4611686018427387904	4611686018427387904	4611686018427387904	4611686018427387904` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:19:29.230913+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 913
- Unknown function TODOs: 790
- Unsupported feature TODOs: 6784
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `geomean` | 1 | sql/function/numeric/test_geomean.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:19:39.610256+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 914
- Unknown function TODOs: 792
- Unsupported feature TODOs: 6784
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `isnan` | 2 | sql/function/numeric/test_is_nan.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:19:53.422900+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 916
- Unknown function TODOs: 793
- Unsupported feature TODOs: 6785
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `nextafter` | 1 | sql/function/numeric/test_nextafter.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:20:51.248295+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 918
- Unknown function TODOs: 794
- Unsupported feature TODOs: 6786
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `roundbankers` | 1 | sql/function/numeric/test_round_even.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:20:59.576364+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 919
- Unknown function TODOs: 795
- Unsupported feature TODOs: 6786
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `signbit` | 1 | sql/function/numeric/test_sign_bit.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:28:12.189478+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 920
- Unknown function TODOs: 796
- Unsupported feature TODOs: 6786
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `format_bytes` | 1 | sql/function/string/format_bytes.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:28:14.605224+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 921
- Unknown function TODOs: 796
- Unsupported feature TODOs: 6787
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `hex` | `unsupported feature` | 1 | sql/function/string/hex.test | `query I<br>SELECT hex(blob '\x00\x00\x80');<br>----<br>000080` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:28:22.526850+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 922
- Unknown function TODOs: 796
- Unsupported feature TODOs: 6788
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `md5` | `unsupported feature` | 1 | sql/function/string/md5.test | `query I<br>select md5('\xff\xff'::BLOB)<br>----<br>ab2a0d28de6b77ffdd6c72afead099ab` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:28:30.254695+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 923
- Unknown function TODOs: 797
- Unsupported feature TODOs: 6788
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `parse_formatted_bytes` | 1 | sql/function/string/parse_formatted_bytes.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:28:31.311569+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 924
- Unknown function TODOs: 798
- Unsupported feature TODOs: 6788
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `parse_path` | 1 | sql/function/string/parse_path.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:28:39.786249+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 927
- Unknown function TODOs: 800
- Unsupported feature TODOs: 6789
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `regexp_escape` | 1 | sql/function/string/regex_escape.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:28:47.174818+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 928
- Unknown function TODOs: 800
- Unsupported feature TODOs: 6790
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `regexp_extract` | `unsupported feature` | 1 | sql/function/string/regex_extract.test | `query T<br>SELECT regexp_extract('foobarbaz', 'B..', 0, 'i')<br>----<br>bar` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:28:50.721961+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 929
- Unknown function TODOs: 800
- Unsupported feature TODOs: 6791
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `regexp_extract_all` | `unsupported feature` | 1 | sql/function/string/regex_extract_all.test | `query I<br>SELECT regexp_extract_all('1a 2b 14m', '(\\d+)([a-z]+)', -1);<br>----<br>[]` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:29:17.197872+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 934
- Unknown function TODOs: 802
- Unsupported feature TODOs: 6794
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `sha1` | `unsupported feature` | 1 | sql/function/string/sha1.test | `query I<br>SELECT sha1('\xff\xff'::BLOB)<br>----<br>a19f987b885f5a96069f4bc7f12b9e84ceba7dfa` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:29:19.112295+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 935
- Unknown function TODOs: 803
- Unsupported feature TODOs: 6794
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `sha256` | 1 | sql/function/string/sha256.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:29:20.639948+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 936
- Unknown function TODOs: 804
- Unsupported feature TODOs: 6794
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `strip_accents` | 1 | sql/function/string/strip_accents.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:29:26.146837+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 937
- Unknown function TODOs: 805
- Unsupported feature TODOs: 6794
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `array_extract` | 1 | sql/function/string/test_array_extract.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:29:30.039914+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 938
- Unknown function TODOs: 806
- Unsupported feature TODOs: 6794
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `bar` | 1 | sql/function/string/test_bar.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:29:35.857567+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 939
- Unknown function TODOs: 807
- Unsupported feature TODOs: 6794
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `length_grapheme` | 1 | sql/function/string/test_complex_unicode.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:30:32.263146+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 941
- Unknown function TODOs: 807
- Unsupported feature TODOs: 6796
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `contains` | `unsupported feature` | 1 | sql/function/string/test_contains.test | `query IIIIIIIIII<br>SELECT CONTAINS('hello world', 'h'),<br>       CONTAINS('hello world', 'he'),<br>       CONTAINS('hello world', 'hel'),<br>       CONTAINS('hello world', 'hell'),<br>       CONTAINS('hello world', 'hello'),<br>       CONTAINS('hello world', 'hello '),<br>       CONTAINS('hello world', 'hello w'),<br>       CONTAINS('hello world', 'hello wo'),<br>       CONTAINS('hello world', 'hello wor'),<br>       CONTAINS('hello world', 'hello worl')<br>----<br>1	1	1	1	1	1	1	1	1	1` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:30:43.389868+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 943
- Unknown function TODOs: 808
- Unsupported feature TODOs: 6797
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `damerau_levenshtein` | 1 | sql/function/string/test_damerau_levenshtein.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:30:44.462571+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 944
- Unknown function TODOs: 809
- Unsupported feature TODOs: 6797
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `format` | 1 | sql/function/string/test_format.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:30:45.481428+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 945
- Unknown function TODOs: 810
- Unsupported feature TODOs: 6797
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `printf` | 1 | sql/function/string/test_format_extensions.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:32:58.936183+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 946
- Unknown function TODOs: 811
- Unsupported feature TODOs: 6797
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `jaccard` | 1 | sql/function/string/test_jaccard.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:33:00.469843+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 947
- Unknown function TODOs: 812
- Unsupported feature TODOs: 6797
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `jaro_winkler_similarity` | 1 | sql/function/string/test_jaro_winkler.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:33:17.900582+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 949
- Unknown function TODOs: 813
- Unsupported feature TODOs: 6798
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `levenshtein` | 1 | sql/function/string/test_levenshtein.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:33:27.050168+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 951
- Unknown function TODOs: 814
- Unsupported feature TODOs: 6799
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `mismatches` | 1 | sql/function/string/test_mismatches.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:33:29.498239+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 952
- Unknown function TODOs: 814
- Unsupported feature TODOs: 6800
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `lpad` | `unsupported feature` | 1 | sql/function/string/test_pad.test | `query TTTT<br>select LPAD('Base', 7, '-'), LPAD('Base', 4, '-'), LPAD('Base', 2, ''), LPAD('Base', -1, '-')<br>----<br>---Base	Base	Ba	(empty)` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:33:31.456963+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 953
- Unknown function TODOs: 815
- Unsupported feature TODOs: 6800
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `prefix` | 1 | sql/function/string/test_prefix.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:33:49.504298+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 957
- Unknown function TODOs: 817
- Unsupported feature TODOs: 6802
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `starts_with` | 1 | sql/function/string/test_starts_with_function.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:35:59.707012+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 963
- Unknown function TODOs: 819
- Unsupported feature TODOs: 6806
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `substring` | `unsupported feature` | 1 | sql/function/string/test_substring.test | `query T<br>SELECT substring(s, 2, -2) FROM strings<br>----<br>h<br>w<br>b<br>NULL` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:36:10.624494+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 965
- Unknown function TODOs: 820
- Unsupported feature TODOs: 6807
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `suffix` | 1 | sql/function/string/test_suffix.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:36:13.869563+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 966
- Unknown function TODOs: 821
- Unsupported feature TODOs: 6807
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `to_base` | 1 | sql/function/string/test_to_base.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:36:32.658146+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 967
- Unknown function TODOs: 822
- Unsupported feature TODOs: 6807
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `url_encode` | 1 | sql/function/string/test_url_encode.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:36:33.782017+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 968
- Unknown function TODOs: 822
- Unsupported feature TODOs: 6808
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `epoch` | `unsupported feature` | 1 | sql/function/time/epoch.test | `query I<br>select epoch(TIME '14:21:13')<br>----<br>51673` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:43:28.888404+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 972
- Unknown function TODOs: 825
- Unsupported feature TODOs: 6809
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `age` | `unsupported feature` | 1 | sql/function/timestamp/test_icu_age.test | `statement ok<br>SELECT AGE(TIMESTAMPTZ '1957-06-13') t;` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:43:40.493950+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 973
- Unknown function TODOs: 826
- Unsupported feature TODOs: 6809
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `strptime` | 1 | sql/function/timestamp/test_icu_strptime.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:43:58.321716+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 976
- Unknown function TODOs: 828
- Unsupported feature TODOs: 6810
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `try_strptime` | 1 | sql/function/timestamp/test_try_strptime.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:44:03.918020+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 977
- Unknown function TODOs: 828
- Unsupported feature TODOs: 6811
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `sql_tokenize` | `unsupported feature` | 1 | sql/function/tokenize/simple_tokenize.test | `query III<br>select start, token_type, word from sql_tokenize($$select a$$);<br>----<br>0	KEYWORD	select<br>7	IDENTIFIER	a` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:44:08.601699+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 978
- Unknown function TODOs: 829
- Unsupported feature TODOs: 6811
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `uuid_extract_version` | 1 | sql/function/uuid/test_uuid_function.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:44:09.684143+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 979
- Unknown function TODOs: 830
- Unsupported feature TODOs: 6811
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `variant_extract` | 1 | sql/function/variant/variant_extract.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T12:44:16.961167+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 981
- Unknown function TODOs: 831
- Unsupported feature TODOs: 6812
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `variant_typeof` | 1 | sql/function/variant/variant_typeof.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-17T13:32:26.016585+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 24
- Files with issues: 978
- Unknown function TODOs: 817
- Unsupported feature TODOs: 6808
- Unknown function unique issues: 0
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `left_grapheme` | `unsupported feature` | 11 | sql/function/string/test_left.test | `query T<br>SELECT LEFT_GRAPHEME(a, b) FROM strings<br>----<br>NULL<br>NULL<br>NULL` |  |
| `right_grapheme` | `unsupported feature` | 13 | sql/function/string/test_right.test | `statement ok<br>SELECT right_grapheme('a', -9223372036854775808);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T01:16:23.467093+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 9
- Files with issues: 969
- Unknown function TODOs: 823
- Unsupported feature TODOs: 6809
- Unknown function unique issues: 3
- Unsupported feature unique issues: 5

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `md5_number` | 1 | sql/function/string/md5.test |  |
| `md5_number_lower` | 1 | sql/function/string/md5.test |  |
| `md5_number_upper` | 1 | sql/function/string/md5.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `from_binary` | `unsupported feature` | 2 | sql/function/string/hex.test | `query I<br>SELECT from_binary('011001000111010101100011011010110110010001100010');<br>----<br>duckdb` |  |
| `from_hex` | `unsupported feature` | 1 | sql/function/string/hex.test | `query IIIIIIIIIIII<br>SELECT from_hex(to_hex(columns('^(.*int\|varchar\|bignum)$'))) FROM test_all_types();<br>----<br>\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x80	\xFF\xFF\xFF\xFF\xFF\xFF\x80\x00	\xFF\xFF\xFF\xFF\x80\x00\x00\x00	\x80\x00\x00\x00\x00\x00\x00\x00	\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00	\x00	\x00	\x00	\x00	\x00	\x7F\xFF\x7F\x00\x00\x00\x00\x00\x00\x07\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF	\xF0\x9F\xA6\x86\xF0\x9F\xA6\x86\xF0\x9F\xA6\x86\xF0\x9F\xA6\x86\xF0\x9F\xA6\x86\xF0\x9F\xA6\x86<br>\x7F	\x7F\xFF	\x7F\xFF\xFF\xFF	\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF	\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF	\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF	\xFF	\xFF\xFF	\xFF\xFF\xFF\xFF	\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF	\x80\x00\x80\xFF\xFF\xFF\xFF\xFF\xFF\xF8\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00	goo\x00se<br>NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL` |  |
| `sha256` | `unsupported feature` | 1 | sql/function/string/sha256.test | `query I<br>SELECT sha256('\xff\xff'::BLOB)<br>----<br>ca2fd00fa001190744c15c317643ab092e7048ce086a243e2be9437c898de1bb` |  |
| `to_binary` | `unsupported feature` | 1 | sql/function/string/hex.test | `query IIIIIIIIIIII<br>SELECT to_binary(columns('^(.*int\|varchar\|bignum)$')) FROM test_all_types();<br>----<br>1111111111111111111111111111111111111111111111111111111110000000	1111111111111111111111111111111111111111111111111000000000000000	1111111111111111111111111111111110000000000000000000000000000000	1000000000000000000000000000000000000000000000000000000000000000	10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000	0	0	0	0	0	0111111111111111011111110000000000000000000000000000000000000000000000000000011111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111	111100001001111110100110100001101111000010011111101001101000011011110000100111111010011010000110111100001001111110100110100001101111000010011111101001101000011011110000100111111010011010000110<br>1111111	111111111111111	1111111111111111111111111111111	111111111111111111111111111111111111111111111111111111111111111	1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111	11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111	11111111	1111111111111111	11111111111111111111111111111111	1111111111111111111111111111111111111111111111111111111111111111	1000000000000000100000001111111111111111111111111111111111111111111111111111100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000	011001110110111101101111000000000111001101100101<br>NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL` |  |
| `to_hex` | `unsupported feature` | 1 | sql/function/string/hex.test | `query IIIIIIIIIIII<br>SELECT to_hex(columns('^(.*int\|varchar\|bignum)$')) FROM test_all_types();<br>----<br>FFFFFFFFFFFFFF80	FFFFFFFFFFFF8000	FFFFFFFF80000000	8000000000000000	80000000000000000000000000000000	0	0	0	0	0	7FFF7F00000000000007FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF	F09FA686F09FA686F09FA686F09FA686F09FA686F09FA686<br>7F	7FFF	7FFFFFFF	7FFFFFFFFFFFFFFF	7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF	FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF	FF	FFFF	FFFFFFFF	FFFFFFFFFFFFFFFF	800080FFFFFFFFFFFFF800000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000	676F6F007365<br>NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL	NULL` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T02:41:28.463145+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 3503
- Files with issues: 1082
- Unknown function TODOs: 1102
- Unsupported feature TODOs: 13476
- Unknown function unique issues: 10
- Unsupported feature unique issues: 310

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `avg` | 2 | sql/function/string/test_jaro_winkler.test |  |
| `data` | 1 | sql/function/string/parse_formatted_bytes.test |  |
| `formatreadabledecimalsize` | 3 | sql/function/string/format_bytes.test |  |
| `formatreadablesize` | 3 | sql/function/string/format_bytes.test |  |
| `parse_dirname` | 23 | sql/function/string/parse_path.test<br>sql/function/string/parse_path_windows.test |  |
| `parse_dirpath` | 23 | sql/function/string/parse_path.test<br>sql/function/string/parse_path_windows.test |  |
| `parse_filename` | 38 | sql/function/string/parse_path.test<br>sql/function/string/parse_path_windows.test |  |
| `pg_size_pretty` | 1 | sql/function/string/format_bytes.test |  |
| `pg_sleep` | 10 | sql/function/generic/test_sleep.test |  |
| `url_decode` | 3 | sql/function/string/test_url_encode.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `_` | `unsupported feature` | 1 | sql/function/list/generate_series.test | `query II<br>SELECT * FROM (SELECT 1 UNION ALL SELECT 0 UNION ALL SELECT 2) AS _(x), generate_series(1, x) AS __(y) ORDER BY x, y<br>----<br>1	1<br>2	1<br>2	2` |  |
| `acos` | `unsupported feature` | 7 | sql/function/numeric/test_trigo.test | `statement error<br>select acos(-2);` |  |
| `acosh` | `unsupported feature` | 1 | sql/function/numeric/test_trigo.test | `query IIII<br>select acosh(-1), acosh(0), acosh(1), acosh(2);<br>----<br>NaN	NaN	0.0	1.3169578969248166` |  |
| `addresses` | `unsupported feature` | 1 | sql/function/list/test_lambda_with_struct_aliases.test | `statement ok<br>CREATE TABLE addresses (i INT, b INT);` |  |
| `aggr2` | `unsupported feature` | 2 | sql/function/list/aggregates/entropy.test<br>sql/function/list/aggregates/skewness.test | `statement ok<br>create table aggr2 (k int[]);` |  |
| `ago` | `unsupported feature` | 9 | sql/function/timestamp/current_timestamp.test | `query II<br>select <br>	current_timestamp - interval 1 day = ago(interval 1 day),<br>	current_timestamp - interval 1 day = ago('1 day');<br>----<br>True	True` |  |
| `and` | `unsupported feature` | 1 | sql/function/operator/test_arithmetic_sqllogic.test | `query I<br>SELECT DISTINCT - col2 AS col2 FROM tab1 WHERE NOT 18 BETWEEN NULL AND ( + col0 * + CAST ( NULL AS INTEGER ) + - 3 / col2 ) OR NOT col0 BETWEEN col2 + + col1 AND NULL ORDER BY 1 DESC;<br>----<br>-68<br>-96` |  |
| `array_aggr` | `unsupported feature` | 1 | sql/function/list/aggregates/nested.test | `query I<br>SELECT array_aggr([1, 2], 'min')<br>----<br>1` |  |
| `array_aggregate` | `unsupported feature` | 1 | sql/function/list/aggregates/nested.test | `query I<br>SELECT array_aggregate([1, 2], 'min')<br>----<br>1` |  |
| `array_apply` | `unsupported feature` | 2 | sql/function/list/lambdas/arrow/transform_deprecated.test<br>sql/function/list/lambdas/transform.test | `query I<br>SELECT array_apply([1, NULL], arr_elem -> arr_elem - 4)<br>----<br>[-3, NULL]` |  |
| `array_cat` | `unsupported feature` | 1 | sql/function/list/list_concat.test | `query T<br>SELECT array_cat([1, 2], [3, 4])<br>----<br>[1, 2, 3, 4]` |  |
| `array_contains` | `unsupported feature` | 1 | sql/function/list/list_contains.test | `query II<br>SELECT id, name FROM test WHERE ARRAY_CONTAINS(name, '2Pac');<br>----<br>3	[Oasis, 2Pac]` |  |
| `array_extract` | `unsupported feature` | 17 | sql/function/string/test_array_extract.test | `query T<br>SELECT array_extract(s, 1) FROM strings<br>----<br>h<br>w<br>b<br>NULL` |  |
| `array_filter` | `unsupported feature` | 2 | sql/function/list/lambdas/arrow/filter_deprecated.test<br>sql/function/list/lambdas/filter.test | `query I<br>SELECT array_filter([1, NULL], arr_elem -> arr_elem < 4)<br>----<br>[1]` |  |
| `array_has_all` | `unsupported feature` | 1 | sql/function/list/list_has_any_and_has_all.test | `query I<br>select array_has_all(list_intersect(l1, l2), list_intersect(l2, l1)) from tbl;<br>----<br>1` |  |
| `array_has_any` | `unsupported feature` | 1 | sql/function/list/list_has_any_and_has_all.test | `query I<br>select array_has_any(list_intersect(l1, l2), list_intersect(l2, l1)) from tbl;<br>----<br>1` |  |
| `array_prepend` | `unsupported feature` | 1 | sql/function/list/list_concat.test | `query T<br>SELECT array_prepend(1, [2, 3])<br>----<br>[1, 2, 3]` |  |
| `array_push_back` | `unsupported feature` | 2 | sql/function/list/list_concat.test | `query T<br>SELECT array_push_back(NULL, 3);<br>----<br>[3]` |  |
| `array_push_front` | `unsupported feature` | 2 | sql/function/list/list_concat.test | `query T<br>SELECT array_push_front(NULL, 1);<br>----<br>[1]` |  |
| `array_resize` | `unsupported feature` | 3 | sql/function/list/list_resize.test<br>sql/function/list/list_resize_error.test | `statement ok<br>prepare q2 as select array_resize(?, ?);` |  |
| `array_reverse` | `unsupported feature` | 1 | sql/function/list/list_reverse.test | `query I<br>SELECT array_reverse([1, 42, 2]);<br>----<br>[2, 42, 1]` |  |
| `array_table` | `unsupported feature` | 2 | sql/function/list/list_value_arrays.test | `statement ok<br>CREATE TABLE array_table (a STRING[3], b STRING[3], c STRING[3]);` |  |
| `array_to_string_comma_default` | `unsupported feature` | 8 | sql/function/list/array_to_string_comma_default.test | `query I<br>SELECT array_to_string_comma_default(NULL, sep:='-')<br>----<br>NULL` |  |
| `array_transform` | `unsupported feature` | 2 | sql/function/list/lambdas/arrow/transform_deprecated.test<br>sql/function/list/lambdas/transform.test | `query I<br>SELECT array_transform([1, NULL], arr_elem -> arr_elem - 4)<br>----<br>[-3, NULL]` |  |
| `array_unique` | `unsupported feature` | 1 | sql/function/list/list_unique.test | `query I<br>SELECT array_unique([1, 2, 2, NULL])<br>----<br>2` |  |
| `arrow` | `unsupported feature` | 1 | sql/function/list/lambdas/arrow/warn_deprecated_arrow.test | `query II<br>SELECT log_level, message[0:37] FROM duckdb_logs<br>----<br>WARNING	Deprecated lambda arrow (->) detected` |  |
| `ascii` | `unsupported feature` | 1 | sql/function/string/null_byte.test | `query I<br>select ascii(chr(0));<br>----<br>0` |  |
| `asin` | `unsupported feature` | 8 | sql/function/numeric/test_trigo.test | `statement error<br>select asin(-2);` |  |
| `asinh` | `unsupported feature` | 1 | sql/function/numeric/test_trigo.test | `query III<br>select asinh(-1), asinh(0), asinh(2);<br>----<br>-0.881373587019543	0.0	1.4436354751788103` |  |
| `atan` | `unsupported feature` | 6 | sql/function/numeric/test_trigo.test | `query I<br>SELECT cast(ATAN(n::float)*1000 as bigint) FROM numbers ORDER BY n<br>----<br>NULL<br>-1547<br>-785<br>0<br>785<br>1547` |  |
| `atan2` | `unsupported feature` | 6 | sql/function/numeric/test_trigo.test | `query I<br>SELECT cast(ATAN2(n::float, 42)*1000 as bigint) FROM numbers ORDER BY n<br>----<br>NULL<br>-785<br>-24<br>0<br>24<br>785` |  |
| `atanh` | `unsupported feature` | 4 | sql/function/numeric/test_trigo.test | `statement error<br>select atanh(1.1);` |  |
| `bar` | `unsupported feature` | 33 | sql/function/string/test_bar.test | `query I<br>select bar(9, 10, 20)<br>----` |  |
| `bitwise_test` | `unsupported feature` | 4 | sql/function/operator/test_bitwise_ops_types.test | `statement ok<br>CREATE TABLE bitwise_test(i BIGINT, j BIGINT)` |  |
| `bool_table` | `unsupported feature` | 1 | sql/function/list/list_resize.test | `statement ok<br>CREATE TABLE bool_table(a bool[], b int);` |  |
| `bools` | `unsupported feature` | 2 | sql/function/list/aggregates/bool_and_or.test<br>sql/function/list/list_zip.test | `statement ok<br>CREATE TABLE bools (b bool)` |  |
| `cast_test` | `unsupported feature` | 1 | sql/function/list/lambdas/reduce_initial.test | `statement ok<br>CREATE TABLE cast_test (l int[], initial float);` |  |
| `cbrt` | `unsupported feature` | 1 | sql/function/numeric/test_pg_math.test | `query R<br>select cbrt(27.0)<br>----<br>3.0` |  |
| `ceil` | `unsupported feature` | 7 | sql/function/numeric/test_floor_ceil.test<br>sql/function/numeric/test_pg_math.test | `query I<br>select ceil(-42.8)<br>----<br>-42` |  |
| `ceiling` | `unsupported feature` | 2 | sql/function/numeric/test_floor_ceil.test<br>sql/function/numeric/test_pg_math.test | `query I<br>select ceiling(-95.3)<br>----<br>-95` |  |
| `century` | `unsupported feature` | 3 | sql/function/time/test_date_part.test<br>sql/function/timestamp/test_icu_datepart.test<br>sql/function/timetz/test_date_part.test | `statement error<br>SELECT century(i) FROM times` |  |
| `chr` | `unsupported feature` | 4 | sql/function/list/aggregates/minmax_nested.test<br>sql/function/string/null_byte.test<br>sql/function/string/regexp_unicode_literal.test | `query T<br>SELECT chr(0)<br>----<br> ` |  |
| `cities` | `unsupported feature` | 2 | sql/function/list/lambdas/arrow/table_functions_deprecated.test<br>sql/function/list/lambdas/table_functions.test | `statement ok<br>CREATE TABLE cities AS<br>SELECT * FROM (VALUES ('Amsterdam', [90, 10]), ('London', [89, 102])) cities (name, prices);` |  |
| `coalesce` | `unsupported feature` | 1 | sql/function/list/test_lambda_with_struct_aliases.test | `query I<br>SELECT COALESCE(*COLUMNS(lambda c: {'title': c}.title IN ('a', 'c')))<br>FROM (SELECT NULL, 2, 3) t(a, b, c);<br>----<br>3` |  |
| `corr_test` | `unsupported feature` | 4 | sql/function/list/lambdas/arrow/filter_deprecated.test<br>sql/function/list/lambdas/arrow/transform_deprecated.test<br>sql/function/list/lambdas/filter.test<br>sql/function/list/lambdas/transform.test | `statement ok<br>CREATE TABLE corr_test (n integer, l varchar[], g integer)` |  |
| `cos` | `unsupported feature` | 8 | sql/function/list/lambdas/incorrect.test<br>sql/function/numeric/test_trigo.test | `statement error<br>SELECT cos(x -> x + 1);` |  |
| `cosh` | `unsupported feature` | 1 | sql/function/numeric/test_trigo.test | `query IIII<br>select cosh(-1), cosh(0), cosh(1), cosh(1000);<br>----<br>1.5430806348152437	1.0	1.5430806348152437	inf` |  |
| `cot` | `unsupported feature` | 6 | sql/function/numeric/test_trigo.test | `query I<br>SELECT cast(COT(n::float)*1000 as bigint) FROM numbers  WHERE n > 0.1 OR N < -0.1 ORDER BY n<br>----<br>-436<br>-642<br>642<br>436` |  |
| `countif` | `unsupported feature` | 1 | sql/function/list/aggregates/sum_bool.test | `query III<br>SELECT COUNTIF(i > 500), COUNT_IF(i=1), COUNTIF(i IS NULL) FROM integers<br>----<br>6332	1	3334` |  |
| `damerau_levenshtein` | `unsupported feature` | 51 | sql/function/string/test_damerau_levenshtein.test | `statement error<br>SELECT damerau_levenshtein()` |  |
| `date_diff` | `unsupported feature` | 4 | sql/function/timestamp/test_icu_datediff.test | `query I<br>select date_diff('day', '2022-01-04 19:00:00'::timestamptz, '2024-03-01'::date) as c1;<br>----<br>787` |  |
| `date_part` | `unsupported feature` | 163 | sql/function/interval/test_date_part.test<br>sql/function/time/test_date_part.test<br>sql/function/timestamp/test_icu_datepart.test<br>sql/function/timetz/test_date_part.test | `statement error<br>select date_part('era', time '10:00:00');` |  |
| `date_trunc` | `unsupported feature` | 48 | sql/function/interval/test_interval_trunc.test<br>sql/function/timestamp/test_icu_datetrunc.test | `query T<br>SELECT date_trunc(NULL, d) FROM timestamps LIMIT 3;<br>----<br>NULL<br>NULL<br>NULL` |  |
| `datediff` | `unsupported feature` | 228 | sql/function/timestamp/test_date_diff_epoch.test<br>sql/function/timestamp/test_icu_datediff.test<br>sql/function/uuid/test_uuid_function.test | `query I<br>SELECT DATEDIFF('day', startdate, enddate) FROM datetime1<br>----<br>1` |  |
| `dates` | `unsupported feature` | 5 | sql/function/list/aggregates/approx_count_distinct.test<br>sql/function/list/aggregates/mode.test<br>sql/function/numeric/test_unary.test<br>sql/function/operator/test_date_arithmetic.test<br>sql/function/timestamp/make_date.test | `statement ok<br>CREATE TABLE dates(d DATE)` |  |
| `datesub` | `unsupported feature` | 235 | sql/function/timestamp/test_icu_datesub.test | `query I<br>SELECT DATESUB('day', 'infinity'::TIMESTAMPTZ, 'infinity'::TIMESTAMPTZ);<br>----<br>NULL` |  |
| `datetrunc` | `unsupported feature` | 1 | sql/function/timestamp/test_icu_datetrunc.test | `query II<br>SELECT datetrunc(s, d), s FROM timestamps;<br>----<br>-infinity	year<br>1000-01-01 00:00:00-07:52	millennium<br>1900-01-01 00:00:00-08	century<br>1990-01-01 00:00:00-08	decade<br>1992-01-01 00:00:00-08	year<br>1992-01-01 00:00:00-08	quarter<br>1992-02-01 00:00:00-08	month<br>1992-01-27 00:00:00-08	week<br>1992-02-02 00:00:00-08	day<br>1992-02-02 02:00:00-08	hour<br>1992-02-02 02:02:00-08	minute<br>1992-02-02 02:02:03-08	second<br>1992-02-02 02:02:03.123-08	milliseconds<br>1992-02-02 02:02:03.123456-08	microseconds<br>infinity	month` |  |
| `day` | `unsupported feature` | 3 | sql/function/time/test_date_part.test<br>sql/function/timestamp/test_icu_datepart.test<br>sql/function/timetz/test_date_part.test | `statement error<br>SELECT day(i) FROM times` |  |
| `dayname` | `unsupported feature` | 1 | sql/function/timestamp/test_icu_datepart.test | `query II<br>SELECT dayname(ts), monthname(ts) from timestamps;<br>----<br>Wednesday	March<br>Tuesday	July<br>Thursday	December<br>Monday	February<br>Friday	November<br>Monday	November<br>Monday	November<br>Friday	December<br>NULL	NULL<br>NULL	NULL<br>NULL	NULL` |  |
| `dayofmonth` | `unsupported feature` | 1 | sql/function/timestamp/test_icu_datepart.test | `query II<br>SELECT dayofmonth(ts), dayofmonth(ts::TIMESTAMP) FROM timestamps;<br>----<br>13	13<br>31	31<br>31	31<br>1	1<br>26	26<br>15	15<br>15	15<br>24	24<br>NULL	NULL<br>NULL	NULL<br>NULL	NULL` |  |
| `dayofweek` | `unsupported feature` | 4 | sql/function/interval/test_date_part.test<br>sql/function/time/test_date_part.test<br>sql/function/timestamp/test_icu_datepart.test<br>sql/function/timetz/test_date_part.test | `statement error<br>SELECT dayofweek(i) FROM times` |  |
| `dayofyear` | `unsupported feature` | 4 | sql/function/interval/test_date_part.test<br>sql/function/time/test_date_part.test<br>sql/function/timestamp/test_icu_datepart.test<br>sql/function/timetz/test_date_part.test | `statement error<br>SELECT dayofyear(i) FROM times` |  |
| `decade` | `unsupported feature` | 3 | sql/function/time/test_date_part.test<br>sql/function/timestamp/test_icu_datepart.test<br>sql/function/timetz/test_date_part.test | `statement error<br>SELECT decade(i) FROM times` |  |
| `decimals` | `unsupported feature` | 1 | sql/function/list/aggregates/bigints_sum_avg.test | `statement ok<br>CREATE TABLE decimals (i DECIMAL(18,1)[]);` |  |
| `def` | `unsupported feature` | 1 | sql/function/list/list_resize.test | `statement ok<br>CREATE TABLE def(tbl INT[], b INT, d INT);` |  |
| `degrees` | `unsupported feature` | 2 | sql/function/numeric/test_invalid_math.test<br>sql/function/numeric/test_pg_math.test | `query R<br>SELECT DEGREES(1e308)<br>----<br>inf` |  |
| `demo` | `unsupported feature` | 12 | sql/function/list/lambdas/arrow/lambdas_and_functions_deprecated.test<br>sql/function/list/lambdas/lambdas_and_functions.test | `query I<br>FROM demo(3, 0);<br>----<br>[0, 0, 0]` |  |
| `df` | `unsupported feature` | 4 | sql/function/list/lambdas/arrow/reduce_deprecated.test<br>sql/function/list/lambdas/arrow/reduce_initial_deprecated.test<br>sql/function/list/lambdas/reduce.test<br>sql/function/list/lambdas/reduce_initial.test | `statement ok<br>CREATE OR REPLACE TABLE df(s STRUCT(a INT, b INT)[]);` |  |
| `double_values` | `unsupported feature` | 1 | sql/function/list/aggregates/minmax_nested.test | `statement ok<br>CREATE TABLE double_values(d DOUBLE);` |  |
| `doubles` | `unsupported feature` | 4 | sql/function/list/aggregates/avg.test<br>sql/function/list/aggregates/sum.test | `statement ok<br>CREATE TABLE doubles(n DOUBLE[]);` |  |
| `dummy_tbl` | `unsupported feature` | 1 | sql/function/list/lambdas/incorrect.test | `statement ok<br>CREATE TABLE dummy_tbl (y INT);` |  |
| `editdist3` | `unsupported feature` | 2 | sql/function/string/test_levenshtein.test | `query I<br>SELECT editdist3('hallo', 'hello')<br>----<br>1` |  |
| `empty_lists` | `unsupported feature` | 2 | sql/function/list/lambdas/arrow/filter_deprecated.test<br>sql/function/list/lambdas/filter.test | `statement ok<br>CREATE TABLE empty_lists (l integer[])` |  |
| `entr` | `unsupported feature` | 1 | sql/function/list/aggregates/entropy.test | `statement ok<br>CREATE TABLE entr (l INTEGER[]);` |  |
| `epoch_ms` | `unsupported feature` | 3 | sql/function/interval/test_date_part.test<br>sql/function/time/test_date_part.test<br>sql/function/timetz/test_date_part.test | `query II<br>SELECT d, epoch_ms(d) FROM times ORDER BY ALL;<br>----<br>00:01:20	80000<br>20:08:10.001	72490001<br>20:08:10.33	72490330<br>20:08:10.998	72490998` |  |
| `epoch_ns` | `unsupported feature` | 3 | sql/function/interval/test_date_part.test<br>sql/function/time/test_date_part.test<br>sql/function/timetz/test_date_part.test | `query II<br>SELECT d, epoch_ns(d) FROM times ORDER BY ALL;<br>----<br>00:01:20	80000000000<br>20:08:10.001	72490001000000<br>20:08:10.33	72490330000000<br>20:08:10.998	72490998000000` |  |
| `epoch_us` | `unsupported feature` | 3 | sql/function/interval/test_date_part.test<br>sql/function/time/test_date_part.test<br>sql/function/timetz/test_date_part.test | `query II<br>SELECT d, epoch_us(d) FROM times ORDER BY ALL;<br>----<br>00:01:20	80000000<br>20:08:10.001	72490001000<br>20:08:10.33	72490330000<br>20:08:10.998	72490998000` |  |
| `era` | `unsupported feature` | 6 | sql/function/interval/test_date_part.test<br>sql/function/time/test_date_part.test<br>sql/function/timestamp/test_icu_datepart.test<br>sql/function/timestamp/test_icu_makedate.test<br>sql/function/timetz/test_date_part.test | `statement error<br>SELECT era(i) FROM times` |  |
| `even` | `unsupported feature` | 8 | sql/function/numeric/test_even.test | `query I<br>SELECT even(NULL)<br>----<br>NULL` |  |
| `exp` | `unsupported feature` | 2 | sql/function/numeric/test_invalid_math.test<br>sql/function/numeric/test_pg_math.test | `query R<br>select exp(1.0)<br>----<br>2.718281828459045` |  |
| `fdiv` | `unsupported feature` | 6 | sql/function/numeric/test_fdiv_fmod.test | `query I<br>SELECT fdiv(3, 2.1)<br>----<br>1` |  |
| `float_values` | `unsupported feature` | 1 | sql/function/list/aggregates/minmax_nested.test | `statement ok<br>CREATE TABLE float_values(f FLOAT);` |  |
| `floor` | `unsupported feature` | 7 | sql/function/numeric/test_floor_ceil.test<br>sql/function/numeric/test_pg_math.test | `query I<br>select floor(-42.8)<br>----<br>-43` |  |
| `fmod` | `unsupported feature` | 4 | sql/function/numeric/test_fdiv_fmod.test | `query I<br>SELECT fmod(0, 0)<br>----<br>NULL` |  |
| `format` | `unsupported feature` | 14 | sql/function/string/test_format_extensions.test | `statement error<br>select format('{1}', 123456789)` |  |
| `formats` | `unsupported feature` | 1 | sql/function/timestamp/test_icu_strftime.test | `statement ok<br>CREATE TABLE formats (f VARCHAR);` |  |
| `functions` | `unsupported feature` | 1 | sql/function/list/list_contains.test | `statement ok<br>CREATE TABLE functions (function_name varchar, function_type varchar, parameter_types varchar[]);` |  |
| `gamma` | `unsupported feature` | 9 | sql/function/numeric/test_gamma.test | `query I<br>SELECT gamma(1)<br>----<br>1` |  |
| `gen_random_uuid` | `unsupported feature` | 4 | sql/function/uuid/test_uuid.test | `statement ok<br>SELECT * FROM uuids ORDER BY gen_random_uuid();` |  |
| `generate_subscripts` | `unsupported feature` | 3 | sql/function/list/generate_subscripts.test | `query I<br>SELECT generate_subscripts([], 1)<br>----` |  |
| `geomean` | `unsupported feature` | 2 | sql/function/numeric/test_geomean.test | `query I<br>SELECT geomean(x::integer) FROM numbers<br>----<br>1.414213562373095` |  |
| `greatest` | `unsupported feature` | 4 | sql/function/generic/least_greatest_enum.test<br>sql/function/generic/least_greatest_types.test<br>sql/function/generic/test_least_greatest.test | `query II<br>SELECT greatest('x'::t, 'z'::t), 'x'::t > 'z'::t;<br>----<br>x	1` |  |
| `hist_data` | `unsupported feature` | 1 | sql/function/list/aggregates/histogram.test | `statement ok<br>CREATE TABLE hist_data (g INTEGER[])` |  |
| `hour` | `unsupported feature` | 1 | sql/function/timestamp/test_icu_datepart.test | `query II<br>SELECT hour(ts), hour(ts::TIMESTAMP) FROM timestamps;<br>----<br>1	1<br>5	5<br>16	16<br>16	16<br>2	2<br>2	2<br>1	1<br>14	14<br>NULL	NULL<br>NULL	NULL<br>NULL	NULL` |  |
| `incorrect_test` | `unsupported feature` | 1 | sql/function/list/lambdas/incorrect.test | `statement ok<br>CREATE TABLE incorrect_test (i INTEGER);` |  |
| `index_key` | `unsupported feature` | 45 | sql/function/index/index_key.test | `statement error<br>SELECT index_key({'table': 'tbl1'});` |  |
| `instr` | `unsupported feature` | 8 | sql/function/string/null_byte.test<br>sql/function/string/test_instr_utf8.test | `query I<br>SELECT instr(v, chr(0)) FROM null_byte<br>----<br>4` |  |
| `integers2` | `unsupported feature` | 1 | sql/function/list/list_zip.test | `statement ok<br>CREATE TABLE integers2 (j int[])` |  |
| `interval_muldiv_tbl` | `unsupported feature` | 1 | sql/function/interval/test_interval_muldiv.test | `statement ok<br>CREATE TABLE INTERVAL_MULDIV_TBL (span interval);` |  |
| `intervals` | `unsupported feature` | 4 | sql/function/interval/test_date_part.test<br>sql/function/interval/test_extract.test<br>sql/function/interval/test_interval_trunc.test<br>sql/function/list/aggregates/mode.test | `statement ok<br>CREATE TABLE intervals(i INTERVAL);` |  |
| `isodow` | `unsupported feature` | 4 | sql/function/interval/test_date_part.test<br>sql/function/time/test_date_part.test<br>sql/function/timestamp/test_icu_datepart.test<br>sql/function/timetz/test_date_part.test | `statement error<br>SELECT isodow(i) FROM times` |  |
| `isoyear` | `unsupported feature` | 1 | sql/function/timestamp/test_icu_datepart.test | `query II<br>SELECT isoyear(ts), isoyear(ts::TIMESTAMP) FROM timestamps;<br>----<br>-43	-43<br>1962	1962<br>2020	2020<br>2021	2021<br>2021	2021<br>2021	2021<br>2021	2021<br>2021	2021<br>NULL	NULL<br>NULL	NULL<br>NULL	NULL` |  |
| `issue3588` | `unsupported feature` | 1 | sql/function/generic/test_between.test | `statement ok<br>CREATE TABLE issue3588(c0 INT);` |  |
| `issue9673` | `unsupported feature` | 1 | sql/function/timestamp/test_icu_datediff.test | `statement ok<br>CREATE TABLE issue9673(starttime TIMESTAMPTZ, recordtime TIMESTAMPTZ);` |  |
| `julian` | `unsupported feature` | 2 | sql/function/interval/test_date_part.test<br>sql/function/timestamp/test_icu_datepart.test | `statement error<br>SELECT julian(i) FROM intervals;` |  |
| `l_filter_test` | `unsupported feature` | 3 | sql/function/list/lambdas/arrow/lambda_scope_deprecated.test<br>sql/function/list/lambdas/incorrect.test<br>sql/function/list/lambdas/lambda_scope.test | `statement ok<br>CREATE TABLE l_filter_test (l integer[]);` |  |
| `l_test` | `unsupported feature` | 2 | sql/function/list/lambdas/arrow/lambda_scope_deprecated.test<br>sql/function/list/lambdas/lambda_scope.test | `statement ok<br>CREATE TABLE l_test (l integer[]);` |  |
| `lambda_check` | `unsupported feature` | 4 | sql/function/list/lambdas/incorrect.test | `statement error<br>CREATE TABLE lambda_check (i BIGINT[],<br>    CHECK ([x + 1 FOR x IN i IF x > 0] = []));` |  |
| `lambda_macro` | `unsupported feature` | 1 | sql/function/list/lambdas/test_lambda_storage.test | `query I<br>SELECT lambda_macro(1, 2);<br>----<br>[3, 5]` |  |
| `large_lists` | `unsupported feature` | 1 | sql/function/list/list_intersect.test | `statement ok<br>create table large_lists(l1 int[], l2 int[]);` |  |
| `last_day` | `unsupported feature` | 2 | sql/function/timestamp/test_icu_datepart.test | `query TTT<br>SELECT ts, LAST_DAY(ts), LAST_DAY(ts::TIMESTAMP) FROM februaries;<br>----<br>1900-02-12 00:00:00-08	1900-02-28	1900-02-28<br>1992-02-12 00:00:00-08	1992-02-29	1992-02-29<br>2000-02-12 00:00:00-08	2000-02-29	2000-02-29` |  |
| `lcm` | `unsupported feature` | 5 | sql/function/numeric/test_gcd_lcm.test | `query I<br>select lcm(120,25);<br>----<br>600` |  |
| `length_grapheme` | `unsupported feature` | 3 | sql/function/string/null_byte.test<br>sql/function/string/test_complex_unicode.test | `query I<br>SELECT length_grapheme('🤦🏼‍♂️')<br>----<br>1` |  |
| `levenshtein` | `unsupported feature` | 35 | sql/function/string/test_levenshtein.test | `query I<br>SELECT levenshtein('', '')<br>----<br>0` |  |
| `lgamma` | `unsupported feature` | 11 | sql/function/numeric/test_gamma.test | `query I<br>SELECT lgamma(1)<br>----<br>0` |  |
| `list_aggregate` | `unsupported feature` | 1 | sql/function/list/aggregates/nested.test | `query I<br>SELECT list_aggregate([1, 2], 'min')<br>----<br>1` |  |
| `list_any_value` | `unsupported feature` | 14 | sql/function/list/aggregates/any_value.test | `statement error<br>SELECT list_any_value()` |  |
| `list_append` | `unsupported feature` | 4 | sql/function/list/list_concat.test | `query T<br>SELECT list_append(NULL, 3)<br>----<br>[3]` |  |
| `list_apply` | `unsupported feature` | 24 | sql/function/list/lambdas/arrow/filter_deprecated.test<br>sql/function/list/lambdas/arrow/lambda_scope_deprecated.test<br>sql/function/list/lambdas/arrow/rhs_parameters_deprecated.test<br>sql/function/list/lambdas/arrow/transform_deprecated.test<br>sql/function/list/lambdas/filter.test<br>sql/function/list/lambdas/incorrect.test<br>sql/function/list/lambdas/lambda_scope.test<br>sql/function/list/lambdas/rhs_parameters.test<br>sql/function/list/lambdas/transform.test | `query I<br>SELECT list_apply(['hello'], lambda x: x) FROM t1;<br>----<br>[hello]` |  |
| `list_approx_count_distinct` | `unsupported feature` | 5 | sql/function/list/aggregates/approx_count_distinct.test | `query I<br>select list_approx_count_distinct([]) from list_ints;<br>----<br>0` |  |
| `list_avg` | `unsupported feature` | 12 | sql/function/list/aggregates/avg.test<br>sql/function/list/aggregates/bigints_sum_avg.test | `statement error<br>SELECT list_avg()` |  |
| `list_bit_and` | `unsupported feature` | 5 | sql/function/list/aggregates/bit_and.test | `statement error<br>SELECT list_bit_and()` |  |
| `list_bit_or` | `unsupported feature` | 5 | sql/function/list/aggregates/bit_or.test | `statement error<br>SELECT list_bit_or()` |  |
| `list_bit_xor` | `unsupported feature` | 5 | sql/function/list/aggregates/bit_xor.test | `statement error<br>SELECT list_bit_xor()` |  |
| `list_bool_and` | `unsupported feature` | 2 | sql/function/list/aggregates/bool_and_or.test | `statement error<br>select list_bool_and()` |  |
| `list_bool_or` | `unsupported feature` | 2 | sql/function/list/aggregates/bool_and_or.test | `statement error<br>select list_bool_or()` |  |
| `list_concat` | `unsupported feature` | 103 | sql/function/array/array_list_functions.test<br>sql/function/list/aggregates/nested.test<br>sql/function/list/lambdas/arrow/filter_deprecated.test<br>sql/function/list/lambdas/filter.test<br>sql/function/list/list_concat.test<br>sql/function/string/test_concat_binding.test | `query T<br>SELECT list_concat([], [])<br>----<br>[]` |  |
| `list_contains` | `unsupported feature` | 81 | sql/function/array/array_list_functions.test<br>sql/function/list/list_contains.test<br>sql/function/list/list_position.test<br>sql/function/list/list_position_nan.test<br>sql/function/list/list_value_arrays.test | `statement error<br>SELECT list_contains(1)` |  |
| `list_cosine_distance` | `unsupported feature` | 2 | sql/function/list/list_cosine_similarity.test | `query I<br>SELECT list_cosine_distance([1, 2, 3]::FLOAT[], [1, 2, 3]::FLOAT[]) = 1 - list_cosine_similarity([1, 2, 3]::FLOAT[], [1, 2, 3]::FLOAT[]);<br>----<br>1` |  |
| `list_cosine_similarity` | `unsupported feature` | 12 | sql/function/list/list_cosine_similarity.test | `query I<br>SELECT list_cosine_similarity([], []);<br>----<br>NULL` |  |
| `list_count` | `unsupported feature` | 16 | sql/function/list/aggregates/approx_count_distinct.test<br>sql/function/list/aggregates/count.test<br>sql/function/list/aggregates/string_agg.test<br>sql/function/list/lambdas/arrow/filter_deprecated.test<br>sql/function/list/lambdas/arrow/transform_deprecated.test<br>sql/function/list/lambdas/filter.test<br>sql/function/list/lambdas/transform.test | `statement error<br>select list_count()` |  |
| `list_data` | `unsupported feature` | 2 | sql/function/list/list_has_any_and_has_all.test<br>sql/function/list/list_intersect.test | `statement ok<br>CREATE TABLE list_data(l1 int[], l2 int[]);` |  |
| `list_distance` | `unsupported feature` | 12 | sql/function/list/list_distance.test | `query I<br>SELECT list_distance([], []);<br>----<br>0.0` |  |
| `list_distinct` | `unsupported feature` | 60 | sql/function/list/list_distinct.test | `statement error<br>SELECT list_distinct()` |  |
| `list_entropy` | `unsupported feature` | 5 | sql/function/list/aggregates/entropy.test | `statement error<br>select list_entropy()` |  |
| `list_extract` | `unsupported feature` | 3 | sql/function/array/array_list_functions.test<br>sql/function/string/test_array_extract.test | `query I<br>SELECT list_extract('1', -1);<br>----<br>1` |  |
| `list_filter` | `unsupported feature` | 91 | sql/function/array/array_list_functions.test<br>sql/function/list/lambdas/arrow/expression_iterator_cases_deprecated.test<br>sql/function/list/lambdas/arrow/filter_deprecated.test<br>sql/function/list/lambdas/arrow/lambda_scope_deprecated.test<br>sql/function/list/lambdas/arrow/lambdas_and_group_by_deprecated.test<br>sql/function/list/lambdas/arrow/lambdas_and_macros_deprecated.test<br>sql/function/list/lambdas/arrow/list_comprehension_deprecated.test<br>sql/function/list/lambdas/arrow/table_functions_deprecated.test<br>sql/function/list/lambdas/arrow/transform_with_index_deprecated.test<br>sql/function/list/lambdas/expression_iterator_cases.test<br>sql/function/list/lambdas/filter.test<br>sql/function/list/lambdas/incorrect.test<br>sql/function/list/lambdas/lambda_scope.test<br>sql/function/list/lambdas/lambdas_and_group_by.test<br>sql/function/list/lambdas/lambdas_and_macros.test<br>sql/function/list/lambdas/list_comprehension.test<br>sql/function/list/lambdas/table_functions.test<br>sql/function/list/lambdas/transform_with_index.test<br>sql/function/list/list_contains.test | `statement error<br>SELECT list_filter();` |  |
| `list_first` | `unsupported feature` | 16 | sql/function/list/aggregates/first.test<br>sql/function/list/aggregates/hugeint.test<br>sql/function/list/aggregates/uhugeint.test | `statement error<br>SELECT list_first()` |  |
| `list_has` | `unsupported feature` | 1 | sql/function/list/list_contains.test | `query I<br>SELECT function_name FROM functions<br>WHERE function_type = 'scalar' AND list_has(parameter_types, 'TIMESTAMP');<br>----<br>last_day<br>scalar_part` |  |
| `list_has_all` | `unsupported feature` | 20 | sql/function/list/lambdas/arrow/lambda_scope_deprecated.test<br>sql/function/list/lambdas/lambda_scope.test<br>sql/function/list/list_has_any_and_has_all.test | `statement error<br>select list_has_all([1, 2], 1);` |  |
| `list_has_any` | `unsupported feature` | 16 | sql/function/list/lambdas/arrow/lambda_scope_deprecated.test<br>sql/function/list/lambdas/lambda_scope.test<br>sql/function/list/list_has_any_and_has_all.test | `query I<br>select list_has_any(l1, l2) from tbl;<br>----<br>1<br>1` |  |
| `list_histogram` | `unsupported feature` | 13 | sql/function/list/aggregates/histogram.test | `statement error<br>select list_histogram()` |  |
| `list_inner_product` | `unsupported feature` | 10 | sql/function/list/list_inner_product.test | `query I<br>SELECT list_inner_product([1, 1, 1]::FLOAT[], [1, 1, 1]::FLOAT[]);<br>----<br>3.0` |  |
| `list_intersect` | `unsupported feature` | 15 | sql/function/list/lambdas/arrow/lambda_scope_deprecated.test<br>sql/function/list/lambdas/lambda_scope.test<br>sql/function/list/list_intersect.test | `statement ok<br>prepare q1 as select list_intersect(?, ?);` |  |
| `list_ints` | `unsupported feature` | 1 | sql/function/list/aggregates/approx_count_distinct.test | `statement ok<br>CREATE TABLE list_ints (l INTEGER[]);` |  |
| `list_ints_2` | `unsupported feature` | 1 | sql/function/list/aggregates/approx_count_distinct.test | `statement ok<br>CREATE TABLE list_ints_2 (a INTEGER[], b INTEGER[]);` |  |
| `list_kurtosis` | `unsupported feature` | 4 | sql/function/list/aggregates/kurtosis.test | `statement error<br>select list_kurtosis()` |  |
| `list_kurtosis_pop` | `unsupported feature` | 1 | sql/function/list/aggregates/kurtosis.test | `query I<br>select list_kurtosis_pop(k) from aggr;<br>----<br>6.100000<br>-1.676857<br>-1.358688<br>NULL<br>NULL<br>NULL` |  |
| `list_last` | `unsupported feature` | 15 | sql/function/list/aggregates/last.test<br>sql/function/list/aggregates/nested.test | `statement error<br>SELECT list_last()` |  |
| `list_mad` | `unsupported feature` | 41 | sql/function/list/aggregates/mad.test | `statement ok<br>SELECT list_mad([NULL::date])` |  |
| `list_max` | `unsupported feature` | 14 | sql/function/list/aggregates/max.test | `statement error<br>SELECT list_max()` |  |
| `list_median` | `unsupported feature` | 9 | sql/function/list/aggregates/median.test | `query I<br>SELECT list_median(r) FROM quantile<br>----<br>4999.5` |  |
| `list_min` | `unsupported feature` | 17 | sql/function/list/aggregates/hugeint.test<br>sql/function/list/aggregates/min.test<br>sql/function/list/aggregates/nested.test<br>sql/function/list/aggregates/uhugeint.test | `statement error<br>SELECT list_min()` |  |
| `list_mode` | `unsupported feature` | 9 | sql/function/list/aggregates/mode.test | `statement error<br>select list_mode()` |  |
| `list_negative_inner_product` | `unsupported feature` | 2 | sql/function/list/list_inner_product.test | `query I<br>SELECT list_negative_inner_product([1,2,3]::FLOAT[], [1,2,3]::FLOAT[]) = -list_inner_product([1,2,3]::FLOAT[], [1,2,3]::FLOAT[]);<br>----<br>1` |  |
| `list_of_list` | `unsupported feature` | 3 | sql/function/list/list_contains.test<br>sql/function/list/list_has_any_and_has_all.test<br>sql/function/list/list_intersect.test | `statement ok<br>CREATE TABLE list_of_list(l1 int[][], l2 int[][]);` |  |
| `list_of_strings` | `unsupported feature` | 2 | sql/function/list/list_has_any_and_has_all.test<br>sql/function/list/list_intersect.test | `statement ok<br>create table list_of_strings(l1 string[], l2 string[]);` |  |
| `list_position` | `unsupported feature` | 90 | sql/function/array/array_list_functions.test<br>sql/function/list/list_position.test<br>sql/function/list/list_position_nan.test | `statement error<br>SELECT list_position(1)` |  |
| `list_prepend` | `unsupported feature` | 1 | sql/function/list/list_concat.test | `query T<br>SELECT list_prepend(1, [2, 3])<br>----<br>[1, 2, 3]` |  |
| `list_product` | `unsupported feature` | 4 | sql/function/list/aggregates/product.test | `statement error<br>select list_product()` |  |
| `list_resize` | `unsupported feature` | 41 | sql/function/array/array_list_functions.test<br>sql/function/list/list_resize.test<br>sql/function/list/list_resize_error.test | `statement ok<br>SELECT list_resize([1,2,3], NULL);` |  |
| `list_reverse` | `unsupported feature` | 26 | sql/function/list/list_reverse.test | `statement error<br>SELECT list_reverse()` |  |
| `list_sem` | `unsupported feature` | 3 | sql/function/list/aggregates/sem.test | `statement error<br>select list_sem()` |  |
| `list_skewness` | `unsupported feature` | 6 | sql/function/list/aggregates/skewness.test | `statement error<br>select list_skewness()` |  |
| `list_slice` | `unsupported feature` | 4 | sql/function/array/array_list_functions.test<br>sql/function/string/test_string_array_slice.test | `query T<br>SELECT list_slice(s, 0, 2) FROM strings<br>----<br>he<br>wo<br>b<br>NULL` |  |
| `list_sort` | `unsupported feature` | 33 | sql/function/array/array_list_functions.test<br>sql/function/list/flatten.test<br>sql/function/list/lambdas/arrow/transform_deprecated.test<br>sql/function/list/lambdas/transform.test<br>sql/function/list/list_distinct.test<br>sql/function/list/list_intersect.test<br>sql/function/list/list_reverse.test<br>sql/function/list/list_sort_vector_types.test | `query I<br>select list_sort(${f}([1,2,3], [2,3,4]));<br>----<br>[2, 3]` |  |
| `list_stddev_pop` | `unsupported feature` | 1 | sql/function/list/aggregates/var_stddev.test | `statement error<br>SELECT list_stddev_pop(c0) FROM t0;` |  |
| `list_stddev_samp` | `unsupported feature` | 2 | sql/function/list/aggregates/var_stddev.test | `statement error<br>SELECT list_stddev_samp()` |  |
| `list_string_agg` | `unsupported feature` | 8 | sql/function/list/aggregates/string_agg.test | `statement error<br>SELECT list_string_agg()` |  |
| `list_sum` | `unsupported feature` | 14 | sql/function/list/aggregates/bigints_sum_avg.test<br>sql/function/list/aggregates/sum.test<br>sql/function/list/aggregates/var_stddev.test<br>sql/function/list/lambdas/arrow/transform_deprecated.test<br>sql/function/list/lambdas/transform.test | `query I<br>SELECT list_sum(i) FROM bigints;<br>----<br>6` |  |
| `list_tbl` | `unsupported feature` | 1 | sql/function/list/list_resize.test | `statement ok<br>create table list_tbl(a int[][], b int);` |  |
| `list_unique` | `unsupported feature` | 69 | sql/function/list/list_distinct.test<br>sql/function/list/list_unique.test | `statement error<br>SELECT list_unique()` |  |
| `list_value` | `unsupported feature` | 43 | sql/function/list/lambdas/incorrect.test<br>sql/function/list/list_value_arrays.test<br>sql/function/list/list_value_nested_lists.test<br>sql/function/list/list_value_structs.test | `statement error<br>SELECT list_value(a, b, c) FROM mixed_array_table;` |  |
| `list_var_pop` | `unsupported feature` | 1 | sql/function/list/aggregates/var_stddev.test | `statement error<br>select list_var_pop([1e301, -1e301])` |  |
| `list_var_samp` | `unsupported feature` | 2 | sql/function/list/aggregates/var_stddev.test | `query I<br>SELECT list_var_samp([1])<br>----<br>NULL` |  |
| `list_where` | `unsupported feature` | 67 | sql/function/list/list_where.test | `statement ok<br>SELECT list_where([1::float], [true])` |  |
| `list_zip` | `unsupported feature` | 84 | sql/function/list/list_zip.test | `statement error<br>SELECT list_zip('')` |  |
| `ln` | `unsupported feature` | 3 | sql/function/numeric/test_invalid_math.test<br>sql/function/numeric/test_pg_math.test | `statement error<br>SELECT ln(0);` |  |
| `log` | `unsupported feature` | 17 | sql/function/numeric/test_invalid_math.test<br>sql/function/numeric/test_pg_math.test | `statement error<br>SELECT log(-1);` |  |
| `log10` | `unsupported feature` | 3 | sql/function/numeric/test_invalid_math.test<br>sql/function/numeric/test_pg_math.test | `statement error<br>SELECT log10(0);` |  |
| `log2` | `unsupported feature` | 1 | sql/function/numeric/test_pg_math.test | `query R<br>select log2(4.0)<br>----<br>2.0` |  |
| `macro_with_lambda` | `unsupported feature` | 2 | sql/function/list/lambdas/arrow/lambdas_and_macros_deprecated.test<br>sql/function/list/lambdas/lambdas_and_macros.test | `statement ok<br>CREATE MACRO macro_with_lambda(list, num) AS (list_transform(list, x -> x + num))` |  |
| `make_date` | `unsupported feature` | 7 | sql/function/timestamp/make_date.test | `query I<br>SELECT *<br>FROM dates<br>WHERE d <> make_date((d - date '1970-01-01')::INT)<br>----` |  |
| `make_time` | `unsupported feature` | 5 | sql/function/timestamp/make_date.test | `query I<br>SELECT md<br>FROM (SELECT make_time(hour(d), minute(d), NULL) md FROM dates) t<br>WHERE md IS NOT NULL<br>----` |  |
| `make_timestamp` | `unsupported feature` | 11 | sql/function/timestamp/make_date.test | `statement error<br>SELECT make_timestamp(294247, 1, 10, 4, 0, 54.775807);` |  |
| `make_timestamp_ms` | `unsupported feature` | 1 | sql/function/timestamp/epoch.test | `query TTTTTT<br>SELECT <br>	make_timestamp_ms(0) as epoch1, <br>	make_timestamp_ms(1574802684123) as epoch2, <br>	make_timestamp_ms(-291044928000) as epoch3, <br>	make_timestamp_ms(-291081600000) as epoch4,  <br>	make_timestamp_ms(-291081600001) as epoch5, <br>	make_timestamp_ms(-290995201000) as epoch6<br>----<br>1970-01-01 00:00:00<br>2019-11-26 21:11:24.123<br>1960-10-11 10:11:12<br>1960-10-11 00:00:00<br>1960-10-10 23:59:59.999<br>1960-10-11 23:59:59` |  |
| `make_timestamp_ns` | `unsupported feature` | 3 | sql/function/timestamp/make_date.test | `statement error<br>SELECT make_timestamp_ns(9223372036854775807); -- Infinity` |  |
| `make_timestamptz` | `unsupported feature` | 20 | sql/function/timestamp/test_icu_makedate.test | `statement error<br>SELECT make_timestamptz(294248, 1, 10, 4, 0, 54.775807);` |  |
| `map_tbl` | `unsupported feature` | 1 | sql/function/list/lambdas/incorrect.test | `statement ok<br>CREATE TABLE map_tbl(m MAP(INTEGER, INTEGER));` |  |
| `microsecond` | `unsupported feature` | 1 | sql/function/timestamp/test_icu_datepart.test | `query II<br>SELECT microsecond(ts), microsecond(ts::TIMESTAMP) FROM timestamps;<br>----<br>43987654	43987654<br>48123456	48123456<br>0	0<br>0	0<br>13123456	13123456<br>0	0<br>0	0<br>0	0<br>NULL	NULL<br>NULL	NULL<br>NULL	NULL` |  |
| `millennium` | `unsupported feature` | 3 | sql/function/time/test_date_part.test<br>sql/function/timestamp/test_icu_datepart.test<br>sql/function/timetz/test_date_part.test | `statement error<br>SELECT millennium(i) FROM times` |  |
| `millisecond` | `unsupported feature` | 1 | sql/function/timestamp/test_icu_datepart.test | `query II<br>SELECT millisecond(ts), millisecond(ts::TIMESTAMP) FROM timestamps;<br>----<br>43987	43987<br>48123	48123<br>0	0<br>0	0<br>13123	13123<br>0	0<br>0	0<br>0	0<br>NULL	NULL<br>NULL	NULL<br>NULL	NULL` |  |
| `minima` | `unsupported feature` | 1 | sql/function/numeric/test_unary.test | `statement ok<br>CREATE TABLE minima (t TINYINT, s SMALLINT, i INTEGER, b BIGINT);` |  |
| `minute` | `unsupported feature` | 1 | sql/function/timestamp/test_icu_datepart.test | `query II<br>SELECT minute(ts), minute(ts::TIMESTAMP) FROM timestamps;<br>----<br>40	40<br>20	20<br>0	0<br>0	0<br>15	15<br>30	30<br>30	30<br>0	0<br>NULL	NULL<br>NULL	NULL<br>NULL	NULL` |  |
| `mismatches` | `unsupported feature` | 20 | sql/function/string/test_mismatches.test | `statement error<br>SELECT mismatches('', '')` |  |
| `mixed_array_table` | `unsupported feature` | 1 | sql/function/list/list_value_arrays.test | `statement ok<br>CREATE TABLE mixed_array_table (a INTEGER[2], b VARCHAR[2], c DOUBLE[2]);` |  |
| `mixed_structs` | `unsupported feature` | 1 | sql/function/list/list_value_structs.test | `statement ok<br>CREATE TABLE mixed_structs (s struct(a int[], b varchar, c int, d varchar[], e struct(a int, b varchar)));` |  |
| `month` | `unsupported feature` | 3 | sql/function/time/test_date_part.test<br>sql/function/timestamp/test_icu_datepart.test<br>sql/function/timetz/test_date_part.test | `statement error<br>SELECT month(i) FROM times` |  |
| `multiples` | `unsupported feature` | 1 | sql/function/timestamp/test_icu_strptime.test | `statement ok<br>CREATE TABLE multiples (s VARCHAR, f VARCHAR);` |  |
| `my_filter` | `unsupported feature` | 6 | sql/function/list/lambdas/arrow/storage_deprecated.test<br>sql/function/list/lambdas/storage.test | `query I<br>SELECT my_filter([41, 42, NULL, 43, 44])<br>----<br>[43, 44]` |  |
| `my_nested_lambdas` | `unsupported feature` | 6 | sql/function/list/lambdas/arrow/storage_deprecated.test<br>sql/function/list/lambdas/storage.test | `query I<br>SELECT my_nested_lambdas([[40, NULL], [20, 21], [10, 10, 20]])<br>----<br>[[20, 21], [10, 10, 20]]` |  |
| `my_reduce` | `unsupported feature` | 6 | sql/function/list/lambdas/arrow/storage_deprecated.test<br>sql/function/list/lambdas/storage.test | `query I<br>SELECT my_reduce([1, 2, 3])<br>----<br>6` |  |
| `my_transform` | `unsupported feature` | 6 | sql/function/list/lambdas/arrow/storage_deprecated.test<br>sql/function/list/lambdas/storage.test | `query I<br>SELECT my_transform([1, 2, 3])<br>----<br>[1, 4, 9]` |  |
| `my_window` | `unsupported feature` | 2 | sql/function/list/lambdas/arrow/expression_iterator_cases_deprecated.test<br>sql/function/list/lambdas/expression_iterator_cases.test | `statement ok<br>CREATE TABLE my_window (l integer[], g integer, o integer)` |  |
| `nanosecond` | `unsupported feature` | 3 | sql/function/interval/test_date_part.test<br>sql/function/time/test_date_part.test<br>sql/function/timetz/test_date_part.test | `query II<br>SELECT i, nanosecond(i) AS parts<br>FROM intervals<br>ORDER BY 1;<br>----<br>00:34:26.3434	26343400000<br>42 days	0<br>1 year 4 months	0<br>2 years	0` |  |
| `nested` | `unsupported feature` | 20 | sql/function/list/lambdas/arrow/reduce_deprecated.test<br>sql/function/list/lambdas/arrow/reduce_initial_deprecated.test<br>sql/function/list/lambdas/reduce.test<br>sql/function/list/lambdas/reduce_initial.test | `statement ok<br>CREATE TABLE nested (n integer[][])` |  |
| `nested_array_table` | `unsupported feature` | 1 | sql/function/list/list_value_arrays.test | `statement ok<br>CREATE TABLE nested_array_table (a INTEGER[2][2], b INTEGER[2][2], c INTEGER[2][2]);` |  |
| `nested_list` | `unsupported feature` | 1 | sql/function/list/lambdas/incorrect.test | `statement ok<br>CREATE TABLE nested_list(i INT[][], other INT[]);` |  |
| `nested_list_table` | `unsupported feature` | 1 | sql/function/list/list_value_nested_lists.test | `statement ok<br>CREATE TABLE nested_list_table(a INTEGER[][], b INTEGER[][], c INTEGER[][]);` |  |
| `nextafter` | `unsupported feature` | 13 | sql/function/numeric/test_nextafter.test | `statement error<br>select nextafter()` |  |
| `no_index_tbl` | `unsupported feature` | 1 | sql/function/index/index_key.test | `statement ok<br>CREATE TABLE no_index_tbl(id INTEGER, value VARCHAR);` |  |
| `now` | `unsupported feature` | 3 | sql/function/generic/test_between_sideeffects.test<br>sql/function/timestamp/current_time.test<br>sql/function/timestamp/test_now_prepared.test | `statement ok<br>PREPARE v1 AS INSERT INTO timestamps VALUES(NOW());` |  |
| `null_table` | `unsupported feature` | 1 | sql/function/list/list_position.test | `statement ok<br>CREATE TABLE NULL_TABLE (n int[], i int);` |  |
| `nulls` | `unsupported feature` | 1 | sql/function/list/list_resize.test | `statement ok<br>CREATE TABLE nulls(l INT[], b INT);` |  |
| `nulltable` | `unsupported feature` | 1 | sql/function/string/test_string_slice.test | `statement ok<br>CREATE TABLE nulltable(n VARCHAR);` |  |
| `numbers` | `unsupported feature` | 4 | sql/function/numeric/test_floor_ceil.test<br>sql/function/numeric/test_geomean.test<br>sql/function/numeric/test_random.test<br>sql/function/numeric/test_trigo.test | `statement ok<br>CREATE TABLE numbers(n DOUBLE)` |  |
| `o` | `unsupported feature` | 2 | sql/function/string/test_subscript.test | `query T<br>SELECT s[2] FROM strings<br>----<br>e<br>o<br>(empty)<br>NULL` |  |
| `or` | `unsupported feature` | 1 | sql/function/string/test_string_slice.test | `query T<br>SELECT s[2:3] FROM strings<br>----<br>el<br>or<br>(empty)<br>NULL` |  |
| `orl` | `unsupported feature` | 1 | sql/function/string/test_string_slice.test | `query T<br>SELECT s[-4:-2] FROM strings<br>----<br>ell<br>orl<br>(empty)<br>NULL` |  |
| `orld` | `unsupported feature` | 1 | sql/function/string/test_string_slice.test | `query T<br>SELECT s[2:] FROM strings<br>----<br>ello<br>orld<br>(empty)<br>NULL` |  |
| `other_reduce_macro` | `unsupported feature` | 4 | sql/function/list/lambdas/arrow/lambdas_and_macros_deprecated.test<br>sql/function/list/lambdas/lambdas_and_macros.test | `statement error<br>SELECT other_reduce_macro([1, 2, 3, 4], 5, 6);` |  |
| `palindromes` | `unsupported feature` | 1 | sql/function/list/list_reverse.test | `statement ok<br>CREATE TABLE palindromes (s VARCHAR);` |  |
| `params` | `unsupported feature` | 1 | sql/function/string/regex_extract_all_struct.test | `statement error<br>WITH params(name_list) AS (SELECT ['g1','g2'])<br>SELECT regexp_extract_all('abc', '(a)(b)', name_list) FROM params;` |  |
| `parts` | `unsupported feature` | 1 | sql/function/timestamp/test_icu_datepart.test | `statement error<br>WITH parts(p) AS (VALUES (['year', 'month', 'day']), (['hour', 'minute', 'microsecond']))<br>SELECT DATE_PART(p, ts) FROM parts, timestamps;<br>----` |  |
| `pi` | `unsupported feature` | 1 | sql/function/numeric/test_pg_math.test | `query R<br>select pi()<br>----<br>3.141592653589793` |  |
| `position` | `unsupported feature` | 1 | sql/function/string/test_instr_utf8.test | `query I<br>SELECT POSITION('á' in s) FROM strings<br>----<br>0<br>1` |  |
| `pow` | `unsupported feature` | 1 | sql/function/numeric/test_invalid_math.test | `query RRR<br>SELECT POW(1e300,100), POW(-1e300,100), POW(-1.0, 0.5)<br>----<br>inf	inf	nan` |  |
| `printf` | `unsupported feature` | 17 | sql/function/string/null_byte.test<br>sql/function/string/test_format_extensions.test | `statement error<br>select printf('%:', 'str')` |  |
| `q1` | `unsupported feature` | 14 | sql/function/list/list_intersect.test<br>sql/function/list/list_resize.test<br>sql/function/list/list_resize_error.test | `statement error<br>execute q1();` |  |
| `q2` | `unsupported feature` | 13 | sql/function/list/list_resize.test<br>sql/function/list/list_resize_error.test | `statement error<br>execute q2();` |  |
| `q3` | `unsupported feature` | 3 | sql/function/list/list_resize_error.test | `statement error<br>execute q3([1, 2, 3], 2, 3, 4);` |  |
| `q4` | `unsupported feature` | 3 | sql/function/list/list_resize_error.test | `statement error<br>execute q4([1, 2, 3], 2, 3, 4);` |  |
| `qualified_tbl` | `unsupported feature` | 2 | sql/function/list/lambdas/arrow/lambda_scope_deprecated.test<br>sql/function/list/lambdas/lambda_scope.test | `statement ok<br>CREATE TABLE qualified_tbl (x INTEGER[]);` |  |
| `quarter` | `unsupported feature` | 3 | sql/function/time/test_date_part.test<br>sql/function/timestamp/test_icu_datepart.test<br>sql/function/timetz/test_date_part.test | `statement error<br>SELECT quarter(i) FROM times` |  |
| `r` | `unsupported feature` | 1 | sql/function/string/test_subscript.test | `query T<br>SELECT s[3] FROM strings<br>----<br>l<br>r<br>(empty)<br>NULL` |  |
| `radians` | `unsupported feature` | 1 | sql/function/numeric/test_pg_math.test | `query R<br>select radians(45.0)<br>----<br>0.7853981633974483` |  |
| `reduce_macro` | `unsupported feature` | 4 | sql/function/list/lambdas/arrow/lambdas_and_macros_deprecated.test<br>sql/function/list/lambdas/lambdas_and_macros.test | `query I<br>SELECT reduce_macro([1, 2, 3, 4], 5);<br>----<br>25` |  |
| `regex` | `unsupported feature` | 1 | sql/function/string/regex_filter_pushdown.test | `statement ok<br>CREATE TABLE regex(s STRING)` |  |
| `regexp_escape` | `unsupported feature` | 9 | sql/function/string/regex_escape.test | `query T<br>SELECT regexp_escape('@');<br>----<br>\@` |  |
| `regexp_full_match` | `unsupported feature` | 1 | sql/function/string/null_byte.test | `query I<br>SELECT * FROM null_byte WHERE regexp_full_match(v, concat('goo', chr(0), 'se'))<br>----<br>goo\0se` |  |
| `replace_type` | `unsupported feature` | 7 | sql/function/generic/replace_type.test | `statement error<br>SELECT replace_type(42, NULL::INTEGER, NULL);` |  |
| `reverse` | `unsupported feature` | 5 | sql/function/string/test_complex_unicode.test | `query T<br>SELECT REVERSE('S̈a︍')<br>----<br>a︍S̈` |  |
| `right_only` | `unsupported feature` | 4 | sql/function/list/lambdas/arrow/reduce_deprecated.test<br>sql/function/list/lambdas/arrow/reduce_initial_deprecated.test<br>sql/function/list/lambdas/reduce.test<br>sql/function/list/lambdas/reduce_initial.test | `statement ok<br>CREATE TABLE right_only (v varchar[], i int);` |  |
| `round_even` | `unsupported feature` | 2 | sql/function/numeric/test_round_even.test | `query II<br>select i, round_even(i + 0.5, 0) from generate_series(-2,4) tbl(i);<br>----<br>-2	-2<br>-1	0<br>0	0<br>1	2<br>2	2<br>3	4<br>4	4` |  |
| `roundbankers` | `unsupported feature` | 11 | sql/function/numeric/test_round_even.test | `query II<br>SELECT roundBankers(45, -1), roundBankers(35, -1)<br>----<br>40	40` |  |
| `rpad` | `unsupported feature` | 9 | sql/function/string/test_pad.test | `statement error<br>select RPAD()` |  |
| `rs` | `unsupported feature` | 1 | sql/function/numeric/test_fdiv_fmod.test | `statement ok<br>CREATE TABLE rs(x DOUBLE, y INTEGER)` |  |
| `scoping_macro` | `unsupported feature` | 4 | sql/function/list/lambdas/arrow/lambdas_and_macros_deprecated.test<br>sql/function/list/lambdas/lambdas_and_macros.test | `query I<br>SELECT scoping_macro([11, 22], 3, 4);<br>----<br>[18, 29]` |  |
| `second` | `unsupported feature` | 1 | sql/function/timestamp/test_icu_datepart.test | `query II<br>SELECT second(ts), second(ts::TIMESTAMP) FROM timestamps;<br>----<br>43	43<br>48	48<br>0	0<br>0	0<br>13	13<br>0	0<br>0	0<br>0	0<br>NULL	NULL<br>NULL	NULL<br>NULL	NULL` |  |
| `seeds` | `unsupported feature` | 1 | sql/function/numeric/test_random.test | `statement ok<br>CREATE TABLE seeds(a DOUBLE)` |  |
| `selections` | `unsupported feature` | 1 | sql/function/list/list_where.test | `statement ok<br>CREATE TABLE selections (j boolean[])` |  |
| `sems` | `unsupported feature` | 1 | sql/function/list/aggregates/sem.test | `statement ok<br>create table sems (l int[]);` |  |
| `setseed` | `unsupported feature` | 6 | sql/function/numeric/test_random.test | `statement ok<br>select setseed(0.1)` |  |
| `sign` | `unsupported feature` | 4 | sql/function/numeric/test_pg_math.test | `query I<br>select sign(0)<br>----<br>0` |  |
| `signbit` | `unsupported feature` | 5 | sql/function/numeric/test_sign_bit.test | `statement ok<br>SELECT signbit(1.0 / 0.0);` |  |
| `sin` | `unsupported feature` | 6 | sql/function/numeric/test_trigo.test | `query I<br>SELECT cast(SIN(n::double)*1000 as bigint) FROM numbers ORDER BY n<br>----<br>0<br>841<br>-917<br>NULL` |  |
| `sinh` | `unsupported feature` | 1 | sql/function/numeric/test_trigo.test | `query IIII<br>select sinh(-1), sinh(0), sinh(1), sinh(1000);<br>----<br>-1.1752011936438014	0.0	1.1752011936438014	inf` |  |
| `some_macro` | `unsupported feature` | 4 | sql/function/list/lambdas/arrow/lambdas_and_macros_deprecated.test<br>sql/function/list/lambdas/lambdas_and_macros.test | `statement error<br>SELECT some_macro([1, 2], 3, 4);` |  |
| `sqrt` | `unsupported feature` | 3 | sql/function/numeric/test_invalid_math.test<br>sql/function/numeric/test_pg_math.test | `query I<br>SELECT SQRT(0)<br>----<br>0` |  |
| `stddev_test` | `unsupported feature` | 1 | sql/function/list/aggregates/var_stddev.test | `statement ok<br>create table stddev_test(val integer[])` |  |
| `str_aggs` | `unsupported feature` | 1 | sql/function/list/aggregates/string_agg.test | `statement ok<br>CREATE TABLE str_aggs (str varchar[]);` |  |
| `str_test` | `unsupported feature` | 3 | sql/function/list/list_contains.test<br>sql/function/list/list_position.test | `statement ok<br>create table STR_TEST (i string[]);` |  |
| `string_table` | `unsupported feature` | 1 | sql/function/list/list_value_nested_lists.test | `statement ok<br>CREATE TABLE string_table(a VARCHAR[], b VARCHAR[], c VARCHAR[]);` |  |
| `string_tbl` | `unsupported feature` | 1 | sql/function/list/list_resize.test | `statement ok<br>create table string_tbl(a string[], b int);` |  |
| `strings` | `unsupported feature` | 23 | sql/function/list/aggregates/string_agg.test<br>sql/function/string/test_array_extract.test<br>sql/function/string/test_damerau_levenshtein.test<br>sql/function/string/test_glob.test<br>sql/function/string/test_instr_utf8.test<br>sql/function/string/test_levenshtein.test<br>sql/function/string/test_mismatches.test<br>sql/function/string/test_pad.test<br>sql/function/string/test_repeat.test<br>sql/function/string/test_similar_to.test<br>sql/function/string/test_string_array_slice.test<br>sql/function/string/test_string_slice.test<br>sql/function/string/test_subscript.test<br>sql/function/string/test_substring.test<br>sql/function/string/test_substring_utf8.test | `statement ok<br>CREATE TABLE strings(s VARCHAR)` |  |
| `strlen` | `unsupported feature` | 3 | sql/function/string/null_byte.test<br>sql/function/string/test_complex_unicode.test | `query I<br>SELECT strlen('S̈a')<br>----<br>4` |  |
| `strptime` | `unsupported feature` | 144 | sql/function/timestamp/test_icu_strptime.test<br>sql/function/timestamp/test_strptime.test | `statement error<br>SELECT strptime('', '')` |  |
| `struct_cast_data` | `unsupported feature` | 1 | sql/function/variant/variant_extract.test | `statement ok<br>CREATE MACRO struct_cast_data() AS TABLE (<br>	SELECT {'a': [<br>		{<br>			'b': 'hello',<br>			'c': NULL,<br>			'a': '1970/03/15'::DATE<br>		},<br>		{<br>			'b': NULL,<br>			'c': True,<br>			'a': '2020/11/03'::DATE<br>		}<br>	]}::VARIANT AS a<br>	UNION ALL<br>	SELECT {'a': [<br>		{<br>			'b': 'this is a long string',<br>			'c': False,<br>			'a': '1953/9/16'::DATE<br>		}<br>	]}::VARIANT<br>);` |  |
| `struct_table` | `unsupported feature` | 1 | sql/function/list/list_value_nested_lists.test | `statement ok<br>CREATE TABLE struct_table(a ROW(a INTEGER, b INTEGER)[], b ROW(a INTEGER, b INTEGER)[], c ROW(a INTEGER, b INTEGER)[]);` |  |
| `struct_update` | `unsupported feature` | 18 | sql/function/nested/test_struct_update.test | `statement error<br>SELECT struct_update();` |  |
| `substring_grapheme` | `unsupported feature` | 35 | sql/function/string/test_complex_unicode.test<br>sql/function/string/test_substring.test<br>sql/function/string/test_substring_utf8.test | `query I<br>SELECT substring_grapheme('a', -1)<br>----<br>a` |  |
| `suffix` | `unsupported feature` | 3 | sql/function/timestamp/current_timestamp.test | `query I<br>SELECT SUFFIX(now()::VARCHAR, '-06');<br>----<br>True` |  |
| `t_struct` | `unsupported feature` | 4 | sql/function/list/lambdas/arrow/reduce_deprecated.test<br>sql/function/list/lambdas/arrow/reduce_initial_deprecated.test<br>sql/function/list/lambdas/reduce.test<br>sql/function/list/lambdas/reduce_initial.test | `statement ok<br>CREATE TABLE t_struct (s STRUCT(v VARCHAR, i INTEGER)[]);` |  |
| `tab0` | `unsupported feature` | 1 | sql/function/operator/test_arithmetic_sqllogic.test | `statement ok<br>CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER);` |  |
| `tab1` | `unsupported feature` | 1 | sql/function/operator/test_arithmetic_sqllogic.test | `statement ok<br>CREATE TABLE tab1(col0 INTEGER, col1 INTEGER, col2 INTEGER);` |  |
| `tab2` | `unsupported feature` | 1 | sql/function/operator/test_arithmetic_sqllogic.test | `statement ok<br>CREATE TABLE tab2(col0 INTEGER, col1 INTEGER, col2 INTEGER);` |  |
| `tan` | `unsupported feature` | 6 | sql/function/numeric/test_trigo.test | `query I<br>SELECT cast(TAN(n::float)*1000 as bigint) FROM numbers ORDER BY n<br>----<br>NULL<br>-2291<br>-1557<br>0<br>1557<br>2291` |  |
| `tanh` | `unsupported feature` | 1 | sql/function/numeric/test_trigo.test | `query IIII<br>select tanh(-0.5), tanh(0), tanh(0.5), tanh(1000);<br>----<br>-0.46211715726000974	0.0	0.46211715726000974	1.0` |  |
| `tbl1` | `unsupported feature` | 5 | sql/function/index/index_key.test | `statement ok<br>CREATE UNIQUE INDEX tbl1_idx_txt ON tbl1(txt);` |  |
| `tbl3` | `unsupported feature` | 3 | sql/function/index/index_key.test | `statement ok<br>CREATE UNIQUE INDEX tbl3_idx ON tbl3(key1, key2, key3);` |  |
| `tbl3_wrong_type` | `unsupported feature` | 1 | sql/function/index/index_key.test | `statement ok<br>CREATE TABLE tbl3_wrong_type(c1 VARCHAR, c2 INTEGER, c3 BIGINT);` |  |
| `test0` | `unsupported feature` | 47 | sql/function/list/list_contains.test<br>sql/function/list/list_position.test | `statement ok<br>CREATE TABLE test0 (i float[])` |  |
| `test_twoc` | `unsupported feature` | 1 | sql/function/numeric/test_nextafter.test | `statement ok<br>create table test_twoc (a FLOAT, b FLOAT)` |  |
| `test_vector_types` | `unsupported feature` | 3 | sql/function/list/lambdas/vector_types.test<br>sql/function/list/list_reverse.test | `query I nosort q1<br>select true from test_vector_types(null::int[], false);<br>----` |  |
| `time_bucket` | `unsupported feature` | 101 | sql/function/timestamp/test_icu_time_bucket_timestamptz.test<br>sql/function/timestamp/test_time_bucket_timestamp.test | `query I<br>select time_bucket('-1 month'::interval, null::timestamp);<br>----<br>NULL` |  |
| `timestamp` | `unsupported feature` | 2 | sql/function/list/aggregates/approx_count_distinct.test<br>sql/function/timestamp/age.test | `statement ok<br>CREATE TABLE timestamp(t1 TIMESTAMP, t2 TIMESTAMP)` |  |
| `timestamps_default` | `unsupported feature` | 1 | sql/function/timestamp/test_now_prepared.test | `statement ok<br>CREATE TABLE timestamps_default(ts TIMESTAMP DEFAULT NOW());` |  |
| `timestamps_tz` | `unsupported feature` | 1 | sql/function/timestamp/test_icu_time_bucket_timestamptz.test | `statement ok<br>CREATE TABLE timestamps_tz(w INTERVAL, t TIMESTAMPTZ, shift INTERVAL, origin TIMESTAMPTZ, timezone VARCHAR);` |  |
| `to_timestamp` | `unsupported feature` | 3 | sql/function/timestamp/epoch.test | `statement error<br>SELECT to_timestamp(1284352323::DOUBLE * 100000000);` |  |
| `transformed_lists` | `unsupported feature` | 6 | sql/function/list/lambdas/arrow/transform_deprecated.test<br>sql/function/list/lambdas/transform.test | `statement ok<br>CREATE TABLE transformed_lists (g integer, l integer[]);` |  |
| `transpose` | `unsupported feature` | 4 | sql/function/list/lambdas/arrow/test_lambda_arrow_storage_deprecated.test<br>sql/function/list/lambdas/incorrect.test | `statement ok<br>CREATE OR REPLACE FUNCTION transpose(lst) AS (<br>	SELECT list_transform(range(1, 1 + length(lst[1])),<br>		j -> list_transform(range(1, length(lst) + 1),<br>			i -> lst[i][j]<br>		)<br>	)<br>);` |  |
| `trunc` | `unsupported feature` | 94 | sql/function/numeric/test_trunc.test<br>sql/function/numeric/test_trunc_precision.test | `query R<br>select trunc(54::BIGINT)<br>----<br>54` |  |
| `truncme` | `unsupported feature` | 2 | sql/function/numeric/test_trunc.test<br>sql/function/numeric/test_trunc_precision.test | `statement ok<br>CREATE TABLE truncme(a DOUBLE, b INTEGER, c UINTEGER)` |  |
| `try_strptime` | `unsupported feature` | 127 | sql/function/timestamp/test_icu_strptime.test<br>sql/function/timestamp/test_try_strptime.test | `statement error<br>SELECT try_strptime('', '')` |  |
| `unit2` | `unsupported feature` | 1 | sql/function/list/lambdas/incorrect.test | `statement error<br>CREATE TABLE unit2(<br>	price INTEGER[],<br>	total_price INTEGER GENERATED ALWAYS AS (list_transform(price, x -> x + 1)) VIRTUAL<br>);` |  |
| `uuid_extract_timestamp` | `unsupported feature` | 2 | sql/function/uuid/test_uuid_function.test | `statement error<br>SELECT uuid_extract_timestamp(uuidv4());` |  |
| `uuid_extract_version` | `unsupported feature` | 4 | sql/function/uuid/test_uuid_function.test | `query I<br>SELECT uuid_extract_version(uuidv7());<br>----<br>7` |  |
| `uuids` | `unsupported feature` | 2 | sql/function/uuid/test_uuid.test | `statement ok<br>CREATE TABLE uuids(u UUID NOT NULL DEFAULT gen_random_uuid(), a INTEGER);` |  |
| `v_data` | `unsupported feature` | 1 | sql/function/list/flatten.test | `query IIII rowsort<br>with v_data (col, list) as ( select * FROM (VALUES ('a', [1,2,3]), ('b', [4,5]), ('a', [2,6])) ),<br>        v_list_of_lists ( col, list, list_of_lists ) as ( select v.*, array_agg(v.list) over (partition by v.col order by v.list) from v_data v )<br>select v.*, flatten(v.list_of_lists) from v_list_of_lists v;<br>----<br>a	[1, 2, 3]	[[1, 2, 3]]	[1, 2, 3]<br>a	[2, 6]	[[1, 2, 3], [2, 6]]	[1, 2, 3, 2, 6]<br>b	[4, 5]	[[4, 5]]	[4, 5]` |  |
| `variant_extract` | `unsupported feature` | 2 | sql/function/variant/variant_extract.test | `query I<br>select variant_extract(a, 'a[1].c') from struct_cast_data();<br>----<br>NULL<br>NULL` |  |
| `variant_typeof` | `unsupported feature` | 8 | sql/function/variant/variant_typeof.test | `query I<br>select variant_typeof(({'a': 42}::VARIANT).variant_extract('a'));<br>----<br>INT32` |  |
| `week` | `unsupported feature` | 4 | sql/function/interval/test_date_part.test<br>sql/function/time/test_date_part.test<br>sql/function/timestamp/test_icu_datepart.test<br>sql/function/timetz/test_date_part.test | `statement error<br>SELECT week(i) FROM times` |  |
| `weekday` | `unsupported feature` | 1 | sql/function/timestamp/test_icu_datepart.test | `query II<br>SELECT weekday(ts), weekday(ts::TIMESTAMP) FROM timestamps;<br>----<br>3	3<br>2	2<br>4	4<br>1	1<br>5	5<br>1	1<br>1	1<br>5	5<br>NULL	NULL<br>NULL	NULL<br>NULL	NULL` |  |
| `weekofyear` | `unsupported feature` | 1 | sql/function/timestamp/test_icu_datepart.test | `query II<br>SELECT weekofyear(ts), weekofyear(ts::TIMESTAMP) FROM timestamps;<br>----<br>11	11<br>31	31<br>53	53<br>5	5<br>47	47<br>46	46<br>46	46<br>51	51<br>NULL	NULL<br>NULL	NULL<br>NULL	NULL` |  |
| `where_clause` | `unsupported feature` | 4 | sql/function/list/lambdas/arrow/reduce_deprecated.test<br>sql/function/list/lambdas/arrow/reduce_initial_deprecated.test<br>sql/function/list/lambdas/reduce.test<br>sql/function/list/lambdas/reduce_initial.test | `statement ok<br>CREATE table where_clause (a int[]);` |  |
| `wheretest` | `unsupported feature` | 1 | sql/function/list/list_distinct.test | `statement ok<br>CREATE TABLE wheretest (name VARCHAR, l INTEGER[]);` |  |
| `xor` | `unsupported feature` | 5 | sql/function/operator/test_bitwise_ops.test<br>sql/function/operator/test_bitwise_ops_types.test | `query IIIII<br>SELECT xor(1, 1), xor(1, 0), xor(0, 0), xor(NULL, 1), xor(1, NULL)<br>----<br>0	1	0	NULL	NULL` |  |
| `year` | `unsupported feature` | 4 | sql/function/time/test_date_part.test<br>sql/function/timestamp/test_icu_datepart.test<br>sql/function/timetz/test_date_part.test | `statement error<br>SELECT year(i) FROM times` |  |
| `yeartz` | `unsupported feature` | 1 | sql/function/timestamp/test_icu_makedate.test | `statement ok<br>CREATE MACRO yeartz(ts) AS year(ts::TIMESTAMPTZ) * (CASE WHEN ERA(ts::TIMESTAMPTZ) > 0 THEN 1 ELSE -1 END);` |  |
| `yearweek` | `unsupported feature` | 3 | sql/function/time/test_date_part.test<br>sql/function/timestamp/test_icu_datepart.test<br>sql/function/timetz/test_date_part.test | `statement error<br>SELECT yearweek(i) FROM times` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T09:32:24.554568+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1009
- Unknown function TODOs: 822
- Unsupported feature TODOs: 11409
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `unit` | `unsupported feature` | 1 | sql/generated_columns/virtual/ambiguity.test | `statement ok<br>CREATE TABLE unit (<br>	price INTEGER,<br>	amount_sold INTEGER,<br>	total_profit AS (price * amount_sold)<br>);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T09:32:34.820938+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1012
- Unknown function TODOs: 822
- Unsupported feature TODOs: 11426
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `chk` | `unsupported feature` | 1 | sql/generated_columns/virtual/check.test | `statement ok<br>CREATE TABLE chk (<br>	g1 AS (x),<br>	g2 AS (x),<br>	x INTEGER,<br>	CHECK (x > 5)<br>);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T09:32:38.044526+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1014
- Unknown function TODOs: 822
- Unsupported feature TODOs: 11430
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `coll` | `unsupported feature` | 1 | sql/generated_columns/virtual/collate.test | `statement ok<br>CREATE TABLE coll (<br>	g1 AS (x),<br>	g2 AS (x),<br>	x VARCHAR COLLATE NOCASE<br>);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T09:46:51.061064+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 15
- Files with issues: 1037
- Unknown function TODOs: 822
- Unsupported feature TODOs: 11588
- Unknown function unique issues: 0
- Unsupported feature unique issues: 5

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `amount_sold` | `unsupported feature` | 3 | sql/generated_columns/virtual/select_alias.test | `query II nosort swapped<br>select a, b from tbl amount_sold(e, c, b, d, a);<br>----` |  |
| `order_id` | `unsupported feature` | 3 | sql/generated_columns/virtual/select_alias.test | `query II nosort swapped<br>select a, b from tbl order_id(e, c, b, d, a);<br>----` |  |
| `price` | `unsupported feature` | 3 | sql/generated_columns/virtual/select_alias.test | `query II nosort swapped<br>select a, b from tbl price(e, c, b, d, a);<br>----` |  |
| `total_profit` | `unsupported feature` | 3 | sql/generated_columns/virtual/select_alias.test | `query II nosort swapped<br>select a, b from tbl total_profit(e, c, b, d, a);<br>----` |  |
| `y` | `unsupported feature` | 3 | sql/generated_columns/virtual/select_alias.test | `query II nosort swapped<br>select a, b from tbl y(e, c, b, d, a);<br>----` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T09:47:14.158539+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1043
- Unknown function TODOs: 822
- Unsupported feature TODOs: 11617
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `tbl_comp` | `unsupported feature` | 1 | sql/generated_columns/virtual/update_index.test | `statement ok<br>CREATE TABLE tbl_comp (<br>	a INT,<br>	gen AS (2 * a),<br>	b INT,<br>	c VARCHAR,<br>	PRIMARY KEY(c));` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T09:47:20.855764+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1044
- Unknown function TODOs: 822
- Unsupported feature TODOs: 11627
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `test1` | `unsupported feature` | 1 | sql/index/art/constraints/test_art_eager_batch_insert.test | `statement ok<br>CREATE TABLE test1 (id INT PRIMARY KEY, payload VARCHAR);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T09:49:58.408380+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 18
- Files with issues: 1045
- Unknown function TODOs: 822
- Unsupported feature TODOs: 11684
- Unknown function unique issues: 0
- Unsupported feature unique issues: 13

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `a_4214` | `unsupported feature` | 2 | sql/index/art/constraints/test_art_eager_constraint_checking.test | `statement ok<br>INSERT INTO a_4214 (id, c_id) VALUES (1, 1);` |  |
| `c_4214` | `unsupported feature` | 2 | sql/index/art/constraints/test_art_eager_constraint_checking.test | `statement ok<br>INSERT INTO c_4214 (id) VALUES (1), (2);` |  |
| `t_11288` | `unsupported feature` | 1 | sql/index/art/constraints/test_art_eager_constraint_checking.test | `statement ok<br>CREATE TABLE t_11288 (i INT PRIMARY KEY, j MAP(VARCHAR, VARCHAR));` |  |
| `t_14133` | `unsupported feature` | 1 | sql/index/art/constraints/test_art_eager_constraint_checking.test | `statement ok<br>CREATE TABLE t_14133 (i INT PRIMARY KEY, s VARCHAR);` |  |
| `t_4807` | `unsupported feature` | 1 | sql/index/art/constraints/test_art_eager_constraint_checking.test | `statement ok<br>CREATE TABLE t_4807 (id INT PRIMARY KEY, u UNION (i INT));` |  |
| `t_7182` | `unsupported feature` | 1 | sql/index/art/constraints/test_art_eager_constraint_checking.test | `statement ok<br>CREATE TABLE t_7182 (it INTEGER PRIMARY KEY, jt INTEGER);` |  |
| `tag_8764` | `unsupported feature` | 4 | sql/index/art/constraints/test_art_eager_constraint_checking.test | `statement ok<br>CREATE UNIQUE INDEX idx_name_8764 ON tag_8764(name);` |  |
| `tbl_1631` | `unsupported feature` | 1 | sql/index/art/constraints/test_art_eager_constraint_checking.test | `statement ok<br>CREATE TABLE tbl_1631 (<br>	id INTEGER PRIMARY KEY,<br>	c1 text NOT NULL UNIQUE,<br>	c2 text NOT NULL);` |  |
| `tbl_6500` | `unsupported feature` | 1 | sql/index/art/constraints/test_art_eager_constraint_checking.test | `statement ok<br>CREATE UNIQUE INDEX idx_6500 ON tbl_6500 (i);` |  |
| `test_4886` | `unsupported feature` | 1 | sql/index/art/constraints/test_art_eager_constraint_checking.test | `statement ok<br>CREATE TABLE test_4886 (i INTEGER PRIMARY KEY);` |  |
| `tunion_5807` | `unsupported feature` | 1 | sql/index/art/constraints/test_art_eager_constraint_checking.test | `statement ok<br>CREATE TABLE tunion_5807 (id INTEGER PRIMARY KEY, u UNION (i int));` |  |
| `u_7182` | `unsupported feature` | 1 | sql/index/art/constraints/test_art_eager_constraint_checking.test | `statement ok<br>CREATE TABLE u_7182 (iu INTEGER PRIMARY KEY, ju INTEGER REFERENCES t_7182 (it));` |  |
| `workers_5771` | `unsupported feature` | 1 | sql/index/art/constraints/test_art_eager_constraint_checking.test | `statement ok<br>CREATE TABLE IF NOT EXISTS workers_5771 (<br>    id INTEGER PRIMARY KEY NOT NULL,<br>    worker VARCHAR(150) UNIQUE NOT NULL,<br>    phone VARCHAR(20) NOT NULL);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T09:50:34.554828+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1049
- Unknown function TODOs: 822
- Unsupported feature TODOs: 11709
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `tbl_rollback` | `unsupported feature` | 1 | sql/index/art/constraints/test_art_tx_deletes_rollback.test | `statement ok<br>CREATE TABLE tbl_rollback (id INT PRIMARY KEY, payload VARCHAR[]);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T09:50:53.038433+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1051
- Unknown function TODOs: 822
- Unsupported feature TODOs: 11721
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `tbl_list` | `unsupported feature` | 1 | sql/index/art/constraints/test_art_tx_returning.test | `statement ok<br>CREATE TABLE tbl_list (id INT PRIMARY KEY, payload VARCHAR[]);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T09:51:01.446595+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1053
- Unknown function TODOs: 822
- Unsupported feature TODOs: 11733
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `tbl_local` | `unsupported feature` | 1 | sql/index/art/constraints/test_art_tx_upserts_local.test | `statement ok<br>CREATE TABLE tbl_local (id INT PRIMARY KEY, payload VARCHAR[]);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T09:51:05.409998+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 3
- Files with issues: 1055
- Unknown function TODOs: 822
- Unsupported feature TODOs: 11741
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `hero` | `unsupported feature` | 3 | sql/index/art/constraints/test_art_upsert_duplicate.test | `statement ok<br>CREATE INDEX ix_hero_age ON hero (age);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T09:51:08.562758+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 1056
- Unknown function TODOs: 822
- Unsupported feature TODOs: 11749
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `kvp` | `unsupported feature` | 2 | sql/index/art/constraints/test_art_upsert_other_index.test | `statement ok<br>CREATE INDEX kve_idx ON kvp (expiration);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T09:51:11.129204+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1057
- Unknown function TODOs: 822
- Unsupported feature TODOs: 11750
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `v0` | `unsupported feature` | 1 | sql/index/art/create_drop/test_art_big_compound_key.test | `statement ok<br>CREATE UNIQUE INDEX v3 ON v0<br>	(v1, v1, v1, v1, v1, v2, v1, v2, v1, v2, v2, v1, v2, v2,<br>	v2, v2, v2, v2, v1, v1, v2, v2, v1, v1, v2, v1);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T10:04:04.695095+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 1075
- Unknown function TODOs: 822
- Unsupported feature TODOs: 11814
- Unknown function unique issues: 0
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `t21` | `unsupported feature` | 1 | sql/index/art/issues/test_art_fuzzer.test | `statement ok<br>CREATE INDEX i21 ON t21 (c1, "decode"('\x00'::BLOB));` |  |
| `t4` | `unsupported feature` | 1 | sql/index/art/issues/test_art_fuzzer.test | `statement ok<br>CREATE INDEX i4 ON t4 (c1);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T10:04:35.987152+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 3
- Files with issues: 1077
- Unknown function TODOs: 822
- Unsupported feature TODOs: 11826
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `reservoir` | `unsupported feature` | 3 | sql/index/art/issues/test_art_internal_issue_4742.test | `statement ok<br>create or replace table sample as select i from test using sample reservoir(10) repeatable (42);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T10:05:14.968498+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 3
- Files with issues: 1079
- Unknown function TODOs: 822
- Unsupported feature TODOs: 11833
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `edge` | `unsupported feature` | 3 | sql/index/art/issues/test_art_issue_6603.test | `statement ok<br>CREATE INDEX edge1_idx ON edge (x1);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T10:05:20.555707+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 1080
- Unknown function TODOs: 822
- Unsupported feature TODOs: 11838
- Unknown function unique issues: 0
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `key_value_pairs` | `unsupported feature` | 1 | sql/index/art/issues/test_art_issue_6799.test | `statement ok<br>CREATE TABLE key_value_pairs (key VARCHAR PRIMARY KEY, value VARCHAR)` |  |
| `keys_to_lookup` | `unsupported feature` | 1 | sql/index/art/issues/test_art_issue_6799.test | `statement ok<br>CREATE TABLE keys_to_lookup (key VARCHAR PRIMARY KEY)` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T10:06:53.676466+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 1082
- Unknown function TODOs: 822
- Unsupported feature TODOs: 11847
- Unknown function unique issues: 0
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `t14` | `unsupported feature` | 1 | sql/index/art/issues/test_art_issue_7530.test | `statement ok<br>CREATE INDEX i1 ON t14(c0 );` |  |
| `td` | `unsupported feature` | 1 | sql/index/art/issues/test_art_issue_7349.test | `statement ok<br>CREATE UNIQUE INDEX sqlsim0 ON td(tz);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T10:13:52.634527+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1087
- Unknown function TODOs: 822
- Unsupported feature TODOs: 11859
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `duplicates` | `unsupported feature` | 1 | sql/index/art/nodes/test_art_leaf_coverage.test | `statement ok<br>CREATE TABLE duplicates (id UBIGINT);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T10:19:36.266028+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1091
- Unknown function TODOs: 822
- Unsupported feature TODOs: 11867
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `tbl_varchar` | `unsupported feature` | 1 | sql/index/art/nodes/test_art_prefixes_restart.test | `statement ok<br>CREATE INDEX idx_varchar ON tbl_varchar(id);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T11:16:25.871402+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 4
- Files with issues: 1103
- Unknown function TODOs: 822
- Unsupported feature TODOs: 11966
- Unknown function unique issues: 0
- Unsupported feature unique issues: 4

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `conflict` | `unsupported feature` | 1 | sql/index/art/constraints/test_art_compound_key_changes.test | `statement ok<br>INSERT INTO tbl_comp VALUES (2, 'hello', 1, 'world')<br>ON CONFLICT (c, b) DO UPDATE SET a = excluded.a, d = excluded.d;` |  |
| `leaf_merge_1` | `unsupported feature` | 1 | sql/index/art/nodes/test_art_leaf_coverage.test | `statement ok<br>CREATE INDEX idx_merge_1 ON leaf_merge_1(id, id2);` |  |
| `leaf_merge_2` | `unsupported feature` | 1 | sql/index/art/nodes/test_art_leaf_coverage.test | `statement ok<br>CREATE INDEX idx_merge_2 ON leaf_merge_2(id, id2);` |  |
| `tbl_dup_ser` | `unsupported feature` | 1 | sql/index/art/nodes/test_art_leaf_coverage.test | `statement ok<br>CREATE INDEX idx_dup_ser ON tbl_dup_ser(id);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T12:44:26.701081+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 34
- Files with issues: 1105
- Unknown function TODOs: 856
- Unsupported feature TODOs: 12055
- Unknown function unique issues: 3
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `json_extract` | 29 | sql/json/scalar/test_json_extract.test |  |
| `json_extract_path` | 1 | sql/json/scalar/test_json_extract.test |  |
| `json_extract_string` | 3 | sql/json/scalar/test_json_extract.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `json_extract_path_text` | `unsupported feature` | 1 | sql/json/scalar/test_json_extract.test | `query T<br>select json_extract_path_text('{"my_field": {"my_nested_field": ["goose", "duck"]}}', '/my_field/my_nested_field/1')<br>----<br>duck` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T12:44:44.704809+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1107
- Unknown function TODOs: 856
- Unsupported feature TODOs: 12071
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `varchars` | `unsupported feature` | 1 | sql/index/art/scan/test_art_null_bytes.test | `statement ok<br>CREATE TABLE varchars(v VARCHAR PRIMARY KEY);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T12:46:24.660899+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 4
- Files with issues: 1110
- Unknown function TODOs: 856
- Unsupported feature TODOs: 12125
- Unknown function unique issues: 0
- Unsupported feature unique issues: 3

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `not` | `unsupported feature` | 2 | sql/index/art/scan/test_art_scan_coverage.test | `query I rowsort label-225<br>SELECT pk FROM tab1 WHERE NOT (col3 = 54 AND col1 <= 76.83)<br>----` |  |
| `t0_scan` | `unsupported feature` | 1 | sql/index/art/scan/test_art_scan_coverage.test | `statement ok<br>CREATE INDEX t0i0 ON t0_scan(c0 DESC);` |  |
| `t0_varchar` | `unsupported feature` | 1 | sql/index/art/scan/test_art_scan_coverage.test | `statement ok<br>CREATE INDEX t0i0_idx ON t0_varchar(c0 );` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T12:46:30.874370+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 7
- Files with issues: 1111
- Unknown function TODOs: 856
- Unsupported feature TODOs: 12133
- Unknown function unique issues: 0
- Unsupported feature unique issues: 4

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `t_1` | `unsupported feature` | 1 | sql/index/art/scan/test_art_scan_duplicate_filters.test | `statement ok<br>CREATE TABLE t_1 (fIdx VARCHAR, sIdx UUID,);` |  |
| `t_3` | `unsupported feature` | 1 | sql/index/art/scan/test_art_scan_duplicate_filters.test | `statement ok<br>CREATE TABLE t_3 (fIdx VARCHAR, sIdx UUID);` |  |
| `t_4` | `unsupported feature` | 2 | sql/index/art/scan/test_art_scan_duplicate_filters.test | `statement ok<br>CREATE TABLE t_4 (sIdx UUID);` |  |
| `t_5` | `unsupported feature` | 3 | sql/index/art/scan/test_art_scan_duplicate_filters.test | `statement ok<br>CREATE TABLE t_5 (sIdx UUID);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T12:58:46.730036+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 6
- Files with issues: 1121
- Unknown function TODOs: 857
- Unsupported feature TODOs: 12210
- Unknown function unique issues: 0
- Unsupported feature unique issues: 3

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `duckdb_constraints` | `unsupported feature` | 3 | sql/index/art/storage/test_art_duckdb_versions.test | `query II<br>SELECT table_name, constraint_type FROM duckdb_constraints() ORDER BY ALL;<br>----<br>fk_tbl	FOREIGN KEY<br>pk_tbl	NOT NULL<br>pk_tbl	PRIMARY KEY<br>pk_tbl	UNIQUE` |  |
| `get_block_size` | `unsupported feature` | 2 | sql/index/art/storage/test_art_duckdb_versions.test | `query I<br>SELECT used_blocks > 2621440 / get_block_size('test_art_import') FROM pragma_database_size();<br>----<br>1` |  |
| `idx_tbl` | `unsupported feature` | 1 | sql/index/art/storage/test_art_duckdb_versions.test | `statement ok<br>CREATE INDEX ART_index ON idx_tbl(i);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T12:58:50.507863+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 1122
- Unknown function TODOs: 857
- Unsupported feature TODOs: 12212
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `tracking` | `unsupported feature` | 2 | sql/index/art/storage/test_art_import.test | `statement ok<br>CREATE INDEX nflid_idx ON tracking (nflid)` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T12:58:54.277914+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1123
- Unknown function TODOs: 857
- Unsupported feature TODOs: 12213
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `raw` | `unsupported feature` | 1 | sql/index/art/storage/test_art_import_export.test | `statement ok<br>CREATE UNIQUE INDEX customer_year_month_idx ON raw (customer_ID, year, month);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T12:58:56.093178+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1124
- Unknown function TODOs: 857
- Unsupported feature TODOs: 12216
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `duckdb_memory` | `unsupported feature` | 1 | sql/index/art/storage/test_art_mem_limit.test | `statement ok<br>FROM duckdb_memory();` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T13:01:57.700592+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1127
- Unknown function TODOs: 857
- Unsupported feature TODOs: 12230
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `tbl_deser_scan` | `unsupported feature` | 1 | sql/index/art/storage/test_art_storage.test | `statement ok<br>CREATE INDEX idx_deser_scan ON tbl_deser_scan(id);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T13:02:00.086487+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 5
- Files with issues: 1128
- Unknown function TODOs: 857
- Unsupported feature TODOs: 12237
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `history` | `unsupported feature` | 5 | sql/index/art/storage/test_art_storage_long_prefixes.test | `statement ok<br>CREATE TABLE history(id TEXT, type TEXT, PRIMARY KEY(id, type));` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T13:02:02.365020+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 1129
- Unknown function TODOs: 857
- Unsupported feature TODOs: 12242
- Unknown function unique issues: 0
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `pk_integers` | `unsupported feature` | 1 | sql/index/art/storage/test_art_storage_multi_checkpoint.test | `statement ok<br>CREATE TABLE pk_integers(i INTEGER PRIMARY KEY)` |  |
| `pk_integers2` | `unsupported feature` | 1 | sql/index/art/storage/test_art_storage_multi_checkpoint.test | `statement ok<br>CREATE TABLE pk_integers2(i INTEGER PRIMARY KEY)` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T13:02:15.663040+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1130
- Unknown function TODOs: 857
- Unsupported feature TODOs: 12244
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `minimal_tbl` | `unsupported feature` | 1 | sql/index/art/storage/test_art_wal_checkpoint_minimal.test | `statement ok<br>CREATE UNIQUE INDEX idx_minimal ON minimal_tbl(i);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T13:03:08.060756+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1132
- Unknown function TODOs: 857
- Unsupported feature TODOs: 12258
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `drop_test` | `unsupported feature` | 1 | sql/index/art/storage/test_art_wal_replay_in_tx.test | `statement ok<br>CREATE INDEX drop_idx ON drop_test(a);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T13:04:25.726510+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 8
- Files with issues: 1137
- Unknown function TODOs: 857
- Unsupported feature TODOs: 12301
- Unknown function unique issues: 0
- Unsupported feature unique issues: 7

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `int128_first` | `unsupported feature` | 2 | sql/index/art/types/test_art_coverage_types.test | `statement ok<br>CREATE TABLE int128_first (id INT128, id2 INT128);` |  |
| `int128_point` | `unsupported feature` | 1 | sql/index/art/types/test_art_coverage_types.test | `statement ok<br>CREATE INDEX idx_int128_point ON int128_point(id);` |  |
| `uint32_point` | `unsupported feature` | 1 | sql/index/art/types/test_art_coverage_types.test | `statement ok<br>CREATE INDEX idx_uint32_point ON uint32_point(id);` |  |
| `uint64_first` | `unsupported feature` | 1 | sql/index/art/types/test_art_coverage_types.test | `statement ok<br>CREATE INDEX idx_3 ON uint64_first(id, id2, id3, id4);` |  |
| `uint64_point` | `unsupported feature` | 1 | sql/index/art/types/test_art_coverage_types.test | `statement ok<br>CREATE INDEX idx_uint64_point ON uint64_point(id);` |  |
| `uint8_first` | `unsupported feature` | 1 | sql/index/art/types/test_art_coverage_types.test | `statement ok<br>CREATE INDEX idx_2 ON uint8_first(id, id2);` |  |
| `uint8_point` | `unsupported feature` | 1 | sql/index/art/types/test_art_coverage_types.test | `statement ok<br>CREATE INDEX idx_uint8_point ON uint8_point(id);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T13:06:25.473189+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 5
- Files with issues: 1145
- Unknown function TODOs: 857
- Unsupported feature TODOs: 12399
- Unknown function unique issues: 0
- Unsupported feature unique issues: 5

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `t5` | `unsupported feature` | 1 | sql/index/art/vacuum/test_art_vacuum_rollback.test | `statement ok<br>CREATE TABLE t5(i INTEGER UNIQUE);` |  |
| `t6` | `unsupported feature` | 1 | sql/index/art/vacuum/test_art_vacuum_rollback.test | `statement ok<br>CREATE TABLE t6(i INTEGER UNIQUE);` |  |
| `t7` | `unsupported feature` | 1 | sql/index/art/vacuum/test_art_vacuum_rollback.test | `statement ok<br>CREATE TABLE t7(i INTEGER UNIQUE);` |  |
| `t8` | `unsupported feature` | 1 | sql/index/art/vacuum/test_art_vacuum_rollback.test | `statement ok<br>CREATE TABLE t8(i INTEGER UNIQUE);` |  |
| `t9` | `unsupported feature` | 1 | sql/index/art/vacuum/test_art_vacuum_rollback.test | `statement ok<br>CREATE TABLE t9(i INTEGER UNIQUE);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T13:07:02.712321+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 3
- Files with issues: 1146
- Unknown function TODOs: 857
- Unsupported feature TODOs: 12413
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `name` | `unsupported feature` | 3 | sql/insert/insert_by_name.test | `statement ok<br>INSERT INTO tbl2 BY NAME (SELECT 22 AS b);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T13:07:35.961494+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 1147
- Unknown function TODOs: 857
- Unsupported feature TODOs: 12419
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `sets` | `unsupported feature` | 2 | sql/insert/insert_from_many_grouping_sets.test | `statement ok<br>CREATE TABLE integers2 AS SELECT * FROM integers GROUP BY GROUPING SETS ((), (i), (i, j), (j));` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T13:08:52.472545+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1148
- Unknown function TODOs: 857
- Unsupported feature TODOs: 12421
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `presentations` | `unsupported feature` | 1 | sql/insert/test_insert.test | `statement ok<br>CREATE TABLE IF NOT EXISTS presentations(presentation_date Date NOT NULL UNIQUE, author VARCHAR NOT NULL, title VARCHAR NOT NULL, bio VARCHAR, abstract VARCHAR, zoom_link VARCHAR);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T13:14:13.713423+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 8
- Files with issues: 1154
- Unknown function TODOs: 857
- Unsupported feature TODOs: 12502
- Unknown function unique issues: 0
- Unsupported feature unique issues: 8

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `prices_array` | `unsupported feature` | 1 | sql/join/asof/test_asof_join_prefix.test | `statement ok<br>CREATE TABLE prices_array("when" TIMESTAMP, symbol INTEGER[2], price INTEGER);` |  |
| `prices_list` | `unsupported feature` | 1 | sql/join/asof/test_asof_join_prefix.test | `statement ok<br>CREATE TABLE prices_list("when" TIMESTAMP, symbol INTEGER[], price INTEGER);` |  |
| `prices_nested` | `unsupported feature` | 1 | sql/join/asof/test_asof_join_prefix.test | `statement ok<br>CREATE TABLE prices_nested("when" TIMESTAMP, symbol STRUCT(ticker VARCHAR[], exchange INTEGER), price INTEGER);` |  |
| `prices_struct` | `unsupported feature` | 1 | sql/join/asof/test_asof_join_prefix.test | `statement ok<br>CREATE TABLE prices_struct("when" TIMESTAMP, symbol STRUCT(ticker VARCHAR, exchange INTEGER), price INTEGER);` |  |
| `trades_array` | `unsupported feature` | 1 | sql/join/asof/test_asof_join_prefix.test | `statement ok<br>CREATE TABLE trades_array("when" timestamp, symbol INTEGER[2]);` |  |
| `trades_list` | `unsupported feature` | 1 | sql/join/asof/test_asof_join_prefix.test | `statement ok<br>CREATE TABLE trades_list("when" timestamp, symbol INTEGER[]);` |  |
| `trades_nested` | `unsupported feature` | 1 | sql/join/asof/test_asof_join_prefix.test | `statement ok<br>CREATE TABLE trades_nested("when" timestamp, symbol STRUCT(ticker VARCHAR[], exchange INTEGER));` |  |
| `trades_struct` | `unsupported feature` | 1 | sql/join/asof/test_asof_join_prefix.test | `statement ok<br>CREATE TABLE trades_struct("when" timestamp, symbol STRUCT(ticker VARCHAR, exchange INTEGER));` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T13:18:11.494177+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 3
- Files with issues: 1164
- Unknown function TODOs: 857
- Unsupported feature TODOs: 12609
- Unknown function unique issues: 0
- Unsupported feature unique issues: 3

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `df1` | `unsupported feature` | 1 | sql/join/full_outer/test_full_outer_join_issue_4252.test | `statement ok<br>CREATE TABLE df1(day DATE, value INTEGER, organization VARCHAR);<br>INSERT INTO df1 VALUES <br>    ('2022-01-01', 10, 'org1'),<br>    ('2022-01-05', 20, 'org2'),<br>    ('2022-01-10', 30, 'org3');` |  |
| `df2` | `unsupported feature` | 1 | sql/join/full_outer/test_full_outer_join_issue_4252.test | `statement ok<br>CREATE TABLE df2(day DATE, value INTEGER, organization VARCHAR);<br>INSERT INTO df2 VALUES <br>    ('2022-01-01', 100, 'org1'),<br>    ('2022-09-01', 200, 'org2'),<br>    ('2022-03-01', 300, 'org3');` |  |
| `df3` | `unsupported feature` | 1 | sql/join/full_outer/test_full_outer_join_issue_4252.test | `statement ok<br>CREATE TABLE df3(day DATE, value INTEGER, organization VARCHAR);<br>INSERT INTO df3 VALUES<br>    ('2022-01-02',  1000, 'org1'),<br>    ('2022-02-03',  2000, 'org2'),<br>    ('2022-04-01',  3000, 'org3');` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T13:20:10.213680+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1172
- Unknown function TODOs: 858
- Unsupported feature TODOs: 12687
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `interval` | 1 | sql/join/iejoin/test_iejoin_events.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T13:26:54.876917+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 5
- Files with issues: 1176
- Unknown function TODOs: 858
- Unsupported feature TODOs: 12739
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `u` | `unsupported feature` | 5 | sql/join/inner/equality_join_limits.test | `statement ok<br>CREATE TABLE u(u_k0 HUGEINT);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-18T13:30:19.032256+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 8
- Files with issues: 1180
- Unknown function TODOs: 858
- Unsupported feature TODOs: 12786
- Unknown function unique issues: 0
- Unsupported feature unique issues: 6

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `tb1` | `unsupported feature` | 2 | sql/join/inner/test_join_is_distinct.test | `statement ok<br>insert into tb1 (select NULL from range(0, 20));` |  |
| `tb2` | `unsupported feature` | 2 | sql/join/inner/test_join_is_distinct.test | `statement ok<br>insert into tb2 (select NULL from range(0, 20));` |  |
| `tbl_l` | `unsupported feature` | 1 | sql/join/inner/test_join_is_distinct.test | `statement ok<br>CREATE TABLE tbl_l (col0 INTEGER[], col1 INTEGER[]);` |  |
| `tbl_l_null` | `unsupported feature` | 1 | sql/join/inner/test_join_is_distinct.test | `statement ok<br>CREATE TABLE tbl_l_null (col0 INTEGER[], col1 INTEGER[]);` |  |
| `tbl_s` | `unsupported feature` | 1 | sql/join/inner/test_join_is_distinct.test | `statement ok<br>CREATE TABLE tbl_s (col0 STRUCT(x INTEGER), col1 STRUCT(x INTEGER));` |  |
| `tbl_s_null` | `unsupported feature` | 1 | sql/join/inner/test_join_is_distinct.test | `statement ok<br>CREATE TABLE tbl_s_null (col0 STRUCT(x INTEGER), col1 STRUCT(x INTEGER));` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-19T00:11:07.477554+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1184
- Unknown function TODOs: 824
- Unsupported feature TODOs: 12866
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `tbl_m` | `unsupported feature` | 1 | sql/index/art/storage/test_art_storage.test | `statement ok<br>CREATE UNIQUE INDEX idx_m on tbl_m (m(tbl_m.x));` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-19T00:49:11.142760+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1186
- Unknown function TODOs: 824
- Unsupported feature TODOs: 12870
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `v3` | `unsupported feature` | 1 | sql/catalog/view/test_stacked_view.test | `statement ok<br>CREATE VIEW v3 (v3c1, v3c2) AS SELECT v2c1, v2c3 FROM v2 WHERE v2c1 > 43` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-19T00:51:01.830105+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1191
- Unknown function TODOs: 824
- Unsupported feature TODOs: 12888
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `integers4` | `unsupported feature` | 1 | sql/constraints/check/test_check.test | `statement ok<br>CREATE TABLE integers4(i INTEGER CHECK(integers4.i < 10), j INTEGER)` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-19T00:52:47.375544+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1208
- Unknown function TODOs: 824
- Unsupported feature TODOs: 12930
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `iiiiiiiiii` | `unsupported feature` | 1 | sql/copy/csv/auto/test_auto_cranlogs.test | `query IIIIIIIIII<br>(SELECT * FROM cranlogs EXCEPT SELECT * FROM cranlogs2)<br>UNION ALL<br>(SELECT * FROM cranlogs2 EXCEPT SELECT * FROM cranlogs)<br>----` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-19T00:52:51.849298+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1210
- Unknown function TODOs: 824
- Unsupported feature TODOs: 12937
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `iiiii` | `unsupported feature` | 1 | sql/copy/csv/auto/test_auto_imdb.test | `query IIIII<br>(FROM movie_info EXCEPT FROM movie_info2)<br>UNION ALL<br>(FROM movie_info2 EXCEPT FROM movie_info)<br>----` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-19T00:52:56.790700+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1211
- Unknown function TODOs: 824
- Unsupported feature TODOs: 12942
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `iiiiiiiiiiiiii` | `unsupported feature` | 1 | sql/copy/csv/auto/test_auto_web_page.test | `query IIIIIIIIIIIIII<br>(SELECT * FROM web_page EXCEPT SELECT * FROM web_page2)<br>UNION ALL<br>(SELECT * FROM web_page2 EXCEPT SELECT * FROM web_page)<br>----` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-19T00:54:25.599660+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1220
- Unknown function TODOs: 825
- Unsupported feature TODOs: 13094
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `tbl` | 1 | sql/join/left_outer/non_foldable_left_join.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-19T00:55:54.117067+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1224
- Unknown function TODOs: 825
- Unsupported feature TODOs: 13122
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `sqlancer_v0` | `unsupported feature` | 1 | sql/join/natural/natural_join.test | `statement ok<br>CREATE VIEW sqlancer_v0(c0, c1) AS SELECT sqlancer_t0.c0, ((sqlancer_t0.rowid)//(-1694294358))<br>FROM sqlancer_t0<br>ORDER BY TIMESTAMP '1970-01-08 16:19:01' ASC;` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-19T02:06:49.368483+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 1235
- Unknown function TODOs: 825
- Unsupported feature TODOs: 13207
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `t63` | `unsupported feature` | 2 | sql/collate/collate_filter_pushdown.test | `statement ok<br>insert into t63(c0) values ('1');` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-19T02:11:25.130125+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 3
- Files with issues: 1238
- Unknown function TODOs: 825
- Unsupported feature TODOs: 13216
- Unknown function unique issues: 0
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `collate_test` | `unsupported feature` | 2 | sql/collate/test_combined_collation.test<br>sql/collate/test_unsupported_collations.test | `statement ok<br>CREATE TABLE collate_test(s VARCHAR COLLATE NOACCENT.NOCASE)` |  |
| `pragma_collations` | `unsupported feature` | 1 | sql/collate/test_pragma_collations.test | `query I<br>from pragma_collations() <br>where collname like 'n%'<br>order by all<br>----<br>nb<br>nb_no<br>ne<br>nfc<br>nl<br>nn<br>no<br>noaccent<br>nocase<br>nso` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-19T02:24:34.629106+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 1248
- Unknown function TODOs: 825
- Unsupported feature TODOs: 13242
- Unknown function unique issues: 0
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `icu_sort_key` | `unsupported feature` | 1 | sql/collate/test_icu_collate.test | `statement ok<br>SELECT icu_sort_key('Ş', 'ro')` |  |
| `strpos` | `unsupported feature` | 1 | sql/collate/test_strpos_collate.test | `query I<br>SELECT strpos('HELLO' COLLATE NOCASE, 'el')<br>----<br>2` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-19T02:26:59.098652+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1248
- Unknown function TODOs: 826
- Unsupported feature TODOs: 13242
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `strpos` | 1 | sql/collate/test_strpos_collate.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-19T02:40:55.228644+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 59
- Files with issues: 1289
- Unknown function TODOs: 826
- Unsupported feature TODOs: 13947
- Unknown function unique issues: 0
- Unsupported feature unique issues: 29

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `agency` | `unsupported feature` | 3 | sql/constraints/foreignkey/foreign_key_matching_columns.test | `statement ok<br>CREATE TABLE agency (<br>	agency_id TEXT,<br>	agency_name TEXT NOT NULL<br>);` |  |
| `child` | `unsupported feature` | 3 | sql/constraints/foreignkey/test_fk_create_type.test | `statement ok<br>create table child (<br>	parent integer references parent<br>);` |  |
| `d` | `unsupported feature` | 1 | sql/constraints/foreignkey/fk_19469.test | `statement ok<br>CREATE TABLE D (<br>    d1 INTEGER,<br>    d2 INTEGER,<br>    d3 VARCHAR(1),<br>    d4 VARCHAR(1),<br>    payload INTEGER,<br>    FOREIGN KEY (d1, d2) REFERENCES C (c1, c2),<br>    FOREIGN KEY (d3, d4) REFERENCES C (c3, c4)<br>);` |  |
| `departments` | `unsupported feature` | 1 | sql/constraints/foreignkey/test_fk_alter.test | `statement ok<br>CREATE TABLE departments (<br>    department_id INTEGER PRIMARY KEY,<br>    department_name VARCHAR(100) NOT NULL<br>);` |  |
| `employee` | `unsupported feature` | 2 | sql/constraints/foreignkey/test_fk_self_referencing.test | `statement ok<br>CREATE TABLE employee(<br>	id INTEGER PRIMARY KEY,<br>	managerid INTEGER,<br>	name VARCHAR,<br>	FOREIGN KEY(managerid) REFERENCES employee(id));` |  |
| `employees` | `unsupported feature` | 1 | sql/constraints/foreignkey/test_fk_alter.test | `statement ok<br>CREATE TABLE employees (<br>    employee_id INTEGER PRIMARY KEY,<br>    employee_name VARCHAR(100) NOT NULL,<br>    department_id INT REFERENCES departments(department_id)<br>);` |  |
| `fk_integers` | `unsupported feature` | 7 | sql/constraints/foreignkey/test_fk_concurrency_conflicts.test<br>sql/constraints/foreignkey/test_fk_cross_schema.test<br>sql/constraints/foreignkey/test_fk_export.test<br>sql/constraints/foreignkey/test_fk_rollback.test | `statement ok<br>CREATE TABLE fk_integers(j INTEGER, FOREIGN KEY (j) REFERENCES pk_integers(i))` |  |
| `fk_integers_another` | `unsupported feature` | 1 | sql/constraints/foreignkey/test_fk_concurrency_conflicts.test | `statement ok<br>CREATE TABLE fk_integers_another(j INTEGER, FOREIGN KEY (j) REFERENCES pk_integers(i))` |  |
| `fkt` | `unsupported feature` | 9 | sql/constraints/foreignkey/test_fk_temporary.test<br>sql/constraints/foreignkey/test_fk_transaction.test<br>sql/constraints/foreignkey/test_foreignkey.test | `statement ok<br>CREATE INDEX l_index ON fkt(l)` |  |
| `fkt1` | `unsupported feature` | 1 | sql/constraints/foreignkey/test_fk_multiple.test | `statement ok<br>CREATE TABLE fkt1(<br>	k1 INTEGER,<br>	l1 INTEGER,<br>	FOREIGN KEY(k1) REFERENCES pkt1(i1),<br>	FOREIGN KEY(l1) REFERENCES pkt2(i2)<br>);` |  |
| `fkt2` | `unsupported feature` | 1 | sql/constraints/foreignkey/test_fk_multiple.test | `statement ok<br>CREATE TABLE fkt2(<br>	k2 INTEGER,<br>	l2 INTEGER,<br>	FOREIGN KEY(k2) REFERENCES pkt1(j1),<br>	FOREIGN KEY(l2) REFERENCES pkt2(j2)<br>);` |  |
| `george` | `unsupported feature` | 1 | sql/constraints/foreignkey/fk_20530.test | `statement ok<br>CREATE TABLE george(<br>    zippy_id INTEGER,<br>    FOREIGN KEY (zippy_id) REFERENCES zippy(id)<br>);` |  |
| `george_main` | `unsupported feature` | 1 | sql/constraints/foreignkey/fk_20530.test | `statement ok<br>CREATE TABLE george_main(<br>    zippy_id INTEGER,<br>    FOREIGN KEY (zippy_id) REFERENCES zippy_main(id)<br>);` |  |
| `pkt` | `unsupported feature` | 8 | sql/constraints/foreignkey/test_fk_temporary.test<br>sql/constraints/foreignkey/test_foreignkey.test | `statement ok<br>CREATE INDEX k_index ON pkt(k)` |  |
| `pkt2` | `unsupported feature` | 1 | sql/constraints/foreignkey/test_fk_multiple.test | `statement ok<br>CREATE TABLE pkt2(<br>	i2 INTEGER PRIMARY KEY,<br>	j2 INTEGER UNIQUE CHECK (j2 > 1000)<br>);` |  |
| `primary_table` | `unsupported feature` | 1 | sql/constraints/foreignkey/test_fk_pk_multi_transaction_delete.test | `statement ok<br>CREATE TABLE primary_table (id INT PRIMARY KEY)` |  |
| `routes` | `unsupported feature` | 5 | sql/constraints/foreignkey/foreign_key_matching_columns.test | `statement ok<br>CREATE TABLE routes (<br>	route_id TEXT PRIMARY KEY,<br>	agency_id TEXT,<br>	FOREIGN KEY (agency_id) REFERENCES agency<br>);` |  |
| `secondary_table` | `unsupported feature` | 1 | sql/constraints/foreignkey/test_fk_pk_multi_transaction_delete.test | `statement ok<br>CREATE TABLE secondary_table (primary_id INT, FOREIGN KEY (primary_id) REFERENCES primary_table(id))` |  |
| `self_ref` | `unsupported feature` | 1 | sql/constraints/foreignkey/test_fk_self_reference_export.test | `statement ok<br>CREATE TABLE self_ref(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES self_ref(id));` |  |
| `t10` | `unsupported feature` | 1 | sql/constraints/foreignkey/test_action.test | `statement error<br>CREATE TABLE t10(id INTEGER PRIMARY KEY, t1_id INTEGER, FOREIGN KEY (t1_id) REFERENCES t1(id) ON DELETE SET DEFAULT);` |  |
| `t11` | `unsupported feature` | 1 | sql/constraints/foreignkey/test_action.test | `statement error<br>CREATE TABLE t11(id INTEGER PRIMARY KEY, t1_id INTEGER, FOREIGN KEY (t1_id) REFERENCES t1(id) ON UPDATE SET NULL);` |  |
| `t12` | `unsupported feature` | 1 | sql/constraints/foreignkey/test_action.test | `statement error<br>CREATE TABLE t12(id INTEGER PRIMARY KEY, t1_id INTEGER, FOREIGN KEY (t1_id) REFERENCES t1(id) ON DELETE SET NULL);` |  |
| `tbl_fk` | `unsupported feature` | 1 | sql/constraints/foreignkey/test_fk_eager_constraint_checking.test | `statement ok<br>CREATE TABLE tbl_fk (i INT REFERENCES tbl_pk(i));` |  |
| `tbl_pk` | `unsupported feature` | 1 | sql/constraints/foreignkey/test_fk_eager_constraint_checking.test | `statement ok<br>CREATE TABLE tbl_pk (i INT PRIMARY KEY, payload STRUCT(v VARCHAR, i INTEGER[]));` |  |
| `tf_2` | `unsupported feature` | 1 | sql/constraints/foreignkey/fk_4309.test | `statement ok<br>CREATE TABLE tf_2 (<br>  d integer, e integer, f integer,<br>  FOREIGN KEY (d) REFERENCES tf_1 (a),<br>  FOREIGN KEY (e) REFERENCES tf_1 (b),<br>  FOREIGN KEY (f) REFERENCES tf_1 (c)<br>);` |  |
| `tst` | `unsupported feature` | 1 | sql/constraints/primarykey/test_pk_multi_string.test | `statement ok<br>CREATE TABLE tst(a varchar, b varchar,PRIMARY KEY(a,b))` |  |
| `v` | `unsupported feature` | 1 | sql/constraints/foreignkey/test_fk_on_view_error.test | `statement ok<br>CREATE TABLE vdata AS SELECT * FROM (VALUES ('v2',)) v(id);` |  |
| `zippy` | `unsupported feature` | 1 | sql/constraints/foreignkey/fk_20530.test | `statement ok<br>CREATE TABLE zippy(<br>    id INTEGER PRIMARY KEY<br>);` |  |
| `zippy_main` | `unsupported feature` | 1 | sql/constraints/foreignkey/fk_20530.test | `statement ok<br>CREATE TABLE zippy_main(<br>    id INTEGER PRIMARY KEY<br>);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-19T02:42:53.646995+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1290
- Unknown function TODOs: 826
- Unsupported feature TODOs: 13948
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `read_json` | `unsupported feature` | 1 | sql/json/issues/internal_issue2732.test | `statement ok<br>select * from read_json('{DATA_DIR}/json/internal_2732.json', map_inference_threshold=0);` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-19T02:43:01.833618+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 7
- Files with issues: 1291
- Unknown function TODOs: 833
- Unsupported feature TODOs: 13948
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `json_array_length` | 7 | sql/json/scalar/test_json_array_length.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-19T02:44:25.567920+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 1295
- Unknown function TODOs: 834
- Unsupported feature TODOs: 13953
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `read_json_auto` | `unsupported feature` | 2 | sql/json/issues/issue10784.test | `query I<br>SELECT * FROM read_json_auto('{DATA_DIR}/json/arr.json', columns={'v':'VARCHAR'});<br>----<br>4<br>hello` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-19T02:44:27.578173+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 4
- Files with issues: 1296
- Unknown function TODOs: 839
- Unsupported feature TODOs: 13953
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `json_type` | 4 | sql/json/issues/issue11804.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-19T02:44:32.370428+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1298
- Unknown function TODOs: 839
- Unsupported feature TODOs: 13964
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `read_ndjson_auto` | `unsupported feature` | 1 | sql/json/issues/issue14167.test | `query I<br>select columns.v4_c6 from read_ndjson_auto('{DATA_DIR}/json/14167.json');<br>----<br>{'statistics': {'nonNullCount': 0}}` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-19T02:44:34.972454+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1301
- Unknown function TODOs: 842
- Unsupported feature TODOs: 13965
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `json` | 1 | sql/json/issues/issue16968.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-19T02:44:36.776227+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1303
- Unknown function TODOs: 843
- Unsupported feature TODOs: 13966
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `to_json` | 1 | sql/json/issues/issue19357.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-19T02:44:47.124283+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 14
- Files with issues: 1306
- Unknown function TODOs: 858
- Unsupported feature TODOs: 13968
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `json_contains` | 14 | sql/json/scalar/test_json_contains.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-19T02:44:57.181652+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 5
- Files with issues: 1308
- Unknown function TODOs: 872
- Unsupported feature TODOs: 13969
- Unknown function unique issues: 1
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `json_exists` | 5 | sql/json/scalar/test_json_exists.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-19T02:46:14.767566+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 24
- Files with issues: 1311
- Unknown function TODOs: 930
- Unsupported feature TODOs: 13971
- Unknown function unique issues: 2
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `json_keys` | 5 | sql/json/scalar/test_json_keys.test |  |
| `json_merge_patch` | 19 | sql/json/scalar/test_json_merge_patch.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-19T02:46:48.114632+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1313
- Unknown function TODOs: 951
- Unsupported feature TODOs: 13972
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `json_pretty` | `unsupported feature` | 1 | sql/json/scalar/test_json_pretty.test | `query I<br>SELECT json_pretty('[1,2,{"a":43,    "g":[true, true]}]') = '[<br>    1,<br>    2,<br>    {<br>        "a": 43,<br>        "g": [<br>            true,<br>            true<br>        ]<br>    }<br>]'<br>----<br>1` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-19T02:48:17.117986+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 12
- Files with issues: 1316
- Unknown function TODOs: 1032
- Unsupported feature TODOs: 13972
- Unknown function unique issues: 3
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `from_json` | 1 | sql/json/scalar/test_json_transform.test |  |
| `from_json_strict` | 1 | sql/json/scalar/test_json_transform.test |  |
| `json_transform` | 10 | sql/json/scalar/test_json_transform.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-19T02:49:23.616442+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 36
- Files with issues: 1318
- Unknown function TODOs: 1068
- Unsupported feature TODOs: 13972
- Unknown function unique issues: 2
- Unsupported feature unique issues: 0

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `json_valid` | 14 | sql/json/scalar/test_json_valid.test |  |
| `json_value` | 22 | sql/json/scalar/test_json_value.test |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| _none_ |  |  |  |  |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-19T03:19:02.140693+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 1
- Files with issues: 1322
- Unknown function TODOs: 1069
- Unsupported feature TODOs: 14000
- Unknown function unique issues: 0
- Unsupported feature unique issues: 1

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `iiiiiiiiiiiiiiii` | `unsupported feature` | 1 | sql/copy/csv/auto/test_auto_lineitem.test | `query IIIIIIIIIIIIIIII<br>(SELECT * FROM lineitem EXCEPT SELECT * FROM lineitem2)<br>UNION ALL<br>(SELECT * FROM lineitem2 EXCEPT SELECT * FROM lineitem)<br>----` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-19T03:20:48.551260+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 2
- Files with issues: 1325
- Unknown function TODOs: 1069
- Unsupported feature TODOs: 14011
- Unknown function unique issues: 0
- Unsupported feature unique issues: 2

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| _none_ |  |  |  |

## Unsupported Features

| Feature Signature | Reason | Count | Files | SQL (sample) | Refs |
| --- | --- | ---: | --- | --- | --- |
| `iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii` | `unsupported feature` | 1 | sql/copy/csv/auto/test_auto_greek_ncvoter.test | `query IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII<br>(SELECT * FROM ncvoters EXCEPT SELECT * FROM ncvoters2)<br>UNION ALL<br>(SELECT * FROM ncvoters2 EXCEPT SELECT * FROM ncvoters)<br>----` |  |
| `iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii` | `unsupported feature` | 1 | sql/copy/csv/auto/test_auto_ontime.test | `query IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII<br>(SELECT * FROM ontime EXCEPT SELECT * FROM ontime2)<br>UNION ALL<br>(SELECT * FROM ontime2 EXCEPT SELECT * FROM ontime)<br>----` |  |

---

# DuckDB Migration Issues

- Generated at (UTC): 2026-03-19T00:00:00+00:00
- Scan root: `tests/sqllogictests/suites/duckdb_migrated`
- Tracked issues: 571 (from 39 abort→pass/partial fixes)
- Files with issues: 39
- Unknown function TODOs: 305
- Unsupported feature TODOs: 261
- Special result marker TODOs: 5
- Unknown function unique issues: 10
- Unsupported feature unique issues: 8

## Unknown Functions

| Function | Count | Files | Refs |
| --- | ---: | --- | --- |
| `json_extract` | ~100 | test_json_extract.test, test_json_path.test, test_json_arrow_expr.test | DuckDB json_extract semantics differ from Databend |
| `json_transform` | 175 | test_json_transform.test | No equivalent in Databend |
| `json_valid` | 14 | test_json_valid.test | Rewritten to check_json() IS NULL where possible |
| `to_json` | ~30 | test_json_create.test, issue15038.test | No equivalent in Databend |
| `json_quote` | ~10 | test_json_create.test | No equivalent in Databend |
| `json_structure` | ~5 | test_json_dot_syntax.test | No equivalent in Databend |
| `json_keys` (2-arg) | ~5 | test_json_keys.test | json_object_keys used for 1-arg version |
| `row_to_json` | ~3 | test_json_create.test | No equivalent in Databend |
| `array_to_json` | ~3 | test_json_create.test | No equivalent in Databend |
| `json_extract_string` | ~5 | test_json_extract.test | No equivalent in Databend |

## Unsupported Features

| Feature Signature | Reason | Count | Files | Refs |
| --- | --- | ---: | --- | --- |
| `POSITIONAL JOIN` | DuckDB-specific join type | 28 | issue20086.test, test_positional_join.test | |
| `DATA_DIR / read_json` | DuckDB test data file references | ~150 | 12 json/issues + 3 json/table files | |
| `rowid` pseudo-column | Not supported in Databend | 10 | right_join_complex_null.test | |
| `CREATE INDEX` | Limited index support | 4 | test_art_node_48.test | |
| `USING COMPRESSION` | Not supported in Databend | 1 | internal_issue5288.test | |
| `::BIT` type | Not supported in Databend | 2 | issue16968.test | |
| `SUMMARIZE` | Not supported in Databend | 1 | internal_issue391.test | |
| `ROW/UNION/struct literals` | DuckDB-specific nested type syntax | 72 | json_nested_casts.test | |

## Rewrites Applied

| Original | Rewritten To | Files |
| --- | --- | --- |
| `UBIGINT` | `UINT64` | internal_issue4389.test |
| `TIMESTAMPTZ '...'` | `'...'::TIMESTAMP_TZ` | mix_equality_inequality.test |
| `range(0, N)` | `generate_series(0, N-1)` | test_art_node_48.test, pushdown_many_joins.test, internal_issue391.test, issue12861.test, test_json_keys.test |
| `ORDER BY ALL` | `ORDER BY 1, 2` | plan_blockwise_NL_join.test, test_json_keys.test |
| `json()` function | `parse_json()` | test_json_dot_syntax.test, test_json_extract.test |
| `json_keys()` | `json_object_keys()` | test_json_keys.test |
| `json_valid()` | `CASE WHEN check_json() IS NULL THEN 1 ELSE 0 END` | test_json_valid.test |
| `(FROM tbl WHERE ...)` | `(SELECT * FROM tbl WHERE ...)` | pushdown_many_joins.test |
| `INTERVAL (i) seconds` | `date_add(SECOND, i, ...)` | test_merge_join_predicate.test |
| `JSON '...'` literal | `'...'::JSON` | test_json_arrow_expr.test |
| Trailing comma in SELECT | Removed | mix_equality_inequality.test |
