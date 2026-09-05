// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

fn main() {
    divan::main();
}

/// Benchmark the per-block cost of `RangeIndex::apply`.
///
/// The workload mirrors a production pruning scenario: a table clustered by
/// `site_code`, blocks carrying statistics for 27 columns, and a pushed-down
/// filter `site_code = '2815' AND contains([N x int64], account)` evaluated
/// against 930 blocks of which only 3 survive range pruning.
///
/// 927 of 930 blocks short-circuit on the first conjunct, so the IN list is
/// never evaluated on that path; benches are parameterized over the IN size
/// (200 = production shape, 2000 = 10x) to make that visible.
#[divan::bench_group(max_time = 2)]
mod range_index_apply {
    use std::sync::Arc;

    use databend_common_expression::ColumnRef;
    use databend_common_expression::Constant;
    use databend_common_expression::Expr;
    use databend_common_expression::FromData;
    use databend_common_expression::FunctionContext;
    use databend_common_expression::Scalar;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::type_check::check_function;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::Int64Type;
    use databend_common_expression::types::NumberDataType;
    use databend_common_functions::BUILTIN_FUNCTIONS;
    use databend_storages_common_index::RangeIndex;
    use databend_storages_common_table_meta::meta::ColumnStatistics;
    use databend_storages_common_table_meta::meta::StatisticsOfColumns;

    const BLOCKS: usize = 930;
    const MATCHING_BLOCKS: usize = 3;
    /// Column count of the production MV; every block carries statistics for
    /// all of them even though the predicate only references two.
    const STAT_COLUMNS: usize = 27;
    const IN_SIZES: [usize; 2] = [200, 2000];

    fn schema() -> Arc<TableSchema> {
        let mut fields = vec![
            TableField::new("site_code", TableDataType::String),
            TableField::new("account", TableDataType::Number(NumberDataType::Int64)),
        ];
        for i in 0..STAT_COLUMNS - 2 {
            fields.push(TableField::new(
                &format!("m{i}"),
                TableDataType::Number(NumberDataType::Int64),
            ));
        }
        Arc::new(TableSchema::new(fields))
    }

    /// `site_code = '2815' and contains([..in_len ids..], account)`, combined
    /// with `and_filters` exactly like the storage pushdown does.
    fn predicate(in_len: usize) -> Expr<String> {
        let site_code = Expr::ColumnRef(ColumnRef {
            span: None,
            id: "site_code".to_string(),
            data_type: DataType::String,
            display_name: "site_code".to_string(),
        });
        let account = Expr::ColumnRef(ColumnRef {
            span: None,
            id: "account".to_string(),
            data_type: DataType::Number(NumberDataType::Int64),
            display_name: "account".to_string(),
        });
        let target = Expr::Constant(Constant {
            span: None,
            scalar: Scalar::String("2815".to_string()),
            data_type: DataType::String,
        });
        let ids: Vec<i64> = (0..in_len as i64)
            .map(|i| 100_000_000 + i * 4_000_000)
            .collect();
        let in_list = Expr::Constant(Constant {
            span: None,
            scalar: Scalar::Array(Int64Type::from_data(ids)),
            data_type: DataType::Array(Box::new(DataType::Number(NumberDataType::Int64))),
        });

        let eq = check_function(None, "eq", &[], &[site_code, target], &BUILTIN_FUNCTIONS).unwrap();
        let contains = check_function(
            None,
            "contains",
            &[],
            &[in_list, account],
            &BUILTIN_FUNCTIONS,
        )
        .unwrap();
        check_function(
            None,
            "and_filters",
            &[],
            &[eq, contains],
            &BUILTIN_FUNCTIONS,
        )
        .unwrap()
    }

    /// Statistics for all 27 columns per block, like a production BlockMeta.
    fn block_stats(schema: &TableSchema) -> Vec<StatisticsOfColumns> {
        let site_code_id = schema.leaf_columns_of(&"site_code".to_string())[0];
        (0..BLOCKS)
            .map(|i| {
                // Clustered layout: only the first MATCHING_BLOCKS blocks cover '2815'.
                let (min, max) = if i < MATCHING_BLOCKS {
                    ("2800".to_string(), "2900".to_string())
                } else {
                    (format!("{:04}", 3000 + i), format!("{:04}", 3001 + i))
                };
                let mut stats = StatisticsOfColumns::default();
                stats.insert(
                    site_code_id,
                    ColumnStatistics::new(Scalar::String(min), Scalar::String(max), 0, 0, None),
                );
                for field in schema.fields().iter().skip(1) {
                    stats.insert(
                        field.column_id(),
                        ColumnStatistics::new(
                            Scalar::Number(100_000_000i64.into()),
                            Scalar::Number(999_999_999i64.into()),
                            0,
                            0,
                            None,
                        ),
                    );
                }
                stats
            })
            .collect()
    }

    fn build_index(in_len: usize) -> (RangeIndex, Vec<StatisticsOfColumns>) {
        let schema = schema();
        let expr = predicate(in_len);
        let index = RangeIndex::try_create(
            FunctionContext::default(),
            &expr,
            schema.clone(),
            Default::default(),
        )
        .unwrap();
        let blocks = block_stats(&schema);
        let kept = blocks
            .iter()
            .filter(|stats| index.apply(stats, None, |_| false).unwrap())
            .count();
        assert_eq!(kept, MATCHING_BLOCKS);
        (index, blocks)
    }

    /// A full sweep over 930 blocks: the wall time the pruner spends on
    /// `should_keep` for one segment of this shape (production IN size).
    #[divan::bench]
    fn sweep_930_blocks(bencher: divan::Bencher) {
        let (index, blocks) = build_index(200);
        bencher.bench(|| {
            let mut kept = 0usize;
            for stats in &blocks {
                if index
                    .apply(divan::black_box(stats), None, |_| false)
                    .unwrap()
                {
                    kept += 1;
                }
            }
            assert_eq!(kept, MATCHING_BLOCKS);
            kept
        });
    }

    /// Per-block cost when the first conjunct short-circuits (927 of 930
    /// blocks). The IN list must not affect this path.
    #[divan::bench(args = IN_SIZES)]
    fn single_block_short_circuit(bencher: divan::Bencher, in_len: usize) {
        let (index, blocks) = build_index(in_len);
        let stats = blocks.last().unwrap();
        assert!(!index.apply(stats, None, |_| false).unwrap());
        bencher.bench(|| {
            index
                .apply(divan::black_box(stats), None, |_| false)
                .unwrap()
        });
    }

    /// Per-block cost when the block survives and the full predicate,
    /// including the IN list, is evaluated.
    #[divan::bench(args = IN_SIZES)]
    fn single_block_kept(bencher: divan::Bencher, in_len: usize) {
        let (index, blocks) = build_index(in_len);
        let stats = blocks.first().unwrap();
        assert!(index.apply(stats, None, |_| false).unwrap());
        bencher.bench(|| {
            index
                .apply(divan::black_box(stats), None, |_| false)
                .unwrap()
        });
    }

    /// Cost of building the index itself; work moved from `apply` to
    /// `try_create` must stay visible here.
    #[divan::bench(args = IN_SIZES)]
    fn try_create(bencher: divan::Bencher, in_len: usize) {
        let schema = schema();
        let expr = predicate(in_len);
        bencher.bench(|| {
            RangeIndex::try_create(
                FunctionContext::default(),
                divan::black_box(&expr),
                schema.clone(),
                Default::default(),
            )
            .unwrap()
        });
    }
}
