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

//! Compare production CSE with a hand-built plan that only materializes the small parent.
//! The manual plan is a pruning oracle: parsing, CSE and plan construction are
//! outside timing, and all three plans must produce identical outputs.
//!
//! cargo bench -p databend-common-sql --bench cse
//! CSE_MEMORY_ONLY=1 cargo bench -p databend-common-sql --bench cse
//! CSE_PAIRED_ONLY=1 cargo bench -p databend-common-sql --bench cse
//! Memory reporting uses Databend's 4 MiB buffered tracker, so peaks are
//! approximate; small cases may report zero even though they allocate memory.
//! Inputs/plans are created before tracking; reported peaks are execution-only,
//! not RSS or the sum of column memory_size() (which double-counts shared buffers).

use std::borrow::Cow;

use databend_common_base::mem_allocator::TrackingGlobalAllocator;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::MemStatBuffer;
use databend_common_base::runtime::ThreadTracker;
use databend_common_expression::ColumnRef;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::type_check;
use databend_common_expression::types::DataType;
use databend_common_expression::types::StringType;
use databend_common_expression_test_support::parse_raw_expr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::evaluator::apply_cse;

#[global_allocator]
static ALLOCATOR: TrackingGlobalAllocator = TrackingGlobalAllocator::create();

const ROWS: usize = 1024;
const SIZES: [usize; 3] = [256, 4096, 65536];

#[derive(Clone, Copy, Debug)]
enum Kind {
    String,
    Json,
}

struct Case {
    input: DataBlock,
    plans: [BlockOperator; 3],
    func_ctx: FunctionContext,
}

impl Case {
    fn new(kind: Kind, bytes: usize, chains: usize) -> Self {
        let strings: Vec<String> = (0..ROWS)
            .map(|row| match kind {
                Kind::String => format!("{row:016x}{}", "x".repeat(48)),
                Kind::Json => format!(r#"{{"id":{row},"payload":"{}"}}"#, "x".repeat(bytes)),
            })
            .collect();
        let input = DataBlock::new_from_columns(vec![StringType::from_data(strings)]);
        let parents: Vec<Expr> = (0..chains)
            .map(|chain| {
                // Different constants keep chains independent, while each complete
                // parent and its large child appear twice in the output list.
                let sql = match kind {
                    Kind::String => format!("octet_length(repeat(a, {}))", bytes / 64 + chain),
                    Kind::Json => format!(
                        "is_object(parse_json(concat(a, '{}')))",
                        " ".repeat(chain + 1)
                    ),
                };
                let raw = parse_raw_expr(&sql, &[("a", DataType::String)], &BUILTIN_FUNCTIONS);
                let expr = type_check::check(&raw, &BUILTIN_FUNCTIONS).unwrap();
                // Match the pipeline's folded input and avoid materializing
                // constant casts inserted by type checking.
                ConstantFolder::fold(
                    Cow::Owned(expr),
                    &FunctionContext::default(),
                    &BUILTIN_FUNCTIONS,
                )
                .0
                .into_owned()
            })
            .collect();
        let outputs: Vec<Expr> = parents
            .iter()
            .flat_map(|parent| [parent.clone(), parent.clone()])
            .collect();
        let raw = BlockOperator::Map {
            projections: Some((1..1 + outputs.len()).collect()),
            exprs: outputs,
        };
        let optimized = apply_cse(vec![raw.clone()], 1).pop().unwrap();
        let BlockOperator::Map { exprs, .. } = &optimized else {
            unreachable!()
        };
        // Pruning should retain only each repeated small parent.
        assert_eq!(exprs.len(), chains * 3);

        // Only keep the small repeated parent. Its nested large children are still
        // evaluated once, but their buffers can die within that evaluation.
        let mut pruned_exprs = parents.clone();
        for (chain, parent) in parents.iter().enumerate() {
            let reference = Expr::ColumnRef(ColumnRef {
                span: None,
                id: 1 + chain,
                data_type: parent.data_type().clone(),
                display_name: format!("parent_{chain}"),
            });
            pruned_exprs.extend([reference.clone(), reference]);
        }
        let pruned = BlockOperator::Map {
            exprs: pruned_exprs,
            projections: Some((1 + chains..1 + chains * 3).collect()),
        };
        let case = Self {
            input,
            plans: [raw, optimized, pruned],
            func_ctx: FunctionContext::default(),
        };
        let expected = case.run(0);
        for plan in [1, 2] {
            let actual = case.run(plan);
            assert_eq!(actual.num_columns(), expected.num_columns());
            assert_eq!(actual.num_rows(), expected.num_rows());
            for column in 0..expected.num_columns() {
                assert_eq!(
                    actual.get_by_offset(column).value(),
                    expected.get_by_offset(column).value(),
                    "{kind:?}, bytes={bytes}, chains={chains}, plan={plan}"
                );
            }
        }
        case.verify_pruned_plan(chains);
        case
    }

    // Production orders equal-size candidates via a HashMap, whereas the oracle
    // uses input order. Verify identical work, projections and output references
    // modulo that candidate permutation and temporary-column display names.
    fn verify_pruned_plan(&self, chains: usize) -> Vec<usize> {
        let BlockOperator::Map {
            exprs: optimized,
            projections: optimized_projection,
        } = &self.plans[1]
        else {
            unreachable!()
        };
        let BlockOperator::Map {
            exprs: manual,
            projections: manual_projection,
        } = &self.plans[2]
        else {
            unreachable!()
        };
        assert_eq!(optimized_projection, manual_projection);
        assert_eq!(optimized.len(), manual.len());
        let order: Vec<usize> = optimized[..chains]
            .iter()
            .map(|parent| {
                manual[..chains]
                    .iter()
                    .position(|expr| expr == parent)
                    .unwrap()
            })
            .collect();
        assert_eq!(
            order
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            chains
        );
        for (actual, expected) in optimized[chains..].iter().zip(&manual[chains..]) {
            let (Expr::ColumnRef(actual), Expr::ColumnRef(expected)) = (actual, expected) else {
                unreachable!()
            };
            assert_eq!(order[actual.id - 1] + 1, expected.id);
            assert_eq!(actual.data_type, expected.data_type);
        }
        order
    }

    fn run(&self, plan: usize) -> DataBlock {
        self.plans[plan]
            .execute(&self.func_ctx, self.input.clone())
            .unwrap()
    }

    fn peak_bytes(&self, plan: usize) -> i64 {
        let stat = MemStat::create("cse_bench".to_string());
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.mem_stat = Some(stat.clone());
        let _guard = ThreadTracker::tracking(payload);
        let result = self.run(plan);
        MemStatBuffer::current().flush::<false>(0).unwrap();
        let peak = stat.get_peak_memory_usage();
        drop(result);
        MemStatBuffer::current().flush::<false>(0).unwrap();
        peak
    }
}

fn main() {
    if std::env::var_os("CSE_MEMORY_ONLY").is_some() {
        println!("kind,rows,bytes,chains,plan,execution_peak_bytes");
        for kind in [Kind::String, Kind::Json] {
            for chains in [1, 8] {
                for bytes in SIZES {
                    let case = Case::new(kind, bytes, chains);
                    for (plan, name) in ["no_cse", "optimized", "manual_pruned"].iter().enumerate()
                    {
                        println!(
                            "{kind:?},{ROWS},{bytes},{chains},{name},{}",
                            case.peak_bytes(plan)
                        );
                    }
                }
            }
        }
    } else if std::env::var_os("CSE_PAIRED_ONLY").is_some() {
        paired_timing();
    } else {
        divan::main();
    }
}

// Reuse the same input/plans in one process and alternate AB/BA ordering.
// Construction, equality checks and printing are outside timing. Separate
// process runs still matter because candidate order is hash-dependent.
fn paired_timing() {
    println!("kind,rows,bytes,chains,sample,first,optimized_ns,manual_ns");
    for kind in [Kind::String, Kind::Json] {
        for chains in [1, 8] {
            let case = Case::new(kind, 65536, chains);
            eprintln!(
                "{kind:?}, chains={chains}, production candidate order={:?}",
                case.verify_pruned_plan(chains)
            );
            for _ in 0..4 {
                drop(divan::black_box(case.run(1)));
                drop(divan::black_box(case.run(2)));
            }
            for sample in 0..30 {
                let order = if sample % 2 == 0 { [1, 2] } else { [2, 1] };
                let mut elapsed = [0; 2];
                for plan in order {
                    let start = std::time::Instant::now();
                    drop(divan::black_box(case.run(plan)));
                    elapsed[plan - 1] = start.elapsed().as_nanos();
                }
                println!(
                    "{kind:?},{ROWS},65536,{chains},{sample},{},{},{}",
                    order[0], elapsed[0], elapsed[1]
                );
            }
        }
    }
}

fn bench(bencher: divan::Bencher, kind: Kind, bytes: usize, chains: usize, plan: usize) {
    let case = Case::new(kind, bytes, chains);
    bencher.bench_local(|| {
        // Include output destruction, consistently across all plans.
        drop(divan::black_box(case.run(plan)));
    });
}

#[divan::bench_group(max_time = 1)]
mod string {
    use super::*;

    #[divan::bench(consts = [1, 8], args = SIZES)]
    fn no_cse<const CHAINS: usize>(bencher: divan::Bencher, bytes: usize) {
        bench(bencher, Kind::String, bytes, CHAINS, 0);
    }

    #[divan::bench(consts = [1, 8], args = SIZES)]
    fn optimized<const CHAINS: usize>(bencher: divan::Bencher, bytes: usize) {
        bench(bencher, Kind::String, bytes, CHAINS, 1);
    }

    #[divan::bench(consts = [1, 8], args = SIZES)]
    fn manual_pruned<const CHAINS: usize>(bencher: divan::Bencher, bytes: usize) {
        bench(bencher, Kind::String, bytes, CHAINS, 2);
    }
}

#[divan::bench_group(max_time = 1)]
mod json {
    use super::*;

    #[divan::bench(consts = [1, 8], args = SIZES)]
    fn no_cse<const CHAINS: usize>(bencher: divan::Bencher, bytes: usize) {
        bench(bencher, Kind::Json, bytes, CHAINS, 0);
    }

    #[divan::bench(consts = [1, 8], args = SIZES)]
    fn optimized<const CHAINS: usize>(bencher: divan::Bencher, bytes: usize) {
        bench(bencher, Kind::Json, bytes, CHAINS, 1);
    }

    #[divan::bench(consts = [1, 8], args = SIZES)]
    fn manual_pruned<const CHAINS: usize>(bencher: divan::Bencher, bytes: usize) {
        bench(bencher, Kind::Json, bytes, CHAINS, 2);
    }
}
