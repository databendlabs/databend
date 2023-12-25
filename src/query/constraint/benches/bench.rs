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

#[macro_use]
extern crate criterion;

use criterion::black_box;
use criterion::Criterion;
use databend_common_constraint::mir::MirBinaryOperator;
use databend_common_constraint::mir::MirConstant;
use databend_common_constraint::mir::MirDataType;
use databend_common_constraint::mir::MirExpr;
use databend_common_constraint::problem::variable_must_not_null;
use databend_common_constraint::simplify::simplify;

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_solver");
    group.sample_size(10);

    group.bench_function("solve_variable_not_null", |b| {
        b.iter(|| {
            // a > 0 and a < 1 => a is not null
            let assertion = MirExpr::BinaryOperator {
                op: MirBinaryOperator::And,
                left: Box::new(MirExpr::BinaryOperator {
                    op: MirBinaryOperator::Gt,
                    left: Box::new(MirExpr::Variable {
                        name: "a".to_string(),
                        data_type: MirDataType::Int,
                    }),
                    right: Box::new(MirExpr::Constant(MirConstant::Int(0))),
                }),
                right: Box::new(MirExpr::BinaryOperator {
                    op: MirBinaryOperator::Lt,
                    left: Box::new(MirExpr::Variable {
                        name: "a".to_string(),
                        data_type: MirDataType::Int,
                    }),
                    right: Box::new(MirExpr::Constant(MirConstant::Int(1))),
                }),
            };
            let result = variable_must_not_null(&assertion, "a");
            black_box(result);
        });
    });

    group.bench_function("solve_variable_not_null_multiple_variables", |b| {
        b.iter(|| {
            // (a > 0 and b > 0) or (a < 0 and b < 0) => a is not null
            let assertion = MirExpr::BinaryOperator {
                op: MirBinaryOperator::Or,
                left: Box::new(MirExpr::BinaryOperator {
                    op: MirBinaryOperator::And,
                    left: Box::new(MirExpr::BinaryOperator {
                        op: MirBinaryOperator::Gt,
                        left: Box::new(MirExpr::Variable {
                            name: "a".to_string(),
                            data_type: MirDataType::Int,
                        }),
                        right: Box::new(MirExpr::Constant(MirConstant::Int(0))),
                    }),
                    right: Box::new(MirExpr::BinaryOperator {
                        op: MirBinaryOperator::Gt,
                        left: Box::new(MirExpr::Variable {
                            name: "b".to_string(),
                            data_type: MirDataType::Int,
                        }),
                        right: Box::new(MirExpr::Constant(MirConstant::Int(0))),
                    }),
                }),
                right: Box::new(MirExpr::BinaryOperator {
                    op: MirBinaryOperator::And,
                    left: Box::new(MirExpr::BinaryOperator {
                        op: MirBinaryOperator::Lt,
                        left: Box::new(MirExpr::Variable {
                            name: "a".to_string(),
                            data_type: MirDataType::Int,
                        }),
                        right: Box::new(MirExpr::Constant(MirConstant::Int(0))),
                    }),
                    right: Box::new(MirExpr::BinaryOperator {
                        op: MirBinaryOperator::Lt,
                        left: Box::new(MirExpr::Variable {
                            name: "b".to_string(),
                            data_type: MirDataType::Int,
                        }),
                        right: Box::new(MirExpr::Constant(MirConstant::Int(0))),
                    }),
                }),
            };
            let result = variable_must_not_null(&assertion, "a");
            black_box(result);
        });
    });

    group.bench_function("simplify", |b| {
        b.iter(|| {
            // a + b < 1 and a >= 0
            let expr = MirExpr::BinaryOperator {
                op: MirBinaryOperator::And,
                left: Box::new(MirExpr::BinaryOperator {
                    op: MirBinaryOperator::Lt,
                    left: Box::new(MirExpr::BinaryOperator {
                        op: MirBinaryOperator::Plus,
                        left: Box::new(MirExpr::Variable {
                            name: "a".to_string(),
                            data_type: MirDataType::Int,
                        }),
                        right: Box::new(MirExpr::Variable {
                            name: "b".to_string(),
                            data_type: MirDataType::Int,
                        }),
                    }),
                    right: Box::new(MirExpr::Constant(MirConstant::Int(1))),
                }),
                right: Box::new(MirExpr::BinaryOperator {
                    op: MirBinaryOperator::Gte,
                    left: Box::new(MirExpr::Variable {
                        name: "a".to_string(),
                        data_type: MirDataType::Int,
                    }),
                    right: Box::new(MirExpr::Constant(MirConstant::Int(0))),
                }),
            };
            let simplified = simplify(&expr).unwrap();
            black_box(simplified);
        });
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
