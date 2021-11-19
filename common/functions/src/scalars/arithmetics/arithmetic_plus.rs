// Copyright 2020 Datafuse Labs.
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

use common_datavalues::DataValueArithmeticOperator;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::function::MonotonicityNode;
use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::ArithmeticFunction;
use crate::scalars::Function;
use crate::scalars::Monotonicity;
use crate::scalars::Range;

pub struct ArithmeticPlusFunction;

impl ArithmeticPlusFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        ArithmeticFunction::try_create_func(DataValueArithmeticOperator::Plus)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic())
    }

    pub fn get_plus_minus_binary_operation_monotonicity(
        op: DataValueArithmeticOperator,
        left: MonotonicityNode,
        right: MonotonicityNode,
    ) -> Result<MonotonicityNode> {
        let func = ArithmeticFunction::new(op.clone());

        match (left, right) {
            // 1. f(x) +/- 12, a function plus/minus a constant value. The monotonicity should be the same as f(x)
            (
                MonotonicityNode::Function(mono, Range { begin, end }),
                MonotonicityNode::Constant(val),
            ) => {
                if !mono.is_monotonic {
                    // not a monotonic function, just return
                    return Ok(MonotonicityNode::Function(Monotonicity::default(), Range {
                        begin: None,
                        end: None,
                    }));
                }

                Ok(MonotonicityNode::Function(mono, Range {
                    begin: func.eval_range_boundary(&[begin, Some(val.clone())])?,
                    end: func.eval_range_boundary(&[end, Some(val)])?,
                }))
            }

            // 2. f(x) +/- x, a function plus/minus the variable.
            (
                MonotonicityNode::Function(
                    mono,
                    Range {
                        begin: begin1,
                        end: end1,
                    },
                ),
                MonotonicityNode::Variable(
                    _column_name,
                    Range {
                        begin: begin2,
                        end: end2,
                    },
                ),
            ) => {
                if !mono.is_monotonic {
                    return Ok(MonotonicityNode::Function(Monotonicity::default(), Range {
                        begin: None,
                        end: None,
                    }));
                }

                match op {
                    // f(x) + x
                    DataValueArithmeticOperator::Plus => {
                        if mono.is_positive {
                            Ok(MonotonicityNode::Function(
                                Monotonicity {
                                    is_monotonic: true,
                                    is_positive: true,
                                },
                                Range {
                                    begin: func.eval_range_boundary(&[begin1, begin2])?,
                                    end: func.eval_range_boundary(&[end1, end2])?,
                                },
                            ))
                        } else {
                            // not monotonic any more
                            Ok(MonotonicityNode::Function(Monotonicity::default(), Range {
                                begin: None,
                                end: None,
                            }))
                        }
                    }
                    // f(x) - x
                    _ => {
                        if !mono.is_positive {
                            Ok(MonotonicityNode::Function(
                                Monotonicity {
                                    is_monotonic: true,
                                    is_positive: false,
                                },
                                Range {
                                    // the begin should be the begin/min of f(x) minus the end/max of x
                                    begin: func.eval_range_boundary(&[begin1, end2])?,
                                    // the end should be the end/max of f(x) minus the begin/min of x
                                    end: func.eval_range_boundary(&[end1, begin2])?,
                                },
                            ))
                        } else {
                            // not monotonic any more
                            Ok(MonotonicityNode::Function(Monotonicity::default(), Range {
                                begin: None,
                                end: None,
                            }))
                        }
                    }
                }
            }

            // 3. f(x) +/- g(x), a function plus/minus another. If they have same monotonicity, then should return that monotonicity
            (
                MonotonicityNode::Function(
                    mono1,
                    Range {
                        begin: begin1,
                        end: end1,
                    },
                ),
                MonotonicityNode::Function(
                    mono2,
                    Range {
                        begin: begin2,
                        end: end2,
                    },
                ),
            ) => {
                if !mono1.is_monotonic || !mono2.is_monotonic {
                    // not a monotonic function, just return
                    return Ok(MonotonicityNode::Function(Monotonicity::default(), Range {
                        begin: None,
                        end: None,
                    }));
                }

                match op {
                    // f(x) + g(x)
                    DataValueArithmeticOperator::Plus => {
                        if mono1.is_positive == mono2.is_positive {
                            Ok(MonotonicityNode::Function(
                                Monotonicity {
                                    is_monotonic: true,
                                    is_positive: mono1.is_positive,
                                },
                                Range {
                                    begin: func.eval_range_boundary(&[begin1, begin2])?,
                                    end: func.eval_range_boundary(&[end1, end2])?,
                                },
                            ))
                        } else {
                            // unknown monotonicity
                            Ok(MonotonicityNode::Function(Monotonicity::default(), Range {
                                begin: None,
                                end: None,
                            }))
                        }
                    }
                    // f(x) - g(x)
                    _ => {
                        if mono1.is_positive != mono2.is_positive {
                            Ok(MonotonicityNode::Function(mono1, Range {
                                begin: func.eval_range_boundary(&[begin1, end2])?,
                                end: func.eval_range_boundary(&[end1, begin2])?,
                            }))
                        } else {
                            // unknown monotonicity
                            Ok(MonotonicityNode::Function(Monotonicity::default(), Range {
                                begin: None,
                                end: None,
                            }))
                        }
                    }
                }
            }

            // 4. 1234 +/- f(x), a constant value plus/minus a function.
            // The monotonicity is_positive should be the same as f(x) for '+' or flipping is_positive for '-'.
            (
                MonotonicityNode::Constant(val),
                MonotonicityNode::Function(mono, Range { begin, end }),
            ) => {
                if !mono.is_monotonic {
                    // not a monotonic function, just return
                    return Ok(MonotonicityNode::Function(Monotonicity::default(), Range {
                        begin: None,
                        end: None,
                    }));
                }

                match op {
                    DataValueArithmeticOperator::Plus => Ok(MonotonicityNode::Function(
                        Monotonicity {
                            is_monotonic: true,
                            is_positive: mono.is_positive,
                        },
                        Range {
                            begin: func.eval_range_boundary(&[Some(val.clone()), begin])?,
                            end: func.eval_range_boundary(&[Some(val), end])?,
                        },
                    )),
                    _ => Ok(MonotonicityNode::Function(
                        Monotonicity {
                            is_monotonic: true,
                            is_positive: !mono.is_positive,
                        },
                        Range {
                            begin: func.eval_range_boundary(&[Some(val.clone()), end])?,
                            end: func.eval_range_boundary(&[Some(val), begin])?,
                        },
                    )),
                }
            }

            // 5. 1234 +/- x, a constant value plus/minus a variable.
            // The monotonicity is_positive should be the true for '+', false for '-'.
            (
                MonotonicityNode::Constant(val),
                MonotonicityNode::Variable(_column_name, Range { begin, end }),
            ) => match op {
                DataValueArithmeticOperator::Plus => Ok(MonotonicityNode::Function(
                    Monotonicity {
                        is_monotonic: true,
                        is_positive: true,
                    },
                    Range {
                        begin: func.eval_range_boundary(&[Some(val.clone()), begin])?,
                        end: func.eval_range_boundary(&[Some(val), end])?,
                    },
                )),
                _ => Ok(MonotonicityNode::Function(
                    Monotonicity {
                        is_monotonic: true,
                        is_positive: false,
                    },
                    Range {
                        begin: func.eval_range_boundary(&[Some(val.clone()), end])?,
                        end: func.eval_range_boundary(&[Some(val), begin])?,
                    },
                )),
            },

            // 6. 1234 +/- 5678, a constant value plus/minus a constant.
            // This should not happen if the constant folding optimizer works well. But no harm to implement it here.
            (MonotonicityNode::Constant(val1), MonotonicityNode::Constant(val2)) => {
                let val = func.eval_range_boundary(&[Some(val1), Some(val2)])?;
                Ok(MonotonicityNode::Constant(val.unwrap()))
            }

            // 7. x +/- 12, a variable plus/minus a constant
            (
                MonotonicityNode::Variable(_column_name, Range { begin, end }),
                MonotonicityNode::Constant(val),
            ) => Ok(MonotonicityNode::Function(
                Monotonicity {
                    is_monotonic: true,
                    is_positive: true,
                },
                Range {
                    begin: func.eval_range_boundary(&[begin, Some(val.clone())])?,
                    end: func.eval_range_boundary(&[end, Some(val)])?,
                },
            )),

            // 8. x +/- x, two variable plus/minus (we assume only one variable exists), should be monotonically increasing
            (
                MonotonicityNode::Variable(
                    _var1,
                    Range {
                        begin: begin1,
                        end: end1,
                    },
                ),
                MonotonicityNode::Variable(
                    _var2,
                    Range {
                        begin: begin2,
                        end: end2,
                    },
                ),
            ) => match op {
                DataValueArithmeticOperator::Plus => Ok(MonotonicityNode::Function(
                    Monotonicity {
                        is_monotonic: true,
                        is_positive: true,
                    },
                    Range {
                        begin: func.eval_range_boundary(&[begin1, begin2])?,
                        end: func.eval_range_boundary(&[end1, end2])?,
                    },
                )),
                // TODO: for expression like x-x, should return zero. But need to figure out the DataValue type.
                _ => Ok(MonotonicityNode::Function(Monotonicity::default(), Range {
                    begin: None,
                    end: None,
                })),
            },

            // 9. x +/- f(x), a variable plus/minus a function
            (
                MonotonicityNode::Variable(
                    _column_name,
                    Range {
                        begin: begin1,
                        end: end1,
                    },
                ),
                MonotonicityNode::Function(
                    mono,
                    Range {
                        begin: begin2,
                        end: end2,
                    },
                ),
            ) => {
                if !mono.is_monotonic {
                    return Ok(MonotonicityNode::Function(Monotonicity::default(), Range {
                        begin: None,
                        end: None,
                    }));
                }

                match op {
                    // x + f(x), should be also monotonically increasing if f(x) is so.
                    DataValueArithmeticOperator::Plus => {
                        if !mono.is_positive {
                            Ok(MonotonicityNode::Function(Monotonicity::default(), Range {
                                begin: None,
                                end: None,
                            }))
                        } else {
                            Ok(MonotonicityNode::Function(
                                Monotonicity {
                                    is_monotonic: true,
                                    is_positive: true,
                                },
                                Range {
                                    begin: func.eval_range_boundary(&[begin1, begin2])?,
                                    end: func.eval_range_boundary(&[end1, end2])?,
                                },
                            ))
                        }
                    }
                    // x - f(x), should be also monotonically increasing if f(x) is monotonically decreasing.
                    _ => {
                        if mono.is_positive {
                            Ok(MonotonicityNode::Function(Monotonicity::default(), Range {
                                begin: None,
                                end: None,
                            }))
                        } else {
                            Ok(MonotonicityNode::Function(
                                Monotonicity {
                                    is_monotonic: true,
                                    is_positive: true,
                                },
                                Range {
                                    begin: func.eval_range_boundary(&[begin1, end2])?,
                                    end: func.eval_range_boundary(&[end1, begin2])?,
                                },
                            ))
                        }
                    }
                }
            }
        }
    }

    pub fn get_monotonicity(args: &[MonotonicityNode]) -> Result<MonotonicityNode> {
        if args.is_empty() || args.len() > 2 {
            return Err(ErrorCode::BadArguments(format!(
                "Invalid argument lengths {} for get_monotonicity",
                args.len()
            )));
        }

        if args.len() == 1 {
            return Ok(args[0].clone());
        }

        Self::get_plus_minus_binary_operation_monotonicity(
            DataValueArithmeticOperator::Plus,
            args[0].clone(),
            args[1].clone(),
        )
    }
}
