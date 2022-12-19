// Copyright 2022 Datafuse Labs.
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

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

use common_datavalues::BooleanType;
use common_datavalues::DataTypeImpl;
use common_datavalues::DataValue;
use common_exception::Result;

use crate::binder::split_conjunctions;
use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::AndExpr;
use crate::plans::ConstantExpr;
use crate::plans::Filter;
use crate::plans::OrExpr;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::Scalar;
use crate::ScalarExpr;

#[derive(Clone, PartialEq, Debug)]
enum PredicateScalar {
    And { args: Vec<PredicateScalar> },
    Or { args: Vec<PredicateScalar> },
    Other { expr: Box<Scalar> },
}

fn predicate_scalar(scalar: &Scalar) -> PredicateScalar {
    match scalar {
        Scalar::AndExpr(and_expr) => {
            let args = vec![
                predicate_scalar(&and_expr.left),
                predicate_scalar(&and_expr.right),
            ];
            PredicateScalar::And { args }
        }
        Scalar::OrExpr(or_expr) => {
            let args = vec![
                predicate_scalar(&or_expr.left),
                predicate_scalar(&or_expr.right),
            ];
            PredicateScalar::Or { args }
        }
        _ => PredicateScalar::Other {
            expr: Box::from(scalar.clone()),
        },
    }
}

fn normalize_predicate_scalar(
    predicate_scalar: PredicateScalar,
    return_type: DataTypeImpl,
) -> Scalar {
    match predicate_scalar {
        PredicateScalar::And { args } => {
            assert!(args.len() >= 2);
            args.iter()
                .map(|arg| normalize_predicate_scalar(arg.clone(), return_type.clone()))
                .reduce(|lhs, rhs| {
                    Scalar::AndExpr(AndExpr {
                        left: Box::from(lhs),
                        right: Box::from(rhs),
                        return_type: Box::new(return_type.clone()),
                    })
                })
                .expect("has at least two args")
        }
        PredicateScalar::Or { args } => {
            assert!(args.len() >= 2);
            args.iter()
                .map(|arg| normalize_predicate_scalar(arg.clone(), return_type.clone()))
                .reduce(|lhs, rhs| {
                    Scalar::OrExpr(OrExpr {
                        left: Box::from(lhs),
                        right: Box::from(rhs),
                        return_type: Box::new(return_type.clone()),
                    })
                })
                .expect("has at least two args")
        }
        PredicateScalar::Other { expr } => *expr,
    }
}

// The rule tries to apply the inverse OR distributive law to the predicate.
// ((A AND B) OR (A AND C))  =>  (A AND (B OR C))
// It'll find all OR expressions and extract the common terms.
pub struct RuleNormalizeDisjunctiveFilter {
    id: RuleID,
    pattern: SExpr,
}

impl RuleNormalizeDisjunctiveFilter {
    pub fn new() -> Self {
        Self {
            id: RuleID::NormalizeDisjunctiveFilter,
            // Filter
            //  \
            //   *
            pattern: SExpr::create_unary(
                PatternPlan {
                    plan_type: RelOp::Filter,
                }
                .into(),
                SExpr::create_leaf(
                    PatternPlan {
                        plan_type: RelOp::Pattern,
                    }
                    .into(),
                ),
            ),
        }
    }
}

impl Rule for RuleNormalizeDisjunctiveFilter {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let filter: Filter = s_expr.plan().clone().try_into()?;
        let predicates = filter.predicates;
        let mut rewritten_predicates = Vec::with_capacity(predicates.len());
        let mut rewritten = false;
        for predicate in predicates.iter() {
            let predicate_scalar = predicate_scalar(predicate);
            let (rewritten_predicate_scalar, has_rewritten) =
                rewrite_predicate_ors(predicate_scalar);
            if has_rewritten {
                rewritten = true;
            }
            rewritten_predicates.push(normalize_predicate_scalar(
                rewritten_predicate_scalar,
                predicate.data_type(),
            ));
        }
        let mut split_predicates: Vec<Scalar> = Vec::with_capacity(rewritten_predicates.len());
        for predicate in rewritten_predicates.iter() {
            split_predicates.extend_from_slice(&split_conjunctions(predicate));
        }
        if rewritten {
            state.add_result(SExpr::create_unary(
                Filter {
                    predicates: split_predicates,
                    is_having: filter.is_having,
                }
                .into(),
                s_expr.child(0)?.clone(),
            ));
        }
        Ok(())
    }

    fn pattern(&self) -> &SExpr {
        &self.pattern
    }
}

// rewrite in post order traversal
fn rewrite_predicate_ors(predicate: PredicateScalar) -> (PredicateScalar, bool) {
    rewrite_predicate_ors_impl(predicate)
}

fn rewrite_predicate_ors_impl(predicate: PredicateScalar) -> (PredicateScalar, bool) {
    // runtime stack frame
    struct Frame<'a> {
        pub predicate: &'a PredicateScalar,
        pub args: Vec<PredicateScalar>,
        pub parent: Option<Rc<RefCell<Frame<'a>>>>,
        pub visited: bool,
    }

    // build runtime stack
    let mut stack: VecDeque<Rc<RefCell<Frame>>> = VecDeque::new();
    // push first predicate
    let root = Frame {
        predicate: &predicate,
        args: Vec::new(),
        parent: None,
        visited: false,
    };
    stack.push_back(Rc::new(RefCell::new(root)));

    while let Some(frame) = stack.pop_back() {
        if frame.borrow().visited {
            // end of this level of iteration
            // all of its children should already optimized
            match frame.borrow().predicate {
                PredicateScalar::And { .. } => {
                    let and_args: Vec<PredicateScalar> = (frame.borrow().args).clone();
                    let and_args = flatten_ands(and_args).into_iter().rev().into();
                    match &frame.borrow().parent {
                        Some(parent) => {
                            parent
                                .borrow_mut()
                                .args
                                .push(PredicateScalar::And { args: and_args });
                            continue;
                        }
                        None => {
                            // root expr, return
                            return (PredicateScalar::And { args: and_args }, false);
                        }
                    }
                }
                PredicateScalar::Or { .. } => {
                    let or_args: Vec<PredicateScalar> = (frame.borrow().args).clone();
                    let or_args = flatten_ors(or_args).into_iter().rev().into();
                    let (pred, optimized) = process_duplicate_or_exprs(&or_args);
                    match &frame.borrow().parent {
                        Some(parent) => {
                            parent.borrow_mut().args.push(pred);
                            continue;
                        }
                        None => {
                            // root expr, return
                            return (pred, optimized);
                        }
                    }
                }
                // should be unreachable
                PredicateScalar::Other { .. } => unreachable!(),
            }
        }
        {
            frame.borrow_mut().visited = true;
        }
        match frame.borrow().predicate {
            PredicateScalar::And { args } => {
                stack.push_back(frame.clone());
                for arg in args.iter() {
                    let child_frame = Frame {
                        predicate: arg,
                        args: Vec::new(),
                        parent: Some(frame.clone()),
                        visited: false,
                    };
                    stack.push_back(Rc::new(RefCell::new(child_frame)));
                }
            }
            PredicateScalar::Or { args } => {
                stack.push_back(frame.clone());
                for arg in args.iter() {
                    let child_frame = Frame {
                        predicate: arg,
                        args: Vec::new(),
                        parent: Some(frame.clone()),
                        visited: false,
                    };
                    stack.push_back(Rc::new(RefCell::new(child_frame)));
                }
            }
            PredicateScalar::Other { .. } => {
                let frame = frame.borrow();
                match &frame.parent {
                    Some(parent) => {
                        parent.borrow_mut().args.push(frame.predicate.clone());
                    }
                    None => {
                        // is root expr, return directly
                        return (frame.predicate.clone(), false);
                    }
                }
            }
        }
    }
    unreachable!();
}

// Flatten the OR expressions.
fn flatten_ors(or_args: impl IntoIterator<Item = PredicateScalar>) -> Vec<PredicateScalar> {
    let mut flattened_ors = vec![];
    let mut stack: VecDeque<PredicateScalar> = VecDeque::from_iter(or_args.into_iter());
    while let Some(or_arg) = stack.pop_back() {
        match or_arg {
            PredicateScalar::Or { args } => stack.extend(args),
            _ => flattened_ors.push(or_arg),
        }
    }
    // flatten should be in order
    // but VecDeque::extend can only extend at the end of stack
    // so returns should be reversed
    flattened_ors.into_iter().rev().collect()
}

// Flatten the AND expressions.
fn flatten_ands(and_args: impl IntoIterator<Item = PredicateScalar>) -> Vec<PredicateScalar> {
    let mut flattened_ands = vec![];
    let mut stack: VecDeque<PredicateScalar> = VecDeque::from_iter(and_args.into_iter());
    while let Some(and_arg) = stack.pop_back() {
        match and_arg {
            PredicateScalar::And { args } => stack.extend(args),
            _ => flattened_ands.push(and_arg),
        }
    }
    // flatten should be in order
    // but VecDeque::extend can only extend at the end of stack
    // so returns should be reversed
    flattened_ands.into_iter().rev().collect()
}

// Apply the inverse OR distributive law.
fn process_duplicate_or_exprs(or_args: &[PredicateScalar]) -> (PredicateScalar, bool) {
    let mut shortest_exprs: Vec<PredicateScalar> = vec![];
    let mut shortest_exprs_len = 0;
    if or_args.is_empty() {
        return (
            PredicateScalar::Other {
                expr: Box::from(Scalar::ConstantExpr(ConstantExpr {
                    value: DataValue::Boolean(false),
                    data_type: Box::new(BooleanType::new_impl()),
                })),
            },
            false,
        );
    }
    if or_args.len() == 1 {
        return (or_args[0].clone(), false);
    }
    // choose the shortest AND expression
    for or_arg in or_args.iter() {
        match or_arg {
            PredicateScalar::And { args } => {
                let args_num = args.len();
                if shortest_exprs.is_empty() || args_num < shortest_exprs_len {
                    shortest_exprs = (*args).clone();
                    shortest_exprs_len = args_num;
                }
            }
            _ => {
                // if there is no AND expression, it must be the shortest expression.
                shortest_exprs = vec![or_arg.clone()];
                break;
            }
        }
    }

    // dedup shortest_exprs
    shortest_exprs.dedup();

    // Check each element in shortest_exprs to see if it's in all the OR arguments.
    let mut exist_exprs: Vec<PredicateScalar> = vec![];
    for expr in shortest_exprs.iter() {
        let found = or_args.iter().all(|or_arg| match or_arg {
            PredicateScalar::And { args } => args.contains(expr),
            _ => or_arg == expr,
        });
        if found {
            exist_exprs.push((*expr).clone());
        }
    }

    if exist_exprs.is_empty() {
        return (
            PredicateScalar::Or {
                args: or_args.to_vec(),
            },
            false,
        );
    }

    // Rebuild the OR predicate.
    // (A AND B) OR A will be optimized to A.
    let mut new_or_args = vec![];
    for or_arg in or_args.iter() {
        match or_arg {
            PredicateScalar::And { args } => {
                let mut new_args = (*args).clone();
                new_args.retain(|expr| !exist_exprs.contains(expr));
                if !new_args.is_empty() {
                    if new_args.len() == 1 {
                        new_or_args.push(new_args[0].clone());
                    } else {
                        new_or_args.push(PredicateScalar::And { args: new_args });
                    }
                } else {
                    new_or_args.clear();
                    break;
                }
            }
            _ => {
                if exist_exprs.contains(or_arg) {
                    new_or_args.clear();
                    break;
                }
            }
        }
    }
    if !new_or_args.is_empty() {
        if new_or_args.len() == 1 {
            exist_exprs.push(new_or_args[0].clone());
        } else {
            exist_exprs.push(PredicateScalar::Or {
                args: flatten_ors(new_or_args),
            });
        }
    }

    if exist_exprs.len() == 1 {
        (exist_exprs[0].clone(), true)
    } else {
        (
            PredicateScalar::And {
                args: flatten_ands(exist_exprs),
            },
            true,
        )
    }
}
