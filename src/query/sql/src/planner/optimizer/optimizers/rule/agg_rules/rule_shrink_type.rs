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

use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_storage::Datum;

use crate::IndexType;
use crate::MetadataRef;
use crate::Visibility;
use crate::binder::ColumnBinding;
use crate::binder::ColumnBindingBuilder;
use crate::optimizer::ir::ColumnStat;
use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::Aggregate;
use crate::plans::AggregateMode;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::EvalScalar;
use crate::plans::Join;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;

pub struct RuleShrinkType {
    id: RuleID,
    matchers: Vec<Matcher>,
    metadata: MetadataRef,
}

impl RuleShrinkType {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            id: RuleID::ShrinkGroupByType,
            matchers: vec![
                // Matcher::MatchOp {
                //     op_type: RelOp::Aggregate,
                //     children: vec![Matcher::Leaf],
                // },
                Matcher::MatchOp {
                    op_type: RelOp::Join,
                    children: vec![Matcher::Leaf, Matcher::Leaf],
                },
            ],
            metadata,
        }
    }

    #[allow(unused)]
    fn apply_aggregate(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let agg: Aggregate = s_expr.plan().clone().try_into()?;
        if agg.group_items.is_empty()
            || agg.mode != AggregateMode::Initial
            || agg.grouping_sets.is_some()
        {
            return Ok(());
        }

        let rel_expr = RelExpr::with_s_expr(s_expr);
        let child_stat = rel_expr.derive_cardinality_child(0)?;
        let column_stats = &child_stat.statistics.column_stats;

        let mut rewrites = Vec::with_capacity(agg.group_items.len());
        for (idx, item) in agg.group_items.iter().enumerate() {
            let ScalarExpr::BoundColumnRef(col_ref) = &item.scalar else {
                continue;
            };

            let Some(stat) = column_stats.get(&item.index) else {
                continue;
            };

            let origin_type = col_ref.column.data_type.as_ref();
            let Some(target_type) = shrink_group_by_data_type(origin_type, stat) else {
                continue;
            };

            if target_type == *origin_type {
                continue;
            }

            let shrink_index = {
                let mut metadata = self.metadata.write();

                metadata.add_derived_column(
                    format!("{}_shrink", col_ref.column.column_name),
                    target_type.clone(),
                )
            };

            let shrink_binding = ColumnBindingBuilder::new(
                format!("{}_shrink", col_ref.column.column_name),
                shrink_index,
                Box::new(target_type.clone()),
                Visibility::InVisible,
            )
            .build();

            rewrites.push(GroupByRewrite {
                position: idx,
                original_binding: col_ref.column.clone(),
                original_index: item.index,
                shrink_binding,
                shrink_type: target_type,
                shrink_index,
            });
        }

        if rewrites.is_empty() {
            return Ok(());
        }

        let mut new_group_items = agg.group_items.clone();
        let mut lower_items = Vec::with_capacity(rewrites.len());
        let mut upper_items = Vec::with_capacity(rewrites.len());

        for rewrite in rewrites.iter() {
            new_group_items[rewrite.position].index = rewrite.shrink_index;
            new_group_items[rewrite.position].scalar = ScalarExpr::BoundColumnRef(BoundColumnRef {
                span: None,
                column: rewrite.shrink_binding.clone(),
            });

            lower_items.push(ScalarItem {
                index: rewrite.shrink_index,
                scalar: ScalarExpr::CastExpr(CastExpr {
                    span: None,
                    is_try: false,
                    argument: Box::new(ScalarExpr::BoundColumnRef(BoundColumnRef {
                        span: None,
                        column: rewrite.original_binding.clone(),
                    })),
                    target_type: Box::new(rewrite.shrink_type.clone()),
                }),
            });

            upper_items.push(ScalarItem {
                index: rewrite.original_index,
                scalar: ScalarExpr::CastExpr(CastExpr {
                    span: None,
                    is_try: false,
                    argument: Box::new(ScalarExpr::BoundColumnRef(BoundColumnRef {
                        span: None,
                        column: rewrite.shrink_binding.clone(),
                    })),
                    target_type: Box::new((*rewrite.original_binding.data_type).clone()),
                }),
            });
        }

        let mut new_child = s_expr.child(0)?.clone();
        if !lower_items.is_empty() {
            new_child = SExpr::create_unary(
                Arc::new(EvalScalar { items: lower_items }.into()),
                Arc::new(new_child),
            );
        }

        let mut new_agg = agg;
        new_agg.group_items = new_group_items;
        let mut new_expr = SExpr::create_unary(Arc::new(new_agg.into()), Arc::new(new_child));

        if !upper_items.is_empty() {
            new_expr = SExpr::create_unary(
                Arc::new(EvalScalar { items: upper_items }.into()),
                Arc::new(new_expr),
            );
        }

        state.add_result(new_expr);
        Ok(())
    }

    fn apply_join(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let join: Join = s_expr.plan().clone().try_into()?;
        if join.equi_conditions.is_empty() {
            return Ok(());
        }

        let rel_expr = RelExpr::with_s_expr(s_expr);
        let left_stat = rel_expr.derive_cardinality_child(0)?;
        let right_stat = rel_expr.derive_cardinality_child(1)?;
        let left_column_stats = &left_stat.statistics.column_stats;
        let right_column_stats = &right_stat.statistics.column_stats;

        let mut rewrites = Vec::new();
        for (idx, condition) in join.equi_conditions.iter().enumerate() {
            let ScalarExpr::BoundColumnRef(left) = &condition.left else {
                continue;
            };

            let ScalarExpr::BoundColumnRef(right) = &condition.right else {
                continue;
            };

            let Some(left_stat) = left_column_stats.get(&left.column.index) else {
                continue;
            };

            let Some(right_stat) = right_column_stats.get(&right.column.index) else {
                continue;
            };

            let Some(target_type) = shrink_join_target_type(
                left.column.data_type.as_ref(),
                left_stat,
                right.column.data_type.as_ref(),
                right_stat,
            ) else {
                continue;
            };

            if target_type == *left.column.data_type && target_type == *right.column.data_type {
                continue;
            }

            let left_rewrite = self.create_join_column_rewrite(
                &left.column,
                &target_type,
                format!("{}_join_shrink_l_{}", left.column.column_name, idx),
            );
            let right_rewrite = self.create_join_column_rewrite(
                &right.column,
                &target_type,
                format!("{}_join_shrink_r_{}", right.column.column_name, idx),
            );

            rewrites.push(JoinRewrite {
                condition_index: idx,
                left: left_rewrite,
                right: right_rewrite,
            });
        }

        if rewrites.is_empty() {
            return Ok(());
        }

        let mut new_left_child = s_expr.child(0)?.clone();
        let mut new_right_child = s_expr.child(1)?.clone();
        let mut left_items = Vec::new();
        let mut right_items = Vec::new();
        let mut new_join = join;

        for rewrite in rewrites.iter() {
            new_join.equi_conditions[rewrite.condition_index].left =
                ScalarExpr::BoundColumnRef(BoundColumnRef {
                    span: None,
                    column: rewrite.left.shrink_binding.clone(),
                });
            new_join.equi_conditions[rewrite.condition_index].right =
                ScalarExpr::BoundColumnRef(BoundColumnRef {
                    span: None,
                    column: rewrite.right.shrink_binding.clone(),
                });

            left_items.push(ScalarItem {
                index: rewrite.left.shrink_index,
                scalar: ScalarExpr::CastExpr(CastExpr {
                    span: None,
                    is_try: false,
                    argument: Box::new(ScalarExpr::BoundColumnRef(BoundColumnRef {
                        span: None,
                        column: rewrite.left.original_binding.clone(),
                    })),
                    target_type: Box::new(rewrite.left.shrink_type.clone()),
                }),
            });

            right_items.push(ScalarItem {
                index: rewrite.right.shrink_index,
                scalar: ScalarExpr::CastExpr(CastExpr {
                    span: None,
                    is_try: false,
                    argument: Box::new(ScalarExpr::BoundColumnRef(BoundColumnRef {
                        span: None,
                        column: rewrite.right.original_binding.clone(),
                    })),
                    target_type: Box::new(rewrite.right.shrink_type.clone()),
                }),
            });
        }

        if !left_items.is_empty() {
            new_left_child = SExpr::create_unary(
                Arc::new(EvalScalar { items: left_items }.into()),
                Arc::new(new_left_child),
            );
        }

        if !right_items.is_empty() {
            new_right_child = SExpr::create_unary(
                Arc::new(EvalScalar { items: right_items }.into()),
                Arc::new(new_right_child),
            );
        }

        let new_expr = SExpr::create_binary(
            Arc::new(new_join.into()),
            Arc::new(new_left_child),
            Arc::new(new_right_child),
        );

        state.add_result(new_expr);
        Ok(())
    }

    fn create_join_column_rewrite(
        &self,
        column: &ColumnBinding,
        target_type: &DataType,
        alias: String,
    ) -> JoinColumnRewrite {
        let shrink_index = {
            let mut metadata = self.metadata.write();
            metadata.add_derived_column(alias.clone(), target_type.clone())
        };

        let shrink_binding = ColumnBindingBuilder::new(
            alias,
            shrink_index,
            Box::new(target_type.clone()),
            Visibility::InVisible,
        )
        .build();

        JoinColumnRewrite {
            original_binding: column.clone(),
            shrink_binding,
            shrink_type: target_type.clone(),
            shrink_index,
        }
    }
}

impl Rule for RuleShrinkType {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        match s_expr.plan().rel_op() {
            // RelOp::Aggregate => self.apply_aggregate(s_expr, state),
            RelOp::Join => self.apply_join(s_expr, state),
            _ => Ok(()),
        }
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

struct GroupByRewrite {
    position: usize,
    original_binding: ColumnBinding,
    original_index: IndexType,
    shrink_binding: ColumnBinding,
    shrink_type: DataType,
    shrink_index: IndexType,
}

struct JoinRewrite {
    condition_index: usize,
    left: JoinColumnRewrite,
    right: JoinColumnRewrite,
}

struct JoinColumnRewrite {
    original_binding: ColumnBinding,
    shrink_binding: ColumnBinding,
    shrink_type: DataType,
    shrink_index: IndexType,
}

fn shrink_group_by_data_type(data_type: &DataType, stat: &ColumnStat) -> Option<DataType> {
    minimal_data_type(data_type, stat).and_then(|minimal| {
        if minimal.eq(data_type) {
            None
        } else {
            Some(minimal)
        }
    })
}

fn shrink_join_target_type(
    left_type: &DataType,
    left_stat: &ColumnStat,
    right_type: &DataType,
    right_stat: &ColumnStat,
) -> Option<DataType> {
    let left_min = minimal_data_type(left_type, left_stat)?;
    let right_min = minimal_data_type(right_type, right_stat)?;
    combine_join_types(&left_min, &right_min)
}

fn minimal_data_type(data_type: &DataType, stat: &ColumnStat) -> Option<DataType> {
    match data_type {
        DataType::Nullable(inner) => {
            minimal_data_type(inner, stat).map(|dt| DataType::Nullable(Box::new(dt)))
        }
        DataType::Number(number_type) => {
            minimal_number_type(*number_type, &stat.origin_min, &stat.origin_max)
                .map(DataType::Number)
        }
        _ => None,
    }
}

fn minimal_number_type(
    number_type: NumberDataType,
    min: &Datum,
    max: &Datum,
) -> Option<NumberDataType> {
    match number_type {
        NumberDataType::UInt8 => minimal_unsigned_type(min, max, &[NumberDataType::UInt8]),
        NumberDataType::UInt16 => {
            minimal_unsigned_type(min, max, &[NumberDataType::UInt8, NumberDataType::UInt16])
        }
        NumberDataType::UInt32 => minimal_unsigned_type(min, max, &[
            NumberDataType::UInt8,
            NumberDataType::UInt16,
            NumberDataType::UInt32,
        ]),
        NumberDataType::UInt64 => minimal_unsigned_type(min, max, &[
            NumberDataType::UInt8,
            NumberDataType::UInt16,
            NumberDataType::UInt32,
            NumberDataType::UInt64,
        ]),
        NumberDataType::Int8 => minimal_signed_type(min, max, &[NumberDataType::Int8]),
        NumberDataType::Int16 => {
            minimal_signed_type(min, max, &[NumberDataType::Int8, NumberDataType::Int16])
        }
        NumberDataType::Int32 => minimal_signed_type(min, max, &[
            NumberDataType::Int8,
            NumberDataType::Int16,
            NumberDataType::Int32,
        ]),
        NumberDataType::Int64 => minimal_signed_type(min, max, &[
            NumberDataType::Int8,
            NumberDataType::Int16,
            NumberDataType::Int32,
            NumberDataType::Int64,
        ]),
        _ => None,
    }
}

fn minimal_unsigned_type(
    min: &Datum,
    max: &Datum,
    candidates: &[NumberDataType],
) -> Option<NumberDataType> {
    let (Some(min_v), Some(max_v)) = (datum_to_u128(min), datum_to_u128(max)) else {
        return None;
    };
    if max_v < min_v {
        return None;
    }

    for candidate in candidates {
        match candidate {
            NumberDataType::UInt8 if max_v <= u8::MAX as u128 => return Some(*candidate),
            NumberDataType::UInt16 if max_v <= u16::MAX as u128 => return Some(*candidate),
            NumberDataType::UInt32 if max_v <= u32::MAX as u128 => return Some(*candidate),
            NumberDataType::UInt64 if max_v <= u64::MAX as u128 => return Some(*candidate),
            _ => continue,
        }
    }
    None
}

fn minimal_signed_type(
    min: &Datum,
    max: &Datum,
    candidates: &[NumberDataType],
) -> Option<NumberDataType> {
    let (Some(min_v), Some(max_v)) = (datum_to_i128(min), datum_to_i128(max)) else {
        return None;
    };

    if max_v < min_v {
        return None;
    }

    for candidate in candidates {
        match candidate {
            NumberDataType::Int8 if min_v >= i8::MIN as i128 && max_v <= i8::MAX as i128 => {
                return Some(*candidate);
            }
            NumberDataType::Int16 if min_v >= i16::MIN as i128 && max_v <= i16::MAX as i128 => {
                return Some(*candidate);
            }
            NumberDataType::Int32 if min_v >= i32::MIN as i128 && max_v <= i32::MAX as i128 => {
                return Some(*candidate);
            }
            NumberDataType::Int64 if min_v >= i64::MIN as i128 && max_v <= i64::MAX as i128 => {
                return Some(*candidate);
            }
            _ => continue,
        }
    }
    None
}

fn combine_join_types(left: &DataType, right: &DataType) -> Option<DataType> {
    match (left, right) {
        (DataType::Number(left_number), DataType::Number(right_number)) => {
            max_number_type(*left_number, *right_number).map(DataType::Number)
        }
        (DataType::Nullable(left_inner), DataType::Nullable(right_inner)) => {
            match (left_inner.as_ref(), right_inner.as_ref()) {
                (DataType::Number(left_number), DataType::Number(right_number)) => {
                    max_number_type(*left_number, *right_number)
                        .map(|ty| DataType::Nullable(Box::new(DataType::Number(ty))))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

fn max_number_type(left: NumberDataType, right: NumberDataType) -> Option<NumberDataType> {
    fn is_signed(ty: NumberDataType) -> Option<bool> {
        match ty {
            NumberDataType::UInt8
            | NumberDataType::UInt16
            | NumberDataType::UInt32
            | NumberDataType::UInt64 => Some(false),
            NumberDataType::Int8
            | NumberDataType::Int16
            | NumberDataType::Int32
            | NumberDataType::Int64 => Some(true),
            _ => None,
        }
    }

    fn rank(ty: NumberDataType) -> Option<u8> {
        match ty {
            NumberDataType::UInt8 | NumberDataType::Int8 => Some(0),
            NumberDataType::UInt16 | NumberDataType::Int16 => Some(1),
            NumberDataType::UInt32 | NumberDataType::Int32 => Some(2),
            NumberDataType::UInt64 | NumberDataType::Int64 => Some(3),
            _ => None,
        }
    }

    if is_signed(left)? != is_signed(right)? {
        return None;
    }

    match rank(left)? >= rank(right)? {
        true => Some(left),
        false => Some(right),
    }
}

fn datum_to_u128(value: &Datum) -> Option<u128> {
    match value {
        Datum::UInt(v) => Some(*v as u128),
        Datum::Int(v) if *v >= 0 => Some(*v as u128),
        _ => None,
    }
}

fn datum_to_i128(value: &Datum) -> Option<i128> {
    match value {
        Datum::Int(v) => Some(*v as i128),
        Datum::UInt(v) => Some(*v as i128),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use databend_common_storage::Datum;

    use super::*;
    use crate::optimizer::ir::ColumnStat;
    use crate::optimizer::ir::Ndv;

    #[test]
    fn test_shrink_unsigned() {
        let stat = ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(100),
            ndv: Ndv::Stat(10.0),
            null_count: 0,
            origin_min: Datum::UInt(0),
            origin_max: Datum::UInt(100),
            histogram: None,
        };
        let t = shrink_group_by_data_type(&DataType::Number(NumberDataType::UInt64), &stat);
        assert_eq!(t, Some(DataType::Number(NumberDataType::UInt8)));
    }

    #[test]
    fn test_shrink_signed() {
        let stat = ColumnStat {
            min: Datum::Int(-100),
            max: Datum::Int(100),
            ndv: Ndv::Stat(10.0),
            null_count: 0,
            origin_min: Datum::Int(-100),
            origin_max: Datum::Int(100),
            histogram: None,
        };
        let t = shrink_group_by_data_type(&DataType::Number(NumberDataType::Int64), &stat);
        assert_eq!(t, Some(DataType::Number(NumberDataType::Int8)));
    }

    #[test]
    fn test_shrink_nullable() {
        let stat = ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(50000),
            ndv: Ndv::Stat(10.0),
            null_count: 0,
            origin_min: Datum::UInt(0),
            origin_max: Datum::UInt(50000),
            histogram: None,
        };
        let t = shrink_group_by_data_type(
            &DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt64))),
            &stat,
        );
        assert_eq!(
            t,
            Some(DataType::Nullable(Box::new(DataType::Number(
                NumberDataType::UInt16
            ))))
        );
    }

    #[test]
    fn test_no_shrink() {
        let stat = ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(u32::MAX as u64 + 1),
            ndv: Ndv::Stat(10.0),
            null_count: 0,
            origin_min: Datum::UInt(0),
            origin_max: Datum::UInt(u32::MAX as u64 + 1),
            histogram: None,
        };
        assert!(
            shrink_group_by_data_type(&DataType::Number(NumberDataType::UInt64), &stat).is_none()
        );
    }

    #[test]
    fn test_shrink_join_target_unsigned() {
        let left_stat = ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(120),
            ndv: Ndv::Stat(10.0),
            null_count: 0,
            origin_min: Datum::UInt(0),
            origin_max: Datum::UInt(120),
            histogram: None,
        };
        let right_stat = ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(200),
            ndv: Ndv::Stat(10.0),
            null_count: 0,
            origin_min: Datum::UInt(0),
            origin_max: Datum::UInt(200),
            histogram: None,
        };

        let target = shrink_join_target_type(
            &DataType::Number(NumberDataType::UInt64),
            &left_stat,
            &DataType::Number(NumberDataType::UInt64),
            &right_stat,
        );
        assert_eq!(target, Some(DataType::Number(NumberDataType::UInt8)));
    }

    #[test]
    fn test_shrink_join_target_nullable() {
        let left_stat = ColumnStat {
            min: Datum::Int(-100),
            max: Datum::Int(100),
            ndv: Ndv::Stat(10.0),
            null_count: 0,
            origin_min: Datum::Int(-100),
            origin_max: Datum::Int(100),
            histogram: None,
        };
        let right_stat = ColumnStat {
            min: Datum::Int(-200),
            max: Datum::Int(200),
            ndv: Ndv::Stat(10.0),
            null_count: 0,
            origin_min: Datum::Int(-200),
            origin_max: Datum::Int(200),
            histogram: None,
        };

        let target = shrink_join_target_type(
            &DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
            &left_stat,
            &DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
            &right_stat,
        );
        assert_eq!(
            target,
            Some(DataType::Nullable(Box::new(DataType::Number(
                NumberDataType::Int16
            ))))
        );
    }

    #[test]
    fn test_shrink_join_target_incompatible() {
        let left_stat = ColumnStat {
            min: Datum::Int(0),
            max: Datum::Int(10),
            ndv: Ndv::Stat(5.0),
            null_count: 0,
            origin_min: Datum::Int(0),
            origin_max: Datum::Int(10),
            histogram: None,
        };
        let right_stat = ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(10),
            ndv: Ndv::Stat(5.0),
            null_count: 0,
            origin_min: Datum::UInt(0),
            origin_max: Datum::UInt(10),
            histogram: None,
        };

        assert!(
            shrink_join_target_type(
                &DataType::Number(NumberDataType::Int64),
                &left_stat,
                &DataType::Number(NumberDataType::UInt64),
                &right_stat,
            )
            .is_none()
        );
    }
}
