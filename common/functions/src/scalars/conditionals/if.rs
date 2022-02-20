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
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_datavalues::remove_nullable;
use common_datavalues::type_coercion::aggregate_types;
use common_datavalues::with_match_scalar_type;
use common_exception::Result;

use crate::scalars::cast_column_field;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;

#[derive(Clone, Debug)]
pub struct IfFunction {
    display_name: String,
}

impl IfFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(IfFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        let mut features = FunctionFeatures::default().num_arguments(3);
        features = features.deterministic();
        FunctionDescription::creator(Box::new(Self::try_create)).features(features)
    }

    // handle cond is const or nullable or null column
    fn eval_cond_const(
        &self,
        cond_col: &ColumnRef,
        columns: &ColumnsWithField,
    ) -> Result<ColumnRef> {
        debug_assert!(cond_col.is_const());
        // whether nullable or not, we can use viewer to make it
        let cond_viewer = bool::try_create_viewer(cond_col)?;
        if cond_viewer.value_at(0) {
            return Ok(columns[0].column().clone());
        } else {
            return Ok(columns[1].column().clone());
        }
    }

    // lhs is const column and:
    // 1. rhs: const
    // 2. rhs: nullable
    // 3. rhs: scalar column
    fn eval_const(
        &self,
        cond_col: &BooleanColumn,
        columns: &ColumnsWithField,
        input_rows: usize,
    ) -> Result<ColumnRef> {
        debug_assert!(columns[0].column().is_const() || columns[1].column().is_const());
        let (lhs_col, rhs_col, reverse) = if columns[0].column().is_const() {
            (&columns[0], &columns[1], false)
        } else {
            (&columns[1], &columns[0], true)
        };

        // cast to least super type
        let dts = vec![lhs_col.data_type().clone(), rhs_col.data_type().clone()];
        let least_supertype = aggregate_types(dts.as_slice())?;

        let lhs = cast_column_field(lhs_col, &least_supertype)?;
        let rhs = cast_column_field(rhs_col, &least_supertype)?;

        let type_id = remove_nullable(&lhs.data_type()).data_type_id();

        if rhs.is_nullable() {
            // rhs is nullable column
            with_match_scalar_type!(type_id.to_physical_type(), |$T| {
                let left_viewer = $T::try_create_viewer(&lhs)?;
                let l_val = left_viewer.value_at(0);
                let rhs_viewer = $T::try_create_viewer(&rhs)?;

                let mut builder: NullableColumnBuilder<$T> = NullableColumnBuilder::with_capacity(input_rows);

                let iter = cond_col.iter().zip(rhs_viewer.iter().enumerate());

                if reverse {
                    for (predicate, (row, r_val)) in iter {
                        if predicate {
                            builder.append(r_val, rhs_viewer.valid_at(row));
                        }else {
                            builder.append(l_val, true);
                        }
                    }
                    return Ok(builder.build(input_rows));
                }else {
                    for (predicate, (row, r_val)) in iter {
                        if predicate {
                            builder.append(l_val, true);
                        }else {
                            builder.append(r_val, rhs_viewer.valid_at(row));
                        }
                    }
                    return Ok(builder.build(input_rows));
                }
            }, {
                unimplemented!()
            });
        } else if rhs.is_const() {
            // rhs is const column
            with_match_scalar_type!(type_id.to_physical_type(), |$T| {
                let left_viewer = $T::try_create_viewer(&lhs)?;
                let l_val = left_viewer.value_at(0);
                let right_viewer = $T::try_create_viewer(&rhs)?;
                let r_val = right_viewer.value_at(0);

                if reverse {
                    let iter = cond_col.iter().map(|predicate| if predicate { r_val } else { l_val });
                    return Ok(Arc::new(ColumnBuilder::<$T>::from_iterator(iter)));
                }else {
                    let iter = cond_col.iter().map(|predicate| if predicate { l_val } else { r_val });
                    return Ok(Arc::new(ColumnBuilder::<$T>::from_iterator(iter)));
                }
            }, {
                unimplemented!()
            });
        } else {
            // rhs is scalar column
            with_match_scalar_type!(type_id.to_physical_type(), |$T| {
                let left_viewer = $T::try_create_viewer(&lhs)?;
                let l_val = left_viewer.value_at(0);
                let rhs = Series::check_get_scalar::<$T>(&rhs)?;

                if reverse {
                    let iter = cond_col.iter().zip(rhs.scalar_iter()).map(|(predicate, r_val)| {
                        if predicate {
                            r_val
                        } else {
                            l_val
                        }
                    });
                    let col = <$T as Scalar>::ColumnType::from_iterator(iter);
                    return Ok(col.arc());
                } else {
                    let iter = cond_col.iter().zip(rhs.scalar_iter()).map(|(predicate, r_val)| {
                        if predicate {
                            l_val
                        } else {
                            r_val
                        }
                    });
                    let col = <$T as Scalar>::ColumnType::from_iterator(iter);
                    return Ok(col.arc());
                }
            }, {
                unimplemented!()
            });
        }
    }

    // handle when one of then is nullable and both are not const
    fn eval_nullable(
        &self,
        cond_col: &BooleanColumn,
        columns: &ColumnsWithField,
        input_rows: usize,
    ) -> Result<ColumnRef> {
        let lhs_col = &columns[0];
        let rhs_col = &columns[1];

        let dts = vec![lhs_col.data_type().clone(), rhs_col.data_type().clone()];
        let least_supertype = aggregate_types(dts.as_slice())?;

        let lhs = cast_column_field(lhs_col, &least_supertype)?;
        let rhs = cast_column_field(rhs_col, &least_supertype)?;

        let type_id = remove_nullable(&least_supertype).data_type_id();

        with_match_scalar_type!(type_id.to_physical_type(), |$T| {
            let lhs_viewer = $T::try_create_viewer(&lhs)?;
            let rhs_viewer = $T::try_create_viewer(&rhs)?;
            let mut builder = NullableColumnBuilder::<$T>::with_capacity(input_rows);

            for ((predicate, l), (row, r)) in cond_col
                .iter()
                .zip(lhs_viewer.iter())
                .zip(rhs_viewer.iter().enumerate())
            {
                if predicate {
                    builder.append(l, lhs_viewer.valid_at(row));
                } else {
                    builder.append(r, rhs_viewer.valid_at(row));
                };
            }

            return Ok(builder.build(input_rows));
        }, {
            unimplemented!()
        });
    }

    // handle when both are not nullable or const
    fn eval_generic(
        &self,
        cond_col: &BooleanColumn,
        columns: &ColumnsWithField,
    ) -> Result<ColumnRef> {
        let lhs_col = &columns[0];
        let rhs_col = &columns[1];

        let dts = vec![lhs_col.data_type().clone(), rhs_col.data_type().clone()];
        let least_supertype = aggregate_types(dts.as_slice())?;

        let lhs = cast_column_field(lhs_col, &least_supertype)?;
        let rhs = cast_column_field(rhs_col, &least_supertype)?;

        debug_assert!(!least_supertype.is_nullable());
        let type_id = least_supertype.data_type_id();

        with_match_scalar_type!(type_id.to_physical_type(), |$T| {
            let lhs = Series::check_get_scalar::<$T>(&lhs)?;
            let rhs = Series::check_get_scalar::<$T>(&rhs)?;

            let iter = cond_col
                .scalar_iter()
                .zip(lhs.scalar_iter())
                .zip(rhs.scalar_iter())
                .map(|((predicate, l), r)| if predicate { l } else { r });

            let col = <$T as Scalar>::ColumnType::from_iterator(iter);
            return Ok(col.arc());
        }, {
            unimplemented!()
        });
    }
}

impl Function for IfFunction {
    fn name(&self) -> &str {
        "IfFunction"
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        let dts = vec![args[1].clone(), args[2].clone()];
        let least_supertype = aggregate_types(dts.as_slice())?;

        Ok(least_supertype)
    }

    fn eval(&self, columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        let cond_col = columns[0].column();
        let cond_col = DataBlock::cast_to_nonull_boolean(cond_col)?;

        // 1. fast path for cond nullable or const or null column
        if cond_col.is_const() {
            return self.eval_cond_const(&cond_col, &columns[1..]);
        }

        let cond_col = Series::check_get_scalar::<bool>(&cond_col)?;

        // 2. handle when lhs / rhs is const
        if columns[1].column().is_const() || columns[2].column().is_const() {
            return self.eval_const(cond_col, &columns[1..], input_rows);
        }

        // 3. handle nullable column
        if columns[1].column().is_nullable() || columns[2].column().is_nullable() {
            return self.eval_nullable(cond_col, &columns[1..], input_rows);
        }

        // 4. all normal type and are not nullable/const
        self.eval_generic(cond_col, &columns[1..])
    }

    fn passthrough_null(&self) -> bool {
        false
    }
}

impl std::fmt::Display for IfFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}()", self.display_name)
    }
}
