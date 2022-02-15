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

use common_datavalues2::prelude::*;
use common_datavalues2::remove_nullable;
use common_datavalues2::type_coercion::aggregate_types;
use common_datavalues2::with_match_scalar_type;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::cast_column_field;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function2;
use crate::scalars::Function2Description;

#[derive(Clone, Debug)]
pub struct IfFunction {
    display_name: String,
}

impl IfFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function2>> {
        Ok(Box::new(IfFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> Function2Description {
        let mut features = FunctionFeatures::default().num_arguments(3);
        features = features.deterministic();
        Function2Description::creator(Box::new(Self::try_create)).features(features)
    }

    // handle cond is const or nullable
    fn eval_cond_const_or_nullable(
        &self,
        columns: &ColumnsWithField,
        input_rows: usize,
    ) -> Result<ColumnRef> {
        let cond_col = &columns[0];
        let lhs_col = &columns[1];
        let rhs_col = &columns[2];

        if cond_col.column().is_const() {
            // whether nullable or not, we can use viewer to make it
            let cond_col = cast_column_field(cond_col, &BooleanType::arc())?;
            let cond_viewer = bool::try_create_viewer(&cond_col)?;
            if cond_viewer.value_at(0) && cond_viewer.valid_at(0) {
                return Ok(lhs_col.column().clone());
            } else {
                return Ok(rhs_col.column().clone());
            }
        }

        if cond_col.column().is_nullable() {
            if cond_col.column().is_const() {
                let cond_col = cast_column_field(cond_col, &BooleanType::arc())?;
                let cond_viewer = bool::try_create_viewer(&cond_col)?;
                if cond_viewer.valid_at(0) && cond_viewer.valid_at(0) {
                    return Ok(lhs_col.column().clone());
                } else {
                    return Ok(rhs_col.column().clone());
                }
            } else {
                // normal nullable cond column, not const
                // make nullable cond to non-nullable BooleanColumn and then call eval
                let cond_col = cast_column_field(cond_col, &BooleanType::arc())?;
                let cond_viewer = bool::try_create_viewer(&cond_col)?;
                let mut boolean_builder: ColumnBuilder<bool> =
                    ColumnBuilder::with_capacity(input_rows);
                for idx in 0..input_rows {
                    if cond_viewer.valid_at(idx) && cond_viewer.value_at(idx) {
                        boolean_builder.append(true);
                    } else {
                        boolean_builder.append(false);
                    }
                }
                let cond_col = boolean_builder.build(input_rows);

                let cond_field = DataField::new("cond", BooleanType::arc());
                let cond_col = ColumnWithField::new(cond_col, cond_field);

                let columns = vec![cond_col, lhs_col.clone(), rhs_col.clone()];
                return self.eval(&columns, input_rows);
            }
        }

        Err(ErrorCode::UnexpectedError("logic bug"))
    }

    // (cond is common column(no const and no nullable))
    // lhs and rhs column should be normal column (no nullable(const) or const(nullable))
    // handle when lhs: const
    // 1. rhs: const
    // 2. rhs: nullable
    fn eval_const(&self, columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        let cond_col = &columns[0];
        assert!(columns[1].column().is_const() || columns[2].column().is_const());
        let (lhs_col, rhs_col, reverse) = if columns[1].column().is_const() {
            (&columns[1], &columns[2], false)
        } else {
            (&columns[2], &columns[1], true)
        };

        let cond_col = cast_column_field(cond_col, &BooleanType::arc())?;
        let cond_col = cond_col.as_any().downcast_ref::<BooleanColumn>().unwrap();

        // cast to least super type
        let dts = vec![lhs_col.data_type().clone(), rhs_col.data_type().clone()];
        let least_supertype = aggregate_types(dts.as_slice())?;

        let lhs = cast_column_field(lhs_col, &least_supertype)?;
        let rhs = cast_column_field(rhs_col, &least_supertype)?;

        let type_id = remove_nullable(&lhs.data_type()).data_type_id();
        if rhs.is_nullable() {
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
            // rhs is const
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
            // rhs is primitive type
            macro_rules! build_with_primitive_type {
                ($column:ident, $T:ident) => {
                    let left_viewer = $T::try_create_viewer(&lhs)?;
                    let l_val = left_viewer.value_at(0);
                    let rhs = $T::try_create_viewer(&rhs)?;

                    if reverse {
                        let iter = cond_col.iter().zip(rhs.iter()).map(|(predicate, r_val)| {
                            if predicate {
                                r_val
                            } else {
                                l_val
                            }
                        });
                        let col = <$T as Scalar>::ColumnType::from_iterator(iter);
                        return Ok(col.arc());
                    } else {
                        let iter = cond_col.iter().zip(rhs.iter()).map(|(predicate, r_val)| {
                            if predicate {
                                l_val
                            } else {
                                r_val
                            }
                        });
                        let col = <$T as Scalar>::ColumnType::from_iterator(iter);
                        return Ok(col.arc());
                    }
                };
            }

            match type_id {
                TypeID::Boolean => {
                    build_with_primitive_type!(BooleanColumn, bool);
                }
                TypeID::String => {
                    build_with_primitive_type!(StringColumn, Vu8);
                }
                TypeID::Int8 => {
                    build_with_primitive_type!(Int8Column, i8);
                }
                TypeID::Int16 => {
                    build_with_primitive_type!(Int16Column, i16);
                }
                TypeID::Int32 => {
                    build_with_primitive_type!(Int32Column, i32);
                }
                TypeID::Int64 => {
                    build_with_primitive_type!(Int64Column, i64);
                }
                TypeID::UInt8 => {
                    build_with_primitive_type!(UInt8Column, u8);
                }
                TypeID::UInt16 => {
                    build_with_primitive_type!(Int8Column, u16);
                }
                TypeID::UInt32 => {
                    build_with_primitive_type!(UInt32Column, u32);
                }
                TypeID::UInt64 => {
                    build_with_primitive_type!(UInt64Column, u64);
                }
                TypeID::Float32 => {
                    build_with_primitive_type!(Float32Column, f32);
                }
                TypeID::Float64 => {
                    build_with_primitive_type!(Float64Column, f64);
                }
                _ => {
                    unimplemented!()
                }
            }
        }
    }

    // (cond is common column(no const and no nullable))
    // handle when one of then is nullable and both are not const
    fn eval_nullable(&self, columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        let cond_col = &columns[0];
        let lhs_col = &columns[1];
        let rhs_col = &columns[2];

        let cond_col = cast_column_field(cond_col, &BooleanType::arc())?;
        let cond_col = cond_col.as_any().downcast_ref::<BooleanColumn>().unwrap();

        let dts = vec![lhs_col.data_type().clone(), rhs_col.data_type().clone()];
        let least_supertype = aggregate_types(dts.as_slice())?;

        let lhs = cast_column_field(lhs_col, &least_supertype)?;
        let rhs = cast_column_field(rhs_col, &least_supertype)?;

        let type_id = remove_nullable(&least_supertype).data_type_id();

        with_match_scalar_type!(type_id.to_physical_type(), |$T| {
            let lhs_viewer = $T:: try_create_viewer(&lhs)?;
            let rhs_viewer = $T:: try_create_viewer(&rhs)?;
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

    // (cond is common column(no const and no nullable))
    // handle when both are not nullable or const
    fn eval_generic(&self, columns: &ColumnsWithField) -> Result<ColumnRef> {
        let cond_col = &columns[0];
        let lhs_col = &columns[1];
        let rhs_col = &columns[2];

        let cond_col = cast_column_field(cond_col, &BooleanType::arc())?;
        let cond_col = cond_col.as_any().downcast_ref::<BooleanColumn>().unwrap();

        let dts = vec![lhs_col.data_type().clone(), rhs_col.data_type().clone()];
        let least_supertype = aggregate_types(dts.as_slice())?;

        let lhs = cast_column_field(lhs_col, &least_supertype)?;
        let rhs = cast_column_field(rhs_col, &least_supertype)?;

        assert!(!least_supertype.is_nullable());
        let type_id = least_supertype.data_type_id();

        with_match_scalar_type!(type_id.to_physical_type(), |$T| {
            let lhs = $T::try_create_viewer(&lhs)?;
            let rhs = $T::try_create_viewer(&rhs)?;

            let iter =
                cond_col
                    .iter()
                    .zip(lhs.iter())
                    .zip(rhs.iter())
                    .map(|((predicate, l_val), r_val)| if predicate { l_val } else { r_val });

            let col = <$T as Scalar>::ColumnType::from_iterator(iter);
            return Ok(col.arc());
        }, {
            unimplemented!()
        });
    }
}

impl Function2 for IfFunction {
    fn name(&self) -> &str {
        "IfFunction"
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        let dts = vec![args[1].clone(), args[2].clone()];
        let least_supertype = aggregate_types(dts.as_slice())?;

        Ok(least_supertype)
    }

    fn eval(&self, columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        let cond_col = &columns[0];
        let lhs_col = &columns[1];
        let rhs_col = &columns[2];

        // 1. fast path for cond nullable or const
        if cond_col.column().is_const() || cond_col.column().is_nullable() {
            return self.eval_cond_const_or_nullable(columns, input_rows);
        }

        // 2. (cond is not const or nullable)
        // handle when lhs / rhs is const
        if lhs_col.column().is_const() || rhs_col.column().is_const() {
            return self.eval_const(columns, input_rows);
        }

        // 3. (cond is not const or nullable)
        // columns are not const
        if lhs_col.column().is_nullable() || rhs_col.column().is_nullable() {
            return self.eval_nullable(columns, input_rows);
        }

        // 4. all normal type and are not nullable/const
        self.eval_generic(columns)
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
