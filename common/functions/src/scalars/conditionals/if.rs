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
use common_exception::Result;

use crate::scalars::cast_column_field;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function2;
use crate::scalars::Function2Description;
use crate::with_match_scalar_type;

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
}

impl Function2 for IfFunction {
    fn name(&self) -> &str {
        "IfFunction"
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        let dts = vec![args[1].clone(), args[2].clone()];

        let mut least_supertype = aggregate_types(dts.as_slice())?;
        if (args[0].is_nullable() || args[1].is_nullable() || args[2].is_nullable())
            && !least_supertype.is_nullable()
        {
            least_supertype = Arc::new(NullableType::create(least_supertype));
        }

        Ok(least_supertype)
    }

    fn eval(&self, columns: &ColumnsWithField, _input_rows: usize) -> Result<ColumnRef> {
        // if predicate / lhs / rhs is nullable, then it will return NullableColumn or Non-NullableColumn
        let mut nullable = false;

        let predicate: ColumnRef;
        if columns[0].data_type().is_nullable() {
            nullable = true;
            let boolean_dt: DataTypePtr = Arc::new(NullableType::create(BooleanType::arc()));
            predicate = cast_column_field(&columns[0], &boolean_dt)?;
        } else {
            let boolean_dt = BooleanType::arc();
            predicate = cast_column_field(&columns[0], &boolean_dt)?;
        }

        let lhs = &columns[1];
        let rhs = &columns[2];
        let dts = vec![lhs.data_type().clone(), rhs.data_type().clone()];

        let mut least_supertype = aggregate_types(dts.as_slice())?;

        if (lhs.data_type().is_nullable() || rhs.data_type().is_nullable())
            && !least_supertype.is_nullable()
        {
            least_supertype = Arc::new(NullableType::create(least_supertype));
        }
        if least_supertype.is_nullable() {
            nullable = true;
        }

        let lhs = cast_column_field(lhs, &least_supertype)?;
        let rhs = cast_column_field(rhs, &least_supertype)?;

        let type_id = remove_nullable(&lhs.data_type()).data_type_id();

        macro_rules! scalar_build {
            (
             $T:ident
        ) => {{
                let predicate_wrapper = ColumnViewer::<bool>::create(&predicate)?;
                let lhs_wrapper = ColumnViewer::<$T>::create(&lhs)?;
                let rhs_wrapper = ColumnViewer::<$T>::create(&rhs)?;
                let size = lhs_wrapper.len();

                if nullable {
                    let mut builder = NullableColumnBuilder::<$T>::with_capacity(size);

                    for row in 0..size {
                        let predicate = predicate_wrapper.value(row);
                        let valid = predicate_wrapper.valid_at(row);
                        if predicate {
                            builder
                                .append(lhs_wrapper.value(row), valid & lhs_wrapper.valid_at(row));
                        } else {
                            builder
                                .append(rhs_wrapper.value(row), valid & rhs_wrapper.valid_at(row));
                        };
                    }

                    Ok(builder.build(size))
                } else {
                    let mut builder = ColumnBuilder::<$T>::with_capacity(size);

                    for row in 0..size {
                        let predicate = predicate_wrapper.value(row);
                        if predicate {
                            builder.append(lhs_wrapper.value(row));
                        } else {
                            builder.append(rhs_wrapper.value(row));
                        };
                    }

                    Ok(builder.build(size))
                }
            }};
        }

        with_match_scalar_type!(type_id, |$T| {
            scalar_build!($T)
        }, {
            unimplemented!()
        })
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
