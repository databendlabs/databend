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

use common_datavalues2::BooleanType;
use common_datavalues2::ColumnBuilder;
use common_datavalues2::ColumnRef;
use common_datavalues2::ColumnViewer;
use common_datavalues2::ColumnsWithField;
use common_datavalues2::DataTypePtr;
use common_datavalues2::NullableColumnBuilder;
use common_datavalues2::NullableType;
use common_exception::Result;

use super::logic2::LogicFunction2;
use crate::scalars::cast_column_field;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function2;
use crate::scalars::Function2Description;

#[derive(Clone)]
pub struct LogicXorFunction2 {
    _display_name: String,
}

impl LogicXorFunction2 {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function2>> {
        Ok(Box::new(Self {
            _display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> Function2Description {
        let mut features = FunctionFeatures::default().num_arguments(2);
        features = features.deterministic();
        Function2Description::creator(Box::new(Self::try_create)).features(features)
    }
}

impl Function2 for LogicXorFunction2 {
    fn name(&self) -> &str {
        "LogicXorFunction"
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        if args[0].is_nullable() || args[1].is_nullable() {
            Ok(Arc::new(NullableType::create(BooleanType::arc())))
        } else {
            Ok(BooleanType::arc())
        }
    }

    fn eval(&self, columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        let mut nullable = false;
        if columns[0].data_type().is_nullable() || columns[1].data_type().is_nullable() {
            nullable = true;
        }

        let dt: DataTypePtr;
        if nullable {
            dt = Arc::new(NullableType::create(BooleanType::arc()));
        } else {
            dt = BooleanType::arc();
        }

        let lhs = cast_column_field(&columns[0], &dt)?;
        let rhs = cast_column_field(&columns[1], &dt)?;

        if nullable {
            let lhs_viewer = ColumnViewer::<bool>::create(&lhs)?;
            let rhs_viewer = ColumnViewer::<bool>::create(&rhs)?;

            let mut builder = NullableColumnBuilder::<bool>::with_capacity(input_rows);

            for idx in 0..input_rows {
                builder.append(
                    lhs_viewer.value(idx) ^ rhs_viewer.value(idx),
                    lhs_viewer.valid_at(idx) & rhs_viewer.valid_at(idx),
                );
            }

            Ok(builder.build(input_rows))
        } else {
            let lhs_viewer = ColumnViewer::<bool>::create(&lhs)?;
            let rhs_viewer = ColumnViewer::<bool>::create(&rhs)?;

            let mut builder = ColumnBuilder::<bool>::with_capacity(input_rows);

            for idx in 0..input_rows {
                builder.append(lhs_viewer.value(idx) ^ rhs_viewer.value(idx));
            }

            Ok(builder.build(input_rows))
        }
    }
}

impl std::fmt::Display for LogicXorFunction2 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}
