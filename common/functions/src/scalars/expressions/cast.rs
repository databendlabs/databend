// Copyright 2021 Datafuse Labs.
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

use std::fmt;
use std::sync::Arc;

use common_datavalues::prelude::*;
use common_exception::Result;

use super::cast_with_type::cast_column_field;
use crate::scalars::function::Function;

#[derive(Clone)]
pub struct CastFunction {
    _display_name: String,
    /// The data type to cast to
    cast_type: DataTypePtr,
}

impl CastFunction {
    pub fn create(display_name: &str, type_name: &str) -> Result<Box<dyn Function>> {
        let factory = TypeFactory::instance();
        let data_type = factory.get(type_name)?;

        Ok(Box::new(Self {
            _display_name: display_name.to_string(),
            cast_type: data_type.clone(),
        }))
    }

    pub fn create_try(display_name: &str, type_name: &str) -> Result<Box<dyn Function>> {
        let factory = TypeFactory::instance();
        let data_type = factory.get(type_name)?;

        if data_type.is_nullable() || !data_type.can_inside_nullable() {
            return Ok(Box::new(Self {
                _display_name: display_name.to_string(),
                cast_type: data_type.clone(),
            }));
        }

        let nullable_type = NullableType::create(data_type.clone());
        Ok(Box::new(Self {
            _display_name: display_name.to_string(),
            cast_type: Arc::new(nullable_type),
        }))
    }
}

impl Function for CastFunction {
    fn name(&self) -> &str {
        "CastFunction"
    }

    fn return_type(&self, _args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        Ok(self.cast_type.clone())
    }

    fn eval(&self, columns: &ColumnsWithField, _input_rows: usize) -> Result<ColumnRef> {
        cast_column_field(&columns[0], &self.cast_type)
    }

    fn passthrough_null(&self) -> bool {
        false
    }
}

impl fmt::Display for CastFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CAST")
    }
}
