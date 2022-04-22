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

use crate::sql::optimizer::property::NamedColumn;
use crate::sql::optimizer::property::RequiredProperty;
use crate::sql::optimizer::ColumnSet;
use crate::sql::BindContext;

pub struct OptimizeContext {
    required_prop: RequiredProperty,
    _output_named_columns: Vec<NamedColumn>,
}

impl OptimizeContext {
    #[allow(dead_code)]
    pub fn create(
        required_prop: RequiredProperty,
        _output_named_columns: Vec<NamedColumn>,
    ) -> Self {
        OptimizeContext {
            required_prop,
            _output_named_columns,
        }
    }

    pub fn create_with_bind_context(bind_context: &BindContext) -> Self {
        let _output_named_columns: Vec<NamedColumn> = bind_context
            .all_column_bindings()
            .iter()
            .map(|col| NamedColumn {
                index: col.index,
                name: col.column_name.clone(),
            })
            .collect();
        let required_columns: ColumnSet =
            _output_named_columns.iter().map(|col| col.index).collect();
        let required_prop = RequiredProperty::create(required_columns);

        OptimizeContext {
            required_prop,
            _output_named_columns,
        }
    }

    pub fn required_prop(&self) -> &RequiredProperty {
        &self.required_prop
    }
}
