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

use databend_common_ast::ast::GroupBy;
use databend_common_ast::ast::Identifier;
use databend_common_expression::types::DataType;
use databend_common_expression::FunctionSignature;
use databend_common_expression::TableSchemaRef;
use databend_common_functions::BUILTIN_FUNCTIONS;
use rand::Rng;

#[derive(Clone, Debug)]
pub(crate) struct Table {
    pub(crate) db_name: Option<Identifier>,
    pub(crate) name: Identifier,
    pub(crate) schema: TableSchemaRef,
}

impl Table {
    pub fn new(db_name: Option<Identifier>, name: Identifier, schema: TableSchemaRef) -> Self {
        Self {
            db_name,
            name,
            schema,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Column {
    pub(crate) table_name: Option<Identifier>,
    pub(crate) name: String,
    pub(crate) index: usize,
    pub(crate) data_type: DataType,
}

impl Column {
    pub fn new(
        table_name: Option<Identifier>,
        name: String,
        index: usize,
        data_type: DataType,
    ) -> Self {
        Self {
            table_name,
            name,
            index,
            data_type,
        }
    }
}

pub(crate) struct SqlGenerator<'a, R: Rng> {
    pub(crate) rng: &'a mut R,
    pub(crate) settings: Vec<(String, DataType)>,
    pub(crate) scalar_func_sigs: Vec<FunctionSignature>,
    pub(crate) tables: Vec<Table>,
    pub(crate) cte_tables: Vec<Table>,
    pub(crate) bound_tables: Vec<Table>,
    pub(crate) bound_columns: Vec<Column>,
    pub(crate) is_join: bool,
    // TODO: Generate expressions of the required type
    pub(crate) only_scalar_expr: bool,
    pub(crate) expr_depth: usize,
    pub(crate) group_by: Option<GroupBy>,
    pub(crate) windows_name: Vec<String>,
}

impl<'a, R: Rng> SqlGenerator<'a, R> {
    pub(crate) fn new(rng: &'a mut R, settings: Vec<(String, DataType)>) -> Self {
        let mut scalar_func_sigs = Vec::new();
        for (name, func_list) in BUILTIN_FUNCTIONS.funcs.iter() {
            // Ignore unsupported binary functions, avoid parse binary operator failure
            // Ignore ai functions, avoid timeouts on http calls
            if name == "div"
                || name == "and"
                || name == "or"
                || name == "xor"
                || name == "like"
                || name == "regexp"
                || name == "rlike"
            {
                continue;
            }
            for (scalar_func, _) in func_list {
                scalar_func_sigs.push(scalar_func.signature.clone());
            }
        }

        SqlGenerator {
            rng,
            settings,
            scalar_func_sigs,
            tables: vec![],
            cte_tables: vec![],
            bound_tables: vec![],
            bound_columns: vec![],
            is_join: false,
            only_scalar_expr: false,
            expr_depth: 2,
            group_by: None,
            windows_name: vec![],
        }
    }
}
