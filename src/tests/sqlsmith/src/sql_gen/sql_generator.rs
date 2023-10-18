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

use common_ast::ast::GroupBy;
use common_expression::types::DataType;
use common_expression::FunctionSignature;
use common_expression::TableSchemaRef;
use common_functions::BUILTIN_FUNCTIONS;
use rand::Rng;

#[derive(Clone, Debug)]
pub(crate) struct Table {
    pub(crate) name: String,
    pub(crate) schema: TableSchemaRef,
}

impl Table {
    pub fn new(name: String, schema: TableSchemaRef) -> Self {
        Self { name, schema }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Column {
    pub(crate) table_name: String,
    pub(crate) name: String,
    pub(crate) index: usize,
    pub(crate) data_type: DataType,
}

pub(crate) struct SqlGenerator<'a, R: Rng> {
    pub(crate) tables: Vec<Table>,
    pub(crate) cte_tables: Vec<Table>,
    pub(crate) bound_tables: Vec<Table>,
    pub(crate) bound_columns: Vec<Column>,
    pub(crate) is_join: bool,
    // TODO: Generate expressions of the required type
    pub(crate) only_scalar_expr: bool,
    pub(crate) expr_depth: usize,
    pub(crate) scalar_func_sigs: Vec<FunctionSignature>,
    pub(crate) rng: &'a mut R,
    pub(crate) group_by: Option<GroupBy>,
    pub(crate) windows_name: Vec<String>,
}

impl<'a, R: Rng> SqlGenerator<'a, R> {
    pub(crate) fn new(rng: &'a mut R) -> Self {
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
                || name == "ai_embedding_vector"
                || name == "ai_text_completion"
            {
                continue;
            }
            for (scalar_func, _) in func_list {
                scalar_func_sigs.push(scalar_func.signature.clone());
            }
        }

        SqlGenerator {
            tables: vec![],
            cte_tables: vec![],
            bound_tables: vec![],
            bound_columns: vec![],
            is_join: false,
            only_scalar_expr: false,
            expr_depth: 2,
            scalar_func_sigs,
            rng,
            group_by: None,
            windows_name: vec![],
        }
    }
}
