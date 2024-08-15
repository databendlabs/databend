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

use databend_common_expression::ComputedExpr;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;

#[allow(clippy::type_complexity)]
pub fn generate_desc_schema(
    schema: TableSchemaRef,
) -> (
    Vec<String>,
    Vec<String>,
    Vec<String>,
    Vec<String>,
    Vec<String>,
) {
    let mut names: Vec<String> = vec![];
    let mut types: Vec<String> = vec![];
    let mut nulls: Vec<String> = vec![];
    let mut default_exprs: Vec<String> = vec![];
    let mut extras: Vec<String> = vec![];

    for field in schema.fields().iter() {
        names.push(field.name().to_string());

        let non_null_type = field.data_type().remove_recursive_nullable();
        types.push(non_null_type.sql_name());
        nulls.push(if field.is_nullable() {
            "YES".to_string()
        } else {
            "NO".to_string()
        });
        match field.default_expr() {
            Some(expr) => {
                default_exprs.push(expr.clone());
            }

            None => {
                let value = Scalar::default_value(&field.data_type().into());
                default_exprs.push(value.to_string());
            }
        }
        let extra = match field.computed_expr() {
            Some(ComputedExpr::Virtual(expr)) => format!("VIRTUAL COMPUTED COLUMN `{}`", expr),
            Some(ComputedExpr::Stored(expr)) => format!("STORED COMPUTED COLUMN `{}`", expr),
            _ => "".to_string(),
        };
        extras.push(extra);
    }
    (names, types, nulls, default_exprs, extras)
}
