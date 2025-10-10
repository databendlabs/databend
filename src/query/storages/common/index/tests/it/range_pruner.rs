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

use std::io::Write;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_storages_common_index::RangeIndex;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use goldenfile::Mint;

use super::eliminate_cast::parse_expr;

#[test]
fn test_range_index() -> Result<()> {
    let mut mint = Mint::new("tests/it/testdata");
    let file = &mut mint.new_goldenfile("test_range_indies.txt").unwrap();

    fn n(n: i32) -> Scalar {
        Scalar::Number(n.into())
    }

    let domains = &[("a", n(-2), n(3)), ("b", n(-2), n(3))];

    run_text(file, "a = 2 and b = 41", domains);
    run_text(file, "a = 2 and b < 7", domains);
    run_text(file, "to_string(a) = '4'", domains);
    run_text(file, "to_string(a) = 'a'", domains);
    run_text(file, "to_int8(a) = 3", domains);
    run_text(file, "to_int8(a) = 4", domains);
    run_text(file, "to_uint8(a) = 3::int64", domains);
    run_text(file, "to_uint8(a) = 4::int64", domains);
    run_text(file, "to_int16(a::int8) = 3", domains);
    run_text(file, "to_int16(a::int8) = 4", domains);

    Ok(())
}

fn create_stats(domains: &[(&str, Scalar, Scalar)], schema: &TableSchema) -> StatisticsOfColumns {
    domains
        .iter()
        .map(|(name, min, max)| {
            (
                schema.leaf_columns_of(&name.to_string())[0],
                ColumnStatistics {
                    min: min.clone(),
                    max: max.clone(),
                    null_count: 0,
                    in_memory_size: 1000,
                    distinct_of_values: None,
                },
            )
        })
        .collect()
}

fn run_text(file: &mut impl Write, text: &str, domains: &[(&str, Scalar, Scalar)]) {
    let func_ctx = FunctionContext::default();

    let schema = Arc::new(TableSchema::new(vec![
        TableField::new("a", TableDataType::Number(NumberDataType::Int32)),
        TableField::new("b", TableDataType::Number(NumberDataType::Int32)),
    ]));

    let stats = create_stats(domains, &schema);

    let columns = [("a", Int32Type::data_type()), ("b", Int32Type::data_type())];
    let expr = parse_expr(text, &columns);

    let index = RangeIndex::try_create(func_ctx, &expr, schema, Default::default()).unwrap();

    writeln!(file, "text      : {text}").unwrap();
    writeln!(file, "expr      : {expr}").unwrap();

    match index.apply(&stats, |_| false) {
        Err(err) => {
            writeln!(file, "err       : {err}").unwrap();
        }

        Ok(keep) => {
            writeln!(file, "keep      : {keep}").unwrap();
        }
    };
    writeln!(file).unwrap();
}
