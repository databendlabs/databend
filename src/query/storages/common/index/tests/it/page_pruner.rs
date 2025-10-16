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
use databend_storages_common_index::PageIndex;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use goldenfile::Mint;

use super::eliminate_cast::parse_expr;

#[test]
fn test_page_index() -> Result<()> {
    let mut mint = Mint::new("tests/it/testdata");
    let file = &mut mint.new_goldenfile("test_page_indies.txt").unwrap();

    fn n(n: i32) -> Scalar {
        Scalar::Number(n.into())
    }

    let stats = ClusterStatistics {
        cluster_key_id: 0,
        min: vec![n(-2), n(30)],
        max: vec![n(10), n(2)],
        level: 0,
        pages: Some(vec![
            Scalar::Tuple(vec![n(-2), n(30)]), // 0
            Scalar::Tuple(vec![n(0), n(40)]),  // 1
            Scalar::Tuple(vec![n(2), n(40)]),  // 2
            Scalar::Tuple(vec![n(2), n(70)]),  // 3
            Scalar::Tuple(vec![n(3), n(17)]),  // 4
            Scalar::Tuple(vec![n(4), n(5)]),   // 5
        ]),
    };

    let stats = &Some(stats);
    run_text(file, "a = 2", stats);
    run_text(file, "a = 2 and b = 41", stats);
    run_text(file, "a = 3", stats);
    run_text(file, "a = 3 and b < 7", stats);
    run_text(file, "to_string(a) = '3'", stats);
    run_text(file, "to_int8(a) = 2", stats);
    run_text(file, "to_uint8(a) = 2::int64", stats);
    run_text(file, "to_int16(a::int8) = 1+2", stats);

    Ok(())
}

fn run_text(file: &mut impl Write, text: &str, stats: &Option<ClusterStatistics>) {
    let func_ctx = FunctionContext::default();
    let cluster_key_id = 0;
    let cluster_keys = vec!["a".to_string(), "b".to_string()];

    let schema = Arc::new(TableSchema::new(vec![
        TableField::new("a", TableDataType::Number(NumberDataType::Int32)),
        TableField::new("b", TableDataType::Number(NumberDataType::Int32)),
    ]));

    let columns = [("a", Int32Type::data_type()), ("b", Int32Type::data_type())];
    let expr = parse_expr(text, &columns);

    let index =
        PageIndex::try_create(func_ctx, cluster_key_id, cluster_keys, &expr, schema).unwrap();

    writeln!(file, "text      : {text}").unwrap();
    writeln!(file, "expr      : {expr}").unwrap();

    match index.apply(stats) {
        Err(err) => {
            writeln!(file, "err       : {err}").unwrap();
        }

        Ok((keep, range)) => {
            writeln!(file, "keep      : {keep}").unwrap();
            writeln!(file, "range     : {range:?}").unwrap();
        }
    };
    writeln!(file).unwrap();
}
