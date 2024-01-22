//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use databend_common_catalog::table_args::TableArgs;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_query::table_functions::GPT2SQLTable;

#[test]
fn test_ai_to_sql_args() -> Result<()> {
    // 1 arg.
    {
        let tbl_args = TableArgs::new_positioned(vec![Scalar::String("prompt".to_string())]);
        let _ = GPT2SQLTable::create("system", "ai_to_sql", 1, tbl_args)?;
    }

    // 2 args.
    {
        let tbl_args = TableArgs::new_positioned(vec![
            Scalar::String("prompt".to_string()),
            Scalar::String("api-key".to_string()),
        ]);
        let result = GPT2SQLTable::create("system", "ai_to_sql", 1, tbl_args);
        assert!(result.is_err());
    }

    Ok(())
}
