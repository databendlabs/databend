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

use databend_query::sql::SQLCommon;

#[test]
fn short_sql_test() {
    {
        let sql = "select 1";
        assert_eq!(SQLCommon::short_sql(sql), sql);
    }
    
    {
        let sql = "insert into a select xxxxxx1";
        assert_eq!(SQLCommon::short_sql(sql), sql);
    }
    
    let size = "INSERT INTO ".len();
    
    {
        let sql = format!("INSERT INTO {}", "x".repeat(100)) ;
        let expect = format!("INSERT INTO {}...", "x".repeat(64 - size)) ;
        assert_eq!(SQLCommon::short_sql(&sql), expect);
    }
    
    {
        let sql = format!("inSerT INTO {}", "x".repeat(100)) ;
        let expect = format!("inSerT INTO {}...", "x".repeat(64 - size)) ;
        assert_eq!(SQLCommon::short_sql(&sql), expect);
    }
}
