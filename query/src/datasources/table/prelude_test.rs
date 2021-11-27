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
//

use common_exception::Result;

use crate::configs::Config;
use crate::datasources::table::register_prelude_tbl_engines;
use crate::datasources::table_engine_registry::TableEngineRegistry;

#[test]
pub fn test_register_prelude_tbl_engines() -> Result<()> {
    let table_engine_registry = std::sync::Arc::new(TableEngineRegistry::default());
    let config = Config::default();

    register_prelude_tbl_engines(&table_engine_registry, config)?;
    assert!(table_engine_registry
        .get_table_factory(String::from("CSV"))
        .is_none());
    assert!(table_engine_registry
        .get_table_factory(String::from("PARQUET"))
        .is_none());
    assert!(table_engine_registry
        .get_table_factory(String::from("MEMORY"))
        .is_some());
    assert!(table_engine_registry
        .get_table_factory(String::from("NULL"))
        .is_some());
    assert!(table_engine_registry
        .get_table_factory(String::from("FUSE"))
        .is_some());
    Ok(())
}
