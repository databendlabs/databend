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

use std::collections::HashMap;
use std::sync::Arc;

use common_metatypes::MetaId;

use crate::catalogs::SYS_TBL_FUC_ID_END;
use crate::catalogs::SYS_TBL_FUNC_ID_BEGIN;
use crate::datasources::table_func::NumbersTable;
use crate::datasources::table_func_engine::TableFuncEngine;
use crate::datasources::table_func_engine_registry::TableFuncEngineRegistry;

pub fn prelude_func_engines() -> TableFuncEngineRegistry {
    let mut id = SYS_TBL_FUNC_ID_BEGIN;
    let mut next_id = || -> MetaId {
        if id >= SYS_TBL_FUC_ID_END {
            panic!("function table id used up")
        } else {
            let r = id;
            id += 1;
            r
        }
    };

    let mut func_factory_registry = HashMap::new();

    let number_table_func_factory: Arc<dyn TableFuncEngine> = Arc::new(NumbersTable::create);

    func_factory_registry.insert(
        "numbers".to_string(),
        (next_id(), number_table_func_factory.clone()),
    );
    func_factory_registry.insert(
        "numbers_mt".to_string(),
        (next_id(), number_table_func_factory.clone()),
    );
    func_factory_registry.insert(
        "numbers_local".to_string(),
        (next_id(), number_table_func_factory),
    );
    func_factory_registry
}
