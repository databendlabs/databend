// Copyright 2023 Datafuse Labs.
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

use databend_query::catalogs::InMemoryMetas;
use databend_query::databases::SystemDatabase;
use databend_query::test_kits::*;
use databend_storages_common_table_meta::table_id_ranges::SYS_DB_ID_BEGIN;
use databend_storages_common_table_meta::table_id_ranges::SYS_TBL_ID_BEGIN;

#[test]
fn test_disable_system_table() -> anyhow::Result<()> {
    let mut conf = ConfigBuilder::create().build();

    // Normal.
    {
        let mut sys_db_meta = InMemoryMetas::create(SYS_DB_ID_BEGIN, SYS_TBL_ID_BEGIN);
        sys_db_meta.init_db("system");
        let _ = SystemDatabase::create(&mut sys_db_meta, Some(&conf), "default");
        let t1 = sys_db_meta.get_by_name("system", "clusters");
        assert!(t1.is_ok());
    }

    // Disable.
    {
        conf.query.common.disable_system_table_load = true;

        let mut sys_db_meta = InMemoryMetas::create(SYS_DB_ID_BEGIN, SYS_TBL_ID_BEGIN);
        sys_db_meta.init_db("system");
        let _ = SystemDatabase::create(&mut sys_db_meta, Some(&conf), "default");
        let t1 = sys_db_meta.get_by_name("system", "clusters");
        assert!(t1.is_err());
    }

    Ok(())
}
