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

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::Series;
use common_datavalues::prelude::SeriesFrom;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_infallible::Mutex;
use common_meta_types::TableMeta;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::InsertIntoPlan;
use tempfile::TempDir;
use uuid::Uuid;

use crate::catalogs::Catalog;
use crate::catalogs::Table;
use crate::configs::Config;
use crate::sessions::DatabendQueryContextRef;

pub struct TestFixture {
    _tmp_dir: TempDir,
    ctx: DatabendQueryContextRef,
    prefix: String,
}

impl TestFixture {
    pub async fn new() -> TestFixture {
        let tmp_dir = TempDir::new().unwrap();
        let mut config = Config::default();
        // make sure we are suing `Disk` storage
        config.storage.storage_type = "Disk".to_string();
        // use `TempDir` as root path (auto clean)
        config.storage.disk.data_path = tmp_dir.path().to_str().unwrap().to_string();
        let ctx = crate::tests::try_create_context_with_config(config).unwrap();

        let random_prefix: String = Uuid::new_v4().to_simple().to_string();
        // prepare a randomly named default database
        let db_name = gen_db_name(&random_prefix);
        let plan = CreateDatabasePlan {
            if_not_exists: false,
            db: db_name,
            options: Default::default(),
        };
        ctx.get_catalog().create_database(plan).await.unwrap();

        Self {
            _tmp_dir: tmp_dir,
            ctx,
            prefix: random_prefix,
        }
    }

    pub fn ctx(&self) -> DatabendQueryContextRef {
        self.ctx.clone()
    }

    pub fn default_db(&self) -> String {
        gen_db_name(&self.prefix)
    }

    pub fn default_table(&self) -> String {
        format!("{}_test_tbl", self.prefix)
    }

    pub fn default_schema() -> DataSchemaRef {
        DataSchemaRefExt::create(vec![DataField::new("id", DataType::Int32, false)])
    }

    pub fn default_crate_table_plan(&self) -> CreateTablePlan {
        CreateTablePlan {
            if_not_exists: false,
            db: self.default_db(),
            table: self.default_table(),
            table_meta: TableMeta {
                schema: TestFixture::default_schema(),
                engine: "FUSE".to_string(),
                options: Default::default(),
            },
        }
    }

    pub fn insert_plan_of_table(&self, table: &dyn Table, block_num: u32) -> InsertIntoPlan {
        InsertIntoPlan {
            db_name: self.default_db(),
            tbl_name: self.default_table(),
            tbl_id: table.get_id(),
            schema: TestFixture::default_schema(),
            input_stream: Arc::new(Mutex::new(Some(Box::pin(futures::stream::iter(
                TestFixture::gen_block_stream(block_num),
            ))))),
        }
    }

    pub fn gen_block_stream(num: u32) -> Vec<DataBlock> {
        (0..num)
            .into_iter()
            .map(|_v| {
                let schema =
                    DataSchemaRefExt::create(vec![DataField::new("a", DataType::Int32, false)]);
                DataBlock::create_by_array(schema, vec![Series::new(vec![1, 2, 3])])
            })
            .collect()
    }
}

fn gen_db_name(prefix: &str) -> String {
    format!("{}_default", prefix)
}
