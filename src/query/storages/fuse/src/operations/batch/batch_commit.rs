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
//

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use common_base::base::Singleton;
use common_exception::Result;
use common_meta_app::schema::TableInfo;
use once_cell::sync::OnceCell;

use crate::operations::batch::table_committer::TableCommitter;
use crate::operations::TableOperationLog;

static BATCH_COMMITTER: OnceCell<Singleton<Arc<BatchCommitter>>> = OnceCell::new();

type TableId = u64;
pub struct BatchCommitter {
    tables: Box<Mutex<HashMap<TableId, Arc<TableCommitter>>>>,
}

impl BatchCommitter {
    pub fn instance() -> Arc<BatchCommitter> {
        match BATCH_COMMITTER.get() {
            None => panic!("BatchCommitter is not init"),
            Some(batch_committer) => batch_committer.get(),
        }
    }

    pub fn init(v: Singleton<Arc<BatchCommitter>>) -> Result<()> {
        let instance = Self {
            tables: Box::new(Mutex::new(Default::default())),
        };
        v.init(Arc::new(instance))?;
        BATCH_COMMITTER.set(v).ok();
        Ok(())
    }

    pub async fn submit(
        &self,
        table_info: &TableInfo,
        log: TableOperationLog,
        overwrite: bool,
    ) -> Result<()> {
        let id = table_info.ident.table_id;
        let table_committer = {
            let mut r = self.tables.lock().unwrap();
            r.entry(id)
                .or_insert_with(|| Arc::new(TableCommitter::new()))
                .clone()
        };
        table_committer.submit(log, overwrite).await
    }
}
