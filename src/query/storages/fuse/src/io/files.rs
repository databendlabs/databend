//  Copyright 2022 Datafuse Labs.
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

use std::mem;
use std::sync::Arc;

use common_catalog::table_context::TableContext;
use common_exception::Result;
use opendal::Operator;

// File related operations.
pub struct Files {
    operator: Operator,
}

impl Files {
    pub fn create(_: Arc<dyn TableContext>, operator: Operator) -> Self {
        Self { operator }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn remove_file_in_batch(
        &self,
        file_locations: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<()> {
        let iter = file_locations.into_iter().map(|v| v.as_ref().to_string());

        // OpenDAL support delete via a stream. But it will cause higher
        // ranked problems. So let's write them by hand.
        let bo = self.operator.batch();

        let mut paths = Vec::with_capacity(1000);
        for path in iter {
            paths.push(path);
            if paths.len() >= 1000 {
                bo.remove(mem::take(&mut paths)).await?;
            }
        }

        if !paths.is_empty() {
            bo.remove(mem::take(&mut paths)).await?;
        }

        Ok(())
    }
}
