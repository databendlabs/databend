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

use anyhow::Result;
use databend_client::{response::QueryResponse, APIClient};

use crate::{rows::Row, schema::SchemaFieldList};

pub struct DatabencConnection {
    pub(crate) client: APIClient,
}

impl DatabencConnection {
    pub fn create(dsn: &str) -> Result<Self> {
        let client = APIClient::from_dsn(dsn)?;
        Ok(Self { client })
    }

    pub async fn query_row(&self, sql: &str) -> Result<Option<Row>> {
        let resp = self.client.query(sql).await?;
        let resp = self.wait_for_data(resp).await?;
        self.finish_query(resp.final_uri).await?;
        let schema = SchemaFieldList::new(resp.schema).try_into()?;
        if resp.data.is_empty() {
            Ok(None)
        } else {
            let row = Row::try_from((schema, resp.data[0].clone()))?;
            Ok(Some(row))
        }
    }

    async fn wait_for_data(&self, pre: QueryResponse) -> Result<QueryResponse> {
        if !pre.data.is_empty() {
            return Ok(pre);
        }
        let mut result = pre;
        // preserve schema since it is no included in the final response
        let schema = result.schema;
        while let Some(next_uri) = result.next_uri {
            result = self.client.query_page(next_uri).await?;
            if !result.data.is_empty() {
                break;
            }
        }
        result.schema = schema;
        Ok(result)
    }

    async fn finish_query(&self, final_uri: Option<String>) -> Result<QueryResponse> {
        match final_uri {
            Some(uri) => self.client.query_page(uri).await,
            None => Err(anyhow::anyhow!("final_uri is empty")),
        }
    }
}
