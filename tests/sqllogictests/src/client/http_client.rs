// Copyright 2021 Datafuse Labs
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

use std::time::Instant;

use databend_common_http::HttpClient as Client;
use databend_common_http::QueryResponse;
use sqllogictest::DBOutput;
use sqllogictest::DefaultColumnType;

use crate::error::Result;
use crate::util::parser_rows;

pub struct HttpClient {
    pub client: Client,
    pub debug: bool,
}

// make error message the same with ErrorCode::display
fn format_error(value: &serde_json::Value) -> String {
    let value = value.as_object().unwrap();
    let detail = value.get("detail").and_then(|v| v.as_str());
    let code = value["code"].as_u64().unwrap();
    let message = value["message"].as_str().unwrap();
    if let Some(detail) = detail {
        format!(
            "http query error: code: {}, Text: {}\n{}",
            code, message, detail
        )
    } else {
        format!("http query error: code: {}, Text: {}", code, message)
    }
}

impl HttpClient {
    pub async fn create() -> Result<Self> {
        let host = "http://127.0.0.1:8000".to_string();
        let username = "root".to_string();
        let password = "".to_string();

        let client = Client::create(host, username, password).await?;

        Ok(Self {
            client,
            debug: false,
        })
    }

    pub async fn query(&mut self, sql: &str) -> Result<DBOutput<DefaultColumnType>> {
        let start = Instant::now();

        let responses = self.client.query(sql).await?;
        if let Some(error) = &responses[0].error {
            return Err(format_error(error).into());
        }

        let mut parsed_rows = vec![];
        for response in responses {
            self.handle_response(&response, &mut parsed_rows)?;
        }
        // Todo: add types to compare
        let mut types = vec![];
        if !parsed_rows.is_empty() {
            types = vec![DefaultColumnType::Any; parsed_rows[0].len()];
        }

        if self.debug {
            println!(
                "Running sql with http client: [{sql}] ({:?})",
                start.elapsed()
            );
        }

        Ok(DBOutput::Rows {
            types,
            rows: parsed_rows,
        })
    }

    fn handle_response(
        &mut self,
        response: &QueryResponse,
        parsed_rows: &mut Vec<Vec<String>>,
    ) -> Result<()> {
        if let Some(data) = &response.data {
            parsed_rows.append(&mut parser_rows(data)?);
        }
        Ok(())
    }
}
