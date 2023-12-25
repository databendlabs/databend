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

use databend_common_openai::OpenAI;

fn create_openai() -> Option<OpenAI> {
    let key = std::env::var("OPENAI_API_KEY").unwrap_or("".to_string());
    if !key.is_empty() {
        Some(OpenAI::create(
            "".to_string(),
            key,
            "".to_string(),
            "".to_string(),
            "".to_string(),
        ))
    } else {
        None
    }
}

#[test]
fn test_openai_text_completion() {
    let openai = create_openai();
    if let Some(openai) = openai {
        let resp = openai
            .completion_text_request("say hello".to_string())
            .unwrap();

        assert!(resp.0.contains("hello"));
    }
}

#[test]
fn test_openai_sql_completion() {
    let openai = create_openai();
    if let Some(openai) = openai {
        let resp = openai
            .completion_sql_request("### Postgres SQL tables, with their properties:
#
# Employee(id, name, department_id)
# Department(id, name, address)
# Salary_Payments(id, employee_id, amount, date)
#
### A query to list the names of the departments which employed more than 10 employees in the last 3 months
SELECT".to_string())
            .unwrap();

        assert!(resp.0.contains("FROM"));
    }
}
