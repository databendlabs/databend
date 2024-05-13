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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;

use chrono::DateTime;
use chrono::Utc;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::share::ShareGrantObjectPrivilege;
use enumflags2::BitFlags;
use poem::error::Result as PoemResult;
use poem::FromRequest;
use poem::Request;
use poem::RequestBody;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LambdaInput {
    pub authorization: String,
    pub share_name: String,
    pub tenant_id: String,
    pub table_name: String,
    pub request_files: Vec<RequestFile>,
    pub request_id: String,
}

// AsRef implement
impl AsRef<LambdaInput> for LambdaInput {
    fn as_ref(&self) -> &LambdaInput {
        self
    }
}

impl LambdaInput {
    pub fn new(
        authorization: String,
        share_name: String,
        tenant_id: String,
        table_name: String,
        request_files: Vec<RequestFile>,
        request_id: Option<String>,
    ) -> Self {
        let request_id = match request_id {
            Some(id) => id,
            None => uuid::Uuid::new_v4().to_string(),
        };
        LambdaInput {
            authorization,
            share_name,
            tenant_id,
            table_name,
            request_files,
            request_id,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct LambdaOutput {
    presign_files: Vec<PresignFileResponse>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SharedTableResponse {
    pub table: String,
    pub id: u64,
    pub location: String,
    presigned_url_timeout: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PresignFileResponse {
    presigned_url: String,
    headers: HashMap<String, String>,
    method: String,
    path: String,
}

// if header key is duplicate, it would adopt the last one.
fn get_presign_headers(h: http::HeaderMap) -> HashMap<String, String> {
    let mut ans = HashMap::new();
    for (k, v) in h {
        if let Some(h) = k {
            let v = String::from_utf8_lossy(v.as_bytes()).into_owned();
            ans.insert(h.to_string(), v);
        }
    }
    ans
}

impl PresignFileResponse {
    pub fn new(res: &opendal::raw::PresignedRequest, path: String) -> PresignFileResponse {
        PresignFileResponse {
            presigned_url: res.uri().to_string(),
            headers: get_presign_headers(res.header().clone()),
            method: res.method().to_string(),
            path,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RequestFile {
    pub file_name: String,
    pub method: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TableMetaResponse {
    pub table_info_map: BTreeMap<String, TableInfo>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TableMetaLambdaInput {
    pub authorization: String,
    pub share_name: String,
    pub tenant_id: String,
    pub request_tables: HashSet<String>,
    pub request_id: String,
}

// AsRef implement
impl AsRef<TableMetaLambdaInput> for TableMetaLambdaInput {
    fn as_ref(&self) -> &TableMetaLambdaInput {
        self
    }
}

impl TableMetaLambdaInput {
    pub fn new(
        authorization: String,
        share_name: String,
        tenant_id: String,
        request_tables: Vec<String>,
        request_id: Option<String>,
    ) -> Self {
        let request_id = match request_id {
            Some(id) => id,
            None => uuid::Uuid::new_v4().to_string(),
        };
        TableMetaLambdaInput {
            authorization,
            share_name,
            tenant_id,
            request_tables: HashSet::from_iter(request_tables),
            request_id,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct TableMetaLambdaOutput {
    table_meta_resp: TableMetaResponse,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Credentials {
    pub(crate) token: String,
}

impl<'a> FromRequest<'a> for &'a Credentials {
    #[async_backtrace::framed]
    async fn from_request(req: &'a Request, _body: &mut RequestBody) -> PoemResult<Self> {
        Ok(req
            .extensions()
            .get::<Credentials>()
            .expect("To use the `Credentials` extractor, the `Credentials` is required"))
    }
}

// SharingConfig contains all shared table information
// it is tenant based, expected to store in file path tenant/share/sharing-config.yaml
// example of a sharing config
#[derive(Debug, Deserialize, Serialize)]
pub struct SharingConfig {
    pub share_specs: HashMap<String, ShareSpec>,
}

impl SharingConfig {
    // get table would return matched table configuration under given share name
    // if requester tenant id is not permitted, it will return error
    pub fn get_tables(self, input: &LambdaInput) -> Result<Option<SharedTableResponse>> {
        let share_name = input.share_name.clone();
        let tenant_id = input.tenant_id.clone();
        let table_name = input.table_name.clone();
        let share_spec = self.share_specs.get(&*share_name);
        if share_spec.is_none() {
            return Ok(None);
        }
        match share_spec {
            None => Ok(None),
            Some(share) => {
                if !share.tenants.contains(&tenant_id) {
                    return Err(ErrorCode::AuthenticateFailure(format!(
                        "tenant {} is not allowed to access share {}",
                        tenant_id, share_name
                    )));
                }
                // find table matches table_name
                let mut target = None;
                for table in &share.tables {
                    if table.name == table_name {
                        target = Some(table.into());
                        break;
                    }
                }
                Ok(target)
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ShareSpec {
    pub name: String,
    pub share_id: u64,
    pub version: u64,
    pub database: Option<DatabaseSpec>,
    pub tables: Vec<TableSpec>,
    pub tenants: Vec<String>,
    pub db_privileges: Option<BitFlags<ShareGrantObjectPrivilege>>,
    pub comment: Option<String>,
    pub share_on: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DatabaseSpec {
    pub name: String,
    pub location: String,
    pub id: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TableSpec {
    pub name: String,
    pub location: String,
    pub database_id: u64,
    pub table_id: u64,
    pub presigned_url_timeout: String,
}

// into SharedTableResponse
impl From<&TableSpec> for SharedTableResponse {
    fn from(table_spec: &TableSpec) -> Self {
        SharedTableResponse {
            table: table_spec.name.to_string(),
            id: table_spec.table_id,
            location: table_spec.location.to_string(),
            presigned_url_timeout: Option::from(table_spec.presigned_url_timeout.clone()),
        }
    }
}
