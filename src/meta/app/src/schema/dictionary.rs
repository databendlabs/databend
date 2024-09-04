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

use core::fmt;
use std::collections::BTreeMap;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchema;
use educe::Educe;

use super::dictionary_name_ident::DictionaryNameIdent;
use crate::tenant::Tenant;
use crate::tenant::ToTenant;

/// Represents the metadata of a dictionary within the system.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct DictionaryMeta {
    /// The source of the dictionary, which specifies where the dictionary data comes from, like `MySQL`.
    pub source: String,
    /// Specify the configuration related to the data source in the form of key-value pairs.
    /// For example, `host='localhost' user='root' password='1234'`
    pub options: BTreeMap<String, String>,
    /// Schema refers to an external table that corresponds to the dictionary.
    /// This is typically used to understand the layout and types of data within the dictionary.
    pub schema: Arc<TableSchema>,
    /// A set of key-value pairs is used to represent the annotations for each field in the dictionary, the key being column_id.
    /// For example, if we have `id, address` fields, then field_comments could be `[ '1=student's number','2=home address']`
    pub field_comments: BTreeMap<u32, String>,
    /// A list of primary column IDs.
    /// For example, vec![1, 2] indicating the first and second columns are the primary keys.
    pub primary_column_ids: Vec<u32>,
    /// A general comment string that can be used to provide additional notes or information about the dictionary.
    pub comment: String,
    /// The timestamp indicating when the dictionary was created, in Coordinated Universal Time (UTC).
    pub created_on: DateTime<Utc>,
    /// if used in CreateDictionaryReq,
    /// `updated_on` MUST set to None.
    pub updated_on: Option<DateTime<Utc>>,
}

impl Display for DictionaryMeta {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Source: {}={:?}, Schema: {:?}, Primary_Column_Id: {:?}, CreatedOn: {:?}",
            self.source, self.options, self.schema, self.primary_column_ids, self.created_on
        )
    }
}

impl Default for DictionaryMeta {
    fn default() -> Self {
        DictionaryMeta {
            source: "".to_string(),
            options: BTreeMap::new(),
            schema: Arc::new(TableSchema::empty()),
            primary_column_ids: Vec::new(),
            created_on: Utc::now(),
            updated_on: None,
            comment: "".to_string(),
            field_comments: BTreeMap::new(),
        }
    }
}

impl DictionaryMeta {
    pub fn build_connection_url(&self) -> Result<String> {
        let mut url = String::new();
        if self.source.to_lowercase() == *"mysql" {
            let username = match self.options.get("username") {
                Some(user) => user,
                None => return Err(ErrorCode::MissingDictionaryOption("Miss option `username`")),
            };
            let password = match self.options.get("password") {
                Some(psw) => psw,
                None => return Err(ErrorCode::MissingDictionaryOption("Miss option `password`")),
            };
            let host = match self.options.get("host") {
                Some(host) => host,
                None => return Err(ErrorCode::MissingDictionaryOption("Miss option `host`")),
            };
            let port = match self.options.get("port") {
                Some(port) => port,
                None => return Err(ErrorCode::MissingDictionaryOption("Miss option `port`")),
            };
            let db = match self.options.get("db") {
                Some(db) => db,
                None => return Err(ErrorCode::MissingDictionaryOption("Miss option `db`")),
            };
            url = format!("mysql://{}:{}@{}:{}/{}", username, password, host, port, db).to_string();
        } else if self.source.to_lowercase() == *"redis" {
            let host = match self.options.get("host") {
                Some(host) => host,
                None => return Err(ErrorCode::MissingDictionaryOption("Miss option `host`")),
            };
            let port = match self.options.get("port") {
                Some(port) => port,
                None => return Err(ErrorCode::MissingDictionaryOption("Miss option `port`")),
            };
            url = format!("tcp://{}:{}", host, port).to_string();
        }
        Ok(url)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateDictionaryReq {
    pub dictionary_ident: DictionaryNameIdent,
    pub dictionary_meta: DictionaryMeta,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct CreateDictionaryReply {
    pub dictionary_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetDictionaryReply {
    pub dictionary_id: u64,
    pub dictionary_meta: DictionaryMeta,
    /// Any change to a dictionary causes the seq to increment
    pub dictionary_meta_seq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListDictionaryReq {
    pub tenant: Tenant,
    pub db_id: u64,
}

impl ListDictionaryReq {
    pub fn new(tenant: impl ToTenant, db_id: u64) -> ListDictionaryReq {
        ListDictionaryReq {
            tenant: tenant.to_tenant(),
            db_id,
        }
    }

    pub fn db_id(&self) -> u64 {
        self.db_id
    }

    pub fn tenant(&self) -> String {
        self.tenant.tenant_name().to_string()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateDictionaryReq {
    pub dictionary_meta: DictionaryMeta,
    pub dictionary_ident: DictionaryNameIdent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateDictionaryReply {
    pub dictionary_id: u64,
}

impl DictionaryMeta {
    fn build_sql_connection_url(&self) -> Result<String> {
        let username = match self.options.get("username") {
            Some(user) => user,
            None => return Err(ErrorCode::MissingDictionaryOption("Miss option `username`")),
        };
        let password = match self.options.get("password") {
            Some(psw) => psw,
            None => return Err(ErrorCode::MissingDictionaryOption("Miss option `password`")),
        };
        let host = match self.options.get("host") {
            Some(host) => host,
            None => return Err(ErrorCode::MissingDictionaryOption("Miss option `host`")),
        };
        let port = match self.options.get("port") {
            Some(port) => port,
            None => return Err(ErrorCode::MissingDictionaryOption("Miss option `port`")),
        };
        let db = match self.options.get("db") {
            Some(db) => db,
            None => return Err(ErrorCode::MissingDictionaryOption("Miss option `db`")),
        };
        Ok(format!(
            "mysql://{}:{}@{}:{}/{}",
            username, password, host, port, db
        ))
    }

    fn build_redis_connection_url(&self) -> Result<String> {
        let host = match self.options.get("host") {
            Some(host) => host,
            None => return Err(ErrorCode::MissingDictionaryOption("Miss option `host`")),
        };
        let port = match self.options.get("port") {
            Some(port) => port,
            None => return Err(ErrorCode::MissingDictionaryOption("Miss option `port`")),
        };
        Ok(format!("tcp://{}:{}", host, port))
    }

    pub fn build_dictionary_source(
        &self,
        key_field: &str,
        value_field: &str,
    ) -> Result<DictionarySource> {
        match self.source.as_str() {
            "mysql" => {
                let connection_url = self.build_sql_connection_url()?;
                let table = match self.options.get("table") {
                    Some(table) => table,
                    None => return Err(ErrorCode::MissingDictionaryOption("Miss option `table`")),
                };
                Ok(DictionarySource::Mysql(SqlSource {
                    connection_url,
                    table: table.to_string(),
                    key_field: key_field.to_string(),
                    value_field: value_field.to_string(),
                }))
            }
            "redis" => {
                let connection_url = self.build_redis_connection_url()?;
                let username = self.options.get("username").cloned();
                let password = self.options.get("password").cloned();
                let db_index = self
                    .options
                    .get("db_index")
                    .map(|i| i.parse::<i64>().unwrap());
                Ok(DictionarySource::Redis(RedisSource {
                    connection_url,
                    username,
                    password,
                    db_index,
                }))
            }
            _ => Err(ErrorCode::Unimplemented(format!(
                "Unsupported source {}",
                self.source
            ))),
        }
    }
}

#[derive(Clone, Debug, Educe, serde::Serialize, serde::Deserialize)]
#[educe(PartialEq, Eq, Hash)]
pub struct RedisSource {
    // Redis connection string `tcp://127.0.0.1:6379`
    pub connection_url: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub db_index: Option<i64>,
}

#[derive(Clone, Debug, Educe, serde::Serialize, serde::Deserialize)]
#[educe(PartialEq, Eq, Hash)]
pub struct SqlSource {
    // MySQL connection string `mysql://user:password@localhost:3306/db`
    pub connection_url: String,
    pub table: String,
    pub key_field: String,
    pub value_field: String,
}

#[derive(Clone, Debug, Educe, serde::Serialize, serde::Deserialize)]
#[educe(PartialEq, Eq, Hash)]
pub enum DictionarySource {
    Mysql(SqlSource),
    Redis(RedisSource),
}
