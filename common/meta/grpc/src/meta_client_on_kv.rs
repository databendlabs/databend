// Copyright 2021 Datafuse Labs.
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

use std::convert::Infallible;
use std::fmt::Debug;
use std::string::FromUtf8Error;
use std::time::Duration;

use common_exception::ErrorCode;
use common_grpc::RpcClientTlsConfig;
use common_meta_types::DatabaseNameIdent;
use common_meta_types::DatabaseTenantIdIdent;
use common_tracing::tracing;

use crate::MetaGrpcClient;
use crate::MetaGrpcClientConf;

/// Metasrv client base on only KVApi.
///
/// It implement transactional operation on the client side, through `KVApi::transaction()`.
///
/// TODO(xp): consider remove the `impl MetaAPI for MetaGrpcClient` when this is done.
pub struct MetaClientOnKV {
    pub(crate) inner: MetaGrpcClient,
}

impl MetaClientOnKV {
    pub async fn try_new(conf: &MetaGrpcClientConf) -> Result<MetaClientOnKV, Infallible> {
        Ok(Self {
            inner: MetaGrpcClient::try_new(conf).await?,
        })
    }

    #[tracing::instrument(level = "debug", skip(password))]
    pub async fn try_create(
        addr: &str,
        username: &str,
        password: &str,
        timeout: Option<Duration>,
        conf: Option<RpcClientTlsConfig>,
    ) -> Result<Self, ErrorCode> {
        Ok(Self {
            inner: MetaGrpcClient::try_create(addr, username, password, timeout, conf).await?,
        })
    }
}

/// Key for database id generator
#[derive(Debug)]
pub struct DatabaseIdGen {}

/// Key for table id generator
#[derive(Debug)]
pub struct TableIdGen {}

const PREFIX_DATABASE: &str = "__fd_database";
// const PREFIX_TABLE: & str = "__fd_table";
const PREFIX_ID_GEN: &str = "__fd_id_gen";
const SEG_BY_NAME: &str = "by_name";
const SEG_BY_ID: &str = "by_id";

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KVApiKeyError {
    #[error(transparent)]
    FromUtf8Error(#[from] FromUtf8Error),

    #[error("Expect {i}-th segment to be '{expect}', but: '{got}'")]
    InvalidSegment {
        i: usize,
        expect: String,
        got: String,
    },

    #[error("Expect {expect} segments, but: '{got}'")]
    WrongNumberOfSegments { expect: usize, got: String },

    #[error("Invalid id string: '{s}': {reason}")]
    InvalidId { s: String, reason: String },
}

/// Convert structured key to a string key used by KVApi and backwards
pub trait KVApiKey: Debug
where Self: Sized
{
    const PREFIX: &'static str;

    fn to_key(&self) -> String;
    fn from_key(s: &str) -> Result<Self, KVApiKeyError>;
}

impl KVApiKey for DatabaseNameIdent {
    const PREFIX: &'static str = PREFIX_DATABASE;

    fn to_key(&self) -> String {
        format!(
            "{}/{}/{}/{}",
            Self::PREFIX,
            escape_for_key(&self.tenant),
            SEG_BY_NAME,
            escape_for_key(&self.db_name),
        )
    }

    fn from_key(s: &str) -> Result<Self, KVApiKeyError> {
        let mut elts = s.split('/');

        let prefix = check_segment_present(elts.next(), 0, s)?;
        check_segment(prefix, 0, Self::PREFIX)?;

        let tenant = check_segment_present(elts.next(), 1, s)?;

        let by_name = check_segment_present(elts.next(), 2, s)?;
        check_segment(by_name, 2, SEG_BY_NAME)?;

        let db_name = check_segment_present(elts.next(), 3, s)?;

        check_segment_absent(elts.next(), 4, s)?;

        let tenant = unescape_for_key(tenant)?;
        let db_name = unescape_for_key(db_name)?;

        Ok(DatabaseNameIdent { tenant, db_name })
    }
}

impl KVApiKey for DatabaseTenantIdIdent {
    const PREFIX: &'static str = PREFIX_DATABASE;

    fn to_key(&self) -> String {
        format!(
            "{}/{}/{}/{}",
            Self::PREFIX,
            escape_for_key(&self.tenant),
            SEG_BY_ID,
            self.db_id,
        )
    }

    fn from_key(s: &str) -> Result<Self, KVApiKeyError> {
        let mut elts = s.split('/');

        let prefix = check_segment_present(elts.next(), 0, s)?;
        check_segment(prefix, 0, Self::PREFIX)?;

        let tenant = check_segment_present(elts.next(), 1, s)?;

        let by_id = check_segment_present(elts.next(), 2, s)?;
        check_segment(by_id, 2, SEG_BY_ID)?;

        let db_id = check_segment_present(elts.next(), 3, s)?;

        check_segment_absent(elts.next(), 4, s)?;

        let tenant = unescape_for_key(tenant)?;
        let db_id = deserialize_id(db_id)?;

        Ok(DatabaseTenantIdIdent { tenant, db_id })
    }
}

impl KVApiKey for DatabaseIdGen {
    const PREFIX: &'static str = PREFIX_ID_GEN;

    fn to_key(&self) -> String {
        format!("{}/database_id", Self::PREFIX)
    }

    fn from_key(_s: &str) -> Result<Self, KVApiKeyError> {
        unimplemented!()
    }
}

impl KVApiKey for TableIdGen {
    const PREFIX: &'static str = PREFIX_ID_GEN;

    fn to_key(&self) -> String {
        format!("{}/table_id", Self::PREFIX)
    }

    fn from_key(_s: &str) -> Result<Self, KVApiKeyError> {
        unimplemented!()
    }
}

/// Function that escapes special characters in a string.
///
/// All characters except digit, alphabet and '_' are treated as special characters.
/// A special character will be converted into "%num" where num is the hexadecimal form of the character.
///
/// # Example
/// ```
/// let key = "data_bend!!";
/// let new_key = escape_for_key(&key);
/// assert_eq!("data_bend%21%21".to_string(), new_key);
/// ```
pub fn escape_for_key(key: &str) -> String {
    let mut new_key = Vec::with_capacity(key.len());

    fn hex(num: u8) -> u8 {
        match num {
            0..=9 => b'0' + num,
            10..=15 => b'a' + (num - 10),
            unreachable => unreachable!("Unreachable branch num = {}", unreachable),
        }
    }

    for char in key.as_bytes() {
        match char {
            b'0'..=b'9' => new_key.push(*char),
            b'_' | b'a'..=b'z' | b'A'..=b'Z' => new_key.push(*char),
            _other => {
                new_key.push(b'%');
                new_key.push(hex(*char / 16));
                new_key.push(hex(*char % 16));
            }
        }
    }

    // Safe unwrap(): there are no invalid utf char in it.
    String::from_utf8(new_key).unwrap()
}

/// The reverse function of escape_for_key.
///
/// # Example
/// ```
/// let key = "data_bend%21%21";
/// let original_key = unescape_for_key(&key);
/// assert_eq!(Ok("data_bend!!".to_string()), original_key);
/// ```
pub fn unescape_for_key(key: &str) -> Result<String, FromUtf8Error> {
    let mut new_key = Vec::with_capacity(key.len());

    fn unhex(num: u8) -> u8 {
        match num {
            b'0'..=b'9' => num - b'0',
            b'a'..=b'f' => num - b'a' + 10,
            unreachable => unreachable!("Unreachable branch num = {}", unreachable),
        }
    }

    let bytes = key.as_bytes();

    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' => {
                // The last byte of the string won't be '%'
                let mut num = unhex(bytes[index + 1]) * 16;
                num += unhex(bytes[index + 2]);
                new_key.push(num);
                index += 3;
            }
            other => {
                new_key.push(other);
                index += 1;
            }
        }
    }

    String::from_utf8(new_key)
}

/// Check if the `i`-th segment absent.
fn check_segment_absent(elt: Option<&str>, i: usize, key: &str) -> Result<(), KVApiKeyError> {
    if elt.is_some() {
        Err(KVApiKeyError::WrongNumberOfSegments {
            expect: i,
            got: key.to_string(),
        })
    } else {
        Ok(())
    }
}

/// Check if the `i`-th segment present.
fn check_segment_present<'a>(
    elt: Option<&'a str>,
    i: usize,
    key: &str,
) -> Result<&'a str, KVApiKeyError> {
    if let Some(s) = elt {
        Ok(s)
    } else {
        Err(KVApiKeyError::WrongNumberOfSegments {
            expect: i + 1,
            got: key.to_string(),
        })
    }
}

/// Check if the `i`-th segment equals `expect`.
fn check_segment(elt: &str, i: usize, expect: &str) -> Result<(), KVApiKeyError> {
    if elt != expect {
        return Err(KVApiKeyError::InvalidSegment {
            i,
            expect: expect.to_string(),
            got: elt.to_string(),
        });
    }
    Ok(())
}

fn deserialize_id(s: &str) -> Result<u64, KVApiKeyError> {
    let id = s.parse::<u64>().map_err(|e| KVApiKeyError::InvalidId {
        s: s.to_string(),
        reason: e.to_string(),
    })?;

    Ok(id)
}
