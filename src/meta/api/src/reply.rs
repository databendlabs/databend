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

use databend_common_meta_types::protobuf::RaftReply;
use databend_common_meta_types::InvalidReply;
use databend_common_meta_types::MetaAPIError;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::MetaNetworkError;
use databend_common_meta_types::TxnOpResponse;
use databend_common_meta_types::TxnReply;
use serde::de::DeserializeOwned;

use crate::compat_errors::Compatible;
use crate::kv_app_error::KVAppError;

/// Compatible layer:
/// Convert either KVAppError or MetaAPIError to MetaAPIError
pub fn reply_to_api_result<T>(msg: RaftReply) -> Result<T, MetaAPIError>
where T: DeserializeOwned {
    if !msg.data.is_empty() {
        let res: T = serde_json::from_str(&msg.data)
            .map_err(|e| InvalidReply::new("can not decode RaftReply.data", &e))?;
        Ok(res)
    } else {
        let err: Compatible<KVAppError, MetaAPIError> = serde_json::from_str(&msg.error)
            .map_err(|e| InvalidReply::new("can not decode RaftReply.error", &e))?;

        let err = err.into_inner();
        Err(err)
    }
}

/// Compatible layer:
/// Convert either KVAppError or MetaError to MetaError
pub fn reply_to_meta_result<T>(raft_reply: RaftReply) -> Result<T, MetaError>
where T: DeserializeOwned {
    if !raft_reply.data.is_empty() {
        let res: T = serde_json::from_str(&raft_reply.data)
            .map_err(|e| InvalidReply::new("can not decode RaftReply.data", &e))?;
        Ok(res)
    } else {
        let err: Compatible<KVAppError, MetaError> = serde_json::from_str(&raft_reply.error)
            .map_err(|e| InvalidReply::new("can not decode RaftReply.error", &e))?;

        let err = err.into_inner();

        Err(err)
    }
}

/// Compatible layer:
/// Convert txn response to `success` and a series of `TxnOpResponse`.
pub fn txn_reply_to_api_result(
    txn_reply: TxnReply,
) -> Result<(bool, Vec<TxnOpResponse>), MetaAPIError> {
    if txn_reply.error.is_empty() {
        Ok((txn_reply.success, txn_reply.responses))
    } else {
        let err: Compatible<KVAppError, MetaAPIError> = serde_json::from_str(&txn_reply.error)
            .map_err(|e| {
                MetaNetworkError::InvalidReply(InvalidReply::new("invalid TxnReply.error", &e))
            })?;
        let err = err.into_inner();
        Err(err)
    }
}

#[cfg(test)]
mod tests {

    #[derive(serde::Serialize, serde::Deserialize)]
    struct Foo {
        i: i32,
    }

    use databend_common_meta_types::protobuf::RaftReply;
    use databend_common_meta_types::MetaAPIError;
    use databend_common_meta_types::MetaNetworkError;

    use crate::reply::reply_to_api_result;

    #[test]
    fn test_valid_reply() -> anyhow::Result<()> {
        // Unable to decode `.data`

        let msg = RaftReply {
            data: "foo".to_string(),
            error: "".to_string(),
        };
        let res: Result<Foo, MetaAPIError> = reply_to_api_result(msg);
        match res {
            Err(MetaAPIError::NetworkError(MetaNetworkError::InvalidReply(inv_reply))) => {
                assert!(
                    inv_reply
                        .to_string()
                        .starts_with("InvalidReply: can not decode RaftReply.data")
                );
            }
            _ => {
                unreachable!("expect InvalidReply")
            }
        }

        // Unable to decode `.error`

        let msg = RaftReply {
            data: "".to_string(),
            error: "foo".to_string(),
        };
        let res: Result<Foo, MetaAPIError> = reply_to_api_result(msg);
        match res {
            Err(MetaAPIError::NetworkError(MetaNetworkError::InvalidReply(inv_reply))) => {
                assert!(
                    inv_reply
                        .to_string()
                        .starts_with("InvalidReply: can not decode RaftReply.error")
                );
            }
            _ => {
                unreachable!("expect InvalidReply")
            }
        }

        Ok(())
    }
}
