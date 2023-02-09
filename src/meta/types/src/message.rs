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

use openraft::raft::AppendEntriesRequest;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::VoteRequest;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::protobuf::RaftReply;
use crate::protobuf::RaftRequest;
use crate::InvalidReply;
use crate::LogEntry;
use crate::TxnOpResponse;
use crate::TxnReply;

impl tonic::IntoRequest<RaftRequest> for LogEntry {
    fn into_request(self) -> tonic::Request<RaftRequest> {
        let mes = RaftRequest {
            data: serde_json::to_string(&self).expect("fail to serialize"),
        };
        tonic::Request::new(mes)
    }
}

impl TryFrom<RaftRequest> for LogEntry {
    type Error = tonic::Status;

    fn try_from(mes: RaftRequest) -> Result<Self, Self::Error> {
        let req: LogEntry = serde_json::from_str(&mes.data)
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
        Ok(req)
    }
}

impl tonic::IntoRequest<RaftRequest> for AppendEntriesRequest<LogEntry> {
    fn into_request(self) -> tonic::Request<RaftRequest> {
        let mes = RaftRequest {
            data: serde_json::to_string(&self).expect("fail to serialize"),
        };
        tonic::Request::new(mes)
    }
}

impl tonic::IntoRequest<RaftRequest> for &AppendEntriesRequest<LogEntry> {
    fn into_request(self) -> tonic::Request<RaftRequest> {
        let mes = RaftRequest {
            data: serde_json::to_string(self).expect("fail to serialize"),
        };
        tonic::Request::new(mes)
    }
}

impl tonic::IntoRequest<RaftRequest> for InstallSnapshotRequest {
    fn into_request(self) -> tonic::Request<RaftRequest> {
        let mes = RaftRequest {
            data: serde_json::to_string(&self).expect("fail to serialize"),
        };
        tonic::Request::new(mes)
    }
}

impl tonic::IntoRequest<RaftRequest> for &InstallSnapshotRequest {
    fn into_request(self) -> tonic::Request<RaftRequest> {
        let mes = RaftRequest {
            data: serde_json::to_string(self).expect("fail to serialize"),
        };
        tonic::Request::new(mes)
    }
}

impl tonic::IntoRequest<RaftRequest> for VoteRequest {
    fn into_request(self) -> tonic::Request<RaftRequest> {
        let mes = RaftRequest {
            data: serde_json::to_string(&self).expect("fail to serialize"),
        };
        tonic::Request::new(mes)
    }
}

impl tonic::IntoRequest<RaftRequest> for &VoteRequest {
    fn into_request(self) -> tonic::Request<RaftRequest> {
        let mes = RaftRequest {
            data: serde_json::to_string(self).expect("fail to serialize"),
        };
        tonic::Request::new(mes)
    }
}

impl<T, E> From<RaftReply> for Result<T, E>
where
    T: DeserializeOwned,
    E: DeserializeOwned + From<InvalidReply>,
{
    fn from(msg: RaftReply) -> Self {
        if !msg.data.is_empty() {
            let res: T = serde_json::from_str(&msg.data)
                .map_err(|e| InvalidReply::new("can not decode RaftReply.data", &e))?;
            Ok(res)
        } else {
            let err: E = serde_json::from_str(&msg.error)
                .map_err(|e| InvalidReply::new("can not decode RaftReply.error", &e))?;
            Err(err)
        }
    }
}

impl<T, E> From<Result<T, E>> for RaftReply
where
    T: Serialize,
    E: Serialize,
{
    fn from(r: Result<T, E>) -> Self {
        match r {
            Ok(x) => {
                let data = serde_json::to_string(&x).expect("fail to serialize");
                RaftReply {
                    data,
                    error: Default::default(),
                }
            }
            Err(e) => {
                let error = serde_json::to_string(&e).expect("fail to serialize");
                RaftReply {
                    data: Default::default(),
                    error,
                }
            }
        }
    }
}

/// Convert txn response to `success` and a series of `TxnOpResponse`.
/// If `success` is false, then the vec is empty
impl<E> From<TxnReply> for Result<(bool, Vec<TxnOpResponse>), E>
where E: DeserializeOwned
{
    fn from(msg: TxnReply) -> Self {
        if msg.error.is_empty() {
            Ok((msg.success, msg.responses))
        } else {
            let err: E = serde_json::from_str(&msg.error).expect("fail to deserialize");
            Err(err)
        }
    }
}

#[cfg(test)]
mod tests {

    #[derive(serde::Serialize, serde::Deserialize)]
    struct Foo {
        i: i32,
    }

    use crate::protobuf::RaftReply;
    use crate::MetaNetworkError;

    #[test]
    fn test_valid_reply() -> anyhow::Result<()> {
        // Unable to decode `.data`

        let msg = RaftReply {
            data: "foo".to_string(),
            error: "".to_string(),
        };
        let res: Result<Foo, MetaNetworkError> = msg.into();
        match res {
            Err(MetaNetworkError::InvalidReply(inv_reply)) => {
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
        let res: Result<Foo, MetaNetworkError> = msg.into();
        match res {
            Err(MetaNetworkError::InvalidReply(inv_reply)) => {
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
