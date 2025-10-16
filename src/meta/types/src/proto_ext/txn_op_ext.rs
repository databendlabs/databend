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

mod request_ext;

use std::fmt;
use std::time::Duration;

use display_more::DisplayOptionExt;

use crate::protobuf as pb;

impl pb::TxnOp {
    /// Create a txn operation that puts a record.
    pub fn put(key: impl ToString, value: Vec<u8>) -> pb::TxnOp {
        pb::TxnOp {
            request: Some(pb::txn_op::Request::Put(pb::TxnPutRequest::new(
                key, value, true, None, None,
            ))),
        }
    }

    /// Create a txn operation that puts a record with ttl.
    ///
    /// `ttl` is relative expire time while `expire_at` is absolute expire time.
    pub fn put_with_ttl(key: impl ToString, value: Vec<u8>, ttl: Option<Duration>) -> pb::TxnOp {
        pb::TxnOp {
            request: Some(pb::txn_op::Request::Put(pb::TxnPutRequest::new(
                key,
                value,
                true,
                None,
                ttl.map(|d| d.as_millis() as u64),
            ))),
        }
    }

    pub fn with_ttl(mut self, ttl: Option<Duration>) -> Self {
        let ttl_ms = ttl.map(|d| d.as_millis() as u64);
        match &mut self.request {
            Some(pb::txn_op::Request::Put(p)) => p.ttl_ms = ttl_ms,
            Some(pb::txn_op::Request::PutSequential(p)) => p.ttl_ms = ttl_ms,
            _ => {}
        }
        self
    }

    pub fn with_expires_at(mut self, expires_at: Option<Duration>) -> Self {
        match &mut self.request {
            // Require 1.2.770 that support expire_at in milliseconds
            Some(pb::txn_op::Request::Put(p)) => {
                p.expire_at = expires_at.map(|d| d.as_millis() as u64)
            }
            Some(pb::txn_op::Request::PutSequential(p)) => {
                p.expires_at_ms = expires_at.map(|d| d.as_millis() as u64)
            }
            _ => {}
        }
        self
    }

    pub fn with_expires_at_ms(self, expires_at_ms: Option<u64>) -> Self {
        self.with_expires_at(expires_at_ms.map(Duration::from_millis))
    }

    /// Create a new `TxnOp` with a `Delete` operation.
    pub fn delete(key: impl ToString) -> Self {
        Self::delete_exact(key, None)
    }

    /// Create a new `TxnOp` with a `Delete` operation that will be executed only when the `seq` matches.
    pub fn delete_exact(key: impl ToString, seq: Option<u64>) -> Self {
        pb::TxnOp {
            request: Some(pb::txn_op::Request::Delete(pb::TxnDeleteRequest::new(
                key, true, seq,
            ))),
        }
    }

    /// Create a new `TxnOp` with a `Get` operation.
    pub fn get(key: impl ToString) -> Self {
        pb::TxnOp {
            request: Some(pb::txn_op::Request::Get(pb::TxnGetRequest::new(key))),
        }
    }

    /// Fetch and add delta to a u64 value (backward compatible, uses max_value=0).
    pub fn fetch_add_u64(key: impl ToString, delta: i64) -> Self {
        pb::TxnOp {
            request: Some(pb::txn_op::Request::FetchIncreaseU64(
                pb::FetchIncreaseU64 {
                    key: key.to_string(),
                    match_seq: None,
                    delta,
                    max_value: 0,
                },
            )),
        }
    }

    /// Fetch and increase: after = max(current, max_value) + delta
    pub fn fetch_increase_u64(key: impl ToString, max_value: u64, delta: i64) -> Self {
        pb::TxnOp {
            request: Some(pb::txn_op::Request::FetchIncreaseU64(
                pb::FetchIncreaseU64 {
                    key: key.to_string(),
                    match_seq: None,
                    delta,
                    max_value,
                },
            )),
        }
    }

    /// Fetch and update to max value (convenience method: delta=0)
    pub fn fetch_max_u64(key: impl ToString, max_value: u64) -> Self {
        Self::fetch_increase_u64(key, max_value, 0)
    }

    pub fn put_sequential(
        prefix: impl ToString,
        sequence_key: impl ToString,
        value: Vec<u8>,
    ) -> Self {
        let put_sequential = pb::PutSequential::new(prefix, sequence_key, value);
        pb::TxnOp {
            request: Some(pb::txn_op::Request::PutSequential(put_sequential)),
        }
    }

    /// Add a match-sequence-number condition to the operation.
    ///
    /// If the sequence number does not match, the operation won't be take place.
    pub fn match_seq(mut self, seq: Option<u64>) -> Self {
        let req = self
            .request
            .as_mut()
            .expect("TxnOp must have a non-None request field");

        match req {
            pb::txn_op::Request::Delete(p) => p.match_seq = seq,
            pb::txn_op::Request::FetchIncreaseU64(d) => d.match_seq = seq,
            _ => {
                unreachable!("Not support match_seq for: {}", req)
            }
        }

        self
    }
}

impl fmt::Display for pb::TxnOp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.request.display())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_display_txn_op() {
        let op = pb::TxnOp::put("k1", b"v1".to_vec());
        assert_eq!(format!("{}", op), "Put(Put key=k1)");
    }

    #[test]
    fn test_put() {
        let op = pb::TxnOp::put("test_key", b"test_value".to_vec());

        let Some(pb::txn_op::Request::Put(put_req)) = &op.request else {
            panic!("Expected Put request");
        };

        assert_eq!(put_req.key, "test_key");
        assert_eq!(put_req.value, b"test_value");
        assert!(put_req.prev_value);
        assert!(put_req.expire_at.is_none());
        assert!(put_req.ttl_ms.is_none());
    }

    #[test]
    fn test_put_with_ttl() {
        let ttl = Duration::from_secs(300);
        let op = pb::TxnOp::put_with_ttl("ttl_key", b"ttl_value".to_vec(), Some(ttl));

        let Some(pb::txn_op::Request::Put(put_req)) = &op.request else {
            panic!("Expected Put request");
        };

        assert_eq!(put_req.key, "ttl_key");
        assert_eq!(put_req.value, b"ttl_value");
        assert!(put_req.prev_value);
        assert!(put_req.expire_at.is_none());
        assert_eq!(put_req.ttl_ms, Some(300_000)); // 300 seconds = 300,000 ms
    }

    #[test]
    fn test_put_with_ttl_none() {
        let op = pb::TxnOp::put_with_ttl("no_ttl_key", b"no_ttl_value".to_vec(), None);

        let Some(pb::txn_op::Request::Put(put_req)) = &op.request else {
            panic!("Expected Put request");
        };

        assert_eq!(put_req.key, "no_ttl_key");
        assert_eq!(put_req.value, b"no_ttl_value");
        assert!(put_req.ttl_ms.is_none());
    }

    #[test]
    fn test_with_ttl() {
        let mut op = pb::TxnOp::put("key", b"value".to_vec());
        op = op.with_ttl(Some(Duration::from_millis(5000)));

        let Some(pb::txn_op::Request::Put(put_req)) = &op.request else {
            panic!("Expected Put request");
        };

        assert_eq!(put_req.ttl_ms, Some(5000));
    }

    #[test]
    fn test_with_ttl_non_put_operation() {
        // with_ttl should only work on Put operations
        let mut op = pb::TxnOp::get("key");
        op = op.with_ttl(Some(Duration::from_millis(5000)));

        // Should remain a Get operation unchanged
        assert!(matches!(op.request, Some(pb::txn_op::Request::Get(_))));
    }

    #[test]
    fn test_delete() {
        let op = pb::TxnOp::delete("delete_key");

        let Some(pb::txn_op::Request::Delete(delete_req)) = &op.request else {
            panic!("Expected Delete request");
        };

        assert_eq!(delete_req.key, "delete_key");
        assert!(delete_req.prev_value);
        assert!(delete_req.match_seq.is_none());
    }

    #[test]
    fn test_delete_exact_with_seq() {
        let op = pb::TxnOp::delete_exact("exact_key", Some(42));

        let Some(pb::txn_op::Request::Delete(delete_req)) = &op.request else {
            panic!("Expected Delete request");
        };

        assert_eq!(delete_req.key, "exact_key");
        assert!(delete_req.prev_value);
        assert_eq!(delete_req.match_seq, Some(42));
    }

    #[test]
    fn test_delete_exact_without_seq() {
        let op = pb::TxnOp::delete_exact("key_no_seq", None);

        let Some(pb::txn_op::Request::Delete(delete_req)) = &op.request else {
            panic!("Expected Delete request");
        };

        assert_eq!(delete_req.key, "key_no_seq");
        assert!(delete_req.match_seq.is_none());
    }

    #[test]
    fn test_get() {
        let op = pb::TxnOp::get("get_key");

        let Some(pb::txn_op::Request::Get(get_req)) = &op.request else {
            panic!("Expected Get request");
        };

        assert_eq!(get_req.key, "get_key");
    }

    #[test]
    fn test_fetch_add_u64() {
        let op = pb::TxnOp::fetch_add_u64("counter_key", 10);

        let Some(pb::txn_op::Request::FetchIncreaseU64(fetch_req)) = &op.request else {
            panic!("Expected FetchIncreaseU64 request");
        };

        assert_eq!(fetch_req.key, "counter_key");
        assert_eq!(fetch_req.delta, 10);
        assert_eq!(fetch_req.max_value, 0);
        assert!(fetch_req.match_seq.is_none());
    }

    #[test]
    fn test_fetch_add_u64_negative_delta() {
        let op = pb::TxnOp::fetch_add_u64("counter_key", -5);

        let Some(pb::txn_op::Request::FetchIncreaseU64(fetch_req)) = &op.request else {
            panic!("Expected FetchIncreaseU64 request");
        };

        assert_eq!(fetch_req.key, "counter_key");
        assert_eq!(fetch_req.delta, -5);
        assert_eq!(fetch_req.max_value, 0);
    }

    #[test]
    fn test_match_seq_on_delete() {
        let op = pb::TxnOp::delete("delete_key").match_seq(Some(123));

        let Some(pb::txn_op::Request::Delete(delete_req)) = &op.request else {
            panic!("Expected Delete request");
        };

        assert_eq!(delete_req.key, "delete_key");
        assert_eq!(delete_req.match_seq, Some(123));
    }

    #[test]
    fn test_match_seq_on_fetch_add_u64() {
        let op = pb::TxnOp::fetch_add_u64("counter", 1).match_seq(Some(456));

        let Some(pb::txn_op::Request::FetchIncreaseU64(fetch_req)) = &op.request else {
            panic!("Expected FetchIncreaseU64 request");
        };

        assert_eq!(fetch_req.key, "counter");
        assert_eq!(fetch_req.delta, 1);
        assert_eq!(fetch_req.max_value, 0);
        assert_eq!(fetch_req.match_seq, Some(456));
    }

    #[test]
    fn test_match_seq_none() {
        let op = pb::TxnOp::delete("key").match_seq(None);

        let Some(pb::txn_op::Request::Delete(delete_req)) = &op.request else {
            panic!("Expected Delete request");
        };

        assert!(delete_req.match_seq.is_none());
    }

    #[test]
    fn test_method_chaining() {
        // Test that methods can be chained together
        let op = pb::TxnOp::put("chain_key", b"chain_value".to_vec())
            .with_ttl(Some(Duration::from_millis(1000)));

        let Some(pb::txn_op::Request::Put(put_req)) = &op.request else {
            panic!("Expected Put request");
        };

        assert_eq!(put_req.key, "chain_key");
        assert_eq!(put_req.value, b"chain_value");
        assert_eq!(put_req.ttl_ms, Some(1000));
    }

    #[test]
    fn test_method_chaining_with_match_seq() {
        let op = pb::TxnOp::fetch_add_u64("counter", 5).match_seq(Some(100));

        let Some(pb::txn_op::Request::FetchIncreaseU64(fetch_req)) = &op.request else {
            panic!("Expected FetchIncreaseU64 request");
        };

        assert_eq!(fetch_req.key, "counter");
        assert_eq!(fetch_req.delta, 5);
        assert_eq!(fetch_req.max_value, 0);
        assert_eq!(fetch_req.match_seq, Some(100));
    }

    #[test]
    fn test_display_different_operations() {
        // Test display for different operation types
        let put_op = pb::TxnOp::put("k1", b"v1".to_vec());
        let get_op = pb::TxnOp::get("k1");
        let delete_op = pb::TxnOp::delete("k1");
        let fetch_add_op = pb::TxnOp::fetch_add_u64("counter", 1);

        // Just ensure they can be displayed without panicking
        let _ = format!("{}", put_op);
        let _ = format!("{}", get_op);
        let _ = format!("{}", delete_op);
        let _ = format!("{}", fetch_add_op);
    }
}
