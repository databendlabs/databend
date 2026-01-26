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

mod response_ext;

use std::fmt::Display;
use std::fmt::Formatter;

use display_more::DisplayOptionExt;

use crate::SeqV;
use crate::protobuf as pb;

impl pb::TxnOpResponse {
    /// Create a new `TxnOpResponse` from a `Response` variant.
    pub fn new<T>(r: T) -> Self
    where pb::txn_op_response::Response: From<T> {
        pb::TxnOpResponse {
            response: Some(pb::txn_op_response::Response::from(r)),
        }
    }

    /// Create a new `TxnOpResponse` of a `Delete` operation.
    pub fn delete(key: impl ToString, success: bool, prev_value: Option<pb::SeqV>) -> Self {
        pb::TxnOpResponse {
            response: Some(pb::txn_op_response::Response::Delete(
                pb::TxnDeleteResponse {
                    key: key.to_string(),
                    success,
                    prev_value,
                },
            )),
        }
    }

    /// Create a new `TxnOpResponse` of a `Put` operation.
    pub fn put(
        key: impl ToString,
        prev_value: Option<pb::SeqV>,
        current: Option<pb::SeqV>,
    ) -> Self {
        pb::TxnOpResponse {
            response: Some(pb::txn_op_response::Response::Put(pb::TxnPutResponse {
                key: key.to_string(),
                prev_value,
                current,
            })),
        }
    }

    pub fn unchanged_fetch_increase_u64(key: impl ToString, seq: u64, value: u64) -> Self {
        Self::fetch_increase_u64(key, seq, value, seq, value)
    }

    pub fn fetch_increase_u64(
        key: impl ToString,
        before_seq: u64,
        before: u64,
        after_seq: u64,
        after: u64,
    ) -> Self {
        pb::TxnOpResponse {
            response: Some(pb::txn_op_response::Response::FetchIncreaseU64(
                pb::FetchIncreaseU64Response {
                    key: key.to_string(),
                    before_seq,
                    before,
                    after_seq,
                    after,
                },
            )),
        }
    }

    pub fn get(key: impl ToString, value: Option<SeqV>) -> Self {
        pb::TxnOpResponse {
            response: Some(pb::txn_op_response::Response::Get(pb::TxnGetResponse {
                key: key.to_string(),
                value: value.map(pb::SeqV::from),
            })),
        }
    }

    /// Consumes and returns the response as a `Get` response if it is one.
    pub fn into_get(self) -> Option<pb::TxnGetResponse> {
        match self.response {
            Some(pb::txn_op_response::Response::Get(resp)) => Some(resp),
            _ => None,
        }
    }

    pub fn as_get(&self) -> &pb::TxnGetResponse {
        self.try_as_get().unwrap()
    }

    /// Returns the response as a `Get` response if it is one.
    pub fn try_as_get(&self) -> Option<&pb::TxnGetResponse> {
        match &self.response {
            Some(pb::txn_op_response::Response::Get(resp)) => Some(resp),
            _ => None,
        }
    }

    pub fn try_as_fetch_increase_u64(&self) -> Option<&pb::FetchIncreaseU64Response> {
        match &self.response {
            Some(pb::txn_op_response::Response::FetchIncreaseU64(resp)) => Some(resp),
            _ => None,
        }
    }

    pub fn try_as_delete(&self) -> Option<&pb::TxnDeleteResponse> {
        match &self.response {
            Some(pb::txn_op_response::Response::Delete(resp)) => Some(resp),
            _ => None,
        }
    }

    pub fn try_as_put(&self) -> Option<&pb::TxnPutResponse> {
        match &self.response {
            Some(pb::txn_op_response::Response::Put(resp)) => Some(resp),
            _ => None,
        }
    }
}

impl Display for pb::TxnOpResponse {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "TxnOpResponse: {}", self.response.display())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delete_success() {
        let prev_value = Some(pb::SeqV {
            seq: 42,
            data: b"deleted_value".to_vec(),
            meta: None,
        });

        let response = pb::TxnOpResponse::delete("test_key", true, prev_value.clone());

        let Some(pb::txn_op_response::Response::Delete(delete_resp)) = &response.response else {
            panic!("Expected Delete response");
        };

        assert_eq!(delete_resp.key, "test_key");
        assert!(delete_resp.success);
        assert_eq!(delete_resp.prev_value, prev_value);
    }

    #[test]
    fn test_delete_failure() {
        let response = pb::TxnOpResponse::delete("missing_key", false, None);

        let Some(pb::txn_op_response::Response::Delete(delete_resp)) = &response.response else {
            panic!("Expected Delete response");
        };

        assert_eq!(delete_resp.key, "missing_key");
        assert!(!delete_resp.success);
        assert!(delete_resp.prev_value.is_none());
    }

    #[test]
    fn test_put_with_prev_and_current() {
        let prev_value = Some(pb::SeqV {
            seq: 10,
            data: b"old_value".to_vec(),
            meta: None,
        });
        let current_value = Some(pb::SeqV {
            seq: 11,
            data: b"new_value".to_vec(),
            meta: None,
        });

        let response = pb::TxnOpResponse::put("put_key", prev_value.clone(), current_value.clone());

        let Some(pb::txn_op_response::Response::Put(put_resp)) = &response.response else {
            panic!("Expected Put response");
        };

        assert_eq!(put_resp.key, "put_key");
        assert_eq!(put_resp.prev_value, prev_value);
        assert_eq!(put_resp.current, current_value);
    }

    #[test]
    fn test_put_new_key() {
        let current_value = Some(pb::SeqV {
            seq: 1,
            data: b"initial_value".to_vec(),
            meta: None,
        });

        let response = pb::TxnOpResponse::put("new_key", None, current_value.clone());

        let Some(pb::txn_op_response::Response::Put(put_resp)) = &response.response else {
            panic!("Expected Put response");
        };

        assert_eq!(put_resp.key, "new_key");
        assert!(put_resp.prev_value.is_none());
        assert_eq!(put_resp.current, current_value);
    }

    #[test]
    fn test_unchanged_fetch_increase_u64() {
        let response = pb::TxnOpResponse::unchanged_fetch_increase_u64("counter", 5, 100);

        let Some(pb::txn_op_response::Response::FetchIncreaseU64(fetch_resp)) = &response.response
        else {
            panic!("Expected FetchIncreaseU64 response");
        };

        assert_eq!(fetch_resp.key, "counter");
        assert_eq!(fetch_resp.before_seq, 5);
        assert_eq!(fetch_resp.before, 100);
        assert_eq!(fetch_resp.after_seq, 5); // Same as before for unchanged
        assert_eq!(fetch_resp.after, 100); // Same as before for unchanged
    }

    #[test]
    fn test_fetch_increase_u64() {
        let response = pb::TxnOpResponse::fetch_increase_u64("counter", 10, 50, 11, 55);

        let Some(pb::txn_op_response::Response::FetchIncreaseU64(fetch_resp)) = &response.response
        else {
            panic!("Expected FetchIncreaseU64 response");
        };

        assert_eq!(fetch_resp.key, "counter");
        assert_eq!(fetch_resp.before_seq, 10);
        assert_eq!(fetch_resp.before, 50);
        assert_eq!(fetch_resp.after_seq, 11);
        assert_eq!(fetch_resp.after, 55);
    }

    #[test]
    fn test_fetch_increase_u64_large_values() {
        let response = pb::TxnOpResponse::fetch_increase_u64(
            "big_counter",
            u64::MAX - 1,
            u64::MAX - 100,
            u64::MAX,
            u64::MAX,
        );

        let Some(pb::txn_op_response::Response::FetchIncreaseU64(fetch_resp)) = &response.response
        else {
            panic!("Expected FetchIncreaseU64 response");
        };

        assert_eq!(fetch_resp.before_seq, u64::MAX - 1);
        assert_eq!(fetch_resp.before, u64::MAX - 100);
        assert_eq!(fetch_resp.after_seq, u64::MAX);
        assert_eq!(fetch_resp.after, u64::MAX);
    }

    #[test]
    fn test_get_with_value() {
        let seq_v = SeqV::new(20, b"retrieved_value".to_vec());
        let response = pb::TxnOpResponse::get("get_key", Some(seq_v.clone()));

        let Some(pb::txn_op_response::Response::Get(get_resp)) = &response.response else {
            panic!("Expected Get response");
        };

        assert_eq!(get_resp.key, "get_key");
        assert!(get_resp.value.is_some());

        let value = get_resp.value.as_ref().unwrap();
        assert_eq!(value.seq, 20);
        assert_eq!(value.data, b"retrieved_value");
    }

    #[test]
    fn test_get_no_value() {
        let response = pb::TxnOpResponse::get("missing_key", None);

        let Some(pb::txn_op_response::Response::Get(get_resp)) = &response.response else {
            panic!("Expected Get response");
        };

        assert_eq!(get_resp.key, "missing_key");
        assert!(get_resp.value.is_none());
    }

    #[test]
    fn test_into_get_success() {
        let seq_v = SeqV::new(15, b"test_data".to_vec());
        let response = pb::TxnOpResponse::get("key", Some(seq_v));

        let get_resp = response.into_get().expect("Should convert to Get response");
        assert_eq!(get_resp.key, "key");
        assert!(get_resp.value.is_some());
    }

    #[test]
    fn test_into_get_failure() {
        let response = pb::TxnOpResponse::delete("key", true, None);

        let result = response.into_get();
        assert!(result.is_none());
    }

    #[test]
    fn test_as_get_success() {
        let seq_v = SeqV::new(25, b"as_get_test".to_vec());
        let response = pb::TxnOpResponse::get("key", Some(seq_v));

        let get_resp = response.as_get();
        assert_eq!(get_resp.key, "key");
        assert!(get_resp.value.is_some());
    }

    #[test]
    #[should_panic(expected = "unwrap")]
    fn test_as_get_panic_on_non_get() {
        let response = pb::TxnOpResponse::delete("key", true, None);
        let _ = response.as_get(); // Should panic
    }

    #[test]
    fn test_try_as_get_success() {
        let seq_v = SeqV::new(30, b"try_as_get_test".to_vec());
        let response = pb::TxnOpResponse::get("key", Some(seq_v));

        let get_resp = response.try_as_get().expect("Should return Get response");
        assert_eq!(get_resp.key, "key");
        assert!(get_resp.value.is_some());
    }

    #[test]
    fn test_try_as_get_failure() {
        let response = pb::TxnOpResponse::put("key", None, None);

        let result = response.try_as_get();
        assert!(result.is_none());
    }

    #[test]
    fn test_try_as_fetch_increase_u64_success() {
        let response = pb::TxnOpResponse::fetch_increase_u64("counter", 1, 10, 2, 15);

        let fetch_resp = response
            .try_as_fetch_increase_u64()
            .expect("Should return FetchIncreaseU64 response");
        assert_eq!(fetch_resp.key, "counter");
        assert_eq!(fetch_resp.before, 10);
        assert_eq!(fetch_resp.after, 15);
    }

    #[test]
    fn test_try_as_fetch_increase_u64_failure() {
        let response = pb::TxnOpResponse::get("key", None);

        let result = response.try_as_fetch_increase_u64();
        assert!(result.is_none());
    }

    #[test]
    fn test_string_keys() {
        // Test that different string types work for keys
        let resp1 = pb::TxnOpResponse::delete("static_str", true, None);
        let resp2 = pb::TxnOpResponse::put(String::from("owned_string"), None, None);
        let key_ref = "string_ref";
        let resp3 = pb::TxnOpResponse::get(key_ref, None);
        let resp4 = pb::TxnOpResponse::fetch_increase_u64(key_ref, 1, 1, 1, 1);

        // Extract keys and verify
        assert_eq!(resp1.try_as_delete().unwrap().key, "static_str");
        assert_eq!(resp2.try_as_put().unwrap().key, "owned_string");
        assert_eq!(resp3.try_as_get().unwrap().key, "string_ref");
        assert_eq!(resp4.try_as_fetch_increase_u64().unwrap().key, "string_ref");
    }

    #[test]
    fn test_display_implementation() {
        // Test that all response types can be displayed
        let delete_resp = pb::TxnOpResponse::delete("key", true, None);
        let put_resp = pb::TxnOpResponse::put("key", None, None);
        let get_resp = pb::TxnOpResponse::get("key", None);
        let fetch_resp = pb::TxnOpResponse::fetch_increase_u64("key", 1, 1, 1, 1);

        assert_eq!(
            format!("{}", delete_resp),
            "TxnOpResponse: Delete: Delete-resp: success: true, key=key, prev_seq=None"
        );
        assert_eq!(
            format!("{}", put_resp),
            "TxnOpResponse: Put: Put-resp: key=key, prev_seq=None, current_seq=None"
        );
        assert_eq!(
            format!("{}", get_resp),
            "TxnOpResponse: Get: Get-resp: key=key, prev_seq=None"
        );
        assert_eq!(
            format!("{}", fetch_resp),
            "TxnOpResponse: FetchIncreaseU64: FetchIncreaseU64Response{ key=key before=(seq=1 1), after=(seq=1 1), delta=0 }"
        );
    }
}
