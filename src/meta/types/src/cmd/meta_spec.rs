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

use std::time::Duration;

use deepsize::Context;

use crate::cmd::CmdContext;
use crate::seq_value::KVMeta;
use crate::time::Interval;

/// Specifies the metadata associated with a kv record, used in an `upsert` cmd.
///
/// This is similar to [`KVMeta`] but differs, [`KVMeta`] is used in storage,
/// as this instance is employed for transport purposes.
/// When an `upsert` cmd is applied, this instance is evaluated and a `KVMeta` is built.
#[derive(serde::Serialize, serde::Deserialize, Debug, Default, Clone, Eq, PartialEq)]
pub struct MetaSpec {
    /// expiration time in second since 1970
    pub(crate) expire_at: Option<u64>,

    /// Relative expiration time interval since when the raft log is applied.
    ///
    /// Use this field if possible to avoid the clock skew between client and meta-service.
    /// `expire_at` may already be expired when it is applied to state machine.
    ///
    /// If it is not None, once applied, the `expire_at` field will be replaced with the calculated absolute expiration time.
    ///
    /// For backward compatibility, this field is not serialized if it `None`, as if it does not exist.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) ttl: Option<Interval>,
}

impl deepsize::DeepSizeOf for MetaSpec {
    fn deep_size_of_children(&self, _context: &mut Context) -> usize {
        0
    }
}

impl MetaSpec {
    /// Create a new KVMeta
    pub fn new(expire_at: Option<u64>, ttl: Option<Interval>) -> Self {
        Self { expire_at, ttl }
    }

    /// Create a KVMeta with a absolute expiration time in second since 1970-01-01.
    pub fn new_expire(expire_at_sec: u64) -> Self {
        Self {
            expire_at: Some(expire_at_sec),
            ttl: None,
        }
    }

    /// Create a KVMeta with relative expiration time(ttl).
    pub fn new_ttl(ttl: Duration) -> Self {
        Self {
            expire_at: None,
            ttl: Some(Interval::from_duration(ttl)),
        }
    }

    /// Convert meta spec into a [`KVMeta`] to be stored in storage.
    pub fn to_kv_meta(&self, cmd_ctx: &CmdContext) -> KVMeta {
        // If `ttl` is set, override `expire_at`
        if let Some(ttl) = self.ttl {
            return KVMeta::new_expire((cmd_ctx.time() + ttl).seconds());
        }

        // No `ttl`, check if absolute expire time `expire_at` is set.
        KVMeta::new(self.expire_at)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::MetaSpec;
    use crate::cmd::CmdContext;
    use crate::KVMeta;
    use crate::Time;

    #[test]
    fn test_serde() {
        let meta = MetaSpec::new_expire(100);
        let s = serde_json::to_string(&meta).unwrap();
        assert_eq!(r#"{"expire_at":100}"#, s);

        let got: KVMeta = serde_json::from_str(&s).unwrap();
        assert_eq!(Some(100), got.expire_at);

        let meta = MetaSpec::new_ttl(Duration::from_millis(100));
        let s = serde_json::to_string(&meta).unwrap();
        assert_eq!(r#"{"expire_at":null,"ttl":{"millis":100}}"#, s);
    }

    #[test]
    fn test_to_kv_meta() {
        let cmd_ctx = CmdContext::new(Time::from_millis(2000));

        // ttl
        let meta = MetaSpec::new_ttl(Duration::from_millis(1000));
        let kv_meta = meta.to_kv_meta(&cmd_ctx);
        assert_eq!(kv_meta.get_expire_at_ms().unwrap(), 3000);

        // expire_at
        let meta = MetaSpec::new_expire(5);
        let kv_meta = meta.to_kv_meta(&cmd_ctx);
        assert_eq!(kv_meta.get_expire_at_ms().unwrap(), 5_000);
    }
}
