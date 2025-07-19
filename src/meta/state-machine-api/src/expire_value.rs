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

use map_api::Marked;
use map_api::SeqMarked;

/// The value of an expiration index is the record key.
#[derive(Default, Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ExpireValue {
    #[serde(skip_serializing_if = "is_zero")]
    #[serde(default)]
    pub seq: u64,
    pub key: String,
}

fn is_zero(v: &u64) -> bool {
    *v == 0
}

impl ExpireValue {
    pub fn new(key: impl ToString, seq: u64) -> Self {
        Self {
            key: key.to_string(),
            seq,
        }
    }
    /// Convert internally used expire-index value `Marked<String>` to externally used type `ExpireValue`.
    ///
    /// `Marked<String>` is the value of an expire-index in the state machine.
    /// `ExpireValue.seq` equals to the seq of the str-map record,
    /// i.e., when an expire-index is inserted, the seq does not increase.
    pub fn from_marked(seq_marked: SeqMarked<String>) -> Option<Self> {
        let (seq, mm) = seq_marked.into_parts();

        match mm {
            Marked::TombStone => None,
            Marked::Normal(s) => Some(ExpireValue::new(s, seq)),
        }
    }
}

impl From<ExpireValue> for SeqMarked<String> {
    fn from(value: ExpireValue) -> Self {
        SeqMarked::new_normal(value.seq, value.key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expire_value_serde() -> anyhow::Result<()> {
        {
            let v = ExpireValue {
                seq: 0,
                key: "a".to_string(),
            };
            let s = serde_json::to_string(&v)?;
            let want = r#"{"key":"a"}"#;
            assert_eq!(want, s);

            let got = serde_json::from_str::<ExpireValue>(want)?;
            assert_eq!(v, got);
        }

        {
            let v = ExpireValue {
                seq: 5,
                key: "a".to_string(),
            };
            let s = serde_json::to_string(&v)?;
            let want = r#"{"seq":5,"key":"a"}"#;
            assert_eq!(want, s);

            let got = serde_json::from_str::<ExpireValue>(want)?;
            assert_eq!(v, got);
        }

        Ok(())
    }

    #[test]
    fn test_from_seq_marked() {
        // Test normal case - should return Some(ExpireValue)
        {
            let seq = 42;
            let key = "test_key".to_string();
            let marked = Marked::Normal(key.clone());
            let seq_marked = SeqMarked::new(seq, marked);

            let result = ExpireValue::from_marked(seq_marked);

            assert!(result.is_some());
            let expire_value = result.unwrap();
            assert_eq!(expire_value.seq, seq);
            assert_eq!(expire_value.key, key);
        }

        // Test tombstone case - should return None
        {
            let seq = 100;
            let marked = Marked::TombStone;
            let seq_marked = SeqMarked::new(seq, marked);

            let result = ExpireValue::from_marked(seq_marked);

            assert!(result.is_none());
        }

        // Test with zero seq
        {
            let seq = 0;
            let key = "zero_seq_key".to_string();
            let marked = Marked::Normal(key.clone());
            let seq_marked = SeqMarked::new(seq, marked);

            let result = ExpireValue::from_marked(seq_marked);

            assert!(result.is_some());
            let expire_value = result.unwrap();
            assert_eq!(expire_value.seq, seq);
            assert_eq!(expire_value.key, key);
        }
    }

    // Test From<ExpireValue> for Marked<String>
    #[test]
    fn test_from_expire_value_for_marked() -> anyhow::Result<()> {
        let m = SeqMarked::new_normal(1, "2".to_string());
        let s = ExpireValue::new("2", 1);
        assert_eq!(m, s.into());

        Ok(())
    }

    // Test From<Marked<String>> for Option<ExpireValue>
    #[test]
    fn test_from_marked_for_option_expire_value() -> anyhow::Result<()> {
        let m = SeqMarked::new_normal(1, "2".to_string());
        let s: Option<ExpireValue> = Some(ExpireValue::new("2".to_string(), 1));
        assert_eq!(s, ExpireValue::from_marked(m));

        let m = SeqMarked::new_tombstone(1);
        let s: Option<ExpireValue> = None;
        assert_eq!(s, ExpireValue::from_marked(m));

        Ok(())
    }
}
