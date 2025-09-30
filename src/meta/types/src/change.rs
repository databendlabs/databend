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

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;
use state_machine_api::SeqV;
use state_machine_api::SeqValue;

use crate::reduce_seqv::ReduceSeqV;

/// `Change` describes a state transition: the states before and after an operation.
///
/// Note that a success add-operation has a None `prev`, and a maybe-Some `result`.
/// Because the inserted value may have already expired and will be deleted at once,
/// the `result` could also be possible to be None.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, derive_more::From)]
pub struct Change<T, ID = u64>
where ID: Clone + PartialEq
{
    /// identity of the resource that is changed.
    pub ident: Option<ID>,
    pub prev: Option<SeqV<T>>,
    pub result: Option<SeqV<T>>,
}

impl<T, ID> Change<T, ID>
where
    ID: Clone + PartialEq + Debug,
    T: Debug,
{
    pub fn new(prev: Option<SeqV<T>>, result: Option<SeqV<T>>) -> Self {
        Change {
            ident: None,
            prev,
            result,
        }
    }

    pub fn with_id(mut self, id: ID) -> Self {
        self.ident = Some(id);
        self
    }

    /// Maps `Option<SeqV<T>>` to `Option<U>` for `prev` and `result`.
    pub fn map<F, U>(self, f: F) -> (Option<U>, Option<U>)
    where F: Fn(SeqV<T>) -> U + Copy {
        (self.prev.map(f), self.result.map(f))
    }

    pub fn unwrap(self) -> (SeqV<T>, SeqV<T>) {
        (self.prev.unwrap(), self.result.unwrap())
    }

    /// Extract `prev.seq` and `result.seq`.
    pub fn unpack_seq(self) -> (Option<u64>, Option<u64>) {
        self.map(|x| x.seq)
    }

    /// Extract `prev.data` and `result.data`.
    pub fn unpack_data(self) -> (Option<T>, Option<T>) {
        self.map(|x| x.data)
    }

    pub fn is_changed(&self) -> bool {
        self.prev.seq() != self.result.seq()
    }

    /// Assumes it is a state transition of an add operation and return Ok if the add operation succeed.
    /// Otherwise, it returns an error that is built by provided function.
    ///
    /// Note that a success add-operation has a None `prev`, and a maybe-Some `result`.
    /// Because the inserted value may have already expired and will be deleted at once,
    /// the `result` could also be possible to be None.
    #[allow(dead_code)]
    pub fn added_or_else<F, E>(self, make_err: F) -> Result<Option<SeqV<T>>, E>
    where F: FnOnce(SeqV<T>) -> E {
        let (prev, result) = self.unpack();
        if let Some(p) = prev {
            return Err(make_err(p));
        }

        Ok(result)
    }

    /// Return the `seq` in an Ok if an add operation succeed.
    /// Otherwise it returns an error that is built by provided function.
    pub fn added_seq_or_else<F, E>(self, make_err: F) -> Result<u64, E>
    where F: FnOnce(SeqV<T>) -> E {
        let (prev, result) = self.unpack();
        if let Some(p) = prev {
            return Err(make_err(p));
        }

        // result could be None if it expired.
        Ok(result.seq())
    }
}

impl<T, ID> Change<T, ID>
where ID: Clone + PartialEq
{
    /// Extract `prev` and `result`.
    pub fn unpack(self) -> (Option<SeqV<T>>, Option<SeqV<T>>) {
        (self.prev, self.result)
    }

    /// Assumes it is a state transition of a remove operation and return Ok if the succeeded.
    /// Otherwise it returns an error that is built by provided function with the `prev` value as argument.
    ///
    /// Note that a success remove has a Some `prev`, and a None `result`.
    pub fn removed_or_else<F, E>(self, make_err: F) -> Result<SeqV<T>, E>
    where F: FnOnce(Option<SeqV<T>>) -> E {
        let (prev, result) = self.unpack();
        if result.is_none() {
            if let Some(p) = prev {
                return Ok(p);
            }
        }
        Err(make_err(prev))
    }
}

impl<T, ID> Display for Change<T, ID>
where
    T: Debug + Clone + PartialEq,
    ID: Debug + Clone + PartialEq,
{
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "id: {:?}", self.ident)?;
        write!(f, "prev: {:?}", self.prev)?;
        write!(f, "result: {:?}", self.result)
    }
}

impl<T, ID> ReduceSeqV for Change<T, ID>
where ID: Clone + PartialEq
{
    fn erase_proposed_at(self) -> Self {
        Change {
            ident: self.ident,
            prev: self.prev.erase_proposed_at(),
            result: self.result.erase_proposed_at(),
        }
    }

    fn reduce(self) -> Self {
        Change {
            ident: self.ident,
            prev: self.prev.reduce(),
            result: self.result.reduce(),
        }
    }
}

#[cfg(test)]
mod tests {
    use state_machine_api::KVMeta;

    use super::*;

    #[test]
    fn test_change_erase_proposed_at() {
        // Test with both prev and result having proposed_at_ms
        let change = Change {
            ident: Some(123u64),
            prev: Some(SeqV {
                seq: 1,
                meta: Some(KVMeta {
                    expire_at: Some(1723102819),
                    proposed_at_ms: Some(1_723_102_800_000),
                }),
                data: b"prev_value".to_vec(),
            }),
            result: Some(SeqV {
                seq: 2,
                meta: Some(KVMeta {
                    expire_at: None,
                    proposed_at_ms: Some(1_723_102_900_000),
                }),
                data: b"new_value".to_vec(),
            }),
        };

        let erased = change.erase_proposed_at();

        // Check ident is preserved
        assert_eq!(erased.ident, Some(123u64));

        // Check prev: expire_at should remain, proposed_at_ms should be None
        let prev = erased.prev.unwrap();
        assert_eq!(prev.seq, 1);
        assert_eq!(prev.data, b"prev_value");
        let prev_meta = prev.meta.unwrap();
        assert_eq!(prev_meta.expire_at, Some(1723102819));
        assert_eq!(prev_meta.proposed_at_ms, None);

        // Check result: meta should be removed (default after erasing proposed_at_ms)
        let result = erased.result.unwrap();
        assert_eq!(result.seq, 2);
        assert_eq!(result.data, b"new_value");
        assert_eq!(result.meta, None);
    }

    #[test]
    fn test_change_reduce() {
        // Test reduce without touching proposed_at_ms
        let change = Change {
            ident: Some(456u64),
            prev: Some(SeqV {
                seq: 1,
                meta: Some(KVMeta {
                    expire_at: None,
                    proposed_at_ms: Some(1_723_102_800_000),
                }),
                data: b"prev_value".to_vec(),
            }),
            result: Some(SeqV {
                seq: 2,
                meta: Some(KVMeta {
                    expire_at: Some(1723102819),
                    proposed_at_ms: None,
                }),
                data: b"new_value".to_vec(),
            }),
        };

        let reduced = change.reduce();

        // Check ident is preserved
        assert_eq!(reduced.ident, Some(456u64));

        // Check prev: proposed_at_ms should remain (not default)
        let prev = reduced.prev.unwrap();
        let prev_meta = prev.meta.unwrap();
        assert_eq!(prev_meta.proposed_at_ms, Some(1_723_102_800_000));
        assert_eq!(prev_meta.expire_at, None);

        // Check result: meta should remain (has expire_at)
        let result = reduced.result.unwrap();
        let result_meta = result.meta.unwrap();
        assert_eq!(result_meta.expire_at, Some(1723102819));
        assert_eq!(result_meta.proposed_at_ms, None);
    }

    #[test]
    fn test_change_reduce_removes_default_meta() {
        // Test that reduce removes default metadata
        let change: Change<Vec<u8>> = Change {
            ident: None,
            prev: Some(SeqV {
                seq: 1,
                meta: Some(KVMeta {
                    expire_at: None,
                    proposed_at_ms: None,
                }),
                data: b"prev".to_vec(),
            }),
            result: Some(SeqV {
                seq: 2,
                meta: Some(Default::default()),
                data: b"result".to_vec(),
            }),
        };

        let reduced = change.reduce();

        // Both prev and result should have meta removed
        assert_eq!(reduced.prev.unwrap().meta, None);
        assert_eq!(reduced.result.unwrap().meta, None);
    }

    #[test]
    fn test_change_with_none_values() {
        // Test with None prev and result
        let change: Change<Vec<u8>> = Change {
            ident: Some(789u64),
            prev: None,
            result: None,
        };

        let erased = change.clone().erase_proposed_at();
        let reduced = change.reduce();

        assert_eq!(erased.ident, Some(789u64));
        assert_eq!(erased.prev, None);
        assert_eq!(erased.result, None);

        assert_eq!(reduced.ident, Some(789u64));
        assert_eq!(reduced.prev, None);
        assert_eq!(reduced.result, None);
    }

    #[test]
    fn test_change_erase_proposed_at_then_reduce() {
        // Test the chain: erase_proposed_at calls reduce
        let change = Change {
            ident: Some(999u64),
            prev: Some(SeqV {
                seq: 1,
                meta: Some(KVMeta {
                    expire_at: None,
                    proposed_at_ms: Some(1_723_102_800_000),
                }),
                data: b"data".to_vec(),
            }),
            result: None,
        };

        let erased = change.erase_proposed_at();

        // Meta should be removed entirely (was only proposed_at_ms, now default)
        assert_eq!(erased.prev.unwrap().meta, None);
    }

    #[test]
    fn test_change_preserves_ident_type() {
        // Test with string ident type
        let change: Change<Vec<u8>, String> = Change {
            ident: Some("test-id".to_string()),
            prev: Some(SeqV {
                seq: 1,
                meta: Some(KVMeta {
                    expire_at: Some(1723102819),
                    proposed_at_ms: Some(1_723_102_800_000),
                }),
                data: b"data".to_vec(),
            }),
            result: None,
        };

        let erased = change.erase_proposed_at();

        assert_eq!(erased.ident, Some("test-id".to_string()));
        // proposed_at_ms should be erased but expire_at should remain
        let prev_meta = erased.prev.unwrap().meta.unwrap();
        assert_eq!(prev_meta.proposed_at_ms, None);
        assert_eq!(prev_meta.expire_at, Some(1723102819));
    }
}
