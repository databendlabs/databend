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

use databend_common_meta_types::KVMeta;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::SeqValue;

use crate::marked::InternalSeq;
use crate::marked::Marked;
use crate::state_machine::ExpireValue;

#[test]
fn test_from_tuple() -> anyhow::Result<()> {
    let m = Marked::from((1, 2u64, Some(KVMeta::new_expire(3))));

    assert_eq!(m, Marked::new_with_meta(1, 2, Some(KVMeta::new_expire(3))));

    Ok(())
}

#[test]
fn test_impl_trait_seq_value() -> anyhow::Result<()> {
    let m = Marked::new_with_meta(1, 2, None);
    assert_eq!(m.seq(), 1);
    assert_eq!(m.value(), Some(&2));
    assert_eq!(m.meta(), None);

    let m = Marked::new_with_meta(1, 2, Some(KVMeta::new_expire(3)));
    assert_eq!(m.seq(), 1);
    assert_eq!(m.value(), Some(&2));
    assert_eq!(m.meta(), Some(&KVMeta::new_expire(3)));

    let m: Marked<u64> = Marked::new_tombstone(1);
    assert_eq!(m.seq(), 0, "internal_seq is not returned to application");
    assert_eq!(m.value(), None);
    assert_eq!(m.meta(), None);

    Ok(())
}

// Test Marked::empty()
#[test]
fn test_empty() -> anyhow::Result<()> {
    let m = Marked::<u64>::empty();
    assert_eq!(m, Marked::TombStone { internal_seq: 0 });

    Ok(())
}

// Test Marked::order_key()
#[test]
fn test_order_key() -> anyhow::Result<()> {
    let m = Marked::new_with_meta(1, 2, None);
    assert_eq!(m.order_key(), InternalSeq::normal(1));

    let m: Marked<u64> = Marked::new_tombstone(1);
    assert_eq!(m.order_key(), InternalSeq::tombstone(1));

    Ok(())
}

// Test Marked::unpack()
#[test]
fn test_unpack() -> anyhow::Result<()> {
    let m = Marked::new_with_meta(1, 2, None);
    assert_eq!(m.unpack_ref(), Some((&2, None)));

    let m = Marked::new_with_meta(1, 2, Some(KVMeta::new_expire(3)));
    assert_eq!(m.unpack_ref(), Some((&2, Some(&KVMeta::new_expire(3)))));

    let m: Marked<u64> = Marked::new_tombstone(1);
    assert_eq!(m.unpack_ref(), None);

    Ok(())
}

// Test Marked::max()
#[test]
fn test_max() -> anyhow::Result<()> {
    let m1 = Marked::new_with_meta(1, 2, None);
    let m2 = Marked::new_with_meta(3, 2, None);
    let m3: Marked<u64> = Marked::new_tombstone(2);

    assert_eq!(Marked::max_ref(&m1, &m2), &m2);
    assert_eq!(Marked::max_ref(&m1, &m3), &m3);
    assert_eq!(Marked::max_ref(&m2, &m3), &m2);

    assert_eq!(Marked::max_ref(&m1, &m1), &m1);
    assert_eq!(Marked::max_ref(&m2, &m2), &m2);
    assert_eq!(Marked::max_ref(&m3, &m3), &m3);

    Ok(())
}

// Test From<Marked<T>> for Option<SeqV<T>>
#[test]
fn test_from_marked_for_option_seqv() -> anyhow::Result<()> {
    let m = Marked::new_with_meta(1, 2, None);
    let s: Option<SeqV<u64>> = Some(SeqV::new(1, 2));
    assert_eq!(s, m.into());

    let m = Marked::new_with_meta(1, 2, Some(KVMeta::new_expire(3)));
    let s: Option<SeqV<u64>> = Some(SeqV::with_meta(1, Some(KVMeta::new_expire(3)), 2));
    assert_eq!(s, m.into());

    let m: Marked<u64> = Marked::new_tombstone(1);
    let s: Option<SeqV<u64>> = None;
    assert_eq!(s, m.into());

    Ok(())
}

// Test From<ExpireValue> for Marked<String>
#[test]
fn test_from_expire_value_for_marked() -> anyhow::Result<()> {
    let m = Marked::new_with_meta(1, "2".to_string(), None);
    let s = ExpireValue::new("2", 1);
    assert_eq!(m, s.into());

    Ok(())
}

// Test From<Marked<String>> for Option<ExpireValue>
#[test]
fn test_from_marked_for_option_expire_value() -> anyhow::Result<()> {
    let m = Marked::new_with_meta(1, "2".to_string(), None);
    let s: Option<ExpireValue> = Some(ExpireValue::new("2".to_string(), 1));
    assert_eq!(s, m.into());

    let m = Marked::new_tombstone(1);
    let s: Option<ExpireValue> = None;
    assert_eq!(s, m.into());

    Ok(())
}
