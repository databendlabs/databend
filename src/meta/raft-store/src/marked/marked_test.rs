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

use crate::marked::SeqMarked;
use crate::state_machine::ExpireValue;

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
