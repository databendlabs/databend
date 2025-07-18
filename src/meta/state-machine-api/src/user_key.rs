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

use std::fmt;
use std::ops::Deref;

use map_api::MapKey;

use crate::KVMeta;

/// Key for the user data in state machine
#[derive(Default, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct UserKey {
    pub key: String,
}

impl Deref for UserKey {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.key
    }
}

impl AsRef<UserKey> for String {
    fn as_ref(&self) -> &UserKey {
        // SAFETY: UserKey is a transparent wrapper around String
        // This is safe because:
        // 1. UserKey is marked as #[repr(transparent)]
        // 2. UserKey contains only a single String field
        // 3. The memory layout of UserKey is identical to String
        // 4. Both &String and &UserKey are references of the same size
        unsafe { std::mem::transmute(self) }
    }
}

impl fmt::Display for UserKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.key)
    }
}

impl MapKey for UserKey {
    type V = (Option<KVMeta>, Vec<u8>);
}

impl UserKey {
    pub fn new(key: impl ToString) -> Self {
        Self {
            key: key.to_string(),
        }
    }

    /// Create a reference to a UserKey from a &str
    pub fn from_string_ref(key: &String) -> &Self {
        // SAFETY: UserKey is a transparent wrapper around String
        // This is safe because:
        // 1. UserKey is marked as #[repr(transparent)]
        // 2. UserKey contains only a single String field
        // 3. The memory layout of UserKey is identical to String
        unsafe { std::mem::transmute(key) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_key_new() {
        let key = UserKey::new("test_key");
        assert_eq!(key.key, "test_key");

        let key_from_int = UserKey::new(42);
        assert_eq!(key_from_int.key, "42");
    }

    #[test]
    fn test_user_key_default() {
        let key = UserKey::default();
        assert_eq!(key.key, "");
        assert!(key.key.is_empty());
    }

    #[test]
    fn test_user_key_deref() {
        let key = UserKey::new("test_deref");

        // Test deref functionality
        assert_eq!(*key, "test_deref");
        assert_eq!(key.len(), 10);
        assert_eq!(key.chars().count(), 10);
    }

    #[test]
    fn test_user_key_clone() {
        let key1 = UserKey::new("clone_test");
        let key2 = key1.clone();

        assert_eq!(key1, key2);
        assert_eq!(key1.key, key2.key);
    }

    #[test]
    fn test_user_key_equality() {
        let key1 = UserKey::new("same_key");
        let key2 = UserKey::new("same_key");
        let key3 = UserKey::new("different_key");

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
        assert_ne!(key2, key3);
    }

    #[test]
    fn test_user_key_ordering() {
        let key_a = UserKey::new("a");
        let key_b = UserKey::new("b");
        let key_c = UserKey::new("c");

        assert!(key_a < key_b);
        assert!(key_b < key_c);
        assert!(key_a < key_c);

        let mut keys = vec![key_c.clone(), key_a.clone(), key_b.clone()];
        keys.sort();
        assert_eq!(keys, vec![key_a, key_b, key_c]);
    }

    #[test]
    fn test_user_key_debug() {
        let key = UserKey::new("debug_test");
        let debug_str = format!("{:?}", key);
        assert!(debug_str.contains("UserKey"));
        assert!(debug_str.contains("debug_test"));
    }

    #[test]
    fn test_string_as_ref_user_key() {
        let s = "test_string".to_string();
        let user_key_ref: &UserKey = s.as_ref();

        // Test that the reference points to the same data
        assert_eq!(user_key_ref.key, "test_string");
    }

    #[test]
    fn test_string_as_ref_user_key_memory_layout() {
        let s = "memory_test".to_string();
        let user_key_ref: &UserKey = s.as_ref();

        // Test that the memory addresses are the same
        let string_ptr = &s as *const String as *const u8;
        let user_key_ptr = &user_key_ref.key as *const String as *const u8;
        assert_eq!(string_ptr, user_key_ptr);

        // Test that the data is identical
        assert_eq!(user_key_ref.key.as_ptr(), s.as_ptr());
        assert_eq!(user_key_ref.key.len(), s.len());
        assert_eq!(user_key_ref.key.capacity(), s.capacity());
    }

    #[test]
    fn test_user_key_partial_ord() {
        let key1 = UserKey::new("apple");
        let key2 = UserKey::new("banana");
        let key3 = UserKey::new("cherry");

        assert!(key1.partial_cmp(&key2) == Some(std::cmp::Ordering::Less));
        assert!(key2.partial_cmp(&key3) == Some(std::cmp::Ordering::Less));
        assert!(key1.partial_cmp(&key1) == Some(std::cmp::Ordering::Equal));
        assert!(key3.partial_cmp(&key1) == Some(std::cmp::Ordering::Greater));
    }

    #[test]
    fn test_user_key_from_str() {
        let a = String::from("test_from_str");
        let key = UserKey::from_string_ref(&a);
        assert_eq!(key.key, "test_from_str");
    }

    #[test]
    fn test_to_string() {
        let key = UserKey::new("to_string_test");
        let string_representation = key.to_string();
        assert_eq!(string_representation, "to_string_test");

        // Test with an empty key
        let empty_key = UserKey::default();
        assert_eq!(empty_key.to_string(), "");
    }
}
