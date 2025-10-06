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

use state_machine_api::SeqV;

/// Normalizes metadata fields in `SeqV` for simplified comparison and testing.
///
/// This trait provides two levels of metadata simplification:
/// - `normalize()`: Removes metadata if all fields are at their default values
/// - `without_proposed_at()`: Erases the `proposed_at_ms` field, then normalizes
///
/// # Use Cases
///
/// - **Testing**: Simplify assertions by ignoring time-sensitive or implementation-detail fields
/// - **Comparison**: Focus on semantic equality rather than exact metadata match
/// - **Serialization**: Reduce output size by omitting default/unnecessary fields
pub trait NormalizeMeta {
    /// Erases the `proposed_at_ms` field from metadata, then normalizes.
    ///
    /// This is useful in tests where the exact proposal timestamp is an implementation detail
    /// that shouldn't affect logical equality. After erasing `proposed_at_ms`, this method
    /// calls `normalize()` to remove the metadata entirely if all fields are now default.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let result = sm.get_kv("key").await?.without_proposed_at();
    /// assert_eq!(result, Some(SeqV::new(1, value)));
    /// ```
    fn without_proposed_at(self) -> Self;

    /// Removes metadata if all fields are at their default values.
    ///
    /// If `meta` equals `Some(KVMeta::default())`, it's replaced with `None`.
    /// This simplifies the structure when metadata carries no meaningful information.
    fn normalize(self) -> Self;
}

impl<T> NormalizeMeta for SeqV<T> {
    fn without_proposed_at(mut self) -> Self {
        if let Some(meta) = &mut self.meta {
            meta.proposed_at_ms = None;
        }
        self.normalize()
    }

    fn normalize(mut self) -> Self {
        if self.meta == Some(Default::default()) {
            self.meta = None;
        }
        self
    }
}

impl<T> NormalizeMeta for Option<SeqV<T>> {
    fn without_proposed_at(self) -> Self {
        self.map(|v| v.without_proposed_at())
    }

    fn normalize(self) -> Self {
        self.map(|v| v.normalize())
    }
}

impl<T> NormalizeMeta for Vec<SeqV<T>> {
    fn without_proposed_at(self) -> Self {
        self.into_iter().map(|v| v.without_proposed_at()).collect()
    }

    fn normalize(self) -> Self {
        self.into_iter().map(|v| v.normalize()).collect()
    }
}

impl<T> NormalizeMeta for Vec<Option<SeqV<T>>> {
    fn without_proposed_at(self) -> Self {
        self.into_iter().map(|v| v.without_proposed_at()).collect()
    }

    fn normalize(self) -> Self {
        self.into_iter().map(|v| v.normalize()).collect()
    }
}

impl<K, T> NormalizeMeta for Vec<(K, SeqV<T>)> {
    fn without_proposed_at(self) -> Self {
        self.into_iter()
            .map(|(k, v)| (k, v.without_proposed_at()))
            .collect()
    }

    fn normalize(self) -> Self {
        self.into_iter().map(|(k, v)| (k, v.normalize())).collect()
    }
}

impl<T, E> NormalizeMeta for Vec<Result<T, E>>
where T: NormalizeMeta
{
    fn without_proposed_at(self) -> Self {
        self.into_iter()
            .map(|item| item.map(|v| v.without_proposed_at()))
            .collect()
    }

    fn normalize(self) -> Self {
        self.into_iter()
            .map(|item| item.map(|v| v.normalize()))
            .collect()
    }
}
