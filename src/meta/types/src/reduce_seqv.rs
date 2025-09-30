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

/// Trait to reduce the size of `SeqV` by removing default or unnecessary fields.
pub trait ReduceSeqV {
    /// Erase the `proposed_at_ms` field in the metadata.
    ///
    /// This is useful when the exact proposal time is not important.
    /// And is used for testing
    fn erase_proposed_at(self) -> Self;

    /// Remove the metadata if it is default.
    fn reduce(self) -> Self;
}

impl<T> ReduceSeqV for SeqV<T> {
    fn erase_proposed_at(mut self) -> Self {
        if let Some(meta) = &mut self.meta {
            meta.proposed_at_ms = None;
        }
        self.reduce()
    }

    fn reduce(mut self) -> Self {
        if self.meta == Some(Default::default()) {
            self.meta = None;
        }
        self
    }
}

impl<T> ReduceSeqV for Option<SeqV<T>> {
    fn erase_proposed_at(self) -> Self {
        self.map(|v| v.erase_proposed_at())
    }

    fn reduce(self) -> Self {
        self.map(|v| v.reduce())
    }
}

impl<T> ReduceSeqV for Vec<SeqV<T>> {
    fn erase_proposed_at(self) -> Self {
        self.into_iter().map(|v| v.erase_proposed_at()).collect()
    }

    fn reduce(self) -> Self {
        self.into_iter().map(|v| v.reduce()).collect()
    }
}

impl<T> ReduceSeqV for Vec<Option<SeqV<T>>> {
    fn erase_proposed_at(self) -> Self {
        self.into_iter().map(|v| v.erase_proposed_at()).collect()
    }

    fn reduce(self) -> Self {
        self.into_iter().map(|v| v.reduce()).collect()
    }
}

impl<K, T> ReduceSeqV for Vec<(K, SeqV<T>)> {
    fn erase_proposed_at(self) -> Self {
        self.into_iter()
            .map(|(k, v)| (k, v.erase_proposed_at()))
            .collect()
    }

    fn reduce(self) -> Self {
        self.into_iter().map(|(k, v)| (k, v.reduce())).collect()
    }
}

impl<T, E> ReduceSeqV for Vec<Result<T, E>>
where T: ReduceSeqV
{
    fn erase_proposed_at(self) -> Self {
        self.into_iter()
            .map(|item| item.map(|v| v.erase_proposed_at()))
            .collect()
    }

    fn reduce(self) -> Self {
        self.into_iter()
            .map(|item| item.map(|v| v.reduce()))
            .collect()
    }
}
