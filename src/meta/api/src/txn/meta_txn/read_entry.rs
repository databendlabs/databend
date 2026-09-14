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

use databend_meta_client::types::SeqV;

/// One key retrieved while building a [`MetaTxn`](super::MetaTxn).
///
/// It holds the raw [`SeqV`] as read from the backend, so the read set is
/// type-independent: the seq drives the `eq_seq` guard at commit, the meta is
/// preserved, and the encoded value serves as a snapshot cache decoded per read.
pub(crate) struct ReadEntry {
    /// Whether any read of this key was "for update", i.e. whether it should
    /// produce an `eq_seq` guard at commit.
    pub(crate) for_update: bool,

    /// The raw record read, `None` if the key was absent. `SeqV<Vec<u8>>` carries
    /// the seq and meta as well as the encoded value.
    pub(crate) seqv: Option<SeqV<Vec<u8>>>,
}
