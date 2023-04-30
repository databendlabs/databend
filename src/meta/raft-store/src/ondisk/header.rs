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

use common_meta_sled_store::sled;
use common_meta_sled_store::SledBytesError;
use common_meta_sled_store::SledSerde;

use crate::ondisk::DataVersion;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct Header {
    /// Current data version
    pub version: DataVersion,

    /// The target version to upgrade to.
    ///
    /// If it is present, the data is upgrading.
    pub upgrading: Option<DataVersion>,
}

impl SledSerde for Header {
    fn ser(&self) -> Result<sled::IVec, SledBytesError> {
        let x = serde_json::to_vec(self)?;
        Ok(x.into())
    }

    fn de<T: AsRef<[u8]>>(v: T) -> Result<Self, SledBytesError>
    where Self: Sized {
        let x = serde_json::from_slice(v.as_ref())?;
        Ok(x)
    }
}
