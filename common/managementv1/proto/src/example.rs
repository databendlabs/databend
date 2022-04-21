//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use common_exception::Result;
use serde::Deserialize;
use serde::Serialize;

// This is an example for versioned struct and backward compatible with the old versions.
// We solved this problem:
// 1. Write ser<V1> key/value to metasrv: <key, ser<V1>>
// 2. Want to add/remove/modify a filed in V1 struct
// 3. How the new ser<V1-variant> is backward compatible with the old value ser<V1>?

// How we solve it?
// 1. Write ser<V1> key/value to metasrv: <key, ser<V1>>
// 2. Upgrade V1 to a new struct V2 with a new filed
// 3. Write ser<V2> to metasrv: <key, ser<V2>>
// 4. Introduce a new struct named V2, which is upgrade from V1.
// 5. Read ser<V1> data from metasrv, cast it to V2 with `Impl From<V1> for V2`
// 6. Read ser<V2> data from metasrv, return V2

// Rules:
// 1. All the struct is versioned(immutable)
// 2. If you want add/remove/modify the type struct, you must create a new struct(it's immutable too)
// 3. Impl From<V1> for V2
// 4. Impl TryFrom<Vec<u8>> for V2: serialize V2 to bytes
// 5. Impl TryFrom<V2> for Vec<u8>: deserialize bytes to V2

// Version wrapper for Serialize/Deserialize.
// The serialized bytes will put into meta through KV API:
// <key, ser<VersionExample>>
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "version")]
enum VersionedExample {
    #[serde(rename = "v1")]
    V1(ExampleV1),
    #[serde(rename = "v2")]
    V2(ExampleV2),
}

// V1(please don't modify this struct).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ExampleV1 {
    pub tenant: String,
}

// From V1 to string.
// This method no need anymore if we have a new version V2.
// Here is for test only.
impl TryFrom<ExampleV1> for Vec<u8> {
    type Error = common_exception::ErrorCode;

    fn try_from(value: ExampleV1) -> Result<Vec<u8>> {
        Ok(serde_json::to_string(&VersionedExample::V1(value))?.into_bytes())
    }
}

// V2(please don't modify this struct).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ExampleV2 {
    // Rename.
    pub tenant_rename: String,
    // New field.
    pub meta: MetaData,
}

// This is a normal struct, we should do any serde with it.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct MetaData {
    pub database_name: String,
}

// Backward compatible with old versions.
impl From<ExampleV1> for ExampleV2 {
    fn from(v1: ExampleV1) -> Self {
        ExampleV2 {
            tenant_rename: v1.tenant,
            meta: MetaData {
                database_name: "default".to_string(),
            },
        }
    }
}

// From V2 to bytes.
impl TryFrom<ExampleV2> for Vec<u8> {
    type Error = common_exception::ErrorCode;

    fn try_from(value: ExampleV2) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(&VersionedExample::V2(value))?)
    }
}

// From bytes to V2.
impl TryFrom<Vec<u8>> for ExampleV2 {
    type Error = common_exception::ErrorCode;

    fn try_from(value: Vec<u8>) -> Result<Self> {
        let version = serde_json::from_slice::<VersionedExample>(&value)?;
        match version {
            VersionedExample::V1(v1) => Ok(ExampleV2::from(v1)),
            VersionedExample::V2(v2) => Ok(v2),
        }
    }
}
