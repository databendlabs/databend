//  Copyright 2021 Datafuse Labs.
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

use std::fmt::Display;
use std::fmt::Formatter;

use common_datavalues::chrono::DateTime;
use common_datavalues::chrono::Utc;
use common_exception::ErrorCode;
use common_exception::Result;
use nanoid::nanoid;

const SAFE: [char; 26] = [
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's',
    't', 'u', 'v', 'w', 'x', 'y', 'z',
];

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct WarehouseInfo {
    pub warehouse_id: String,
    pub tenant: String,
    pub meta: WarehouseMeta,
}

impl Display for WarehouseInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Warehouse Id: {}, Tenant Id: {}, Meta: {}",
            self.warehouse_id, self.tenant, self.meta
        )
    }
}

impl TryFrom<Vec<u8>> for WarehouseInfo {
    type Error = ErrorCode;

    fn try_from(value: Vec<u8>) -> Result<Self> {
        match serde_json::from_slice(&value) {
            Ok(info) => Ok(info),
            Err(serialize_error) => Err(ErrorCode::IllegalWarehouseInfoFormat(format!(
                "Cannot deserialize stage from bytes. cause {}",
                serialize_error
            ))),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct WarehouseMeta {
    pub warehouse_name: String,
    pub size: String,
    pub created_on: DateTime<Utc>,
}

impl Default for WarehouseMeta {
    fn default() -> Self {
        WarehouseMeta {
            warehouse_name: String::from(""),
            size: String::from(""),
            created_on: Utc::now(),
        }
    }
}

impl TryFrom<Vec<u8>> for WarehouseMeta {
    type Error = ErrorCode;

    fn try_from(value: Vec<u8>) -> Result<Self> {
        match serde_json::from_slice(&value) {
            Ok(info) => Ok(info),
            Err(serialize_error) => Err(ErrorCode::IllegalWarehouseMetaFormat(format!(
                "Cannot deserialize stage from bytes. cause {}",
                serialize_error
            ))),
        }
    }
}

impl Display for WarehouseMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Name: {:?}, Size: {:?}, CreatedOn: {:?}",
            self.warehouse_name, self.size, self.created_on
        )
    }
}

impl Default for WarehouseInfo {
    fn default() -> Self {
        WarehouseInfo {
            warehouse_id: String::from(""),
            tenant: String::from(""),
            meta: WarehouseMeta::default(),
        }
    }
}

impl WarehouseInfo {
    pub fn id(&self) -> &str {
        &self.warehouse_id
    }
    pub fn new(
        tenant: impl Into<String>,
        warehouse_name: impl Into<String>,
        size: impl Into<String>,
    ) -> Self {
        WarehouseInfo {
            warehouse_id: Self::generate_id(),
            tenant: tenant.into(),
            meta: WarehouseMeta {
                warehouse_name: warehouse_name.into(),
                size: size.into(),
                created_on: Utc::now(),
            },
        }
    }
    /// generate an id conform to the general naming rule
    /// https://datatracker.ietf.org/doc/html/rfc1035
    pub fn generate_id() -> String {
        let rand = nanoid!(15, &SAFE);
        format!("{}-{}", rand, Utc::now().format("%Y-%m-%d-%H-%M-%S-%6f"))
    }
}
