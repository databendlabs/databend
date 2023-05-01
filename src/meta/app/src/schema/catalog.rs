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

use std::fmt::Display;

use chrono::DateTime;
use chrono::Utc;

use crate::storage::StorageParams;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CatalogType {
    Default = 1,
    Hive = 2,
    Iceberg = 3,
}

impl Display for CatalogType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogType::Default => write!(f, "DEFAULT"),
            CatalogType::Hive => write!(f, "HIVE"),
            CatalogType::Iceberg => write!(f, "ICEBERG"),
        }
    }
}

/// Option for creating a iceberg catalog
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergCatalogOption {
    pub storage_params: Box<StorageParams>,
    /// is the remote iceberg storage storing
    /// tables directly in the root directory
    pub flatten: bool,
}

/// different options for creating catalogs
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogOption {
    // hms_address
    Hive(String),
    // Uri location for iceberg
    Iceberg(IcebergCatalogOption),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogMeta {
    pub catalog_option: CatalogOption,
    pub created_on: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogNameIdent {
    pub tenant: String,
    pub catalog_name: String,
}

impl Display for CatalogNameIdent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "'{}'/'{}'", self.tenant, self.catalog_name)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateCatalogReq {
    pub if_not_exists: bool,
    pub name_ident: CatalogNameIdent,
    pub meta: CatalogMeta,
}

impl Display for CreateCatalogReq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "create_catalog(if_not_exists={}):{}/{}={:?}",
            self.if_not_exists, self.name_ident.tenant, self.name_ident.catalog_name, self.meta
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropCatalogReq {
    pub if_exists: bool,
    pub name_ident: CatalogNameIdent,
}

impl Display for DropCatalogReq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "drop_catalog(if_exists={}):{}/{}",
            self.if_exists, self.name_ident.tenant, self.name_ident.catalog_name
        )
    }
}
