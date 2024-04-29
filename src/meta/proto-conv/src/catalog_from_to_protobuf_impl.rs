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

//! This mod is the key point about compatibility.
//! Everytime update anything in this file, update the `VER` and let the tests pass.

use chrono::DateTime;
use chrono::Utc;
use databend_common_meta_app::schema as mt;
use databend_common_meta_app::schema::CatalogOption;
use databend_common_meta_app::schema::HiveCatalogOption;
use databend_common_meta_app::schema::IcebergCatalogOption;
use databend_common_meta_app::storage::StorageParams;
use databend_common_protos::pb;

use crate::reader_check_msg;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;

impl FromToProto for mt::CatalogMeta {
    type PB = pb::CatalogMeta;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: pb::CatalogMeta) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let option = p
            .option
            .ok_or_else(|| Incompatible {
                reason: "CatalogMeta.option is None".to_string(),
            })?
            .catalog_option
            .ok_or_else(|| Incompatible {
                reason: "CatalogMeta.option.catalog_option is None".to_string(),
            })?;

        let v = Self {
            catalog_option: match option {
                pb::catalog_option::CatalogOption::Hive(v) => {
                    CatalogOption::Hive(HiveCatalogOption {
                        address: v.address,
                        storage_params: v.storage_params.map(StorageParams::from_pb).transpose()?.map(Box::new),
                    })
                }
                pb::catalog_option::CatalogOption::Iceberg(v) => {
                    CatalogOption::Iceberg(IcebergCatalogOption {
                        storage_params: Box::new(StorageParams::from_pb(
                            v.storage_params.ok_or_else(|| Incompatible {
                                reason: "CatalogMeta.option.catalog_option.iceberg.StorageParams is None".to_string(),
                            })?,
                        )?),
                    })
                }
            },
            created_on: DateTime::<Utc>::from_pb(p.created_on)?,
        };

        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::CatalogMeta, Incompatible> {
        let p = pb::CatalogMeta {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            option: match self.catalog_option.clone() {
                CatalogOption::Default => {
                    return Err(Incompatible {
                        reason: "CatalogOption.default is invalid for metasrv".to_string(),
                    });
                }
                CatalogOption::Hive(v) => Some(pb::CatalogOption {
                    catalog_option: Some(pb::catalog_option::CatalogOption::Hive(
                        pb::HiveCatalogOption {
                            ver: VER,
                            min_reader_ver: MIN_READER_VER,
                            address: v.address,
                            storage_params: v.storage_params.map(|v| v.to_pb()).transpose()?,
                        },
                    )),
                }),
                CatalogOption::Iceberg(v) => Some(pb::CatalogOption {
                    catalog_option: Some(pb::catalog_option::CatalogOption::Iceberg(
                        pb::IcebergCatalogOption {
                            ver: VER,
                            min_reader_ver: MIN_READER_VER,
                            storage_params: Some(v.storage_params.to_pb()?),
                        },
                    )),
                }),
            },
            created_on: self.created_on.to_pb()?,
        };

        Ok(p)
    }
}
