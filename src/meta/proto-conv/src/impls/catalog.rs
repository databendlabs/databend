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
use databend_common_meta_app::storage::StorageParams;
use databend_common_protos::pb;

use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::ToProtoOptionExt;
use crate::VER;
use crate::reader_check_msg;

impl FromToProto for mt::CatalogMeta {
    type PB = pb::CatalogMeta;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: pb::CatalogMeta) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let option = p
            .option
            .ok_or_else(|| Incompatible::new("CatalogMeta.option is None".to_string()))?;

        let v = Self {
            catalog_option: mt::CatalogOption::from_pb(option)?,
            created_on: DateTime::<Utc>::from_pb(p.created_on)?,
        };

        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::CatalogMeta, Incompatible> {
        let p = pb::CatalogMeta {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            option: Some(mt::CatalogOption::to_pb(&self.catalog_option)?),
            created_on: self.created_on.to_pb()?,
        };

        Ok(p)
    }
}

impl FromToProto for mt::CatalogOption {
    type PB = pb::CatalogOption;

    fn get_pb_ver(_: &Self::PB) -> u64 {
        VER
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        // It's possible that we have written data without catalog_option before.
        //
        // Let's load it as default catalog.
        let Some(catalog_option) = p.catalog_option else {
            return Ok(mt::CatalogOption::Default);
        };

        Ok(match catalog_option {
            pb::catalog_option::CatalogOption::Hive(v) => {
                mt::CatalogOption::Hive(mt::HiveCatalogOption::from_pb(v)?)
            }
            pb::catalog_option::CatalogOption::Iceberg(v) => {
                if v.iceberg_catalog_option.is_none() {
                    return Ok(mt::CatalogOption::Default);
                }
                mt::CatalogOption::Iceberg(mt::IcebergCatalogOption::from_pb(v)?)
            }
            pb::catalog_option::CatalogOption::Share(_v) => mt::CatalogOption::Default,
        })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        let catalog_option = match self {
            mt::CatalogOption::Default => None,
            mt::CatalogOption::Hive(v) => Some(pb::catalog_option::CatalogOption::Hive(v.to_pb()?)),
            mt::CatalogOption::Iceberg(v) => {
                Some(pb::catalog_option::CatalogOption::Iceberg(v.to_pb()?))
            }
        };

        Ok(pb::CatalogOption { catalog_option })
    }
}

impl FromToProto for mt::IcebergCatalogOption {
    type PB = pb::IcebergCatalogOption;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let option = p
            .iceberg_catalog_option
            .ok_or_else(|| Incompatible::new("IcebergCatalogOption.option is None".to_string()))?;

        Ok(match option {
            pb::iceberg_catalog_option::IcebergCatalogOption::RestCatalog(v) => {
                mt::IcebergCatalogOption::Rest(mt::IcebergRestCatalogOption::from_pb(v)?)
            }
            pb::iceberg_catalog_option::IcebergCatalogOption::HmsCatalog(v) => {
                mt::IcebergCatalogOption::Hms(mt::IcebergHmsCatalogOption::from_pb(v)?)
            }
            pb::iceberg_catalog_option::IcebergCatalogOption::GlueCatalog(v) => {
                mt::IcebergCatalogOption::Glue(mt::IcebergGlueCatalogOption::from_pb(v)?)
            }
            pb::iceberg_catalog_option::IcebergCatalogOption::StorageCatalog(v) => {
                mt::IcebergCatalogOption::Storage(mt::IcebergStorageCatalogOption::from_pb(v)?)
            }
        })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        Ok(pb::IcebergCatalogOption {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            iceberg_catalog_option: Some(match self {
                mt::IcebergCatalogOption::Rest(v) => {
                    pb::iceberg_catalog_option::IcebergCatalogOption::RestCatalog(v.to_pb()?)
                }
                mt::IcebergCatalogOption::Hms(v) => {
                    pb::iceberg_catalog_option::IcebergCatalogOption::HmsCatalog(v.to_pb()?)
                }
                mt::IcebergCatalogOption::Glue(v) => {
                    pb::iceberg_catalog_option::IcebergCatalogOption::GlueCatalog(v.to_pb()?)
                }
                mt::IcebergCatalogOption::Storage(v) => {
                    pb::iceberg_catalog_option::IcebergCatalogOption::StorageCatalog(v.to_pb()?)
                }
            }),
        })
    }
}

impl FromToProto for mt::IcebergRestCatalogOption {
    type PB = pb::IcebergRestCatalogOption;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        Ok(Self {
            uri: p.uri,
            warehouse: p.warehouse,
            props: p
                .props
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        Ok(pb::IcebergRestCatalogOption {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            uri: self.uri.clone(),
            warehouse: self.warehouse.clone(),
            props: self
                .props
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        })
    }
}

impl FromToProto for mt::IcebergHmsCatalogOption {
    type PB = pb::IcebergHmsCatalogOption;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        Ok(Self {
            address: p.address,
            warehouse: p.warehouse,
            props: p
                .props
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        Ok(pb::IcebergHmsCatalogOption {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            address: self.address.clone(),
            warehouse: self.warehouse.clone(),
            props: self
                .props
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        })
    }
}

impl FromToProto for mt::IcebergGlueCatalogOption {
    type PB = pb::IcebergGlueCatalogOption;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        Ok(Self {
            address: p.address,
            warehouse: p.warehouse,
            props: p
                .props
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        Ok(pb::IcebergGlueCatalogOption {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            address: self.address.clone(),
            warehouse: self.warehouse.clone(),
            props: self
                .props
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        })
    }
}

impl FromToProto for mt::IcebergStorageCatalogOption {
    type PB = pb::IcebergStorageCatalogOption;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        Ok(Self {
            address: p.address,
            table_bucket_arn: p.table_bucket_arn,
            props: p
                .props
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        Ok(pb::IcebergStorageCatalogOption {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            address: self.address.clone(),
            table_bucket_arn: self.table_bucket_arn.clone(),
            props: self
                .props
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        })
    }
}

impl FromToProto for mt::HiveCatalogOption {
    type PB = pb::HiveCatalogOption;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        Ok(Self {
            address: p.address,
            storage_params: p
                .storage_params
                .map(|v| StorageParams::from_pb(v).map(Box::new))
                .transpose()?,
        })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        Ok(pb::HiveCatalogOption {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            address: self.address.clone(),
            storage_params: self.storage_params.to_pb_opt()?,
        })
    }
}

impl FromToProto for mt::ShareCatalogOption {
    type PB = pb::ShareCatalogOption;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        Ok(Self {
            provider: p.provider,
            share_name: p.share_name,
            share_endpoint: p.share_endpoint,
        })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        Ok(pb::ShareCatalogOption {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            provider: self.provider.clone(),
            share_name: self.share_name.clone(),
            share_endpoint: self.share_endpoint.clone(),
        })
    }
}
