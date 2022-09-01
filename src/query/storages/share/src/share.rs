// Copyright 2022 Datafuse Labs.
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

use std::collections::BTreeMap;

use common_exception::Result;
use common_meta_app::share::ShareGrantObjectName;
use common_storages_fuse::OPT_KEY_LEGACY_SNAPSHOT_LOC;
use common_storages_fuse::OPT_KEY_SNAPSHOT_LOCATION;
use opendal::Operator;

pub const SHARE_CONFIG_PREFIX: &str = "_share_config";

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct ShareTableSpec {
    pub name: String,
    pub id: u64,
    pub location: String,
    pub version: u64,
    pub presigned_url_timeout: String,
}

impl ShareTableSpec {
    pub fn new(name: &str, id: u64, location: String) -> Self {
        ShareTableSpec {
            name: name.to_owned(),
            id,
            location,
            version: 1,
            presigned_url_timeout: "120s".to_string(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct ShareDatabaseSpec {
    pub name: String,
    pub id: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct ShareSpec {
    pub name: String,
    pub database: Option<ShareDatabaseSpec>,
    pub tables: Vec<ShareTableSpec>,
    pub tenants: Vec<String>,
}

impl ShareSpec {
    pub fn new(name: String) -> Self {
        ShareSpec {
            name,
            database: None,
            tables: vec![],
            tenants: vec![],
        }
    }

    pub fn grant_object(
        &mut self,
        name: &ShareGrantObjectName,
        id: &u64,
        options: &Option<BTreeMap<String, String>>,
    ) -> Self {
        match name {
            ShareGrantObjectName::Database(name) => {
                self.database = Some(ShareDatabaseSpec {
                    name: name.clone(),
                    id: *id,
                });
                self.clone()
            }
            ShareGrantObjectName::Table(_db, table_name) => {
                if let Some(options) = options {
                    let location = options
                        .get(OPT_KEY_SNAPSHOT_LOCATION)
                        // for backward compatibility, we check the legacy table option
                        .or_else(|| options.get(OPT_KEY_LEGACY_SNAPSHOT_LOC))
                        .cloned();
                    if let Some(location) = location {
                        self.tables
                            .push(ShareTableSpec::new(table_name, *id, location));
                        self.tables.sort_by(|a, b| a.name.cmp(&b.name));
                    }
                }

                self.clone()
            }
        }
    }

    pub fn revoke_object(&mut self, name: &ShareGrantObjectName) -> Self {
        match name {
            ShareGrantObjectName::Database(_name) => {
                self.database = None;
                self.clone()
            }
            ShareGrantObjectName::Table(_db, table_name) => {
                let index = self.tables.binary_search_by(|a| a.name.cmp(table_name));
                if let Ok(index) = index {
                    self.tables.remove(index);
                }
                self.clone()
            }
        }
    }
}

pub enum ModShareSpec {
    CreateShare {
        share_name: String,
    },
    DropShare,
    GrantShareObject {
        object_name: ShareGrantObjectName,
        id: u64,
        options: Option<BTreeMap<String, String>>,
    },
    RevokeShareObject {
        object_name: ShareGrantObjectName,
    },
    AddShareAccounts {
        accounts: Vec<String>,
    },
    RemoveShareAccounts {
        accounts: Vec<String>,
    },
}

impl ModShareSpec {
    pub async fn save(&self, tenant: String, share_id: u64, operator: Operator) -> Result<()> {
        let location = format!("{}/{}/{}_share.json", SHARE_CONFIG_PREFIX, tenant, share_id);
        let spec = match &self {
            ModShareSpec::CreateShare { share_name } => Some(ShareSpec::new(share_name.clone())),
            ModShareSpec::DropShare {} => None,
            ModShareSpec::GrantShareObject {
                object_name,
                id,
                options,
            } => {
                self.grant_share_object(&location, &operator, object_name, id, options)
                    .await?
            }
            ModShareSpec::RevokeShareObject { object_name } => {
                self.revoke_share_object(&location, &operator, object_name)
                    .await?
            }
            ModShareSpec::AddShareAccounts { accounts } => {
                self.add_share_tenants(&location, &operator, accounts)
                    .await?
            }
            ModShareSpec::RemoveShareAccounts { accounts } => {
                self.remove_share_tenants(&location, &operator, accounts)
                    .await?
            }
        };

        if let Some(spec) = spec {
            operator
                .object(&location)
                .write(serde_json::to_vec(&spec)?)
                .await?;
        } else {
            operator.object(&location).delete().await?;
        }

        Ok(())
    }

    async fn grant_share_object(
        &self,
        location: &str,
        operator: &Operator,
        name: &ShareGrantObjectName,
        id: &u64,
        options: &Option<BTreeMap<String, String>>,
    ) -> Result<Option<ShareSpec>> {
        let data = operator.object(location).read().await?;
        let mut spec: ShareSpec = serde_json::from_slice(&data)?;
        spec.grant_object(name, id, options);

        Ok(Some(spec))
    }

    async fn revoke_share_object(
        &self,
        location: &str,
        operator: &Operator,
        name: &ShareGrantObjectName,
    ) -> Result<Option<ShareSpec>> {
        let data = operator.object(location).read().await?;
        let mut spec: ShareSpec = serde_json::from_slice(&data)?;
        spec.revoke_object(name);

        Ok(Some(spec))
    }

    async fn add_share_tenants(
        &self,
        location: &str,
        operator: &Operator,
        tenants: &[String],
    ) -> Result<Option<ShareSpec>> {
        let data = operator.object(location).read().await?;
        let mut spec: ShareSpec = serde_json::from_slice(&data)?;
        spec.tenants.extend(tenants.iter().cloned());

        Ok(Some(spec))
    }

    async fn remove_share_tenants(
        &self,
        location: &str,
        operator: &Operator,
        tenants: &[String],
    ) -> Result<Option<ShareSpec>> {
        let data = operator.object(location).read().await?;
        let mut spec: ShareSpec = serde_json::from_slice(&data)?;
        for tenant in tenants {
            if let Ok(index) = spec.tenants.binary_search_by(|a| a.cmp(tenant)) {
                spec.tenants.remove(index);
            }
        }

        Ok(Some(spec))
    }
}
