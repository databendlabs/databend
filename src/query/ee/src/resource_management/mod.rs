// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod resources_management_kubernetes;
mod resources_management_self_managed;
mod resources_management_system;

use std::sync::Arc;
use std::time::Duration;

use databend_common_base::base::GlobalInstance;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_management::WarehouseMgr;
use databend_common_meta_store::MetaStoreProvider;
use databend_enterprise_resources_management::ResourcesManagement;
use databend_meta_runtime::DatabendRuntime;
use databend_query::sessions::BuildInfoRef;
pub use resources_management_kubernetes::KubernetesResourcesManagement;
pub use resources_management_self_managed::SelfManagedResourcesManagement;
pub use resources_management_system::SystemResourcesManagement;

pub async fn init_resources_management(cfg: &InnerConfig, version: BuildInfoRef) -> Result<()> {
    let service: Arc<dyn ResourcesManagement> = match &cfg.query.common.resources_management {
        None => match cfg.query.common.cluster_id.is_empty() {
            true => Err(ErrorCode::InvalidConfig(
                "cluster_id is empty without resources management.",
            )),
            false => match cfg.query.common.warehouse_id.is_empty() {
                true => Err(ErrorCode::InvalidConfig(
                    "warehouse_id is empty without resources management.",
                )),
                false => SelfManagedResourcesManagement::create(cfg),
            },
        },
        Some(resources_management) => {
            match resources_management.typ.to_ascii_lowercase().as_str() {
                "self_managed" => SelfManagedResourcesManagement::create(cfg),
                "kubernetes_managed" => KubernetesResourcesManagement::create(),
                "system_managed" => {
                    let meta_api_provider =
                        MetaStoreProvider::new(cfg.meta.to_meta_grpc_client_conf());
                    match meta_api_provider
                        .create_meta_store::<DatabendRuntime>()
                        .await
                    {
                        Err(cause) => {
                            let err = ErrorCode::MetaServiceError(format!(
                                "Failed to create meta store: {}",
                                cause
                            ));

                            Err(err.add_message_back("(while create resources management)."))
                        }
                        Ok(metastore) => {
                            let tenant_id = &cfg.query.tenant_id;
                            let lift_time = Duration::from_secs(60);
                            let warehouse_manager = WarehouseMgr::create(
                                metastore,
                                tenant_id.tenant_name(),
                                lift_time,
                                version,
                            )?;
                            SystemResourcesManagement::create(Arc::new(warehouse_manager))
                        }
                    }
                }
                _ => Err(ErrorCode::InvalidConfig(format!(
                    "unimplemented resources management {}",
                    resources_management.typ
                ))),
            }
        }
    }?;

    GlobalInstance::set(service);
    Ok(())
}
