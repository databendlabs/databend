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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::workload_group::CPU_QUOTA_KEY;
use databend_common_base::runtime::workload_group::MAX_CONCURRENCY_QUOTA_KEY;
use databend_common_base::runtime::workload_group::MEMORY_QUOTA_KEY;
use databend_common_base::runtime::workload_group::QUERY_QUEUED_TIMEOUT_QUOTA_KEY;
use databend_common_base::runtime::workload_group::QUERY_TIMEOUT_QUOTA_KEY;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_management::WorkloadApi;
use databend_common_management::WorkloadMgr;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct ShowWorkloadGroupsInterpreter {
    ctx: Arc<QueryContext>,
}

impl ShowWorkloadGroupsInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>) -> Result<Self> {
        Ok(ShowWorkloadGroupsInterpreter { ctx })
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowWorkloadGroupsInterpreter {
    fn name(&self) -> &str {
        "ShowWorkloadGroupsInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn build_pipeline(&self) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::WorkloadGroup)?;

        let workloads = GlobalInstance::get::<Arc<WorkloadMgr>>().get_all().await?;

        let mut conflict_name = HashSet::with_capacity(workloads.len());
        let mut workload_name = ColumnBuilder::with_capacity(&DataType::String, workloads.len());
        let mut workload_cpu_quota =
            ColumnBuilder::with_capacity(&DataType::String, workloads.len());
        let mut workload_mem_quota =
            ColumnBuilder::with_capacity(&DataType::String, workloads.len());
        let mut workload_query_max_running_time =
            ColumnBuilder::with_capacity(&DataType::String, workloads.len());
        let mut workload_max_query_concurrency =
            ColumnBuilder::with_capacity(&DataType::String, workloads.len());
        let mut workload_statement_queued_timeout_in_seconds =
            ColumnBuilder::with_capacity(&DataType::String, workloads.len());

        for workload in workloads {
            let name = match conflict_name.insert(workload.name.clone()) {
                true => workload.name.clone(),
                false => format!("{}({})", workload.name, workload.id),
            };

            workload_name.push(Scalar::String(name).as_ref());
            workload_cpu_quota.push(Scalar::as_ref(&match workload.get_quota(CPU_QUOTA_KEY) {
                None => Scalar::String(String::new()),
                Some(v) => Scalar::String(format!("{}", v)),
            }));
            workload_mem_quota.push(Scalar::as_ref(
                &match workload.get_quota(MEMORY_QUOTA_KEY) {
                    None => Scalar::String(String::new()),
                    Some(v) => Scalar::String(format!("{}", v)),
                },
            ));
            workload_query_max_running_time.push(Scalar::as_ref(&match workload
                .get_quota(QUERY_TIMEOUT_QUOTA_KEY)
            {
                None => Scalar::String(String::new()),
                Some(v) => Scalar::String(format!("{}", v)),
            }));
            workload_max_query_concurrency.push(Scalar::as_ref(&match workload
                .get_quota(MAX_CONCURRENCY_QUOTA_KEY)
            {
                None => Scalar::String(String::new()),
                Some(v) => Scalar::String(format!("{}", v)),
            }));
            workload_statement_queued_timeout_in_seconds.push(Scalar::as_ref(&match workload
                .get_quota(QUERY_QUEUED_TIMEOUT_QUOTA_KEY)
            {
                None => Scalar::String(String::new()),
                Some(v) => Scalar::String(format!("{}", v)),
            }));
        }

        PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
            workload_name.build(),
            workload_cpu_quota.build(),
            workload_mem_quota.build(),
            workload_query_max_running_time.build(),
            workload_max_query_concurrency.build(),
            workload_statement_queued_timeout_in_seconds.build(),
        ])])
    }
}
