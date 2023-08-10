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

use crate::procedure_sig_factory::ProcedureSigFactory;
use crate::systems::clustering_information::ClusteringInformationProcedureSig;
use crate::systems::execute_job::ExecuteJobProcedureSig;
use crate::systems::fuse_block::FuseBlockProcedureSig;
use crate::systems::fuse_column::FuseColumnProcedureSig;
use crate::systems::fuse_segment::FuseSegmentProcedureSig;
use crate::systems::fuse_snapshot::FuseSnapshotProcedureSig;
use crate::systems::search_tables::SearchTablesProcedureSig;

pub struct SystemProcedureSig;

impl SystemProcedureSig {
    pub fn register(factory: &mut ProcedureSigFactory) {
        factory.register(
            "system$clustering_information",
            Box::new(ClusteringInformationProcedureSig::try_create),
        );
        factory.register(
            "system$fuse_snapshot",
            Box::new(FuseSnapshotProcedureSig::try_create),
        );
        factory.register(
            "system$fuse_segment",
            Box::new(FuseSegmentProcedureSig::try_create),
        );
        factory.register(
            "system$fuse_block",
            Box::new(FuseBlockProcedureSig::try_create),
        );
        factory.register(
            "system$fuse_column",
            Box::new(FuseColumnProcedureSig::try_create),
        );
        factory.register(
            "system$search_tables",
            Box::new(SearchTablesProcedureSig::try_create),
        );
        factory.register(
            "system$execute_background_job",
            Box::new(ExecuteJobProcedureSig::try_create),
        );
    }
}
