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

use std::any::Any;
use std::sync::Arc;

use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FieldIndex;
use databend_common_meta_app::schema::TableInfo;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipe;
use databend_common_pipeline_transforms::create_dummy_item;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::executor::physical_plans::OnConflictField;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::operations::BroadcastProcessor;
use databend_common_storages_fuse::operations::TransformSerializeBlock;
use databend_storages_common_table_meta::meta::BlockSlotDescription;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use tokio::sync::Semaphore;

use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::ReplaceIntoFormatter;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ReplaceInto {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub block_thresholds: BlockThresholds,
    pub table_info: TableInfo,
    pub on_conflicts: Vec<OnConflictField>,
    pub bloom_filter_column_indexes: Vec<FieldIndex>,
    pub segments: Vec<(usize, Location)>,
    pub block_slots: Option<BlockSlotDescription>,
    pub need_insert: bool,
    pub table_meta_timestamps: TableMetaTimestamps,
}

impl IPhysicalPlan for ReplaceInto {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    #[recursive::recursive]
    fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(DataSchemaRef::default())
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(ReplaceIntoFormatter::create(self))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        let input = children.pop().unwrap();
        PhysicalPlan::new(ReplaceInto {
            meta: self.meta.clone(),
            input,
            block_thresholds: self.block_thresholds,
            table_info: self.table_info.clone(),
            on_conflicts: self.on_conflicts.clone(),
            bloom_filter_column_indexes: self.bloom_filter_column_indexes.clone(),
            segments: self.segments.clone(),
            block_slots: self.block_slots.clone(),
            need_insert: self.need_insert,
            table_meta_timestamps: self.table_meta_timestamps,
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        let max_threads = builder.settings.get_max_threads()?;
        let segment_partition_num = std::cmp::min(self.segments.len(), max_threads as usize);
        let table = builder
            .ctx
            .build_table_by_table_info(&self.table_info, None)?;
        let table = FuseTable::try_from_table(table.as_ref())?;
        let schema = DataSchema::from(table.schema()).into();
        let cluster_stats_gen = table.get_cluster_stats_gen(
            builder.ctx.clone(),
            0,
            self.block_thresholds,
            Some(schema),
        )?;

        // connect to broadcast processor and append transform
        let serialize_block_transform = TransformSerializeBlock::try_create(
            builder.ctx.clone(),
            InputPort::create(),
            OutputPort::create(),
            table,
            cluster_stats_gen,
            MutationKind::Replace,
            self.table_meta_timestamps,
        )?;
        let mut block_builder = serialize_block_transform.get_block_builder();
        block_builder.source_schema = table.schema_with_stream();

        if !self.need_insert {
            if segment_partition_num == 0 {
                return Ok(());
            }
            let broadcast_processor = BroadcastProcessor::new(segment_partition_num);
            builder
                .main_pipeline
                .add_pipe(Pipe::create(1, segment_partition_num, vec![
                    broadcast_processor.into_pipe_item(),
                ]));
            let max_threads = builder.settings.get_max_threads()?;
            let io_request_semaphore = Arc::new(Semaphore::new(max_threads as usize));

            let merge_into_operation_aggregators = table.merge_into_mutators(
                builder.ctx.clone(),
                segment_partition_num,
                block_builder,
                self.on_conflicts.clone(),
                self.bloom_filter_column_indexes.clone(),
                &self.segments,
                self.block_slots.clone(),
                io_request_semaphore,
            )?;
            builder.main_pipeline.add_pipe(Pipe::create(
                segment_partition_num,
                segment_partition_num,
                merge_into_operation_aggregators,
            ));
            return Ok(());
        }

        // The Block Size and Rows is promised by DataSource by user.
        if segment_partition_num == 0 {
            let dummy_item = create_dummy_item();
            //                      ┌──────────────────────┐            ┌──────────────────┐
            //                      │                      ├──┬────────►│  SerializeBlock  │
            // ┌─────────────┐      │                      ├──┘         └──────────────────┘
            // │ UpsertSource├─────►│ ReplaceIntoProcessor │
            // └─────────────┘      │                      ├──┐         ┌──────────────────┐
            //                      │                      ├──┴────────►│  DummyTransform  │
            //                      └──────────────────────┘            └──────────────────┘
            // wrap them into pipeline, order matters!
            builder.main_pipeline.add_pipe(Pipe::create(2, 2, vec![
                serialize_block_transform.into_pipe_item(),
                dummy_item,
            ]));
        } else {
            //                      ┌──────────────────────┐            ┌──────────────────┐
            //                      │                      ├──┬────────►│ SerializeBlock   │
            // ┌─────────────┐      │                      ├──┘         └──────────────────┘
            // │ UpsertSource├─────►│ ReplaceIntoProcessor │
            // └─────────────┘      │                      ├──┐         ┌──────────────────┐
            //                      │                      ├──┴────────►│BroadcastProcessor│
            //                      └──────────────────────┘            └──────────────────┘
            let broadcast_processor = BroadcastProcessor::new(segment_partition_num);
            // wrap them into pipeline, order matters!
            builder
                .main_pipeline
                .add_pipe(Pipe::create(2, segment_partition_num + 1, vec![
                    serialize_block_transform.into_pipe_item(),
                    broadcast_processor.into_pipe_item(),
                ]));
        };

        // 4. connect with MergeIntoOperationAggregators
        if segment_partition_num != 0 {
            //      ┌──────────────────┐               ┌────────────────┐
            // ────►│  SerializeBlock  ├──────────────►│ DummyTransform │
            //      └──────────────────┘               └────────────────┘
            //
            //      ┌───────────────────┐              ┌──────────────────────┐
            // ────►│                   ├──┬──────────►│MergeIntoOperationAggr│
            //      │                   ├──┘           └──────────────────────┘
            //      │ BroadcastProcessor│
            //      │                   ├──┐           ┌──────────────────────┐
            //      │                   ├──┴──────────►│MergeIntoOperationAggr│
            //      │                   │              └──────────────────────┘
            //      │                   ├──┐
            //      │                   ├──┴──────────►┌──────────────────────┐
            //      └───────────────────┘              │MergeIntoOperationAggr│
            //                                         └──────────────────────┘

            let item_size = segment_partition_num + 1;
            let mut pipe_items = Vec::with_capacity(item_size);
            // setup the dummy transform
            pipe_items.push(create_dummy_item());

            let max_threads = builder.settings.get_max_threads()?;
            let io_request_semaphore = Arc::new(Semaphore::new(max_threads as usize));

            // setup the merge into operation aggregators
            let mut merge_into_operation_aggregators = table.merge_into_mutators(
                builder.ctx.clone(),
                segment_partition_num,
                block_builder,
                self.on_conflicts.clone(),
                self.bloom_filter_column_indexes.clone(),
                &self.segments,
                self.block_slots.clone(),
                io_request_semaphore,
            )?;
            assert_eq!(
                segment_partition_num,
                merge_into_operation_aggregators.len()
            );
            pipe_items.append(&mut merge_into_operation_aggregators);

            // extend the pipeline
            assert_eq!(builder.main_pipeline.output_len(), item_size);
            assert_eq!(pipe_items.len(), item_size);
            builder
                .main_pipeline
                .add_pipe(Pipe::create(item_size, item_size, pipe_items));
        }
        Ok(())
    }
}

crate::register_physical_plan!(ReplaceInto => crate::physical_plans::physical_replace_into::ReplaceInto);
