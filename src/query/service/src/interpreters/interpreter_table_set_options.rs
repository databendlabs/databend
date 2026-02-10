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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;

use databend_common_ast::ast::Engine;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_pipeline::core::Pipeline;
use databend_common_sql::plans::SetOptionsPlan;
use databend_common_storages_factory::Table;
use databend_common_storages_fuse::FUSE_OPT_KEY_ENABLE_AUTO_ANALYZE;
use databend_common_storages_fuse::FUSE_OPT_KEY_ENABLE_AUTO_VACUUM;
use databend_common_storages_fuse::FuseSegmentFormat;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::TableContext;
use databend_common_storages_fuse::io::SegmentsIO;
use databend_common_storages_fuse::io::read::RowOrientedSegmentReader;
use databend_common_storages_fuse::segment_format_from_location;
use databend_meta_types::MatchSeq;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::meta::column_oriented_segment::AbstractSegment;
use databend_storages_common_table_meta::meta::column_oriented_segment::ColumnOrientedSegmentBuilder;
use databend_storages_common_table_meta::meta::column_oriented_segment::SegmentBuilder;
use databend_storages_common_table_meta::table::OPT_KEY_CHANGE_TRACKING;
use databend_storages_common_table_meta::table::OPT_KEY_CHANGE_TRACKING_BEGIN_VER;
use databend_storages_common_table_meta::table::OPT_KEY_CLUSTER_TYPE;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use databend_storages_common_table_meta::table::OPT_KEY_SEGMENT_FORMAT;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use databend_storages_common_table_meta::table::OPT_KEY_STORAGE_FORMAT;
use databend_storages_common_table_meta::table::OPT_KEY_TEMP_PREFIX;
use log::error;

use crate::interpreters::Interpreter;
use crate::interpreters::common::table_option_validation::is_valid_approx_distinct_columns;
use crate::interpreters::common::table_option_validation::is_valid_block_per_segment;
use crate::interpreters::common::table_option_validation::is_valid_bloom_index_columns;
use crate::interpreters::common::table_option_validation::is_valid_create_opt;
use crate::interpreters::common::table_option_validation::is_valid_data_retention_period;
use crate::interpreters::common::table_option_validation::is_valid_fuse_parquet_dictionary_opt;
use crate::interpreters::common::table_option_validation::is_valid_option_of_type;
use crate::interpreters::common::table_option_validation::is_valid_row_per_block;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::sessions::QueryContext;

pub struct SetOptionsInterpreter {
    ctx: Arc<QueryContext>,
    plan: SetOptionsPlan,
}

impl SetOptionsInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: SetOptionsPlan) -> Result<Self> {
        Ok(SetOptionsInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for SetOptionsInterpreter {
    fn name(&self) -> &str {
        "SetOptionsInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        // valid_options_check and do request to meta_srv
        let mut options_map = HashMap::new();
        // check block_per_segment
        is_valid_block_per_segment(&self.plan.set_options)?;
        // check row_per_block
        is_valid_row_per_block(&self.plan.set_options)?;
        // check data_retention_period
        is_valid_data_retention_period(&self.plan.set_options)?;
        // check enable_parquet_encoding
        is_valid_fuse_parquet_dictionary_opt(&self.plan.set_options)?;

        // check storage_format
        let error_str = "invalid opt for fuse table in alter table statement";
        if self.plan.set_options.contains_key(OPT_KEY_STORAGE_FORMAT) {
            error!("{}", &error_str);
            return Err(ErrorCode::TableOptionInvalid(format!(
                "can't change {} for alter table statement",
                OPT_KEY_STORAGE_FORMAT
            )));
        }

        if self.plan.set_options.contains_key(OPT_KEY_DATABASE_ID) {
            error!("{}", &error_str);
            return Err(ErrorCode::TableOptionInvalid(format!(
                "can't change {} for alter table statement",
                OPT_KEY_DATABASE_ID
            )));
        }
        if self.plan.set_options.contains_key(OPT_KEY_TEMP_PREFIX) {
            error!("{}", &error_str);
            return Err(ErrorCode::TableOptionInvalid(format!(
                "can't change {} for alter table statement",
                OPT_KEY_TEMP_PREFIX
            )));
        }
        if self.plan.set_options.contains_key(OPT_KEY_CLUSTER_TYPE) {
            error!("{}", &error_str);
            return Err(ErrorCode::TableOptionInvalid(format!(
                "can't change {} for alter table statement",
                OPT_KEY_CLUSTER_TYPE
            )));
        }

        // Same as settings of FUSE_OPT_KEY_ENABLE_AUTO_VACUUM, expect value type is unsigned integer
        is_valid_option_of_type::<u32>(&self.plan.set_options, FUSE_OPT_KEY_ENABLE_AUTO_VACUUM)?;

        let catalog = self.ctx.get_catalog(self.plan.catalog.as_str()).await?;
        let database = self.plan.database.as_str();
        let table_name = self.plan.table.as_str();
        let table = catalog
            .get_table(&self.ctx.get_tenant(), database, table_name)
            .await?;

        for table_option in self.plan.set_options.iter() {
            let key = table_option.0.to_lowercase();
            let engine = Engine::from(table.engine());
            if !is_valid_create_opt(&key, &engine) {
                error!("{}", &error_str);
                return Err(ErrorCode::TableOptionInvalid(format!(
                    "table option {key} is invalid for alter table statement",
                )));
            }
            options_map.insert(key, Some(table_option.1.clone()));
        }

        let table = analyze_table(self.ctx.clone(), table, &self.plan.set_options).await?;

        let table_version = table.get_table_info().ident.seq;
        if let Some(value) = self.plan.set_options.get(OPT_KEY_CHANGE_TRACKING) {
            let change_tracking = value.to_lowercase().parse::<bool>()?;
            if table.change_tracking_enabled() != change_tracking {
                let begin_version = if change_tracking {
                    Some(table_version.to_string())
                } else {
                    None
                };
                options_map.insert(OPT_KEY_CHANGE_TRACKING_BEGIN_VER.to_string(), begin_version);
            }
        }

        // check mutability
        table.check_mutable()?;

        // check bloom_index_columns.
        is_valid_bloom_index_columns(&self.plan.set_options, table.schema())?;
        is_valid_approx_distinct_columns(&self.plan.set_options, table.schema())?;

        if let Some(new_snapshot_location) =
            set_segment_format(self.ctx.clone(), table.clone(), &self.plan.set_options).await?
        {
            options_map.insert(
                OPT_KEY_SNAPSHOT_LOCATION.to_string(),
                Some(new_snapshot_location),
            );
        }

        let req = UpsertTableOptionReq {
            table_id: table.get_id(),
            seq: MatchSeq::Exact(table_version),
            options: options_map,
        };

        let _resp = catalog
            .upsert_table_option(&self.ctx.get_tenant(), database, req)
            .await?;
        Ok(PipelineBuildResult::create())
    }
}

async fn set_segment_format(
    ctx: Arc<QueryContext>,
    table: Arc<dyn Table>,
    options: &BTreeMap<String, String>,
) -> Result<Option<String>> {
    let Some(value) = options.get(OPT_KEY_SEGMENT_FORMAT) else {
        return Ok(None);
    };
    let segment_format = FuseSegmentFormat::from_str(value)?;
    let Ok(fuse_table) = FuseTable::try_from_table(table.as_ref()) else {
        return Err(ErrorCode::Unimplemented(format!(
            "table {}, engine type {}, does not support {}",
            table.name(),
            table.get_table_info().engine(),
            OPT_KEY_SEGMENT_FORMAT,
        )));
    };

    let Some(table_snapshot) = fuse_table.read_table_snapshot().await? else {
        return Ok(None);
    };
    let segment_locations = &table_snapshot.segments;
    if segment_locations.is_empty() {
        return Ok(None);
    }
    let segment_format_now = segment_format_from_location(segment_locations[0].0.as_str());
    if segment_format_now == segment_format {
        return Ok(None);
    }
    let chunk_size = ctx.get_settings().get_max_threads()? as usize * 4;
    let mut new_segment_locations = Vec::with_capacity(segment_locations.len());
    let block_per_segment = fuse_table.get_block_thresholds().block_per_segment;
    let table_meta_timestamps =
        ctx.get_table_meta_timestamps(table.as_ref(), Some(table_snapshot.clone()))?;
    for chunk in segment_locations.chunks(chunk_size) {
        let segments_io =
            SegmentsIO::create(ctx.clone(), fuse_table.get_operator(), fuse_table.schema());
        match segment_format {
            FuseSegmentFormat::Row => unimplemented!(),
            FuseSegmentFormat::Column => {
                let mut segment_builder =
                    ColumnOrientedSegmentBuilder::new(table.schema().clone(), block_per_segment);
                let segment_schema = segment_builder.segment_schema();
                let mut projection = HashSet::new();
                for field in segment_schema.fields().iter() {
                    projection.insert(field.name.clone());
                }
                let segments = segments_io
                    .generic_read_segments::<RowOrientedSegmentReader>(chunk, false, &projection)
                    .await?;
                for segment in segments {
                    let segment = segment?;
                    for block in segment.blocks {
                        segment_builder.add_block(block.as_ref().clone())?;
                    }
                    let additional_stats_meta = segment.summary.additional_stats_meta;
                    let segment = segment_builder
                        .build(
                            fuse_table.get_block_thresholds(),
                            fuse_table.cluster_key_id(),
                            additional_stats_meta,
                        )?
                        .serialize()?;
                    let location_gen = fuse_table.meta_location_generator();
                    let location =
                        location_gen.gen_segment_info_location(table_meta_timestamps, true);
                    fuse_table.get_operator().write(&location, segment).await?;
                    new_segment_locations.push((location, SegmentInfo::VERSION));
                }
            }
        }
    }
    let new_snapshot = TableSnapshot::try_new(
        Some(fuse_table.get_table_info().ident.seq),
        Some(table_snapshot.clone()),
        table.schema().as_ref().clone(),
        table_snapshot.summary.clone(),
        new_segment_locations,
        fuse_table.cluster_key_meta(),
        table_snapshot.table_statistics_location(),
        table_meta_timestamps,
    )?;
    let location = fuse_table.meta_location_generator().gen_snapshot_location(
        None,
        &new_snapshot.snapshot_id,
        TableSnapshot::VERSION,
    )?;

    fuse_table
        .get_operator()
        .write(&location, new_snapshot.to_bytes()?)
        .await?;
    Ok(Some(location))
}

async fn analyze_table(
    ctx: Arc<QueryContext>,
    table: Arc<dyn Table>,
    options: &BTreeMap<String, String>,
) -> Result<Arc<dyn Table>> {
    let Some(value) = options.get(FUSE_OPT_KEY_ENABLE_AUTO_ANALYZE).cloned() else {
        return Ok(table);
    };

    let val = value.parse::<u32>().map_err(|e| {
        let msg = format!(
            "Failed to parse value [{value}] for table option 'enable_auto_analyze' as type u32: {e}",
        );
        ErrorCode::TableOptionInvalid(msg)
    })?;
    if val == 0 {
        return Ok(table);
    }

    let Ok(fuse_table) = FuseTable::try_from_table(table.as_ref()) else {
        return Err(ErrorCode::Unimplemented(format!(
            "table {}, engine type {}, does not support {}",
            table.name(),
            table.get_table_info().engine(),
            FUSE_OPT_KEY_ENABLE_AUTO_ANALYZE,
        )));
    };
    let Some(table_snapshot) = fuse_table.read_table_snapshot().await? else {
        return Ok(table);
    };

    let mut pipeline = Pipeline::create();
    fuse_table.do_analyze(
        ctx.clone(),
        table_snapshot,
        &mut pipeline,
        HashMap::new(),
        false,
    )?;
    pipeline.set_max_threads(ctx.get_settings().get_max_threads()? as usize);
    let executor_settings = ExecutorSettings::try_create(ctx.clone())?;
    let pipelines = vec![pipeline];
    let complete_executor = PipelineCompleteExecutor::from_pipelines(pipelines, executor_settings)?;
    ctx.set_executor(complete_executor.get_inner())?;
    complete_executor.execute()?;
    let table = table.refresh(ctx.as_ref()).await?;
    Ok(table)
}
