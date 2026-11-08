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

//! Build independent index contributions and upload branch-local artifacts.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_metrics::storage::metrics_inc_block_spatial_index_generate_milliseconds;
use databend_common_pipeline_transforms::AsyncTransform;
use databend_common_pipeline_transforms::Transform;
use databend_storages_common_index::BloomIndexBuilder;
use databend_storages_common_index::BloomIndexType;
use log::info;
use opendal::Operator;

use crate::FuseTable;
use crate::io::BlockWriter;
use crate::io::SpatialIndexBuilder;
use crate::io::TableMetaLocationGenerator;
use crate::io::VectorIndexBuilder;
use crate::operations::table_index::meta::BuiltIndexes;
use crate::operations::table_index::meta::BuiltSpatialIndex;
use crate::operations::table_index::meta::IndexArtifact;
use crate::operations::table_index::meta::IndexSpec;
use crate::operations::table_index::meta::RefreshInput;
use crate::operations::table_index::meta::WrittenVectorIndex;

pub struct BuildIndex {
    ctx: Arc<dyn TableContext>,
    spec: Arc<IndexSpec>,
    operator: Operator,
    locations: TableMetaLocationGenerator,
}

impl BuildIndex {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        spec: Arc<IndexSpec>,
        operator: Operator,
        locations: TableMetaLocationGenerator,
    ) -> Self {
        Self {
            ctx,
            spec,
            operator,
            locations,
        }
    }
}

impl Transform for BuildIndex {
    const NAME: &'static str = "BuildRefreshIndex";
    fn name(&self) -> String {
        format!("BuildRefreshIndex({})", self.spec.index.name)
    }
    fn transform(&mut self, mut block: DataBlock) -> Result<DataBlock> {
        self.ctx
            .check_aborting()
            .map_err(|error| error.with_context("building refresh index"))?;
        let RefreshInput(plan) = block
            .take_meta()
            .and_then(RefreshInput::downcast_from)
            .ok_or_else(|| ErrorCode::Internal("Missing refresh block input"))?;
        let mut artifacts = Vec::new();
        if plan.missing.contains(&self.spec.index.name) {
            let columns = self
                .spec
                .fields
                .iter()
                .map(|field| {
                    let offset = plan
                        .projection
                        .binary_search(field)
                        .map_err(|_| ErrorCode::Internal("Missing refresh projection"))?;
                    Ok(block.get_by_offset(offset).clone())
                })
                .collect::<Result<Vec<_>>>()?;
            let index_block = DataBlock::new(columns, block.num_rows());
            let indexes = BTreeMap::from([(self.spec.index.name.clone(), self.spec.index.clone())]);
            let artifact = match self.spec.index.index_type {
                TableIndexType::Inverted => IndexArtifact::Inverted(
                    self.spec
                        .inverted
                        .as_ref()
                        .ok_or_else(|| ErrorCode::Internal("Missing inverted index builder"))?
                        .build(
                            &self.operator,
                            &self.locations,
                            &self.spec.schema,
                            &index_block,
                        )?,
                ),
                TableIndexType::Vector => {
                    let mut builder =
                        VectorIndexBuilder::try_create(&indexes, self.spec.schema.clone(), false)?
                            .ok_or_else(|| {
                                ErrorCode::RefreshIndexError("No vector index to refresh")
                            })?;
                    builder.add_block(&index_block)?;
                    IndexArtifact::PendingVector(builder.finalize_block(&self.locations)?)
                }
                TableIndexType::Ngram => {
                    let args =
                        FuseTable::create_ngram_index_args(&indexes, &self.spec.schema, false)?;
                    let mut builder = BloomIndexBuilder::create(
                        self.ctx.get_function_context()?,
                        BloomIndexType::default(),
                        BTreeMap::new(),
                        &args,
                    )?;
                    builder.add_block(&index_block)?;
                    IndexArtifact::Bloom(builder.finalize()?.ok_or_else(|| {
                        ErrorCode::RefreshIndexError("Failed to build Ngram index")
                    })?)
                }
                TableIndexType::Spatial => {
                    let start = Instant::now();
                    info!(
                        "Start build spatial index {} for refresh, block: {}",
                        self.spec.index.name, plan.meta.location.0
                    );
                    let mut builder =
                        SpatialIndexBuilder::try_create(&indexes, self.spec.schema.clone(), false)
                            .ok_or_else(|| {
                                ErrorCode::RefreshIndexError("No spatial index to refresh")
                            })?;
                    builder.add_block(&index_block)?;
                    let (columns, stats) = builder.build_refresh()?;
                    let elapsed_ms = start.elapsed().as_millis() as u64;
                    metrics_inc_block_spatial_index_generate_milliseconds(elapsed_ms);
                    info!(
                        "Finish build spatial index {} for refresh, block: {}, cost={} ms",
                        self.spec.index.name, plan.meta.location.0, elapsed_ms
                    );
                    IndexArtifact::Spatial(BuiltSpatialIndex {
                        columns,
                        stats,
                        ids: self.spec.index.column_ids.clone(),
                        name: self.spec.index.name.clone(),
                    })
                }
                TableIndexType::Bloom => {
                    return Err(ErrorCode::RefreshIndexError(format!(
                        "BLOOM index {} is built with the blocks and cannot be refreshed",
                        self.spec.index.name
                    )));
                }
            };
            artifacts.push(artifact);
        }
        Ok(DataBlock::empty_with_meta(Box::new(BuiltIndexes {
            plan,
            artifacts,
        })))
    }
}

pub struct WriteIndexArtifacts {
    operator: Operator,
}

impl WriteIndexArtifacts {
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }
}

#[async_trait::async_trait]
impl AsyncTransform for WriteIndexArtifacts {
    const NAME: &'static str = "WriteRefreshIndexArtifacts";

    async fn transform(&mut self, block: DataBlock) -> Result<DataBlock> {
        let mut result = block
            .get_owned_meta()
            .and_then(BuiltIndexes::downcast_from)
            .ok_or_else(|| ErrorCode::Internal("Missing built refresh indexes"))?;
        let mut written = Vec::with_capacity(result.artifacts.len());
        for artifact in result.artifacts {
            written.push(match artifact {
                IndexArtifact::PendingVector(pending) => {
                    if pending.files.is_empty() {
                        return Err(ErrorCode::RefreshIndexError("Failed to build vector index"));
                    }
                    let metas = pending.files.iter().map(|(meta, _)| meta.clone()).collect();
                    BlockWriter::write_down_vector_index_files(&self.operator, pending.files)
                        .await?;
                    IndexArtifact::Vector(WrittenVectorIndex {
                        metas,
                        statistics: pending.statistics,
                    })
                }
                other => other,
            });
        }
        result.artifacts = written;
        Ok(DataBlock::empty_with_meta(Box::new(result)))
    }
}
