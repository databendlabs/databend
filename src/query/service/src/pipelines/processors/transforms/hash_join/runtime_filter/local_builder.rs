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

use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Domain;
use databend_common_expression::Evaluator;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;
use databend_common_expression::RawExpr;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::geometry::extract_geo_and_srid;
use databend_common_functions::BUILTIN_FUNCTIONS;
use geo::algorithm::bounding_rect::BoundingRect;

use crate::physical_plans::SpatialRuntimeFilterMode;
use crate::pipelines::processors::transforms::hash_join::desc::RuntimeFilterDesc;
use crate::pipelines::processors::transforms::hash_join::runtime_filter::packet::JoinRuntimeFilterPacket;
use crate::pipelines::processors::transforms::hash_join::runtime_filter::packet::RuntimeFilterPacket;
use crate::pipelines::processors::transforms::hash_join::runtime_filter::packet::SerializableDomain;
use crate::pipelines::processors::transforms::hash_join::runtime_filter::packet::SpatialPacket;
use crate::pipelines::processors::transforms::hash_join::runtime_filter::spatial::build_rtree_from_rects_with_threshold;
use crate::pipelines::processors::transforms::hash_join::util::hash_by_method_for_bloom;

struct SingleFilterBuilder {
    id: usize,
    data_type: DataType,
    hash_method: Option<HashMethodKind>,

    min_max_domain: Option<Domain>,
    min_max_threshold: usize,

    inlist_builder: Option<ColumnBuilder>,
    inlist_threshold: usize,

    bloom_hashes: Option<Vec<u64>>,
    bloom_threshold: usize,

    spatial_mode: Option<SpatialRuntimeFilterMode>,
    spatial_rects: Vec<(f64, f64, f64, f64)>,
    spatial_srid: Option<i32>,
    spatial_srid_mixed: bool,
    spatial_threshold: usize,
}

impl SingleFilterBuilder {
    fn new(
        desc: &RuntimeFilterDesc,
        inlist_threshold: usize,
        bloom_threshold: usize,
        min_max_threshold: usize,
        spatial_threshold: usize,
    ) -> Result<Self> {
        let data_type = desc.build_key.data_type().clone();
        let hash_method = if desc.spatial_mode.is_some() {
            None
        } else {
            Some(DataBlock::choose_hash_method_with_types(&[
                data_type.clone()
            ])?)
        };

        Ok(Self {
            id: desc.id,
            data_type,
            hash_method,
            min_max_domain: None,
            min_max_threshold: if desc.enable_min_max_runtime_filter {
                min_max_threshold
            } else {
                0
            },
            inlist_builder: None,
            inlist_threshold: if desc.enable_inlist_runtime_filter {
                inlist_threshold
            } else {
                0
            },
            bloom_hashes: None,
            bloom_threshold: if desc.enable_bloom_runtime_filter {
                bloom_threshold
            } else {
                0
            },
            spatial_mode: desc.spatial_mode.clone(),
            spatial_rects: Vec::new(),
            spatial_srid: None,
            spatial_srid_mixed: false,
            spatial_threshold,
        })
    }

    fn add_column(&mut self, column: &Column, total_rows: usize) -> Result<()> {
        let new_total = total_rows + column.len();
        if self.spatial_mode.is_some() {
            self.add_spatial_bbox(column)?;
            return Ok(());
        }
        self.add_min_max(column, new_total);
        self.add_inlist(column, new_total);
        self.add_bloom(column, new_total)?;
        Ok(())
    }

    fn add_min_max(&mut self, column: &Column, new_total: usize) {
        if new_total > self.min_max_threshold {
            self.min_max_domain = None;
            return;
        }
        let col_domain = column.remove_nullable().domain();
        self.min_max_domain = Some(match self.min_max_domain.take() {
            Some(d) => d.merge(&col_domain),
            None => col_domain,
        });
    }

    fn add_inlist(&mut self, column: &Column, new_total: usize) {
        if new_total > self.inlist_threshold {
            self.inlist_builder = None;
            return;
        }
        let mut builder = match self.inlist_builder.take() {
            Some(b) => b,
            None => ColumnBuilder::with_capacity(&self.data_type, column.len()),
        };
        builder.append_column(column);
        self.inlist_builder = Some(builder);
    }

    fn add_bloom(&mut self, column: &Column, new_total: usize) -> Result<()> {
        if new_total > self.bloom_threshold {
            self.bloom_hashes = None;
            return Ok(());
        }
        let mut hashes = match self.bloom_hashes.take() {
            Some(h) => h,
            None => Vec::with_capacity(column.len()),
        };
        hashes.reserve(column.len());
        let entry = BlockEntry::from(column.clone());
        let hash_method = self
            .hash_method
            .as_ref()
            .expect("hash_method must exist for non-spatial filters");
        hash_by_method_for_bloom(hash_method, (&[entry]).into(), column.len(), &mut hashes)?;
        self.bloom_hashes = Some(hashes);
        Ok(())
    }

    fn add_spatial_bbox(&mut self, column: &Column) -> Result<()> {
        if self.spatial_srid_mixed {
            return Ok(());
        }

        for value in column.iter() {
            let Some((geo, srid)) = extract_geo_and_srid(value)? else {
                continue;
            };

            if let Some(prev) = self.spatial_srid {
                if prev != srid {
                    self.spatial_srid_mixed = true;
                    self.spatial_rects.clear();
                    return Ok(());
                }
            } else {
                self.spatial_srid = Some(srid);
            }

            if let Some(rect) = geo.bounding_rect() {
                let min = rect.min();
                let max = rect.max();
                self.spatial_rects.push((min.x, min.y, max.x, max.y));
            }
        }

        Ok(())
    }

    fn finish(mut self, func_ctx: &FunctionContext) -> Result<RuntimeFilterPacket> {
        if self.spatial_mode.is_some() {
            let id = self.id;
            let spatial_packet = self.finish_spatial_packet()?;
            Ok(RuntimeFilterPacket {
                id,
                min_max: None,
                inlist: None,
                bloom: None,
                spatial: Some(spatial_packet),
            })
        } else {
            let min_max = self.min_max_domain.take().map(|domain| {
                let (min, max) = domain.to_minmax();
                SerializableDomain { min, max }
            });

            let inlist = if let Some(builder) = self.inlist_builder.take() {
                let column = builder.build();
                if column.len() == 0 {
                    None
                } else {
                    Some(dedup_column(column, func_ctx, &self.data_type)?)
                }
            } else {
                None
            };

            let bloom = self.bloom_hashes.take();

            Ok(RuntimeFilterPacket {
                id: self.id,
                min_max,
                inlist,
                bloom,
                spatial: None,
            })
        }
    }

    fn finish_spatial_packet(self) -> Result<SpatialPacket> {
        let SingleFilterBuilder {
            spatial_rects,
            spatial_srid,
            spatial_srid_mixed,
            spatial_threshold,
            spatial_mode,
            ..
        } = self;

        let spatial_mode = spatial_mode.unwrap();
        if spatial_srid_mixed {
            return Ok(SpatialPacket {
                valid: false,
                srid: None,
                mode: spatial_mode,
                rtrees: Vec::new(),
            });
        }

        let rect_count = spatial_rects.len();
        let packet_mode = spatial_mode.clone();
        let rect_mode = spatial_mode.clone();
        let rects = spatial_rects
            .into_iter()
            .map(move |(min_x, min_y, max_x, max_y)| match rect_mode {
                SpatialRuntimeFilterMode::DistanceWithin(distance) => (
                    min_x - distance,
                    min_y - distance,
                    max_x + distance,
                    max_y + distance,
                ),
                _ => (min_x, min_y, max_x, max_y),
            });
        let rtrees = build_rtree_from_rects_with_threshold(rects, rect_count, spatial_threshold)?;

        Ok(SpatialPacket {
            valid: true,
            srid: spatial_srid,
            mode: packet_mode,
            rtrees,
        })
    }
}

pub struct RuntimeFilterLocalBuilder {
    func_ctx: FunctionContext,
    builders: Vec<SingleFilterBuilder>,
    total_rows: usize,
    runtime_filters: Vec<RuntimeFilterDesc>,
}

impl RuntimeFilterLocalBuilder {
    pub fn try_create(
        func_ctx: &FunctionContext,
        descs: Vec<RuntimeFilterDesc>,
        inlist_threshold: usize,
        bloom_threshold: usize,
        min_max_threshold: usize,
        spatial_threshold: usize,
    ) -> Result<Option<Self>> {
        if descs.is_empty() {
            return Ok(None);
        }

        let mut builders = Vec::with_capacity(descs.len());
        for desc in descs.iter() {
            builders.push(SingleFilterBuilder::new(
                desc,
                inlist_threshold,
                bloom_threshold,
                min_max_threshold,
                spatial_threshold,
            )?);
        }

        Ok(Some(Self {
            func_ctx: func_ctx.clone(),
            builders,
            total_rows: 0,
            runtime_filters: descs,
        }))
    }

    pub fn add_block(&mut self, block: &DataBlock) -> Result<()> {
        if block.is_empty() {
            return Ok(());
        }

        let evaluator = Evaluator::new(block, &self.func_ctx, &BUILTIN_FUNCTIONS);

        for (builder, desc) in self.builders.iter_mut().zip(self.runtime_filters.iter()) {
            let column = evaluator
                .run(&desc.build_key)?
                .convert_to_full_column(desc.build_key.data_type(), block.num_rows());
            builder.add_column(&column, self.total_rows)?;
        }

        self.total_rows += block.num_rows();
        Ok(())
    }

    pub fn finish(self, spill_happened: bool) -> Result<JoinRuntimeFilterPacket> {
        let total_rows = self.total_rows;

        if spill_happened {
            return Ok(JoinRuntimeFilterPacket::disable_all(total_rows));
        }

        if total_rows == 0 {
            return Ok(JoinRuntimeFilterPacket::complete_without_filters(0));
        }

        let packets: Vec<_> = self
            .builders
            .into_iter()
            .map(|b| {
                let id = b.id;
                b.finish(&self.func_ctx).map(|p| (id, p))
            })
            .collect::<Result<_>>()?;

        Ok(JoinRuntimeFilterPacket::complete(
            packets.into_iter().collect(),
            total_rows,
        ))
    }
}

fn dedup_column(
    column: Column,
    func_ctx: &FunctionContext,
    data_type: &DataType,
) -> Result<Column> {
    let array_expr = RawExpr::Constant {
        span: None,
        scalar: Scalar::Array(column),
        data_type: Some(DataType::Array(Box::new(data_type.clone()))),
    };
    let distinct_expr = RawExpr::FunctionCall {
        span: None,
        name: "array_distinct".to_string(),
        params: vec![],
        args: vec![array_expr],
    };

    let expr = databend_common_expression::type_check::check(&distinct_expr, &BUILTIN_FUNCTIONS)?;
    let block = DataBlock::empty();
    let evaluator = Evaluator::new(&block, func_ctx, &BUILTIN_FUNCTIONS);
    let value = evaluator.run(&expr)?;

    Ok(value.into_scalar().unwrap().into_array().unwrap())
}
