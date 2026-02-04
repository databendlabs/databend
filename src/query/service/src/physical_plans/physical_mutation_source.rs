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
use std::collections::HashMap;

use databend_common_base::runtime::Runtime;
use databend_common_catalog::plan::Filters;
use databend_common_catalog::plan::PartInfoType;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::FunctionContext;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::TableInfo;
use databend_common_pipeline::sources::OneBlockSource;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_pipeline_transforms::columns::TransformAddStreamColumns;
use databend_common_sql::ColumnSet;
use databend_common_sql::IndexType;
use databend_common_sql::Metadata;
use databend_common_sql::ScalarExpr;
use databend_common_sql::StreamContext;
use databend_common_sql::binder::MutationType;
use databend_common_sql::executor::cast_expr_to_non_null_boolean;
use databend_common_storages_fuse::FuseLazyPartInfo;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::SegmentLocation;
use databend_common_storages_fuse::operations::CommitMeta;
use databend_common_storages_fuse::operations::ConflictResolveContext;
use databend_common_storages_fuse::operations::MutationAction;
use databend_common_storages_fuse::operations::MutationBlockPruningContext;

use crate::physical_plans::PhysicalPlanBuilder;
use crate::physical_plans::format::MutationSourceFormatter;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MutationSource {
    pub meta: PhysicalPlanMeta,
    pub table_index: IndexType,
    pub table_info: TableInfo,
    pub filters: Option<Filters>,
    pub output_schema: DataSchemaRef,
    pub input_type: MutationType,

    /// Metadata column indices used in the mutation query.
    /// These are column references in the query's metadata, which may include both
    /// base table columns and internal columns (like _block_name).
    pub read_partition_columns: ColumnSet,

    /// Actual table schema positions for base table columns.
    /// This is derived from read_partition_columns by mapping metadata column indices
    /// to their positions in the physical table schema. Internal columns are excluded.
    /// The positions are sorted and deduplicated.
    pub read_column_positions: Vec<usize>,

    /// Internal columns (e.g., _block_name, _segment_name) that need to be materialized.
    /// These columns don't exist in the base table schema but are generated on-the-fly
    /// from block metadata during query execution.
    pub internal_columns: Vec<databend_common_catalog::plan::InternalColumn>,

    pub truncate_table: bool,

    pub partitions: Partitions,
    pub statistics: PartStatistics,
}

#[typetag::serde]
impl IPhysicalPlan for MutationSource {
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
        Ok(self.output_schema.clone())
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(MutationSourceFormatter::create(self))
    }

    fn try_find_mutation_source(&self) -> Option<MutationSource> {
        Some(self.clone())
    }

    fn derive(&self, children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert!(children.is_empty());
        PhysicalPlan::new(MutationSource {
            meta: self.meta.clone(),
            table_index: self.table_index,
            table_info: self.table_info.clone(),
            filters: self.filters.clone(),
            output_schema: self.output_schema.clone(),
            input_type: self.input_type.clone(),
            read_partition_columns: self.read_partition_columns.clone(),
            read_column_positions: self.read_column_positions.clone(),
            internal_columns: self.internal_columns.clone(),
            truncate_table: self.truncate_table,
            partitions: self.partitions.clone(),
            statistics: self.statistics.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        let table = builder
            .ctx
            .build_table_by_table_info(&self.table_info, None, None)?;

        let table = FuseTable::try_from_table(table.as_ref())?.clone();
        let is_delete = self.input_type == MutationType::Delete;
        if self.truncate_table {
            // There is no filter and the mutation type is delete,
            // we can truncate the table directly.
            debug_assert!(self.partitions.is_empty() && is_delete);
            return builder.main_pipeline.add_source(
                |output| {
                    let meta = CommitMeta {
                        conflict_resolve_context: ConflictResolveContext::None,
                        new_segment_locs: vec![],
                        table_id: table.get_id(),
                        virtual_schema: None,
                        hll: HashMap::new(),
                    };
                    let block = DataBlock::empty_with_meta(Box::new(meta));
                    OneBlockSource::create(output, block)
                },
                1,
            );
        }

        let read_partition_columns = self.read_column_positions.clone();

        let is_lazy = self.partitions.partitions_type() == PartInfoType::LazyLevel && is_delete;
        if is_lazy {
            let ctx = builder.ctx.clone();
            let table_clone = table.clone();
            let ctx_clone = builder.ctx.clone();
            let filters_clone = self.filters.clone();
            let projection = Projection::Columns(read_partition_columns.clone());
            let mut segment_locations = Vec::with_capacity(self.partitions.partitions.len());
            for part in &self.partitions.partitions {
                // Safe to downcast because we know the partition is lazy
                let part = FuseLazyPartInfo::from_part(part)?;
                segment_locations.push(SegmentLocation {
                    segment_idx: part.segment_index,
                    location: part.segment_location.clone(),
                    snapshot_loc: None,
                });
            }
            let prune_ctx = MutationBlockPruningContext {
                segment_locations,
                block_count: None,
            };
            Runtime::with_worker_threads(2, Some("do_mutation_block_pruning".to_string()))?
                .block_on(async move {
                    let (_, partitions) = table_clone
                        .do_mutation_block_pruning(
                            ctx_clone,
                            filters_clone,
                            projection,
                            prune_ctx,
                            true,
                        )
                        .await?;
                    ctx.set_partitions(partitions)?;
                    Ok(())
                })?;
        } else {
            builder.ctx.set_partitions(self.partitions.clone())?;
        }

        let filter = self.filters.clone().map(|v| v.filter);
        let mutation_action = if is_delete {
            MutationAction::Deletion
        } else {
            MutationAction::Update
        };
        let col_indices = self.read_column_positions.clone();
        let update_mutation_with_filter =
            self.input_type == MutationType::Update && filter.is_some();
        table.add_mutation_source(
            builder.ctx.clone(),
            filter,
            col_indices,
            self.internal_columns.clone(),
            &mut builder.main_pipeline,
            mutation_action,
        )?;

        if table.change_tracking_enabled() {
            let stream_ctx = StreamContext::try_create(
                builder.ctx.get_function_context()?,
                table.schema_with_stream(),
                table.get_table_info().ident.seq,
                is_delete,
                update_mutation_with_filter,
            )?;
            builder
                .main_pipeline
                .add_transformer(|| TransformAddStreamColumns::new(stream_ctx.clone()));
        }

        Ok(())
    }
}

impl PhysicalPlanBuilder {
    pub async fn build_mutation_source(
        &mut self,
        mutation_source: &databend_common_sql::plans::MutationSource,
    ) -> Result<PhysicalPlan> {
        let filters = if !mutation_source.predicates.is_empty() {
            Some(create_push_down_filters(
                &self.ctx.get_function_context()?,
                &mutation_source.predicates,
            )?)
        } else {
            None
        };
        let mutation_info = self.mutation_build_info.as_ref().unwrap();

        let metadata = self.metadata.read();
        let mut fields = Vec::with_capacity(mutation_source.columns.len());
        for column_index in mutation_source.columns.iter() {
            let column = metadata.column(*column_index);
            // Ignore virtual computed columns.
            if let Ok(column_id) = mutation_source.schema.index_of(&column.name()) {
                fields.push((column.name(), *column_index, column_id));
            }
        }
        fields.sort_by_key(|(_, _, id)| *id);

        let mut fields = fields
            .into_iter()
            .map(|(name, index, _)| {
                let table_field = mutation_source.schema.field_with_name(&name)?;
                let data_type = DataType::from(table_field.data_type());
                Ok(DataField::new(&index.to_string(), data_type))
            })
            .collect::<Result<Vec<_>>>()?;

        if let Some(predicate_index) = mutation_source.predicate_column_index {
            fields.push(DataField::new(
                &predicate_index.to_string(),
                DataType::Boolean,
            ));
        }
        let output_schema = DataSchemaRefExt::create(fields);

        let table_schema = &mutation_info.table_info.meta.schema;
        let (read_column_positions, internal_columns) = resolve_column_positions(
            &metadata,
            mutation_source.read_partition_columns.iter().copied(),
            table_schema,
        )?;

        let truncate_table =
            mutation_source.mutation_type == MutationType::Delete && filters.is_none();
        Ok(PhysicalPlan::new(MutationSource {
            table_index: mutation_source.table_index,
            output_schema,
            table_info: mutation_info.table_info.clone(),
            filters,
            input_type: mutation_source.mutation_type.clone(),
            read_partition_columns: mutation_source.read_partition_columns.clone(),
            read_column_positions,
            internal_columns,
            truncate_table,
            meta: PhysicalPlanMeta::new("MutationSource"),
            partitions: mutation_info.partitions.clone(),
            statistics: mutation_info.statistics.clone(),
        }))
    }
}

/// Resolves metadata column indices to actual table schema positions and internal columns.
///
/// Given a list of metadata column indices, this function separates them into:
/// - Base table column positions: indices of columns in the actual table schema
/// - Internal columns: special columns like _block_name that don't exist in the base schema
///
/// The returned positions are sorted and deduplicated.
///
/// # Arguments
/// * `metadata` - Query metadata containing column entries
/// * `column_indices` - Iterator of metadata column indices to resolve
/// * `table_schema` - The physical table schema
///
/// # Returns
/// A tuple of (read_column_positions, internal_columns)
pub fn resolve_column_positions(
    metadata: &Metadata,
    column_indices: impl Iterator<Item = IndexType>,
    table_schema: &TableSchemaRef,
) -> Result<(
    Vec<usize>,
    Vec<databend_common_catalog::plan::InternalColumn>,
)> {
    let mut read_column_positions = Vec::new();
    let mut internal_columns = Vec::new();

    for column_index in column_indices {
        let column_entry = metadata.column(column_index);

        match column_entry {
            databend_common_sql::ColumnEntry::BaseTableColumn(base) => {
                // Find the column's index in the table schema
                if let Ok(_field) = table_schema.field_with_name(&base.column_name) {
                    let schema_index = table_schema.index_of(&base.column_name).unwrap();
                    read_column_positions.push(schema_index);
                }
            }
            databend_common_sql::ColumnEntry::InternalColumn(internal) => {
                internal_columns.push(internal.internal_column.clone());
            }
            _ => {}
        }
    }

    read_column_positions.sort_unstable();
    read_column_positions.dedup();

    Ok((read_column_positions, internal_columns))
}

/// create push down filters
pub fn create_push_down_filters(
    func_ctx: &FunctionContext,
    predicates: &[ScalarExpr],
) -> Result<Filters> {
    let predicates = predicates
        .iter()
        .map(|p| {
            p.as_expr()?
                .project_column_ref(|col| Ok(col.column_name.clone()))
        })
        .collect::<Result<Vec<_>>>()?;

    let expr = predicates
        .into_iter()
        .try_reduce(|lhs, rhs| {
            check_function(None, "and_filters", &[], &[lhs, rhs], &BUILTIN_FUNCTIONS)
        })?
        .unwrap();
    let expr = cast_expr_to_non_null_boolean(expr)?;
    let (filter, _) = ConstantFolder::fold(&expr, func_ctx, &BUILTIN_FUNCTIONS);
    let remote_filter = filter.as_remote_expr();

    // prepare the inverse filter expression
    let remote_inverted_filter =
        check_function(None, "not", &[], &[filter], &BUILTIN_FUNCTIONS)?.as_remote_expr();

    Ok(Filters {
        filter: remote_filter,
        inverted_filter: remote_inverted_filter,
    })
}
