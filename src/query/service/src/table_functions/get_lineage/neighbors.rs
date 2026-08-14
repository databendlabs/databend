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
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use databend_common_catalog::catalog::CATALOG_DEFAULT;
use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::Filters;
use databend_common_catalog::plan::PartInfo;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::PartInfoType;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::DistributionLevel;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_args::string_value;
use databend_common_catalog::table_function::TableFunction;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::StringType;
use databend_common_expression::types::VariantType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::sources::EmptySource;
use databend_common_pipeline_transforms::AsyncAccumulatingTransform;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_meta_client::types::MetaId;
use jsonb::parse_value;
use parking_lot::Mutex;

use super::ObjectDomain;
use super::edge_reader::EDGE_COLUMNS;
use super::edge_reader::LineageObjectType;
use super::edge_reader::RawLineageEdge;
use super::edge_reader::build_key_filter;
use super::edge_reader::decode_edges;
use super::resolver::ObjectResolver;
use super::resolver::ResolvedObject;
use super::traversal::normalize_column_name;
use super::traversal::process_json;
use super::traversal::resolve_column_ref;
use super::traversal::same_object;
use super::traversal::split_column_name;
use crate::sessions::TableContext;

const GET_LINEAGE_NEIGHBORS_FUNC: &str = "get_lineage_neighbors";
const GET_LINEAGE_NEIGHBORS_ENGINE: &str = "GET_LINEAGE_NEIGHBORS";
const HISTORY_DATABASE: &str = "system_history";
const LINEAGE_TABLE: &str = "lineage_history";

#[derive(Clone, Debug)]
struct GetLineageNeighborsArgs {
    object_domain: ObjectDomain,
    object_name: String,
}

impl GetLineageNeighborsArgs {
    fn parse(table_args: &TableArgs) -> Result<Self> {
        let args = table_args.expect_all_positioned(GET_LINEAGE_NEIGHBORS_FUNC, None)?;
        if args.len() != 2 {
            return Err(ErrorCode::BadArguments(format!(
                "{GET_LINEAGE_NEIGHBORS_FUNC} requires 2 positioned arguments: object_name, object_domain"
            )));
        }
        let object_domain = ObjectDomain::parse(&string_value(&args[1])?)?;
        Ok(Self {
            object_domain,
            object_name: string_value(&args[0])?,
        })
    }
}

#[derive(Clone, Debug)]
struct NeighborRow {
    direction: &'static str,
    object_domain: String,
    catalog: Option<String>,
    database: Option<String>,
    object_name: String,
    column_neighbors: Vec<u8>,
    process: String,
}

#[derive(Clone, Debug)]
struct NeighborStart {
    object: ResolvedObject,
    column_name: Option<String>,
}

pub struct GetLineageNeighborsTable {
    table_info: TableInfo,
    table_args: TableArgs,
    args: GetLineageNeighborsArgs,
    delegated_tables: Mutex<HashMap<DelegatedTableKey, Arc<dyn Table>>>,
}

impl GetLineageNeighborsTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: MetaId,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let args = GetLineageNeighborsArgs::parse(&table_args)?;
        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: table_func_name.to_string(),
            meta: TableMeta {
                schema: Self::schema(),
                engine: GET_LINEAGE_NEIGHBORS_ENGINE.to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        Ok(Arc::new(Self {
            table_info,
            table_args,
            args,
            delegated_tables: Mutex::new(HashMap::new()),
        }))
    }

    fn schema() -> Arc<TableSchema> {
        let nullable_string = || TableDataType::Nullable(Box::new(TableDataType::String));
        TableSchemaRefExt::create(vec![
            TableField::new("direction", TableDataType::String),
            TableField::new("object_domain", TableDataType::String),
            TableField::new("catalog", nullable_string()),
            TableField::new("database", nullable_string()),
            TableField::new("object_name", TableDataType::String),
            TableField::new("column_neighbors", TableDataType::Variant),
            TableField::new("process", TableDataType::String),
        ])
    }

    async fn target_table(&self, ctx: Arc<dyn TableContext>) -> Result<Arc<dyn Table>> {
        ctx.get_table(CATALOG_DEFAULT, HISTORY_DATABASE, LINEAGE_TABLE)
            .await
    }

    async fn resolve_start(
        &self,
        ctx: &Arc<dyn TableContext>,
        resolver: &mut ObjectResolver,
    ) -> Result<Option<NeighborStart>> {
        resolve_neighbor_start(ctx, &self.args, resolver).await
    }

    fn wrap_partitions(
        table_info: TableInfo,
        push_downs: PushDownInfo,
        partitions: Partitions,
    ) -> Partitions {
        let is_lazy = partitions.partitions_type() == PartInfoType::LazyLevel;
        let mut wrapped = Vec::with_capacity(partitions.partitions.len() + 1);
        wrapped.push(Arc::new(Box::new(LineageNeighborsPart {
            table_info: Some(table_info),
            push_downs: Some(push_downs),
            inner: None,
            is_lazy,
        }) as Box<dyn PartInfo>));
        wrapped.extend(partitions.partitions.into_iter().map(|inner| {
            Arc::new(Box::new(LineageNeighborsPart {
                table_info: None,
                push_downs: None,
                inner: Some(inner),
                is_lazy,
            }) as Box<dyn PartInfo>)
        }));
        Partitions::create(partitions.kind, wrapped)
    }

    fn unwrap_partitions(partitions: &Partitions) -> Result<(TableInfo, PushDownInfo, Partitions)> {
        let mut table_info = None;
        let mut push_downs = None;
        let mut inner = Vec::with_capacity(partitions.partitions.len());
        for part in &partitions.partitions {
            let part = part
                .as_any()
                .downcast_ref::<LineageNeighborsPart>()
                .ok_or_else(|| {
                    ErrorCode::Internal("invalid get_lineage_neighbors partition".to_string())
                })?;
            if let Some(info) = &part.table_info {
                table_info = Some(info.clone());
            }
            if let Some(value) = &part.push_downs {
                push_downs = Some(value.clone());
            }
            if let Some(part) = &part.inner {
                inner.push(part.clone());
            }
        }
        Ok((
            table_info.ok_or_else(|| {
                ErrorCode::Internal("missing lineage_history table info".to_string())
            })?,
            push_downs.ok_or_else(|| {
                ErrorCode::Internal("missing lineage_history push downs".to_string())
            })?,
            Partitions::create(partitions.kind.clone(), inner),
        ))
    }

    fn target_plan(&self, plan: &DataSourcePlan) -> Result<DataSourcePlan> {
        let (target_info, push_downs, parts) = Self::unwrap_partitions(&plan.parts)?;
        let target_schema = target_info.meta.schema.clone();
        let projection = Projection::from_column_names(&target_schema, EDGE_COLUMNS)?;
        Ok(DataSourcePlan {
            source_info: DataSourceInfo::TableSource(target_info),
            output_schema: Arc::new(projection.project_schema(&target_schema)),
            parts,
            push_downs: Some(push_downs),
            // The delegated source is the physical lineage_history table. Keeping the outer
            // function arguments would make build_table_from_source_plan try to instantiate
            // `lineage_history` as a table function.
            tbl_args: None,
            ..plan.clone()
        })
    }

    fn delegated_table_key(
        ctx: &Arc<dyn TableContext>,
        plan: &DataSourcePlan,
    ) -> DelegatedTableKey {
        DelegatedTableKey {
            query_id: ctx.get_id(),
            scan_id: plan.scan_id,
            table_index: plan.table_index,
        }
    }
}

#[async_trait::async_trait]
impl Table for GetLineageNeighborsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn distribution_level(&self) -> DistributionLevel {
        // Neighbor selection must see every matching history row before choosing the latest
        // pattern for each direction/object pair.
        DistributionLevel::Local
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
        dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        let target = self.target_table(ctx.clone()).await?;
        let mut resolver = ObjectResolver::try_create(ctx.clone()).await?;
        let Some(start) = self.resolve_start(&ctx, &mut resolver).await? else {
            return Ok((PartStatistics::default(), Partitions::default()));
        };
        let schema = target.schema();
        let projection = Projection::from_column_names(&schema, EDGE_COLUMNS)?;
        let lookup_keys = start.object.lookup_keys.iter().cloned().collect::<Vec<_>>();
        let source_filter = build_key_filter(&schema, "source_lineage_key", &lookup_keys)?;
        let target_filter = build_key_filter(&schema, "target_lineage_key", &lookup_keys)?;
        let filter = or_filters(source_filter, target_filter)?;
        let push_downs = PushDownInfo {
            projection: Some(projection),
            filters: Some(filter),
            ..Default::default()
        };
        let (statistics, partitions) = target
            .read_partitions(ctx, Some(push_downs.clone()), dry_run)
            .await?;
        Ok((
            statistics,
            Self::wrap_partitions(target.get_table_info().clone(), push_downs, partitions),
        ))
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        put_cache: bool,
    ) -> Result<()> {
        if plan.parts.is_empty() {
            pipeline.add_source(EmptySource::create, 1)?;
            return Ok(());
        }
        let target_plan = self.target_plan(plan)?;
        if target_plan.parts.is_empty() {
            pipeline.add_source(EmptySource::create, 1)?;
        } else {
            let target = self
                .delegated_tables
                .lock()
                .remove(&Self::delegated_table_key(&ctx, &target_plan))
                .map(Ok)
                .unwrap_or_else(|| ctx.build_table_from_source_plan(&target_plan))?;
            ctx.set_partitions(target_plan.parts.clone())?;
            target.read_data(ctx.clone(), &target_plan, pipeline, put_cache)?;
        }
        pipeline.try_resize(1)?;
        let args = self.args.clone();
        pipeline.try_add_async_accumulating_transformer(move || {
            Ok(LineageNeighborsTransform::new(
                ctx.clone(),
                args.clone(),
                target_plan.output_schema.clone(),
            ))
        })?;
        Ok(())
    }

    fn build_prune_pipeline(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        source_pipeline: &mut Pipeline,
        plan_id: u32,
    ) -> Result<Option<Pipeline>> {
        if plan.parts.is_empty() {
            return Ok(None);
        }
        let target_plan = self.target_plan(plan)?;
        let target = ctx.build_table_from_source_plan(&target_plan)?;
        let prune_pipeline =
            target.build_prune_pipeline(ctx.clone(), &target_plan, source_pipeline, plan_id)?;
        if target_plan.parts.partitions_type() == PartInfoType::LazyLevel {
            self.delegated_tables
                .lock()
                .insert(Self::delegated_table_key(&ctx, &target_plan), target);
        }
        Ok(prune_pipeline)
    }

    fn table_args(&self) -> Option<TableArgs> {
        Some(self.table_args.clone())
    }

    fn result_can_be_cached(&self) -> bool {
        false
    }
}

impl TableFunction for GetLineageNeighborsTable {
    fn function_name(&self) -> &str {
        GET_LINEAGE_NEIGHBORS_FUNC
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct LineageNeighborsPart {
    table_info: Option<TableInfo>,
    push_downs: Option<PushDownInfo>,
    inner: Option<PartInfoPtr>,
    is_lazy: bool,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct DelegatedTableKey {
    query_id: String,
    scan_id: usize,
    table_index: usize,
}

#[typetag::serde(name = "lineage_neighbors")]
impl PartInfo for LineageNeighborsPart {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        info.as_any().downcast_ref::<Self>().is_some_and(|other| {
            self.table_info.as_ref().map(|info| info.ident)
                == other.table_info.as_ref().map(|info| info.ident)
                && self.is_lazy == other.is_lazy
                && match (&self.inner, &other.inner) {
                    (Some(left), Some(right)) => left.equals(right),
                    (None, None) => true,
                    _ => false,
                }
        })
    }

    fn hash(&self) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.table_info
            .as_ref()
            .map(|info| info.ident.table_id)
            .hash(&mut hasher);
        self.is_lazy.hash(&mut hasher);
        self.inner
            .as_ref()
            .map(|part| part.hash())
            .hash(&mut hasher);
        hasher.finish()
    }

    fn part_type(&self) -> PartInfoType {
        if self.is_lazy {
            PartInfoType::LazyLevel
        } else {
            PartInfoType::BlockLevel
        }
    }

    fn is_reshuffle_header(&self) -> bool {
        self.table_info.is_some() && self.inner.is_none()
    }
}

fn or_filters(left: Filters, right: Filters) -> Result<Filters> {
    let filter = check_function(
        None,
        "or_filters",
        &[],
        &[
            left.filter.as_expr(&BUILTIN_FUNCTIONS),
            right.filter.as_expr(&BUILTIN_FUNCTIONS),
        ],
        &BUILTIN_FUNCTIONS,
    )?;
    let inverted_filter = check_function(
        None,
        "and_filters",
        &[],
        &[
            left.inverted_filter.as_expr(&BUILTIN_FUNCTIONS),
            right.inverted_filter.as_expr(&BUILTIN_FUNCTIONS),
        ],
        &BUILTIN_FUNCTIONS,
    )?;
    Ok(Filters {
        filter: filter.as_remote_expr(),
        inverted_filter: inverted_filter.as_remote_expr(),
    })
}

struct LineageNeighborsTransform {
    ctx: Arc<dyn TableContext>,
    args: GetLineageNeighborsArgs,
    input_schema: Arc<TableSchema>,
    resolver: Option<ObjectResolver>,
    start: Option<NeighborStart>,
    selected: HashMap<(&'static str, String), (RawLineageEdge, ResolvedObject)>,
}

impl LineageNeighborsTransform {
    fn new(
        ctx: Arc<dyn TableContext>,
        args: GetLineageNeighborsArgs,
        input_schema: Arc<TableSchema>,
    ) -> Self {
        Self {
            ctx,
            args,
            input_schema,
            resolver: None,
            start: None,
            selected: HashMap::new(),
        }
    }

    async fn initialize(&mut self) -> Result<bool> {
        if self.resolver.is_none() {
            let mut resolver = ObjectResolver::try_create(self.ctx.clone()).await?;
            self.start = resolve_neighbor_start(&self.ctx, &self.args, &mut resolver).await?;
            self.resolver = Some(resolver);
        }
        Ok(self.start.is_some())
    }

    async fn collect_block(&mut self, block: &DataBlock) -> Result<()> {
        if !self.initialize().await? {
            return Ok(());
        }
        let start = self.start.as_ref().unwrap();
        let lookup_keys = &start.object.lookup_keys;
        for direction in ["UPSTREAM", "DOWNSTREAM"] {
            let match_column = if direction == "UPSTREAM" {
                "target_lineage_key"
            } else {
                "source_lineage_key"
            };
            let edges = decode_edges(block, &self.input_schema, match_column, lookup_keys)?;
            let resolver = self.resolver.as_mut().unwrap();
            for edge in edges {
                let Some(source) = resolver.resolve(&edge.source).await? else {
                    continue;
                };
                let Some(target) = resolver.resolve(&edge.target).await? else {
                    continue;
                };
                if same_object(&source, &target) {
                    continue;
                }
                let neighbor = if direction == "UPSTREAM" {
                    source
                } else {
                    target
                };
                let key = (direction, canonical_object_key(&neighbor));
                match self.selected.get_mut(&key) {
                    Some((current, current_neighbor)) if edge.newer_than(current) => {
                        *current = edge;
                        *current_neighbor = neighbor;
                    }
                    None => {
                        self.selected.insert(key, (edge, neighbor));
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    fn build_block(rows: Vec<NeighborRow>) -> Result<DataBlock> {
        let mut directions = Vec::with_capacity(rows.len());
        let mut domains = Vec::with_capacity(rows.len());
        let mut catalogs = Vec::with_capacity(rows.len());
        let mut databases = Vec::with_capacity(rows.len());
        let mut tables = Vec::with_capacity(rows.len());
        let mut columns = Vec::with_capacity(rows.len());
        let mut processes = Vec::with_capacity(rows.len());
        for row in rows {
            directions.push(row.direction.to_string());
            domains.push(row.object_domain);
            catalogs.push(row.catalog);
            databases.push(row.database);
            tables.push(row.object_name);
            columns.push(row.column_neighbors);
            processes.push(row.process);
        }
        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(directions),
            StringType::from_data(domains),
            StringType::from_opt_data(catalogs),
            StringType::from_opt_data(databases),
            StringType::from_data(tables),
            VariantType::from_data(columns),
            StringType::from_data(processes),
        ]))
    }
}

#[async_trait::async_trait]
impl AsyncAccumulatingTransform for LineageNeighborsTransform {
    const NAME: &'static str = "LineageNeighborsTransform";

    async fn transform(&mut self, data: DataBlock) -> Result<Option<DataBlock>> {
        self.collect_block(&data).await?;
        Ok(None)
    }

    async fn on_finish(&mut self, output: bool) -> Result<Option<DataBlock>> {
        if !output {
            return Ok(None);
        }
        if !self.initialize().await? {
            return Ok(Some(Self::build_block(vec![])?));
        }
        let start = self.start.as_ref().unwrap();
        let mut rows = std::mem::take(&mut self.selected)
            .into_iter()
            .map(|((direction, _), (edge, neighbor))| {
                build_neighbor_row(
                    direction,
                    edge,
                    &start.object,
                    start.column_name.as_deref(),
                    neighbor,
                )
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            (
                left.direction,
                left.catalog.as_deref().unwrap_or_default(),
                left.database.as_deref().unwrap_or_default(),
                left.object_name.as_str(),
            )
                .cmp(&(
                    right.direction,
                    right.catalog.as_deref().unwrap_or_default(),
                    right.database.as_deref().unwrap_or_default(),
                    right.object_name.as_str(),
                ))
        });
        Ok(Some(Self::build_block(rows)?))
    }
}

fn canonical_object_key(object: &ResolvedObject) -> String {
    if object.catalog_type.eq_ignore_ascii_case("DEFAULT")
        && let Some(id) = object.id
    {
        return format!("{}::ID::{id}", object.object_type.as_str());
    }
    if object.object_type == LineageObjectType::Stage {
        return format!("STAGE::NAME::{}", object.name);
    }
    format!(
        "{}::NAME::{}.{}.{}",
        object.object_type.as_str(),
        object.catalog,
        object.database,
        object.name
    )
}

async fn resolve_neighbor_start(
    ctx: &Arc<dyn TableContext>,
    args: &GetLineageNeighborsArgs,
    resolver: &mut ObjectResolver,
) -> Result<Option<NeighborStart>> {
    if args.object_domain != ObjectDomain::Column {
        return Ok(resolver
            .resolve_start(args.object_domain, &args.object_name)
            .await?
            .map(|object| NeighborStart {
                object,
                column_name: None,
            }));
    }

    let (object_name, column_name) = split_column_name(&args.object_name)?;
    let Some(object) = resolver
        .resolve_start(ObjectDomain::Column, &object_name)
        .await?
    else {
        return Ok(None);
    };
    let column_name = normalize_column_name(ctx, &column_name)?;
    let Some((_, column_name)) = object.column_by_name(&column_name) else {
        return Ok(None);
    };
    Ok(Some(NeighborStart {
        object,
        column_name: Some(column_name),
    }))
}

fn build_neighbor_row(
    direction: &'static str,
    edge: RawLineageEdge,
    current: &ResolvedObject,
    current_column: Option<&str>,
    neighbor: ResolvedObject,
) -> Result<Option<NeighborRow>> {
    let column_neighbors =
        build_column_neighbors(direction, &edge, current, &neighbor, current_column);
    // COLUMN mode filters the map only after the newest object-neighbor pattern has been chosen.
    // An older pattern must not revive a column mapping removed by the latest event.
    if current_column.is_some() && column_neighbors.is_empty() {
        return Ok(None);
    }
    let column_neighbors = parse_value(&serde_json::to_vec(&column_neighbors)?)
        .map(|value| value.to_vec())
        .map_err(|error| {
            ErrorCode::Internal(format!(
                "failed to encode lineage column neighbors: {error}"
            ))
        })?;
    let (catalog, database) = match neighbor.object_type {
        LineageObjectType::Stage => (None, None),
        _ => (
            Some(neighbor.catalog.clone()),
            Some(neighbor.database.clone()),
        ),
    };
    Ok(Some(NeighborRow {
        direction,
        object_domain: neighbor.object_type.as_str().to_string(),
        catalog,
        database,
        object_name: neighbor.name.clone(),
        column_neighbors,
        process: process_json(&edge),
    }))
}

fn build_column_neighbors(
    direction: &str,
    edge: &RawLineageEdge,
    current: &ResolvedObject,
    neighbor: &ResolvedObject,
    current_column: Option<&str>,
) -> BTreeMap<String, Vec<String>> {
    let (current_captured, current_kind, neighbor_captured, neighbor_kind, mappings) =
        if direction == "UPSTREAM" {
            (
                &edge.target,
                edge.target_column_address_kind,
                &edge.source,
                edge.source_column_address_kind,
                &edge.target_to_source_columns,
            )
        } else {
            (
                &edge.source,
                edge.source_column_address_kind,
                &edge.target,
                edge.target_column_address_kind,
                &edge.source_to_target_columns,
            )
        };
    let (Some(current_kind), Some(neighbor_kind)) = (current_kind, neighbor_kind) else {
        return BTreeMap::new();
    };
    let mut result = BTreeMap::<String, BTreeSet<String>>::new();
    for (current_ref, neighbor_refs) in mappings {
        let Some((_, current_name)) =
            resolve_column_ref(current_captured, current, current_kind, current_ref)
        else {
            continue;
        };
        if current_column.is_some_and(|column| column != current_name) {
            continue;
        }
        for neighbor_ref in neighbor_refs {
            let Some((_, neighbor_name)) =
                resolve_column_ref(neighbor_captured, neighbor, neighbor_kind, neighbor_ref)
            else {
                continue;
            };
            result
                .entry(current_name.clone())
                .or_default()
                .insert(neighbor_name);
        }
    }
    result
        .into_iter()
        .map(|(column, neighbors)| (column, neighbors.into_iter().collect()))
        .collect()
}
