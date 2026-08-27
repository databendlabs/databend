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
use std::collections::BTreeSet;
use std::collections::HashMap;

use databend_common_catalog::plan::DataSourceInfo;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableField;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::principal::StageType;
use databend_common_meta_app::schema::CatalogType;

use crate::BindContext;
use crate::ColumnEntry;
use crate::Metadata;
use crate::MetadataRef;
use crate::ScalarExpr;
use crate::Symbol;
use crate::ViewLineageSourceColumn;
use crate::optimizer::ir::SExpr;
use crate::plans::BoundColumnRef;
use crate::plans::InsertInputSource;
use crate::plans::Plan;
use crate::plans::RelOperator;
use crate::plans::ScalarItem;
use crate::plans::SubqueryExpr;
use crate::plans::Visitor;

#[derive(Clone, Debug, Eq, PartialEq)]
struct RelationLineage {
    relations: Vec<TableLineage>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TableLineage {
    target: QueryLineageRelation,
    columns: Vec<ColumnLineage>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ColumnLineage {
    target_column: QueryLineageColumn,
    source_tables: Vec<SourceTableColumns>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SourceTableColumns {
    table: QueryLineageRelation,
    columns: Vec<QueryLineageColumn>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryLineage {
    pub kind: QueryLineageKind,
    /// Objects written or defined by the query.
    pub targets: Vec<LineageTarget>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryLineageKind {
    Ctas,
    Dml,
    CreateView,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LineageTarget {
    pub relation: QueryLineageRelation,
    /// Objects whose data flows into this target.
    pub sources: Vec<LineageSource>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LineageSource {
    pub relation: QueryLineageRelation,
    pub columns: Vec<QueryLineageColumnEdge>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct QueryLineageColumnEdge {
    /// The source column whose value contributes to the target column.
    pub source: QueryLineageColumn,
    pub target: QueryLineageColumn,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct QueryLineageRelation {
    pub catalog: String,
    pub database: String,
    pub name: String,
    /// Stable object id when it is known during planning. Newly created targets
    /// and name-resolved execution targets are finalized by the execution layer.
    pub id: Option<u64>,
    pub catalog_type: Option<CatalogType>,
    pub kind: QueryLineageRelationKind,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum QueryLineageRelationKind {
    Table,
    View,
    Stage,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct QueryLineageColumn {
    pub name: String,
    pub id: ColumnId,
}

#[derive(Clone, Debug)]
struct TargetColumnBinding {
    target_relation: QueryLineageRelation,
    target_column: QueryLineageColumn,
    value: TargetValue,
}

#[derive(Clone, Debug)]
enum TargetValue {
    QueryOutput {
        output_column_index: Symbol,
        output_column_name: String,
    },
    Expr {
        scalar: Box<ScalarExpr>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct SourceColumn {
    table: QueryLineageRelation,
    column: QueryLineageColumn,
}

#[derive(Default)]
struct LineageResolver {
    definitions: HashMap<Symbol, Vec<SourceExpr>>,
    active_columns: BTreeSet<Symbol>,
}

#[derive(Clone, Debug)]
enum SourceExpr {
    Symbol(Symbol),
    Scalar(Box<ScalarExpr>),
    Base(SourceColumn),
}

impl Plan {
    pub fn query_lineage(&self) -> Result<Option<QueryLineage>> {
        RelationExtractor::new(self).extract_query_lineage()
    }

    /// Extract CREATE VIEW lineage from a stored View query using its current object identity.
    pub fn query_lineage_for_view(
        &self,
        target_relation: QueryLineageRelation,
    ) -> Result<QueryLineage> {
        if target_relation.kind != QueryLineageRelationKind::View {
            return Err(ErrorCode::Internal(
                "View lineage target must have VIEW relation kind".to_string(),
            ));
        }

        let extractor = RelationExtractor::new(self);
        let target_columns = extractor.view_target_columns(self, &[])?;
        let target_bindings =
            extractor.query_output_columns_targets(self, target_relation, target_columns)?;
        let lineage = RelationLineage::from_query_plan(self, target_bindings)?;
        Ok(QueryLineage::from_relation_lineage(
            QueryLineageKind::CreateView,
            lineage,
        ))
    }
}

impl RelationLineage {
    fn from_query_plan(
        query_plan: &Plan,
        target_bindings: Vec<TargetColumnBinding>,
    ) -> Result<Self> {
        let (s_expr, metadata, _) = query_parts(query_plan)?;
        RelationResolver::resolve(s_expr, metadata, target_bindings)
    }

    #[cfg(test)]
    fn from_query_outputs(
        query_plan: &Plan,
        target_relation: QueryLineageRelation,
        target_columns: Vec<QueryLineageColumn>,
    ) -> Result<Self> {
        let (_, _, bind_context) = query_parts(query_plan)?;
        let mut target_bindings = Vec::with_capacity(target_columns.len());
        for (idx, target_column) in target_columns.into_iter().enumerate() {
            let output = bind_context.columns.get(idx).ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "Missing query output column for target column {}",
                    target_column.name
                ))
            })?;
            target_bindings.push(TargetColumnBinding {
                target_relation: target_relation.clone(),
                target_column,
                value: TargetValue::QueryOutput {
                    output_column_index: output.index,
                    output_column_name: output.column_name.clone(),
                },
            });
        }

        Self::from_query_plan(query_plan, target_bindings)
    }
}

impl QueryLineage {
    fn from_relation_lineage(kind: QueryLineageKind, lineage: RelationLineage) -> Self {
        let targets = lineage
            .relations
            .into_iter()
            .map(|target| {
                let mut sources: BTreeMap<QueryLineageRelation, Vec<QueryLineageColumnEdge>> =
                    BTreeMap::new();
                for column in target.columns {
                    for source_table in column.source_tables {
                        let edges = sources.entry(source_table.table).or_default();
                        edges.extend(source_table.columns.into_iter().map(|source_column| {
                            QueryLineageColumnEdge {
                                source: source_column,
                                target: column.target_column.clone(),
                            }
                        }));
                    }
                }

                LineageTarget {
                    relation: target.target,
                    sources: sources
                        .into_iter()
                        .map(|(relation, mut columns)| {
                            columns.sort();
                            columns.dedup();
                            LineageSource { relation, columns }
                        })
                        .collect(),
                }
            })
            .collect();

        QueryLineage { kind, targets }
    }
}

struct RelationExtractor<'a> {
    plan: &'a Plan,
}

impl<'a> RelationExtractor<'a> {
    fn new(plan: &'a Plan) -> Self {
        Self { plan }
    }

    fn extract_query_lineage(&self) -> Result<Option<QueryLineage>> {
        self.extract_with_kind().map(|lineage| {
            lineage.map(|(kind, relation_lineage)| {
                QueryLineage::from_relation_lineage(kind, relation_lineage)
            })
        })
    }

    fn extract_with_kind(&self) -> Result<Option<(QueryLineageKind, RelationLineage)>> {
        let (kind, target_bindings) = match self.plan {
            Plan::CreateTable(plan) => {
                let Some(select_plan) = plan.as_select.as_deref() else {
                    return Ok(None);
                };
                let target_bindings = self.query_output_targets(
                    select_plan,
                    QueryLineageRelation {
                        catalog: plan.catalog.clone(),
                        database: plan.database.clone(),
                        name: plan.table.clone(),
                        // The target does not have an object id until CREATE succeeds.
                        id: None,
                        catalog_type: Some(
                            if plan.engine == databend_common_ast::ast::Engine::Iceberg {
                                CatalogType::Iceberg
                            } else {
                                CatalogType::Default
                            },
                        ),
                        kind: QueryLineageRelationKind::Table,
                    },
                    plan.schema.fields(),
                )?;
                (QueryLineageKind::Ctas, target_bindings)
            }
            Plan::CreateView(plan) => {
                let Some(query_plan) = plan.query_plan.as_deref() else {
                    return Ok(None);
                };
                let target_bindings = self.query_output_columns_targets(
                    query_plan,
                    QueryLineageRelation {
                        catalog: plan.catalog.clone(),
                        database: plan.database.clone(),
                        name: plan.view_name.clone(),
                        // The target does not have an object id until CREATE succeeds.
                        id: None,
                        catalog_type: Some(CatalogType::Default),
                        kind: QueryLineageRelationKind::View,
                    },
                    self.view_target_columns(query_plan, &plan.column_names)?,
                )?;
                (QueryLineageKind::CreateView, target_bindings)
            }
            Plan::Insert(plan) => {
                let InsertInputSource::SelectPlan(select_plan) = &plan.source else {
                    return Ok(None);
                };
                let target_bindings = self.query_output_targets(
                    select_plan,
                    QueryLineageRelation {
                        catalog: plan.catalog.clone(),
                        database: plan.database.clone(),
                        name: plan.table.clone(),
                        id: plan.lineage_target_table_id,
                        catalog_type: Some(plan.lineage_target_catalog_type),
                        kind: QueryLineageRelationKind::Table,
                    },
                    plan.schema.fields(),
                )?;
                (QueryLineageKind::Dml, target_bindings)
            }
            Plan::Replace(plan) => {
                let InsertInputSource::SelectPlan(select_plan) = &plan.source else {
                    return Ok(None);
                };
                let target_bindings = self.query_output_targets(
                    select_plan,
                    QueryLineageRelation {
                        catalog: plan.catalog.clone(),
                        database: plan.database.clone(),
                        name: plan.table.clone(),
                        id: Some(plan.table_id),
                        catalog_type: Some(CatalogType::Default),
                        kind: QueryLineageRelationKind::Table,
                    },
                    plan.schema.fields(),
                )?;
                (QueryLineageKind::Dml, target_bindings)
            }
            Plan::CopyIntoTable(plan) => {
                // Stage attachments transport client-supplied INSERT values; the stage is not
                // a logical data source and must not produce a stage-to-table lineage edge.
                if plan.no_file_to_copy || plan.from_stage_attachment {
                    return Ok(None);
                }
                let Some(source) = stage_relation(&plan.stage_table_info.stage_info) else {
                    return Ok(None);
                };
                return Ok(Some((QueryLineageKind::Dml, RelationLineage {
                    relations: vec![TableLineage {
                        target: QueryLineageRelation {
                            catalog: plan.catalog_info.catalog_name().to_string(),
                            database: plan.database_name.clone(),
                            name: plan.table_name.clone(),
                            // COPY resolves the target table by name again during execution.
                            // Keep it name-addressed here so persistence can replace this with
                            // the execution-resolved table id instead of retaining a stale id.
                            id: None,
                            catalog_type: Some(plan.catalog_info.catalog_type()),
                            kind: QueryLineageRelationKind::Table,
                        },
                        columns: vec![ColumnLineage {
                            target_column: QueryLineageColumn {
                                name: String::new(),
                                id: 0,
                            },
                            source_tables: vec![SourceTableColumns {
                                table: source,
                                columns: vec![],
                            }],
                        }],
                    }],
                })));
            }
            Plan::CopyIntoLocation(plan) => {
                let Some(target) = stage_relation(&plan.info.stage) else {
                    return Ok(None);
                };
                let (_, _, bind_context) = query_parts(&plan.from)?;
                let target_columns = bind_context
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(idx, output)| QueryLineageColumn {
                        name: output.column_name.clone(),
                        id: idx as ColumnId,
                    })
                    .collect();
                let target_bindings =
                    self.query_output_columns_targets(&plan.from, target, target_columns)?;
                let lineage = RelationLineage::from_query_plan(&plan.from, target_bindings)?;
                return Ok(Some((QueryLineageKind::Dml, lineage)));
            }
            Plan::InsertMultiTable(plan) => {
                let (s_expr, metadata, bind_context) = query_parts(&plan.input_source)?;
                let mut bindings = Vec::new();
                for into in self.multi_insert_intos(plan) {
                    let relation = QueryLineageRelation {
                        catalog: into.catalog.clone(),
                        database: into.database.clone(),
                        name: into.table.clone(),
                        id: multi_insert_target_table_id(plan, into),
                        catalog_type: Some(CatalogType::Default),
                        kind: QueryLineageRelationKind::Table,
                    };
                    for (idx, field) in into.casted_schema.fields().iter().enumerate() {
                        let value = if let Some(source_exprs) = &into.source_scalar_exprs {
                            let scalar = source_exprs.get(idx).ok_or_else(|| {
                                ErrorCode::Internal(format!(
                                    "Missing multi insert source expr for column {}",
                                    field.name()
                                ))
                            })?;
                            TargetValue::Expr {
                                scalar: Box::new(scalar.clone()),
                            }
                        } else {
                            let output = bind_context.columns.get(idx).ok_or_else(|| {
                                ErrorCode::Internal(format!(
                                    "Missing query output column for multi insert column {}",
                                    field.name()
                                ))
                            })?;
                            TargetValue::QueryOutput {
                                output_column_index: output.index,
                                output_column_name: output.column_name.clone(),
                            }
                        };
                        bindings.push(TargetColumnBinding {
                            target_relation: relation.clone(),
                            target_column: QueryLineageColumn {
                                name: field.name().to_string(),
                                id: into
                                    .target_column_ids
                                    .get(idx)
                                    .copied()
                                    .unwrap_or(idx as ColumnId),
                            },
                            value,
                        });
                    }
                }
                let lineage = RelationResolver::resolve(s_expr, metadata, bindings)?;
                return Ok(Some((QueryLineageKind::Dml, lineage)));
            }
            Plan::DataMutation {
                s_expr, metadata, ..
            } => {
                let Some(mutation) = find_mutation(s_expr) else {
                    return Ok(None);
                };
                let target_metadata = mutation.metadata.read();
                let (target_table_id, target_catalog_type) = target_metadata
                    .tables()
                    .get(mutation.target_table_index)
                    .map(|table| {
                        let table = table.table();
                        let table_info = table.get_table_info();
                        let catalog_type = table_info.catalog_info.catalog_type();
                        let table_id = (catalog_type == CatalogType::Default)
                            .then_some(table_info.ident.table_id);
                        (table_id, catalog_type)
                    })
                    .unwrap_or((None, CatalogType::Default));
                let relation = QueryLineageRelation {
                    catalog: mutation.catalog_name.clone(),
                    database: mutation.database_name.clone(),
                    name: mutation.table_name.clone(),
                    id: target_table_id,
                    catalog_type: Some(target_catalog_type),
                    kind: QueryLineageRelationKind::Table,
                };
                let mut bindings = Vec::new();
                for matched in &mutation.matched_evaluators {
                    let Some(update) = &matched.update else {
                        continue;
                    };
                    for (field_index, scalar) in update {
                        let Some(column) = target_column_from_field_index(
                            &target_metadata,
                            mutation,
                            *field_index,
                        ) else {
                            continue;
                        };
                        bindings.push(TargetColumnBinding {
                            target_relation: relation.clone(),
                            target_column: column,
                            value: TargetValue::Expr {
                                scalar: Box::new(scalar.clone()),
                            },
                        });
                    }
                }
                for unmatched in &mutation.unmatched_evaluators {
                    for (idx, scalar) in unmatched.values.iter().enumerate() {
                        let Some(field) = unmatched.source_schema.fields().get(idx) else {
                            continue;
                        };
                        bindings.push(TargetColumnBinding {
                            target_relation: relation.clone(),
                            target_column: target_column_from_schema_field(
                                &target_metadata,
                                mutation.target_table_index,
                                field.name(),
                                idx,
                            ),
                            value: TargetValue::Expr {
                                scalar: Box::new(scalar.clone()),
                            },
                        });
                    }
                }
                drop(target_metadata);
                let lineage = RelationResolver::resolve(s_expr, metadata, bindings)?;
                return Ok(Some((QueryLineageKind::Dml, lineage)));
            }
            _ => return Ok(None),
        };

        RelationLineage::from_query_plan(query_plan(self.plan)?, target_bindings)
            .map(|lineage| Some((kind, lineage)))
    }

    fn query_output_targets(
        &self,
        select_plan: &'a Plan,
        relation: QueryLineageRelation,
        target_fields: &'a [TableField],
    ) -> Result<Vec<TargetColumnBinding>> {
        let target_columns = target_fields
            .iter()
            .map(column_info_from_table_field)
            .collect::<Vec<_>>();
        self.query_output_columns_targets(select_plan, relation, target_columns)
    }

    fn query_output_columns_targets(
        &self,
        select_plan: &'a Plan,
        relation: QueryLineageRelation,
        target_columns: Vec<QueryLineageColumn>,
    ) -> Result<Vec<TargetColumnBinding>> {
        let (_, _, bind_context) = query_parts(select_plan)?;
        let mut bindings = Vec::with_capacity(target_columns.len());
        for (idx, target_column) in target_columns.into_iter().enumerate() {
            let output = bind_context.columns.get(idx).ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "Missing query output column for target column {}",
                    target_column.name
                ))
            })?;
            bindings.push(TargetColumnBinding {
                target_relation: relation.clone(),
                target_column,
                value: TargetValue::QueryOutput {
                    output_column_index: output.index,
                    output_column_name: output.column_name.clone(),
                },
            });
        }
        Ok(bindings)
    }

    fn view_target_columns(
        &self,
        query_plan: &'a Plan,
        column_names: &[String],
    ) -> Result<Vec<QueryLineageColumn>> {
        let (_, _, bind_context) = query_parts(query_plan)?;
        Ok(bind_context
            .columns
            .iter()
            .enumerate()
            .map(|(idx, output)| QueryLineageColumn {
                name: column_names
                    .get(idx)
                    .cloned()
                    .unwrap_or_else(|| output.column_name.clone()),
                id: idx as ColumnId,
            })
            .collect())
    }

    fn multi_insert_intos(
        &self,
        plan: &'a crate::plans::InsertMultiTable,
    ) -> Vec<&'a crate::plans::Into> {
        let mut intos = Vec::new();
        for when in &plan.whens {
            intos.extend(when.intos.iter());
        }
        if let Some(else_clause) = &plan.opt_else {
            intos.extend(else_clause.intos.iter());
        }
        intos.extend(plan.intos.iter());
        intos
    }
}

fn stage_relation(stage: &StageInfo) -> Option<QueryLineageRelation> {
    if stage.is_temporary
        || stage.stage_type == StageType::User
        || stage.stage_name.trim().is_empty()
    {
        return None;
    }

    Some(QueryLineageRelation {
        catalog: String::new(),
        database: String::new(),
        name: stage.stage_name.clone(),
        // Stages are name-addressed objects and do not have table ids.
        id: None,
        catalog_type: None,
        kind: QueryLineageRelationKind::Stage,
    })
}

struct RelationResolver;

impl RelationResolver {
    fn resolve(
        s_expr: &SExpr,
        metadata: &MetadataRef,
        targets: Vec<TargetColumnBinding>,
    ) -> Result<RelationLineage> {
        let mut resolver = LineageResolver::default();
        resolver.collect_s_expr(s_expr, &metadata.read())?;

        let metadata_guard = metadata.read();
        let mut relation_columns: BTreeMap<QueryLineageRelation, Vec<ColumnLineage>> =
            BTreeMap::new();
        for target in targets {
            let sources = resolver.resolve_target(&target.value, &metadata_guard)?;
            relation_columns
                .entry(target.target_relation)
                .or_default()
                .push(ColumnLineage {
                    target_column: target.target_column,
                    source_tables: group_sources_by_table(sources),
                });
        }

        Ok(RelationLineage {
            relations: relation_columns
                .into_iter()
                .map(|(target, columns)| TableLineage { target, columns })
                .collect(),
        })
    }
}

impl LineageResolver {
    fn collect_s_expr(&mut self, s_expr: &SExpr, metadata: &Metadata) -> Result<()> {
        self.collect_base_columns(metadata, s_expr.plan())?;

        match s_expr.plan() {
            RelOperator::EvalScalar(eval_scalar) => {
                self.collect_scalar_items(&eval_scalar.items);
            }
            RelOperator::Aggregate(aggregate) => {
                self.collect_scalar_items(&aggregate.group_items);
                self.collect_scalar_items(&aggregate.aggregate_functions);
            }
            RelOperator::Window(window) => {
                self.definitions.insert(
                    window.index,
                    window
                        .arguments
                        .iter()
                        .map(|item| SourceExpr::Scalar(Box::new(item.scalar.clone())))
                        .chain(
                            window
                                .partition_by
                                .iter()
                                .map(|item| SourceExpr::Scalar(Box::new(item.scalar.clone()))),
                        )
                        .chain(window.order_by.iter().map(|item| {
                            SourceExpr::Scalar(Box::new(item.order_by_item.scalar.clone()))
                        }))
                        .collect(),
                );
            }
            RelOperator::WindowGroup(window_group) => {
                self.collect_scalar_items(&window_group.scalar_items);
                for window in &window_group.windows {
                    self.definitions.insert(
                        window.index,
                        window
                            .arguments
                            .iter()
                            .map(|item| SourceExpr::Scalar(Box::new(item.scalar.clone())))
                            .chain(
                                window
                                    .partition_by
                                    .iter()
                                    .map(|item| SourceExpr::Scalar(Box::new(item.scalar.clone()))),
                            )
                            .chain(window.order_by.iter().map(|item| {
                                SourceExpr::Scalar(Box::new(item.order_by_item.scalar.clone()))
                            }))
                            .collect(),
                    );
                }
            }
            RelOperator::ProjectSet(project_set) => {
                self.collect_scalar_items(&project_set.srfs);
            }
            RelOperator::Udf(udf) => {
                self.collect_scalar_items(&udf.items);
            }
            RelOperator::AsyncFunction(async_function) => {
                self.collect_scalar_items(&async_function.items);
            }
            RelOperator::UnionAll(union_all) => {
                for (idx, (left, right)) in union_all.output_indexes.iter().zip(
                    union_all
                        .left_outputs
                        .iter()
                        .zip(union_all.right_outputs.iter()),
                ) {
                    let mut sources = Vec::with_capacity(2);
                    sources.push(SourceExpr::Symbol(left.0));
                    if let Some(cast) = &left.1 {
                        sources.push(SourceExpr::Scalar(Box::new(cast.clone())));
                    }
                    sources.push(SourceExpr::Symbol(right.0));
                    if let Some(cast) = &right.1 {
                        sources.push(SourceExpr::Scalar(Box::new(cast.clone())));
                    }
                    self.definitions.insert(*idx, sources);
                }
            }
            RelOperator::ExpressionScan(expression_scan) => {
                for (idx, column_index) in expression_scan.column_indexes.iter().enumerate() {
                    let mut sources = Vec::new();
                    for row in &expression_scan.values {
                        if let Some(expr) = row.get(idx) {
                            sources.push(SourceExpr::Scalar(Box::new(expr.clone())));
                        }
                    }
                    self.definitions.insert(*column_index, sources);
                }
            }
            RelOperator::MaterializedCTERef(cte_ref) => {
                self.collect_s_expr(&cte_ref.def, metadata)?;
                for (consumer, producer) in &cte_ref.column_mapping {
                    self.definitions
                        .insert(*consumer, vec![SourceExpr::Symbol(*producer)]);
                }
            }
            _ => {}
        }

        for child in s_expr.children() {
            self.collect_s_expr(child, metadata)?;
        }
        Ok(())
    }

    fn collect_base_columns(&mut self, metadata: &Metadata, operator: &RelOperator) -> Result<()> {
        let RelOperator::Scan(scan) = operator else {
            return Ok(());
        };
        let materialized_cte = metadata.materialized_cte_lineage_source(scan.table_index);
        if let Some(source) = materialized_cte {
            self.collect_s_expr(&source.definition, metadata)?;
        }
        for column in &scan.columns {
            if let Some(output_column) = materialized_cte
                .and_then(|source| source.column_mapping.get(column))
                .copied()
            {
                self.definitions
                    .insert(*column, vec![SourceExpr::Symbol(output_column)]);
                continue;
            }
            let Some(source) = source_column_from_symbol(metadata, *column)? else {
                continue;
            };
            self.definitions
                .insert(*column, vec![SourceExpr::Base(source)]);
        }
        Ok(())
    }

    fn collect_scalar_items(&mut self, items: &[ScalarItem]) {
        for item in items {
            self.definitions
                .insert(item.index, vec![SourceExpr::Scalar(Box::new(
                    item.scalar.clone(),
                ))]);
        }
    }

    fn resolve_target(
        &mut self,
        target: &TargetValue,
        metadata: &Metadata,
    ) -> Result<BTreeSet<SourceColumn>> {
        match target {
            TargetValue::QueryOutput {
                output_column_index,
                output_column_name,
                ..
            } => {
                if let Some(source) =
                    source_column_from_output(metadata, *output_column_index, output_column_name)
                {
                    Ok(BTreeSet::from([source]))
                } else {
                    self.resolve_symbol(*output_column_index, metadata)
                }
            }
            TargetValue::Expr { scalar } => self.resolve_scalar(scalar, metadata),
        }
    }

    fn resolve_symbol(
        &mut self,
        symbol: Symbol,
        metadata: &Metadata,
    ) -> Result<BTreeSet<SourceColumn>> {
        if !self.active_columns.insert(symbol) {
            return Ok(BTreeSet::new());
        }

        let result = if let Some(source) = source_column_from_symbol(metadata, symbol)? {
            BTreeSet::from([source])
        } else if let Some(definitions) = self.definitions.get(&symbol).cloned() {
            let mut sources = BTreeSet::new();
            for definition in definitions {
                sources.extend(self.resolve_source_expr(&definition, metadata)?);
            }
            sources
        } else {
            BTreeSet::new()
        };

        self.active_columns.remove(&symbol);
        Ok(result)
    }

    fn resolve_source_expr(
        &mut self,
        expr: &SourceExpr,
        metadata: &Metadata,
    ) -> Result<BTreeSet<SourceColumn>> {
        match expr {
            SourceExpr::Symbol(symbol) => self.resolve_symbol(*symbol, metadata),
            SourceExpr::Scalar(scalar) => self.resolve_scalar(scalar, metadata),
            SourceExpr::Base(source) => Ok(BTreeSet::from([source.clone()])),
        }
    }

    fn resolve_scalar(
        &mut self,
        scalar: &ScalarExpr,
        metadata: &Metadata,
    ) -> Result<BTreeSet<SourceColumn>> {
        let mut visitor = SourceColumnVisitor {
            resolver: self,
            metadata,
            columns: BTreeSet::new(),
        };
        visitor.visit(scalar)?;
        Ok(visitor.columns)
    }
}

struct SourceColumnVisitor<'a, 'b> {
    resolver: &'a mut LineageResolver,
    metadata: &'b Metadata,
    columns: BTreeSet<SourceColumn>,
}

impl<'a, 'b, 'c> Visitor<'c> for SourceColumnVisitor<'a, 'b> {
    fn visit_bound_column_ref(&mut self, col: &'c BoundColumnRef) -> Result<()> {
        if let Some(source) = source_column_from_bound_column_ref(self.metadata, col) {
            self.columns.insert(source);
        } else {
            self.columns.extend(
                self.resolver
                    .resolve_symbol(col.column.index, self.metadata)?,
            );
        }
        Ok(())
    }

    fn visit_subquery(&mut self, subquery: &'c SubqueryExpr) -> Result<()> {
        if let Some(child_expr) = subquery.child_expr.as_ref() {
            self.visit(child_expr)?;
        }
        self.resolver
            .collect_s_expr(&subquery.subquery, self.metadata)?;
        self.columns.extend(
            self.resolver
                .resolve_symbol(subquery.output_column.index, self.metadata)?,
        );
        Ok(())
    }
}

fn source_column_from_bound_column_ref(
    metadata: &Metadata,
    col: &BoundColumnRef,
) -> Option<SourceColumn> {
    source_column_from_output(metadata, col.column.index, &col.column.column_name)
}

fn source_column_from_output(
    metadata: &Metadata,
    column_index: Symbol,
    column_name: &str,
) -> Option<SourceColumn> {
    metadata
        .view_lineage_source_column_by_name(column_index, column_name)
        .map(source_column_from_view_source)
}

fn source_column_from_symbol(metadata: &Metadata, symbol: Symbol) -> Result<Option<SourceColumn>> {
    if symbol.is_dummy_column() || symbol.as_usize() >= metadata.columns().len() {
        return Ok(None);
    }

    // A user-specified materialized CTE is scanned through a temporary table,
    // but its outputs retain producer definitions in metadata for lineage.
    // Prefer those definitions because the temporary table is only an
    // execution detail, not a lineage source.
    if metadata.materialized_cte_lineage_output(symbol).is_some() {
        return Ok(None);
    }

    // A view is a lineage boundary even though its query is expanded in the
    // plan. Resolve annotated output symbols to the view, not its base tables.
    if let Some(source) = metadata.view_lineage_source_column(symbol) {
        return Ok(Some(source_column_from_view_source(source)));
    }

    let column = metadata.column(symbol);
    // Stream metadata describes the change process and is not source data.
    // Regular stream data columns continue below and resolve through the
    // stream's backing table.
    if column.is_stream_column() {
        return Ok(None);
    }
    let Some(table_index) = column.table_index() else {
        return Ok(None);
    };
    if table_index >= metadata.tables().len() {
        return Ok(None);
    }
    let table = &metadata.tables()[table_index];
    if table.is_source_of_stage() {
        let stage_info = match table.table().get_data_source_info() {
            DataSourceInfo::StageSource(info) => Some(info.stage_info),
            DataSourceInfo::ParquetSource(info) => Some(info.stage_info),
            DataSourceInfo::ORCSource(info) => Some(info.stage_table_info.stage_info),
            _ => None,
        };
        let Some(relation) = stage_info.as_ref().and_then(stage_relation) else {
            return Ok(None);
        };
        return Ok(Some(SourceColumn {
            table: relation,
            column: QueryLineageColumn {
                name: column.name(),
                id: column_id(column),
            },
        }));
    }
    let Some(relation) =
        relation_info_from_table_index(metadata, table_index, QueryLineageRelationKind::Table)?
    else {
        return Ok(None);
    };
    Ok(Some(SourceColumn {
        table: relation,
        column: QueryLineageColumn {
            name: column.name(),
            id: column_id(column),
        },
    }))
}

fn source_column_from_view_source(source: &ViewLineageSourceColumn) -> SourceColumn {
    SourceColumn {
        table: QueryLineageRelation {
            catalog: source.relation.catalog.clone(),
            database: source.relation.database.clone(),
            name: source.relation.name.clone(),
            id: Some(source.relation.id),
            catalog_type: Some(CatalogType::Default),
            kind: QueryLineageRelationKind::View,
        },
        column: QueryLineageColumn {
            name: source.name.clone(),
            id: source.id,
        },
    }
}

fn column_id(column: &ColumnEntry) -> ColumnId {
    match column {
        ColumnEntry::BaseTableColumn(base) => base.column_id,
        ColumnEntry::InternalColumn(internal) => internal.internal_column.column_id(),
        ColumnEntry::VirtualColumn(virtual_column) => virtual_column.column_id,
        ColumnEntry::DerivedColumn(derived) => derived.column_index.as_usize() as ColumnId,
    }
}

fn relation_info_from_table_index(
    metadata: &Metadata,
    table_index: usize,
    kind: QueryLineageRelationKind,
) -> Result<Option<QueryLineageRelation>> {
    let Some(table) = metadata.tables().get(table_index) else {
        return Ok(None);
    };
    // Streams are transparent for lineage: attribute their data columns to the
    // backing table. Views use the per-output-symbol boundary above instead.
    if let Some(lineage_source) = table.stream_lineage_source() {
        return Ok(Some(QueryLineageRelation {
            catalog: lineage_source.catalog.clone(),
            database: lineage_source.database.clone(),
            name: lineage_source.name.clone(),
            id: Some(lineage_source.id),
            catalog_type: Some(CatalogType::Default),
            kind,
        }));
    }
    let table_object = table.table();
    let table_info = table_object.get_table_info();
    if table_object.is_temp()
        || matches!(
            table_info.engine().to_ascii_uppercase().as_str(),
            "MEMORY" | "DELTA"
        )
    {
        return Ok(None);
    }
    let catalog_type = table_info.catalog_info.catalog_type();
    // External catalog ids are runtime identities rather than stable Databend metadata ids.
    // Name-address their objects and columns across history batches.
    let table_id = match catalog_type {
        CatalogType::Default => {
            (table_info.ident.table_id != 0).then_some(table_info.ident.table_id)
        }
        CatalogType::Hive | CatalogType::Iceberg | CatalogType::Paimon => None,
    };
    Ok(Some(QueryLineageRelation {
        catalog: table.catalog().to_string(),
        database: table.database().to_string(),
        name: table.name().to_string(),
        id: table_id,
        catalog_type: Some(catalog_type),
        kind,
    }))
}

fn group_sources_by_table(sources: BTreeSet<SourceColumn>) -> Vec<SourceTableColumns> {
    let mut tables: BTreeMap<QueryLineageRelation, BTreeSet<QueryLineageColumn>> = BTreeMap::new();
    for source in sources {
        tables
            .entry(source.table)
            .or_default()
            .insert(source.column);
    }
    tables
        .into_iter()
        .map(|(table, columns)| SourceTableColumns {
            table,
            columns: columns.into_iter().collect(),
        })
        .collect()
}

fn query_parts(plan: &Plan) -> Result<(&SExpr, &MetadataRef, &BindContext)> {
    match plan {
        Plan::Query {
            s_expr,
            metadata,
            bind_context,
            ..
        } => Ok((s_expr, metadata, bind_context)),
        _ => Err(ErrorCode::Internal(
            "Lineage extraction expects a query plan".to_string(),
        )),
    }
}

fn query_plan(plan: &Plan) -> Result<&Plan> {
    match plan {
        Plan::CreateTable(plan) => plan.as_select.as_deref().ok_or_else(|| {
            ErrorCode::Internal("CTAS lineage extraction expects as_select".to_string())
        }),
        Plan::CreateView(plan) => plan.query_plan.as_deref().ok_or_else(|| {
            ErrorCode::Internal("Create view lineage extraction expects query plan".to_string())
        }),
        Plan::Insert(plan) => match &plan.source {
            InsertInputSource::SelectPlan(query) => Ok(query),
            _ => Err(ErrorCode::Internal(
                "Insert lineage extraction expects select source".to_string(),
            )),
        },
        Plan::Replace(plan) => match &plan.source {
            InsertInputSource::SelectPlan(query) => Ok(query),
            _ => Err(ErrorCode::Internal(
                "Replace lineage extraction expects select source".to_string(),
            )),
        },
        _ => Err(ErrorCode::Internal(
            "Unsupported relation lineage plan".to_string(),
        )),
    }
}

fn column_info_from_table_field(field: &TableField) -> QueryLineageColumn {
    QueryLineageColumn {
        name: field.name().to_string(),
        id: field.column_id(),
    }
}

fn multi_insert_target_table_id(
    plan: &crate::plans::InsertMultiTable,
    into: &crate::plans::Into,
) -> Option<u64> {
    plan.target_tables
        .iter()
        .find(|(_, (database, table))| database == &into.database && table == &into.table)
        .and_then(|(table_id, _)| (*table_id != 0).then_some(*table_id))
}

fn target_column_from_schema_field(
    metadata: &Metadata,
    table_index: usize,
    field_name: &str,
    ordinal: usize,
) -> QueryLineageColumn {
    metadata
        .columns_by_table_index(table_index)
        .find(|column| column.name() == field_name)
        .map(|column| QueryLineageColumn {
            name: column.name(),
            id: column_id(column),
        })
        .unwrap_or_else(|| QueryLineageColumn {
            name: field_name.to_string(),
            id: ordinal as ColumnId,
        })
}

fn find_mutation(s_expr: &SExpr) -> Option<&crate::plans::Mutation> {
    match s_expr.plan() {
        RelOperator::Mutation(mutation) => Some(mutation),
        _ => s_expr.children().find_map(find_mutation),
    }
}

fn target_column_from_field_index(
    metadata: &Metadata,
    mutation: &crate::plans::Mutation,
    field_index: FieldIndex,
) -> Option<QueryLineageColumn> {
    if let Some(column_index) = mutation.field_index_map.get(&field_index)
        && let Some(column_entry) = metadata
            .columns_by_table_index(mutation.target_table_index)
            .find(|entry| entry.index().to_string() == *column_index)
    {
        return Some(QueryLineageColumn {
            name: column_entry.name(),
            id: column_id(column_entry),
        });
    }

    metadata
        .columns_by_table_index(mutation.target_table_index)
        .nth(field_index)
        .map(|entry| QueryLineageColumn {
            name: entry.name(),
            id: column_id(entry),
        })
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::collections::BTreeMap;
    use std::collections::HashMap;
    use std::sync::Arc;

    use databend_common_ast::ast::Engine;
    use databend_common_catalog::plan::StreamColumn;
    use databend_common_catalog::plan::StreamColumnType;
    use databend_common_catalog::table::Table;
    use databend_common_expression::ORIGIN_VERSION_COL_NAME;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_meta_app::schema::CatalogInfo;
    use databend_common_meta_app::schema::CatalogOption;
    use databend_common_meta_app::schema::CreateOption;
    use databend_common_meta_app::schema::DatabaseType;
    use databend_common_meta_app::schema::HiveCatalogOption;
    use databend_common_meta_app::schema::IcebergCatalogOption;
    use databend_common_meta_app::schema::IcebergRestCatalogOption;
    use databend_common_meta_app::schema::PaimonCatalogOption;
    use databend_common_meta_app::schema::TableIdent;
    use databend_common_meta_app::schema::TableInfo;
    use databend_common_meta_app::schema::TableMeta;
    use databend_common_meta_app::tenant::Tenant;
    use parking_lot::RwLock;

    use super::*;
    use crate::ColumnBindingBuilder;
    use crate::LineageSourceRelation;
    use crate::ViewLineageSourceColumn;
    use crate::Visibility;
    use crate::plans::CopyIntoTableMode;
    use crate::plans::CopyIntoTablePlan;
    use crate::plans::CreateTablePlan;
    use crate::plans::EvalScalar;
    use crate::plans::Filter;
    use crate::plans::FunctionCall;
    use crate::plans::MaterializedCTERef;
    use crate::plans::Scan;
    use crate::plans::ValidationMode;

    #[derive(Debug)]
    struct FakeTable {
        table_info: TableInfo,
        stream_source_table_info: Option<TableInfo>,
        stream_columns: Vec<StreamColumn>,
        data_source_info: Option<DataSourceInfo>,
    }

    #[async_trait::async_trait]
    impl Table for FakeTable {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn get_table_info(&self) -> &TableInfo {
            &self.table_info
        }

        fn stream_source_table_info(&self) -> Option<&TableInfo> {
            self.stream_source_table_info.as_ref()
        }

        fn stream_columns(&self) -> Vec<StreamColumn> {
            self.stream_columns.clone()
        }

        fn get_data_source_info(&self) -> DataSourceInfo {
            self.data_source_info
                .clone()
                .unwrap_or_else(|| DataSourceInfo::TableSource(self.table_info.clone()))
        }
    }

    #[test]
    fn test_query_output_lineage_excludes_filter_columns() -> Result<()> {
        // Simulates:
        // INSERT INTO dst SELECT a + b AS x FROM src WHERE c
        // The target column `dst.x` depends on `src.a` and `src.b`; `src.c` is filter-only.
        let metadata = MetadataRef::new(RwLock::new(Metadata::default()));
        let table_index = add_fake_table(&metadata, 10, "src", &["a", "b", "c"]);
        let a = column_index(&metadata, table_index, "a");
        let b = column_index(&metadata, table_index, "b");
        let c = column_index(&metadata, table_index, "c");
        let x = metadata
            .write()
            .add_derived_column("x".to_string(), int_data_type());

        let scan = scan_expr(&metadata, table_index);
        let filter = scan.build_unary(Filter {
            predicates: vec![bound_column(c, "c", Some(table_index))],
        });
        let s_expr = filter.build_unary(EvalScalar {
            items: vec![ScalarItem {
                scalar: plus(
                    bound_column(a, "a", Some(table_index)),
                    bound_column(b, "b", Some(table_index)),
                ),
                index: x,
            }],
        });
        let query = query_plan(metadata.clone(), s_expr, vec![binding(x, "x", None)]);

        let lineage = RelationLineage::from_query_outputs(
            &query,
            relation(
                "default",
                "default",
                "dst",
                Some(20),
                QueryLineageRelationKind::Table,
            ),
            vec![QueryLineageColumn {
                name: "x".to_string(),
                id: 0,
            }],
        )?;

        assert_source_columns(&lineage, "dst", "x", &["a", "b"]);
        Ok(())
    }

    #[test]
    fn test_expr_lineage_resolves_target_binding() -> Result<()> {
        // Simulates:
        // UPDATE dst SET x = src.a + src.b
        // DML adapters pass SET/VALUES expressions as TargetValue::Expr.
        let metadata = MetadataRef::new(RwLock::new(Metadata::default()));
        let table_index = add_fake_table(&metadata, 10, "src", &["a", "b"]);
        let a = column_index(&metadata, table_index, "a");
        let b = column_index(&metadata, table_index, "b");
        let query = query_plan(metadata.clone(), scan_expr(&metadata, table_index), vec![]);

        let lineage = RelationLineage::from_query_plan(&query, vec![TargetColumnBinding {
            target_relation: relation(
                "default",
                "default",
                "dst",
                Some(20),
                QueryLineageRelationKind::Table,
            ),
            target_column: QueryLineageColumn {
                name: "x".to_string(),
                id: 0,
            },
            value: TargetValue::Expr {
                scalar: Box::new(plus(
                    bound_column(a, "a", Some(table_index)),
                    bound_column(b, "b", Some(table_index)),
                )),
            },
        }])?;

        assert_source_columns(&lineage, "dst", "x", &["a", "b"]);
        Ok(())
    }

    #[test]
    fn test_query_lineage_for_existing_view_uses_current_target() -> Result<()> {
        let metadata = MetadataRef::new(RwLock::new(Metadata::default()));
        let table_index = add_fake_table(&metadata, 10, "src", &["a"]);
        let a = column_index(&metadata, table_index, "a");
        let query = query_plan(metadata.clone(), scan_expr(&metadata, table_index), vec![
            binding(a, "renamed", Some(table_index)),
        ]);
        let target = relation(
            "default",
            "db",
            "existing_view",
            Some(42),
            QueryLineageRelationKind::View,
        );

        let lineage = query.query_lineage_for_view(target.clone())?;

        assert_eq!(lineage.kind, QueryLineageKind::CreateView);
        assert_eq!(lineage.targets.len(), 1);
        assert_eq!(lineage.targets[0].relation, target);
        assert_eq!(lineage.targets[0].sources.len(), 1);
        assert_eq!(
            lineage.targets[0].sources[0].columns[0].target.name,
            "renamed"
        );
        assert_eq!(lineage.targets[0].sources[0].columns[0].target.id, 0);
        Ok(())
    }

    #[test]
    fn test_stream_lineage_uses_base_table_and_skips_stream_columns() -> Result<()> {
        // Simulates:
        // INSERT INTO dst SELECT a + _origin_version AS x FROM stream_src
        // Stream data columns are attributed to the base table; stream metadata columns are ignored.
        let metadata = MetadataRef::new(RwLock::new(Metadata::default()));
        let table_index = add_fake_stream_table(
            &metadata,
            10,
            "stream_src",
            relation(
                "default",
                "default",
                "base_src",
                Some(30),
                QueryLineageRelationKind::Table,
            ),
        );
        let a = column_index(&metadata, table_index, "a");
        let origin_version = column_index(&metadata, table_index, ORIGIN_VERSION_COL_NAME);
        let x = metadata
            .write()
            .add_derived_column("x".to_string(), int_data_type());

        let s_expr = scan_expr(&metadata, table_index).build_unary(EvalScalar {
            items: vec![ScalarItem {
                scalar: plus(
                    bound_column(a, "a", Some(table_index)),
                    bound_column(origin_version, ORIGIN_VERSION_COL_NAME, Some(table_index)),
                ),
                index: x,
            }],
        });
        let query = query_plan(metadata.clone(), s_expr, vec![binding(x, "x", None)]);

        let lineage = RelationLineage::from_query_outputs(
            &query,
            relation(
                "default",
                "default",
                "dst",
                Some(20),
                QueryLineageRelationKind::Table,
            ),
            vec![QueryLineageColumn {
                name: "x".to_string(),
                id: 0,
            }],
        )?;

        assert_source_columns_qualified(&lineage, "dst", "x", &["base_src.a"]);
        Ok(())
    }

    #[test]
    fn test_catalog_type_lineage_addressing() -> Result<()> {
        for (catalog_type, engine, expected_id) in [
            (CatalogType::Default, "FUSE", Some(10)),
            (CatalogType::Hive, "HIVE", None),
            (CatalogType::Iceberg, "ICEBERG", None),
            (CatalogType::Paimon, "PAIMON", None),
        ] {
            let metadata = MetadataRef::new(RwLock::new(Metadata::default()));
            let table_index = add_fake_table_with_catalog_type(
                &metadata,
                10,
                "src",
                &["a"],
                engine,
                catalog_type,
            );
            let relation = relation_info_from_table_index(
                &metadata.read(),
                table_index,
                QueryLineageRelationKind::Table,
            )?
            .expect("supported engine should produce a lineage relation");
            assert_eq!(relation.id, expected_id, "catalog_type={catalog_type:?}");
            assert_eq!(relation.catalog_type, Some(catalog_type));
        }

        for engine in ["MEMORY", "DELTA"] {
            let metadata = MetadataRef::new(RwLock::new(Metadata::default()));
            let table_index = add_fake_table_with_engine(&metadata, 10, "src", &["a"], engine);
            assert_eq!(
                relation_info_from_table_index(
                    &metadata.read(),
                    table_index,
                    QueryLineageRelationKind::Table,
                )?,
                None,
                "engine={engine}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_regular_table_has_no_stream_lineage_source() {
        let metadata = MetadataRef::new(RwLock::new(Metadata::default()));
        let table_index = add_fake_table(&metadata, 10, "src", &["a"]);

        assert!(
            metadata
                .read()
                .table(table_index)
                .stream_lineage_source()
                .is_none()
        );
    }

    #[test]
    fn test_stage_scan_lineage_uses_named_stage() -> Result<()> {
        // Simulates: INSERT INTO dst SELECT a FROM @named_stage.
        // Runtime Stage tables use placeholder TableInfo, so lineage must use StageTableInfo.
        let metadata = MetadataRef::new(RwLock::new(Metadata::default()));
        let table_index = add_fake_stage_table(&metadata, StageInfo {
            stage_name: "named_stage".to_string(),
            stage_type: StageType::Internal,
            ..Default::default()
        });
        let a = column_index(&metadata, table_index, "a");
        let query = query_plan(metadata.clone(), scan_expr(&metadata, table_index), vec![
            binding(a, "a", Some(table_index)),
        ]);

        let lineage = RelationLineage::from_query_outputs(
            &query,
            relation(
                "default",
                "default",
                "dst",
                Some(20),
                QueryLineageRelationKind::Table,
            ),
            vec![QueryLineageColumn {
                name: "x".to_string(),
                id: 0,
            }],
        )?;

        let source = &lineage.relations[0].columns[0].source_tables[0].table;
        assert_eq!(source.kind, QueryLineageRelationKind::Stage);
        assert_eq!(source.name, "named_stage");
        assert_eq!(source.id, None);
        Ok(())
    }

    #[test]
    fn test_unstable_stages_are_not_lineage_sources() {
        let temporary = StageInfo {
            stage_name: "s3://bucket/path".to_string(),
            is_temporary: true,
            ..Default::default()
        };
        let user = StageInfo {
            stage_name: "user_name".to_string(),
            stage_type: StageType::User,
            ..Default::default()
        };

        assert_eq!(stage_relation(&temporary), None);
        assert_eq!(stage_relation(&user), None);
    }

    #[test]
    fn test_copy_lineage_distinguishes_stage_attachment() -> Result<()> {
        let normal_copy = copy_into_table_plan(CopyIntoTableMode::Copy, false);
        assert!(!normal_copy.schema().fields().is_empty());
        let lineage = normal_copy
            .query_lineage()?
            .expect("named-stage COPY should have lineage");
        assert_eq!(lineage.targets[0].sources[0].relation.name, "named_stage");

        let insert_from_stage =
            copy_into_table_plan(CopyIntoTableMode::Insert { overwrite: false }, false);
        assert!(insert_from_stage.schema().fields().is_empty());
        assert!(insert_from_stage.query_lineage()?.is_some());

        let attachment_copy =
            copy_into_table_plan(CopyIntoTableMode::Insert { overwrite: false }, true);
        assert!(attachment_copy.schema().fields().is_empty());
        assert_eq!(attachment_copy.query_lineage()?, None);
        Ok(())
    }

    #[test]
    fn test_view_lineage_stops_at_view_boundary() -> Result<()> {
        // Simulates:
        // CREATE VIEW v(vx) AS SELECT a + b FROM src;
        // INSERT INTO dst SELECT vx FROM v;
        // The execution plan expands the view, but lineage records `v.vx` instead of `src.a/src.b`.
        let metadata = MetadataRef::new(RwLock::new(Metadata::default()));
        let table_index = add_fake_table(&metadata, 10, "src", &["a", "b"]);
        let a = column_index(&metadata, table_index, "a");
        let b = column_index(&metadata, table_index, "b");
        let vx = metadata
            .write()
            .add_derived_column("vx".to_string(), int_data_type());
        metadata
            .write()
            .add_view_lineage_source_column(vx, ViewLineageSourceColumn {
                relation: LineageSourceRelation {
                    catalog: "default".to_string(),
                    database: "default".to_string(),
                    name: "v".to_string(),
                    id: 30,
                },
                name: "vx".to_string(),
                id: 0,
            });

        let s_expr = scan_expr(&metadata, table_index).build_unary(EvalScalar {
            items: vec![ScalarItem {
                scalar: plus(
                    bound_column(a, "a", Some(table_index)),
                    bound_column(b, "b", Some(table_index)),
                ),
                index: vx,
            }],
        });
        let query = query_plan(metadata.clone(), s_expr, vec![binding(vx, "vx", None)]);

        let lineage = RelationLineage::from_query_outputs(
            &query,
            relation(
                "default",
                "default",
                "dst",
                Some(20),
                QueryLineageRelationKind::Table,
            ),
            vec![QueryLineageColumn {
                name: "x".to_string(),
                id: 0,
            }],
        )?;

        assert_source_columns_qualified(&lineage, "dst", "x", &["v.vx"]);
        assert_eq!(
            lineage.relations[0].columns[0].source_tables[0].table.kind,
            QueryLineageRelationKind::View
        );
        Ok(())
    }

    #[test]
    fn test_ctas_query_lineage_from_plan() -> Result<()> {
        // Simulates:
        // CREATE TABLE dst AS SELECT a + b AS x FROM src WHERE c
        // CTAS gets its target columns from the create-table schema and query outputs.
        let metadata = MetadataRef::new(RwLock::new(Metadata::default()));
        let table_index = add_fake_table(&metadata, 10, "src", &["a", "b", "c"]);
        let a = column_index(&metadata, table_index, "a");
        let b = column_index(&metadata, table_index, "b");
        let c = column_index(&metadata, table_index, "c");
        let x = metadata
            .write()
            .add_derived_column("x".to_string(), int_data_type());

        let scan = scan_expr(&metadata, table_index);
        let filter = scan.build_unary(Filter {
            predicates: vec![bound_column(c, "c", Some(table_index))],
        });
        let s_expr = filter.build_unary(EvalScalar {
            items: vec![ScalarItem {
                scalar: plus(
                    bound_column(a, "a", Some(table_index)),
                    bound_column(b, "b", Some(table_index)),
                ),
                index: x,
            }],
        });
        let query = query_plan(metadata.clone(), s_expr, vec![binding(x, "x", None)]);
        let plan = Plan::CreateTable(Box::new(create_table_plan("dst", &["x"], Some(query))));

        let lineage = plan.query_lineage()?.expect("CTAS should have lineage");

        assert_eq!(lineage.kind, QueryLineageKind::Ctas);
        assert_query_source_columns(&lineage, "dst", "x", &["src.a", "src.b"]);
        Ok(())
    }

    #[test]
    fn test_materialized_cte_ref_lineage_from_plan() -> Result<()> {
        // Simulates the optimized shape for:
        // INSERT INTO dst WITH q AS MATERIALIZED (SELECT a + b AS x FROM src) SELECT x FROM q
        // The MaterializedCTERef maps each consumer output symbol to its producer symbol in `def`.
        let metadata = MetadataRef::new(RwLock::new(Metadata::default()));
        let table_index = add_fake_table(&metadata, 10, "src", &["a", "b"]);
        let a = column_index(&metadata, table_index, "a");
        let b = column_index(&metadata, table_index, "b");
        let producer_x = metadata
            .write()
            .add_derived_column("producer_x".to_string(), int_data_type());
        let consumer_x = metadata
            .write()
            .add_derived_column("consumer_x".to_string(), int_data_type());

        let def = scan_expr(&metadata, table_index).build_unary(EvalScalar {
            items: vec![ScalarItem {
                scalar: plus(
                    bound_column(a, "a", Some(table_index)),
                    bound_column(b, "b", Some(table_index)),
                ),
                index: producer_x,
            }],
        });
        let s_expr = SExpr::create_leaf(RelOperator::MaterializedCTERef(MaterializedCTERef {
            cte_name: "q".to_string(),
            output_columns: vec![consumer_x],
            def,
            column_mapping: HashMap::from([(consumer_x, producer_x)]),
            stat_info: None,
        }));
        let query = query_plan(metadata.clone(), s_expr, vec![binding(
            consumer_x, "x", None,
        )]);

        let lineage = RelationLineage::from_query_outputs(
            &query,
            relation(
                "default",
                "default",
                "dst",
                Some(20),
                QueryLineageRelationKind::Table,
            ),
            vec![QueryLineageColumn {
                name: "x".to_string(),
                id: 0,
            }],
        )?;

        assert_source_columns_qualified(&lineage, "dst", "x", &["src.a", "src.b"]);
        Ok(())
    }

    fn add_fake_table(
        metadata: &MetadataRef,
        table_id: u64,
        table_name: &str,
        columns: &[&str],
    ) -> usize {
        add_fake_table_with_engine(metadata, table_id, table_name, columns, "FUSE")
    }

    fn add_fake_table_with_engine(
        metadata: &MetadataRef,
        table_id: u64,
        table_name: &str,
        columns: &[&str],
        engine: &str,
    ) -> usize {
        add_fake_table_with_catalog_type(
            metadata,
            table_id,
            table_name,
            columns,
            engine,
            CatalogType::Default,
        )
    }

    fn add_fake_table_with_catalog_type(
        metadata: &MetadataRef,
        table_id: u64,
        table_name: &str,
        columns: &[&str],
        engine: &str,
        catalog_type: CatalogType,
    ) -> usize {
        metadata.write().add_table(
            catalog_name(catalog_type),
            "default".to_string(),
            fake_table_with_catalog_type(table_id, table_name, columns, engine, catalog_type),
            None,
            None,
            false,
            false,
            false,
            None,
        )
    }

    fn add_fake_stream_table(
        metadata: &MetadataRef,
        table_id: u64,
        table_name: &str,
        lineage_source: QueryLineageRelation,
    ) -> usize {
        let mut metadata = metadata.write();
        let table_index = metadata.add_table(
            "default".to_string(),
            "default".to_string(),
            fake_stream_table(table_id, table_name, lineage_source.clone()),
            None,
            None,
            false,
            false,
            false,
            None,
        );
        metadata.set_stream_lineage_source(table_index, LineageSourceRelation {
            catalog: lineage_source.catalog,
            database: lineage_source.database,
            name: lineage_source.name,
            id: lineage_source.id.expect("base source table id must be set"),
        });
        table_index
    }

    fn fake_table_with_catalog_type(
        table_id: u64,
        table_name: &str,
        columns: &[&str],
        engine: &str,
        catalog_type: CatalogType,
    ) -> Arc<dyn Table> {
        Arc::new(FakeTable {
            table_info: table_info_with_catalog_type(
                table_id,
                table_name,
                columns,
                engine,
                catalog_type,
            ),
            stream_source_table_info: None,
            stream_columns: vec![],
            data_source_info: None,
        })
    }

    fn add_fake_stage_table(metadata: &MetadataRef, stage_info: StageInfo) -> usize {
        let schema = Arc::new(TableSchema::new(vec![TableField::new(
            "a",
            TableDataType::Number(NumberDataType::Int32),
        )]));
        let table_info = TableInfo {
            name: "stage".to_string(),
            meta: TableMeta {
                schema: schema.clone(),
                engine: "STAGE".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        metadata.write().add_table(
            "default".to_string(),
            "system".to_string(),
            Arc::new(FakeTable {
                table_info,
                stream_source_table_info: None,
                stream_columns: vec![],
                data_source_info: Some(DataSourceInfo::StageSource(
                    databend_common_catalog::plan::StageTableInfo {
                        stage_info,
                        schema,
                        ..Default::default()
                    },
                )),
            }),
            None,
            None,
            false,
            false,
            true,
            None,
        )
    }

    fn create_table_plan(
        table_name: &str,
        columns: &[&str],
        as_select: Option<Plan>,
    ) -> CreateTablePlan {
        CreateTablePlan {
            create_option: CreateOption::Create,
            tenant: Tenant::new_literal("default"),
            catalog: "default".to_string(),
            database: "default".to_string(),
            table: table_name.to_string(),
            schema: Arc::new(TableSchema::new(
                columns
                    .iter()
                    .map(|column| {
                        TableField::new(column, TableDataType::Number(NumberDataType::Int32))
                    })
                    .collect(),
            )),
            engine: Engine::Fuse,
            engine_options: BTreeMap::new(),
            storage_params: None,
            options: BTreeMap::new(),
            table_properties: None,
            table_partition: None,
            field_comments: vec![],
            field_stats_truncate_len: vec![],
            cluster_key: None,
            as_select: as_select.map(Box::new),
            table_indexes: None,
            table_constraints: None,
            attached_columns: None,
        }
    }

    fn copy_into_table_plan(write_mode: CopyIntoTableMode, from_stage_attachment: bool) -> Plan {
        let table_schema = Arc::new(TableSchema::new(vec![TableField::new(
            "a",
            TableDataType::Number(NumberDataType::Int32),
        )]));
        let data_schema: databend_common_expression::DataSchemaRef =
            Arc::new(table_schema.as_ref().into());
        Plan::CopyIntoTable(Box::new(CopyIntoTablePlan {
            no_file_to_copy: false,
            catalog_info: Arc::new(CatalogInfo::default()),
            database_name: "default".to_string(),
            table_name: "dst".to_string(),
            from_stage_attachment,
            required_values_schema: data_schema.clone(),
            values_consts: vec![],
            required_source_schema: data_schema,
            write_mode,
            validation_mode: ValidationMode::None,
            stage_table_info: databend_common_catalog::plan::StageTableInfo {
                stage_info: StageInfo {
                    stage_name: "named_stage".to_string(),
                    stage_type: StageType::Internal,
                    ..Default::default()
                },
                schema: table_schema,
                ..Default::default()
            },
            query: None,
            is_transform: false,
            enable_distributed: false,
            files_collected: false,
            dedup_full_path: false,
            path_prefix: None,
            enable_schema_evolution: false,
        }))
    }

    fn fake_stream_table(
        table_id: u64,
        table_name: &str,
        lineage_source: QueryLineageRelation,
    ) -> Arc<dyn Table> {
        let fields = vec![TableField::new(
            "a",
            TableDataType::Number(NumberDataType::Int32),
        )];
        Arc::new(FakeTable {
            table_info: TableInfo {
                ident: TableIdent::new(table_id, 0),
                desc: format!("'default'.'{table_name}'"),
                name: table_name.to_string(),
                meta: TableMeta {
                    schema: Arc::new(TableSchema::new(fields)),
                    engine: "STREAM".to_string(),
                    ..Default::default()
                },
                catalog_info: Arc::new(CatalogInfo::default()),
                db_type: DatabaseType::NormalDB,
            },
            stream_source_table_info: Some(table_info(
                lineage_source.id.expect("base source table id must be set"),
                &lineage_source.name,
                &["a"],
                "FUSE",
            )),
            stream_columns: vec![StreamColumn::new(
                ORIGIN_VERSION_COL_NAME,
                StreamColumnType::OriginVersion,
            )],
            data_source_info: None,
        })
    }

    fn table_info(table_id: u64, table_name: &str, columns: &[&str], engine: &str) -> TableInfo {
        table_info_with_catalog_type(table_id, table_name, columns, engine, CatalogType::Default)
    }

    fn table_info_with_catalog_type(
        table_id: u64,
        table_name: &str,
        columns: &[&str],
        engine: &str,
        catalog_type: CatalogType,
    ) -> TableInfo {
        TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'default'.'{table_name}'"),
            name: table_name.to_string(),
            meta: TableMeta {
                schema: Arc::new(TableSchema::new(
                    columns
                        .iter()
                        .map(|column| {
                            TableField::new(column, TableDataType::Number(NumberDataType::Int32))
                        })
                        .collect(),
                )),
                engine: engine.to_string(),
                ..Default::default()
            },
            catalog_info: catalog_info(catalog_type),
            db_type: DatabaseType::NormalDB,
        }
    }

    fn catalog_name(catalog_type: CatalogType) -> String {
        match catalog_type {
            CatalogType::Default => "default",
            CatalogType::Hive => "hive",
            CatalogType::Iceberg => "iceberg",
            CatalogType::Paimon => "paimon",
        }
        .to_string()
    }

    fn catalog_info(catalog_type: CatalogType) -> Arc<CatalogInfo> {
        let mut info = CatalogInfo::default();
        info.name_ident.catalog_name = catalog_name(catalog_type);
        info.meta.catalog_option = match catalog_type {
            CatalogType::Default => CatalogOption::Default,
            CatalogType::Hive => CatalogOption::Hive(HiveCatalogOption {
                address: String::new(),
                storage_params: None,
            }),
            CatalogType::Iceberg => {
                CatalogOption::Iceberg(IcebergCatalogOption::Rest(IcebergRestCatalogOption {
                    uri: String::new(),
                    warehouse: String::new(),
                    props: HashMap::new(),
                }))
            }
            CatalogType::Paimon => CatalogOption::Paimon(PaimonCatalogOption {
                options: HashMap::new(),
            }),
        };
        Arc::new(info)
    }

    fn scan_expr(metadata: &MetadataRef, table_index: usize) -> SExpr {
        let columns = metadata
            .read()
            .columns_by_table_index(table_index)
            .map(|column| column.index())
            .collect();
        SExpr::create_leaf(Arc::new(RelOperator::Scan(Scan {
            table_index,
            columns,
            ..Default::default()
        })))
    }

    fn query_plan(
        metadata: MetadataRef,
        s_expr: SExpr,
        columns: Vec<crate::ColumnBinding>,
    ) -> Plan {
        let bind_context = BindContext {
            columns,
            ..Default::default()
        };
        Plan::Query {
            s_expr: Box::new(s_expr),
            metadata,
            bind_context: Box::new(bind_context),
            rewrite_kind: None,
            formatted_ast: None,
            ignore_result: false,
        }
    }

    fn column_index(metadata: &MetadataRef, table_index: usize, name: &str) -> Symbol {
        metadata
            .read()
            .columns_by_table_index(table_index)
            .find(|column| column.name() == name)
            .unwrap_or_else(|| panic!("missing column {name}"))
            .index()
    }

    fn bound_column(index: Symbol, name: &str, table_index: Option<usize>) -> ScalarExpr {
        BoundColumnRef {
            span: None,
            column: binding(index, name, table_index),
        }
        .into()
    }

    fn binding(index: Symbol, name: &str, table_index: Option<usize>) -> crate::ColumnBinding {
        ColumnBindingBuilder::new(
            name.to_string(),
            index,
            Box::new(int_data_type()),
            Visibility::Visible,
        )
        .table_index(table_index)
        .build()
    }

    fn plus(left: ScalarExpr, right: ScalarExpr) -> ScalarExpr {
        FunctionCall {
            span: None,
            func_name: "plus".to_string(),
            params: vec![],
            arguments: vec![left, right],
            return_type: Box::new(int_data_type()),
        }
        .into()
    }

    fn int_data_type() -> DataType {
        DataType::Number(NumberDataType::Int32)
    }

    fn relation(
        catalog: &str,
        database: &str,
        name: &str,
        id: Option<u64>,
        kind: QueryLineageRelationKind,
    ) -> QueryLineageRelation {
        QueryLineageRelation {
            catalog: catalog.to_string(),
            database: database.to_string(),
            name: name.to_string(),
            id,
            catalog_type: Some(CatalogType::Default),
            kind,
        }
    }

    fn assert_source_columns(
        lineage: &RelationLineage,
        target_table: &str,
        target_column: &str,
        expected_sources: &[&str],
    ) {
        let table = lineage
            .relations
            .iter()
            .find(|table| table.target.name == target_table)
            .unwrap_or_else(|| panic!("missing target table {target_table}: {lineage:?}"));
        let column = table
            .columns
            .iter()
            .find(|column| column.target_column.name == target_column)
            .unwrap_or_else(|| panic!("missing target column {target_column}: {lineage:?}"));
        let mut sources = column
            .source_tables
            .iter()
            .flat_map(|table| table.columns.iter().map(|column| column.name.as_str()))
            .collect::<Vec<_>>();
        sources.sort_unstable();
        assert_eq!(sources, expected_sources, "unexpected lineage: {lineage:?}");
    }

    fn assert_source_columns_qualified(
        lineage: &RelationLineage,
        target_table: &str,
        target_column: &str,
        expected_sources: &[&str],
    ) {
        let table = lineage
            .relations
            .iter()
            .find(|table| table.target.name == target_table)
            .unwrap_or_else(|| panic!("missing target table {target_table}: {lineage:?}"));
        let column = table
            .columns
            .iter()
            .find(|column| column.target_column.name == target_column)
            .unwrap_or_else(|| panic!("missing target column {target_column}: {lineage:?}"));
        let mut sources = column
            .source_tables
            .iter()
            .flat_map(|table| {
                table
                    .columns
                    .iter()
                    .map(|column| format!("{}.{}", table.table.name, column.name))
            })
            .collect::<Vec<_>>();
        sources.sort_unstable();
        assert_eq!(sources, expected_sources, "unexpected lineage: {lineage:?}");
    }

    fn assert_query_source_columns(
        lineage: &QueryLineage,
        target_table: &str,
        target_column: &str,
        expected_sources: &[&str],
    ) {
        let mut sources = lineage
            .targets
            .iter()
            .find(|table| table.relation.name == target_table)
            .unwrap_or_else(|| panic!("missing target table {target_table}: {lineage:?}"))
            .sources
            .iter()
            .flat_map(|table| {
                table
                    .columns
                    .iter()
                    .filter(|column| column.target.name == target_column)
                    .map(|column| format!("{}.{}", table.relation.name, column.source.name))
            })
            .collect::<Vec<_>>();
        sources.sort_unstable();
        assert_eq!(sources, expected_sources, "unexpected lineage: {lineage:?}");
    }
}
