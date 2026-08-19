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
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_expression::Constant;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::filter_helper::FilterHelpers;
use databend_common_expression::type_check::check_number;
use databend_common_expression::type_check::check_string;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::UInt64Type;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::CatalogNameIdent;
use databend_common_meta_app::schema::ListedMaterializedView;
use databend_common_meta_app::schema::MaterializedViewListFilter;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::tenant::Tenant;
use databend_common_sql::format_materialized_view_create_sql;
use databend_common_storages_fuse::TableContext;
use databend_common_users::TablePrivilegeTarget;
use databend_common_users::filter_tables_by_privilege;
use databend_storages_common_table_meta::table::OPT_KEY_MATERIALIZED_VIEW_INVALID_REASON;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;
use crate::util::generate_default_catalog_meta;

pub struct MaterializedViewsTable {
    table_info: TableInfo,
}

struct SourcePermissionTarget {
    database_name: String,
    database_id: u64,
    table_name: Option<String>,
    table_id: u64,
}

#[async_trait::async_trait]
impl AsyncSystemTable for MaterializedViewsTable {
    const NAME: &'static str = "system.materialized_views";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        let catalog_name = self.table_info.catalog();
        let (catalog_filters, filter) = extract_filters(&ctx, &push_downs)?;
        if !catalog_filters.is_empty()
            && !catalog_filters
                .iter()
                .any(|candidate| candidate == catalog_name)
        {
            return Ok(build_block(vec![], catalog_name));
        }

        let tenant = ctx.get_tenant();
        let catalog = ctx.get_catalog(catalog_name).await?;
        let listed = catalog.list_materialized_views(&tenant, &filter).await?;
        if listed.is_empty() {
            return Ok(build_block(vec![], catalog_name));
        }

        let mut unique_sources = BTreeMap::new();
        for item in &listed {
            unique_sources
                .entry(item.source.table_id)
                .or_insert_with(|| SourcePermissionTarget {
                    database_name: item.database_name.clone(),
                    database_id: item.database_id,
                    table_name: item.source.table_name.clone(),
                    table_id: item.source.table_id,
                });
        }
        let unique_sources = unique_sources.into_values().collect::<Vec<_>>();
        let permission_targets = unique_sources
            .iter()
            .map(|source| TablePrivilegeTarget {
                database_name: &source.database_name,
                database_id: source.database_id,
                table_name: source.table_name.as_deref(),
                table_id: source.table_id,
            })
            .collect::<Vec<_>>();
        let user = ctx.get_current_user()?;
        let effective_roles = ctx.get_all_effective_roles().await?;
        let visible_indexes = filter_tables_by_privilege(
            &user,
            &effective_roles,
            &tenant,
            catalog_name,
            &permission_targets,
            UserPrivilegeType::Select,
        )
        .await?;
        let visible_source_ids = visible_indexes
            .into_iter()
            .map(|index| unique_sources[index].table_id)
            .collect::<HashSet<_>>();
        let visible = listed
            .into_iter()
            .filter(|item| visible_source_ids.contains(&item.source.table_id))
            .collect::<Vec<_>>();

        Ok(build_block(visible, catalog_name))
    }
}

impl MaterializedViewsTable {
    pub fn create(table_id: u64, catalog_name: &str) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("created_on", TableDataType::Timestamp),
            TableField::new("catalog", TableDataType::String),
            TableField::new("database", TableDataType::String),
            TableField::new("name", TableDataType::String),
            TableField::new(
                "materialized_view_id",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new("source_catalog", TableDataType::String),
            TableField::new("source_database", TableDataType::String),
            TableField::new(
                "source_table",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "source_table_id",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new("cluster_by", TableDataType::String),
            TableField::new("num_rows", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("data_size", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new(
                "data_compressed_size",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new("invalid", TableDataType::Boolean),
            TableField::new(
                "invalid_reason",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "bound_source_generation",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "current_source_generation",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new("comment", TableDataType::String),
            TableField::new("text", TableDataType::String),
        ]);
        let table_info = TableInfo {
            desc: "'system'.'materialized_views'".to_string(),
            name: "materialized_views".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemMaterializedViews".to_string(),
                ..Default::default()
            },
            catalog_info: Arc::new(CatalogInfo {
                name_ident: CatalogNameIdent::new(Tenant::new_literal("dummy"), catalog_name)
                    .into(),
                meta: generate_default_catalog_meta(),
                ..Default::default()
            }),
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(Self { table_info })
    }
}

fn extract_filters(
    ctx: &Arc<dyn TableContext>,
    push_downs: &Option<PushDownInfo>,
) -> Result<(Vec<String>, MaterializedViewListFilter)> {
    let Some(filter) = push_downs
        .as_ref()
        .and_then(|push_down| push_down.filters.as_ref())
    else {
        return Ok((vec![], MaterializedViewListFilter::default()));
    };

    let func_ctx = ctx.get_function_context()?;
    let expr = filter.filter.as_expr(&BUILTIN_FUNCTIONS);
    let values = FilterHelpers::find_leveled_eq_filters(
        &expr,
        &[
            "catalog",
            "database",
            "name",
            "materialized_view_id",
            "source_table_id",
        ],
        &func_ctx,
        &BUILTIN_FUNCTIONS,
    )?;
    let mut string_values = [Vec::new(), Vec::new(), Vec::new()];
    let mut id_values = [BTreeSet::new(), BTreeSet::new()];
    for (index, scalars) in values.into_iter().enumerate() {
        for scalar in scalars {
            let constant = Expr::Constant(Constant {
                span: None,
                data_type: scalar.as_ref().infer_data_type(),
                scalar,
            });
            if index < string_values.len() {
                if let Ok(value) =
                    check_string::<usize>(None, &func_ctx, &constant, &BUILTIN_FUNCTIONS)
                {
                    string_values[index].push(value);
                }
            } else if let Ok(value) =
                check_number::<u64, usize>(None, &func_ctx, &constant, &BUILTIN_FUNCTIONS)
            {
                id_values[index - string_values.len()].insert(value);
            }
        }
    }

    let [catalogs, databases, names] = string_values;
    let [materialized_view_ids, source_table_ids] = id_values;
    Ok((catalogs, MaterializedViewListFilter {
        materialized_view_ids: (!materialized_view_ids.is_empty()).then_some(materialized_view_ids),
        database_names: (!databases.is_empty()).then(|| databases.into_iter().collect()),
        names: (!names.is_empty()).then(|| names.into_iter().collect()),
        source_table_ids: (!source_table_ids.is_empty()).then_some(source_table_ids),
    }))
}

fn listed_materialized_view_invalid_reason(table_meta: &TableMeta) -> Option<&str> {
    table_meta
        .options
        .get(OPT_KEY_MATERIALIZED_VIEW_INVALID_REASON)
        .map(String::as_str)
}

fn is_listed_materialized_view_invalid(item: &ListedMaterializedView) -> bool {
    let binding_invalid = item.source.bound_source_generation.is_none()
        || item.source.current_source_generation.is_none()
        || item.source.bound_source_generation != item.source.current_source_generation;
    binding_invalid || listed_materialized_view_invalid_reason(&item.mv.table_meta.data).is_some()
}

fn build_block(listed: Vec<ListedMaterializedView>, catalog_name: &str) -> DataBlock {
    let capacity = listed.len();
    let mut created_on = Vec::with_capacity(capacity);
    let mut catalogs = Vec::with_capacity(capacity);
    let mut databases = Vec::with_capacity(capacity);
    let mut names = Vec::with_capacity(capacity);
    let mut mv_ids = Vec::with_capacity(capacity);
    let mut source_catalogs = Vec::with_capacity(capacity);
    let mut source_databases = Vec::with_capacity(capacity);
    let mut source_tables = Vec::with_capacity(capacity);
    let mut source_table_ids = Vec::with_capacity(capacity);
    let mut cluster_by = Vec::with_capacity(capacity);
    let mut num_rows = Vec::with_capacity(capacity);
    let mut data_size = Vec::with_capacity(capacity);
    let mut data_compressed_size = Vec::with_capacity(capacity);
    let mut invalid = Vec::with_capacity(capacity);
    let mut invalid_reasons = Vec::with_capacity(capacity);
    let mut bound_source_generation = Vec::with_capacity(capacity);
    let mut current_source_generation = Vec::with_capacity(capacity);
    let mut comments = Vec::with_capacity(capacity);
    let mut texts = Vec::with_capacity(capacity);

    for item in listed {
        let table_meta = &item.mv.table_meta.data;
        let invalid_reason = listed_materialized_view_invalid_reason(table_meta);
        let invalid_flag = is_listed_materialized_view_invalid(&item);

        created_on.push(table_meta.created_on.timestamp_micros());
        catalogs.push(catalog_name.to_string());
        databases.push(item.database_name.clone());
        names.push(item.name.clone());
        mv_ids.push(item.mv.mv_id);
        source_catalogs.push(catalog_name.to_string());
        source_databases.push(item.database_name.clone());
        source_tables.push(item.source.table_name);
        source_table_ids.push(item.source.table_id);
        cluster_by.push(table_meta.cluster_key_str().unwrap_or_default().to_string());
        num_rows.push(table_meta.statistics.number_of_rows);
        data_size.push(table_meta.statistics.data_bytes);
        data_compressed_size.push(table_meta.statistics.compressed_data_bytes);
        invalid.push(invalid_flag);
        invalid_reasons.push(invalid_reason.map(str::to_string));
        bound_source_generation.push(item.source.bound_source_generation);
        current_source_generation.push(item.source.current_source_generation);
        comments.push(table_meta.comment.clone());
        texts.push(format_materialized_view_create_sql(
            &item.database_name,
            &item.name,
            &item.mv.definition.data,
            table_meta,
        ));
    }

    DataBlock::new_from_columns(vec![
        TimestampType::from_data(created_on),
        StringType::from_data(catalogs),
        StringType::from_data(databases),
        StringType::from_data(names),
        UInt64Type::from_data(mv_ids),
        StringType::from_data(source_catalogs),
        StringType::from_data(source_databases),
        StringType::from_opt_data(source_tables),
        UInt64Type::from_data(source_table_ids),
        StringType::from_data(cluster_by),
        UInt64Type::from_data(num_rows),
        UInt64Type::from_data(data_size),
        UInt64Type::from_data(data_compressed_size),
        BooleanType::from_data(invalid),
        StringType::from_opt_data(invalid_reasons),
        UInt64Type::from_opt_data(bound_source_generation),
        UInt64Type::from_opt_data(current_source_generation),
        StringType::from_data(comments),
        StringType::from_data(texts),
    ])
}

#[cfg(test)]
mod tests {
    use databend_common_expression::TableSchema;
    use databend_common_meta_app::schema::ListedMVSource;
    use databend_common_meta_app::schema::ListedMaterializedView;
    use databend_common_meta_app::schema::MVDefinition;
    use databend_common_meta_app::schema::MVInfo;
    use databend_meta_client::types::SeqV;

    use super::*;

    fn listed_mv(table_meta: TableMeta, source: ListedMVSource) -> ListedMaterializedView {
        ListedMaterializedView {
            mv: MVInfo {
                mv_id: 1,
                definition: SeqV::new(1, MVDefinition {
                    original_query: "SELECT id FROM t".to_string(),
                    query: "SELECT id FROM t".to_string(),
                    logical_schema: TableSchema::empty(),
                    sync_creation: false,
                }),
                table_meta: SeqV::new(1, table_meta),
            },
            database_id: 1,
            database_name: "db".to_string(),
            name: "mv".to_string(),
            source,
        }
    }

    fn valid_source() -> ListedMVSource {
        ListedMVSource {
            table_id: 2,
            table_name: Some("t".to_string()),
            bound_source_generation: Some(0),
            current_source_generation: Some(0),
        }
    }

    #[test]
    fn test_listed_materialized_view_invalid_reason() {
        let mut table_meta = TableMeta::default();
        assert_eq!(listed_materialized_view_invalid_reason(&table_meta), None);

        table_meta.options.insert(
            OPT_KEY_MATERIALIZED_VIEW_INVALID_REASON.to_string(),
            "invalid materialized view logical query".to_string(),
        );
        assert_eq!(
            listed_materialized_view_invalid_reason(&table_meta),
            Some("invalid materialized view logical query")
        );
    }

    #[test]
    fn test_is_listed_materialized_view_invalid() {
        let valid = listed_mv(TableMeta::default(), valid_source());
        assert!(!is_listed_materialized_view_invalid(&valid));

        let mut table_meta = TableMeta::default();
        table_meta.options.insert(
            OPT_KEY_MATERIALIZED_VIEW_INVALID_REASON.to_string(),
            "invalid materialized view logical query".to_string(),
        );
        assert!(is_listed_materialized_view_invalid(&listed_mv(
            table_meta,
            valid_source()
        )));

        let mut generation_mismatch = valid_source();
        generation_mismatch.current_source_generation = Some(1);
        let binding_invalid = listed_mv(TableMeta::default(), generation_mismatch);
        assert!(is_listed_materialized_view_invalid(&binding_invalid));
        assert_eq!(
            listed_materialized_view_invalid_reason(&binding_invalid.mv.table_meta.data),
            None
        );

        let mut missing_source_name = valid_source();
        missing_source_name.table_name = None;
        assert!(!is_listed_materialized_view_invalid(&listed_mv(
            TableMeta::default(),
            missing_source_name
        )));

        let mut missing_generation = valid_source();
        missing_generation.current_source_generation = None;
        assert!(is_listed_materialized_view_invalid(&listed_mv(
            TableMeta::default(),
            missing_generation
        )));
    }
}
