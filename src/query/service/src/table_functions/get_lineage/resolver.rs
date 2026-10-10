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

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_ast::parser::parse_table_ref;
use databend_common_catalog::catalog::CATALOG_DEFAULT;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::infer_table_schema;
use databend_common_meta_api::kv_pb_api::KVPbApi;
use databend_common_meta_app::schema::TableIdToName;
use databend_common_sql::Planner;
use databend_common_sql::planner::NameResolutionContext;
use databend_common_sql::planner::normalize_identifier;
use databend_common_storages_basic::view_table::QUERY;
use databend_common_storages_basic::view_table::VIEW_ENGINE;
use databend_common_storages_stream::stream_table::STREAM_ENGINE;
use databend_common_users::GrantObjectVisibilityChecker;
use databend_common_users::Object;
use databend_common_users::UserApiProvider;
use log::warn;

use super::ObjectDomain;
use super::edge_reader::AddressKind;
use super::edge_reader::CapturedObject;
use super::edge_reader::LineageObjectType;
use crate::meta_service_error;
use crate::sessions::TableContext;

#[derive(Clone, Debug)]
pub(super) struct ResolvedObject {
    pub object_type: LineageObjectType,
    pub catalog_type: String,
    pub catalog: String,
    pub database: String,
    pub name: String,
    pub id: Option<u64>,
    pub schema: Option<TableSchemaRef>,
    pub masked_column_ids: BTreeSet<u32>,
    pub object_key: String,
    pub lookup_keys: BTreeSet<String>,
    pub expandable: bool,
}

impl ResolvedObject {
    pub(super) fn output_address(&self) -> (Option<String>, Option<String>, Option<String>) {
        match self.object_type {
            LineageObjectType::Stage => (None, None, Some(self.name.clone())),
            _ => (
                Some(self.catalog.clone()),
                Some(self.database.clone()),
                Some(self.name.clone()),
            ),
        }
    }

    pub(super) fn column_by_name(&self, name: &str) -> Option<(String, String)> {
        let field = self.schema.as_ref()?.field_with_name(name).ok()?;
        Some((field.column_id.to_string(), field.name().to_string()))
    }

    pub(super) fn column_by_id(&self, id: &str) -> Option<(String, String)> {
        let id = id.parse::<u32>().ok()?;
        let field = self
            .schema
            .as_ref()?
            .fields()
            .iter()
            .find(|field| field.column_id == id)?;
        Some((field.column_id.to_string(), field.name().to_string()))
    }

    pub(super) fn is_column_masked(&self, id: &str) -> bool {
        id.parse()
            .ok()
            .is_some_and(|id| self.masked_column_ids.contains(&id))
    }
}

pub(super) struct ObjectResolver {
    ctx: Arc<dyn TableContext>,
    visibility: Arc<GrantObjectVisibilityChecker>,
    cache: HashMap<CapturedObject, Option<ResolvedObject>>,
    stages: Option<BTreeSet<String>>,
}

impl ObjectResolver {
    pub(super) async fn try_create(ctx: Arc<dyn TableContext>) -> Result<Self> {
        let visibility = ctx.get_visibility_checker(false, Object::All).await?;
        Ok(Self {
            ctx,
            visibility,
            cache: HashMap::new(),
            stages: None,
        })
    }

    pub(super) async fn resolve_start(
        &mut self,
        domain: ObjectDomain,
        value: &str,
    ) -> Result<Option<ResolvedObject>> {
        if domain == ObjectDomain::Stage {
            return self.resolve_stage(value.trim()).await;
        }

        let expected = match domain {
            ObjectDomain::Table => LineageObjectType::Table,
            ObjectDomain::View => LineageObjectType::View,
            ObjectDomain::Column => {
                let (catalog, database, table_name) = parse_object_name(&self.ctx, value)?;
                if let Some(table) = self
                    .resolve_table_by_name(
                        &catalog,
                        &database,
                        &table_name,
                        LineageObjectType::Table,
                        None,
                    )
                    .await?
                {
                    return Ok(Some(table));
                }
                return self
                    .resolve_table_by_name(
                        &catalog,
                        &database,
                        &table_name,
                        LineageObjectType::View,
                        None,
                    )
                    .await;
            }
            ObjectDomain::Stage => unreachable!(),
        };
        let (catalog, database, table_name) = parse_object_name(&self.ctx, value)?;
        self.resolve_table_by_name(&catalog, &database, &table_name, expected, None)
            .await
    }

    pub(super) async fn resolve(
        &mut self,
        captured: &CapturedObject,
    ) -> Result<Option<ResolvedObject>> {
        // TODO: Resolve captured objects in batches in a follow-up PR. The current
        // per-object cache keeps repeated lookups cheap, but each cache miss still
        // performs its own catalog/stage lookup.
        if let Some(resolved) = self.cache.get(captured) {
            return Ok(resolved.clone());
        }

        let resolved = match captured.object_type {
            LineageObjectType::Stage => self.resolve_stage(&captured.name).await?,
            _ if !captured.is_default_catalog() => {
                // External-catalog objects are name-addressed in v1. Resolve their current display
                // name when possible, but keep them terminal because their stable-ID semantics are
                // catalog-specific.
                self.resolve_table_by_name(
                    &captured.catalog,
                    &captured.database,
                    &captured.name,
                    captured.object_type,
                    Some(captured),
                )
                .await?
                .map(|mut object| {
                    object.expandable = false;
                    object
                })
            }
            _ => match captured.address_kind {
                AddressKind::Id => self.resolve_table_by_id(captured).await?,
                AddressKind::Name => {
                    self.resolve_table_by_name(
                        &captured.catalog,
                        &captured.database,
                        &captured.name,
                        captured.object_type,
                        Some(captured),
                    )
                    .await?
                }
            },
        };
        self.cache.insert(captured.clone(), resolved.clone());
        Ok(resolved)
    }

    async fn resolve_table_by_id(
        &self,
        captured: &CapturedObject,
    ) -> Result<Option<ResolvedObject>> {
        let Some(table_id) = captured.id else {
            return Ok(None);
        };
        let meta = UserApiProvider::instance().get_meta_store_client();
        let Some(name_entry) = meta
            .get_pb(&TableIdToName { table_id })
            .await
            .map_err(meta_service_error)?
        else {
            return Ok(None);
        };
        let catalog = self.ctx.get_catalog(CATALOG_DEFAULT).await?;
        let database = match catalog.get_db_name_by_id(name_entry.data.db_id).await {
            Ok(database) => database,
            Err(error) if error.code() == ErrorCode::UNKNOWN_DATABASE_ID => return Ok(None),
            Err(error) => return Err(error),
        };
        self.resolve_table_by_name(
            CATALOG_DEFAULT,
            &database,
            &name_entry.data.table_name,
            captured.object_type,
            Some(captured),
        )
        .await
        .map(|object| object.filter(|object| object.id == Some(table_id)))
    }

    async fn resolve_table_by_name(
        &self,
        catalog_name: &str,
        database_name: &str,
        table_name: &str,
        expected: LineageObjectType,
        captured: Option<&CapturedObject>,
    ) -> Result<Option<ResolvedObject>> {
        let table = match self
            .ctx
            .get_table(catalog_name, database_name, table_name)
            .await
        {
            Ok(table) => table,
            Err(error)
                if matches!(
                    error.code(),
                    ErrorCode::UNKNOWN_TABLE | ErrorCode::UNKNOWN_DATABASE
                ) =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        if !matches_expected_type(table.as_ref(), expected) {
            return Ok(None);
        }

        let table_id = table.get_id();
        let masked_column_ids = table
            .get_table_info()
            .meta
            .column_mask_policy_columns_ids
            .keys()
            .copied()
            .collect();
        let catalog = self.ctx.get_catalog(catalog_name).await?;
        let is_default = catalog_name.eq_ignore_ascii_case(CATALOG_DEFAULT);
        if is_default {
            let database = match catalog
                .get_database(&self.ctx.get_tenant(), database_name)
                .await
            {
                Ok(database) => database,
                Err(error) if error.code() == ErrorCode::UNKNOWN_DATABASE => return Ok(None),
                Err(error) => return Err(error),
            };
            let db_id = database.get_db_info().database_id.db_id;
            if !self.visibility.check_table_visibility(
                catalog_name,
                database_name,
                table_name,
                db_id,
                table_id,
            ) {
                return Ok(None);
            }
        }

        let address_kind = captured
            .map(|captured| captured.address_kind)
            .unwrap_or(AddressKind::Id);
        let object_type = expected;
        let id = Some(table_id);
        let lookup_keys =
            object_lookup_keys(object_type, catalog_name, database_name, table_name, id);
        let object_key = object_key(
            object_type,
            address_kind,
            catalog_name,
            database_name,
            table_name,
            id,
        );
        let schema = if object_type == LineageObjectType::View {
            let Some(query) = table.options().get(QUERY) else {
                warn!(
                    "Skipping lineage view without stored query: {}.{}.{}",
                    catalog_name, database_name, table_name
                );
                return Ok(None);
            };
            let mut planner = Planner::new(self.ctx.clone());
            let (plan, _) = match planner.plan_sql(query).await {
                Ok(result) => result,
                Err(error) => {
                    warn!(
                        "Skipping lineage view whose query cannot be resolved: {}.{}.{}, error: {}",
                        catalog_name, database_name, table_name, error
                    );
                    return Ok(None);
                }
            };
            Some(infer_table_schema(&plan.schema())?)
        } else {
            Some(table.schema())
        };
        Ok(Some(ResolvedObject {
            object_type,
            catalog_type: captured
                .map(|captured| captured.catalog_type.clone())
                .unwrap_or_else(|| {
                    if is_default {
                        "DEFAULT".to_string()
                    } else {
                        "EXTERNAL".to_string()
                    }
                }),
            catalog: catalog_name.to_string(),
            database: database_name.to_string(),
            name: table_name.to_string(),
            id,
            schema,
            masked_column_ids,
            object_key,
            lookup_keys,
            expandable: is_default,
        }))
    }

    async fn resolve_stage(&mut self, name: &str) -> Result<Option<ResolvedObject>> {
        if name.is_empty() {
            return Ok(None);
        }
        if self.stages.is_none() {
            let stages = UserApiProvider::instance()
                .get_stages(&self.ctx.get_tenant())
                .await?
                .into_iter()
                .filter(|stage| self.visibility.check_stage_visibility(&stage.stage_name))
                .map(|stage| stage.stage_name)
                .collect();
            self.stages = Some(stages);
        }
        if !self
            .stages
            .as_ref()
            .is_some_and(|stages| stages.contains(name))
        {
            return Ok(None);
        }

        let key = format!("STAGE::NAME::{name}");
        Ok(Some(ResolvedObject {
            object_type: LineageObjectType::Stage,
            catalog_type: "STAGE".to_string(),
            catalog: String::new(),
            database: String::new(),
            name: name.to_string(),
            id: None,
            schema: None,
            masked_column_ids: BTreeSet::new(),
            object_key: key.clone(),
            lookup_keys: BTreeSet::from([key]),
            expandable: true,
        }))
    }
}

fn matches_expected_type(table: &dyn Table, expected: LineageObjectType) -> bool {
    match expected {
        LineageObjectType::View => {
            !table.is_temp() && table.engine().eq_ignore_ascii_case(VIEW_ENGINE)
        }
        LineageObjectType::Table => is_lineage_table_endpoint(table.engine(), table.is_temp()),
        LineageObjectType::Stage => false,
    }
}

fn is_lineage_table_endpoint(engine: &str, is_temporary: bool) -> bool {
    !is_temporary
        && ![VIEW_ENGINE, STREAM_ENGINE, "MEMORY", "DELTA"]
            .iter()
            .any(|unsupported| engine.eq_ignore_ascii_case(unsupported))
}

fn object_lookup_keys(
    object_type: LineageObjectType,
    catalog: &str,
    database: &str,
    name: &str,
    id: Option<u64>,
) -> BTreeSet<String> {
    let mut keys = BTreeSet::new();
    if let Some(id) = id {
        keys.insert(format!("{}::ID::{id}", object_type.as_str()));
    }
    keys.insert(format!(
        "{}::NAME::{catalog}.{database}.{name}",
        object_type.as_str()
    ));
    keys
}

fn object_key(
    object_type: LineageObjectType,
    address_kind: AddressKind,
    catalog: &str,
    database: &str,
    name: &str,
    id: Option<u64>,
) -> String {
    match (address_kind, id) {
        (AddressKind::Id, Some(id)) => format!("{}::ID::{id}", object_type.as_str()),
        _ => format!(
            "{}::NAME::{catalog}.{database}.{name}",
            object_type.as_str()
        ),
    }
}

pub(super) fn parse_object_name(
    ctx: &Arc<dyn TableContext>,
    value: &str,
) -> Result<(String, String, String)> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ErrorCode::BadArguments("object_name must not be empty"));
    }
    let settings = ctx.get_settings();
    let resolution = NameResolutionContext::try_from(settings.as_ref())?;
    let dialect = settings.get_sql_dialect().unwrap_or_default();
    let table_ref = parse_table_ref(value, dialect).map_err(|error| {
        ErrorCode::BadArguments(format!("invalid object_name '{value}': {}", error.1))
    })?;
    let catalog = table_ref
        .catalog
        .map(|ident| normalize_identifier(&ident, &resolution).name)
        .unwrap_or_else(|| ctx.get_current_catalog());
    let database = table_ref
        .database
        .map(|ident| normalize_identifier(&ident, &resolution).name)
        .unwrap_or_else(|| ctx.get_current_database());
    let table = normalize_identifier(&table_ref.table, &resolution).name;
    Ok((catalog, database, table))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lineage_table_endpoint_engines() {
        for engine in ["FUSE", "HIVE", "ICEBERG", "PAIMON"] {
            assert!(is_lineage_table_endpoint(engine, false), "engine={engine}");
        }
        for engine in [VIEW_ENGINE, STREAM_ENGINE, "MEMORY", "DELTA"] {
            assert!(!is_lineage_table_endpoint(engine, false), "engine={engine}");
        }
        assert!(!is_lineage_table_endpoint("FUSE", true));
    }
}
