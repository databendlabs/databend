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
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use databend_common_catalog::catalog::StorageDescription;
use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartInfo;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::PartInfoType;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::TableInfo;
use databend_common_pipeline::core::Pipeline;

pub const PROXY_OPT_KEY_TARGETS: &str = "targets";
pub const PROXY_OPT_KEY_DEFAULT: &str = "default";

pub struct ProxyTable {
    table_info: TableInfo,
    targets: Vec<String>,
    default_target: String,
}

#[derive(Clone)]
struct RoutedTarget {
    catalog: String,
    database: String,
    table: String,
}

struct Candidate {
    target: String,
    table: Arc<dyn Table>,
    statistics: PartStatistics,
    partitions: Partitions,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ProxyPartInfo {
    target: String,
    target_table_info: TableInfo,
    inner: Option<PartInfoPtr>,
}

impl ProxyTable {
    pub fn try_create(table_info: TableInfo) -> Result<Box<dyn Table>> {
        let targets = parse_targets(table_info.options().get(PROXY_OPT_KEY_TARGETS).ok_or_else(
            || {
                ErrorCode::TableOptionInvalid(
                    "PROXY table requires table option targets".to_string(),
                )
            },
        )?)?;

        let default_target = match table_info.options().get(PROXY_OPT_KEY_DEFAULT) {
            Some(default) => normalize_target(default)?,
            None => targets[0].clone(),
        };
        if !targets.iter().any(|target| target == &default_target) {
            return Err(ErrorCode::TableOptionInvalid(format!(
                "PROXY table default target '{}' must be included in targets",
                default_target
            )));
        }

        Ok(Box::new(Self {
            table_info,
            targets,
            default_target,
        }))
    }

    pub fn description() -> StorageDescription {
        StorageDescription {
            engine_name: "PROXY".to_string(),
            comment: "PROXY Storage Engine".to_string(),
            support_cluster_key: false,
        }
    }

    async fn select_target(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<Candidate> {
        if !has_filter(&push_downs) {
            return self
                .read_target_partitions(ctx, &self.default_target, push_downs)
                .await;
        }

        let mut selected: Option<Candidate> = None;
        let mut default_candidate: Option<Candidate> = None;

        for target in &self.targets {
            let candidate = self
                .read_target_partitions(ctx.clone(), target, push_downs.clone())
                .await?;

            if target == &self.default_target {
                default_candidate = Some(Candidate {
                    target: candidate.target.clone(),
                    table: candidate.table.clone(),
                    statistics: candidate.statistics.clone(),
                    partitions: candidate.partitions.clone(),
                });
            }

            if selected.as_ref().is_none_or(|selected| {
                statistics_cost(&candidate.statistics) < statistics_cost(&selected.statistics)
            }) {
                selected = Some(candidate);
            }
        }

        let selected = selected.ok_or_else(|| {
            ErrorCode::TableOptionInvalid("PROXY table requires at least one target".to_string())
        })?;

        if let Some(default_candidate) = default_candidate {
            if statistics_cost(&default_candidate.statistics)
                == statistics_cost(&selected.statistics)
            {
                return Ok(default_candidate);
            }
        }

        Ok(selected)
    }

    async fn read_target_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        target: &str,
        push_downs: Option<PushDownInfo>,
    ) -> Result<Candidate> {
        let target_ref = self.resolve_target(ctx.clone(), target)?;
        let table = ctx
            .get_table(&target_ref.catalog, &target_ref.database, &target_ref.table)
            .await?;

        if !table.engine().eq_ignore_ascii_case("FUSE") {
            return Err(ErrorCode::TableOptionInvalid(format!(
                "PROXY table target '{}' must use FUSE engine, but got {}",
                target,
                table.engine()
            )));
        }

        let settings = ctx.get_settings();
        let distributed_pruning_enabled = settings.get_enable_distributed_pruning()?;
        if distributed_pruning_enabled {
            settings.set_setting("enable_distributed_pruning".to_string(), "0".to_string())?;
        }

        // PROXY wraps target partitions to remember the selected table. FUSE lazy
        // segment partitions are consumed by its distributed pruning pipeline
        // before block reads, so route with block-pruned partitions for now.
        let read_res = table.read_partitions(ctx, push_downs, false).await;

        if distributed_pruning_enabled {
            settings.set_setting("enable_distributed_pruning".to_string(), "1".to_string())?;
        }

        let (statistics, partitions) = read_res?;

        Ok(Candidate {
            target: target.to_string(),
            table,
            statistics,
            partitions,
        })
    }

    fn resolve_target(&self, ctx: Arc<dyn TableContext>, target: &str) -> Result<RoutedTarget> {
        let parts = target
            .split('.')
            .map(|part| part.trim())
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>();

        let current_catalog = self.table_info.catalog().to_string();
        let current_database =
            database_from_desc(&self.table_info.desc).unwrap_or_else(|| ctx.get_current_database());

        let target_ref = match parts.as_slice() {
            [table] => RoutedTarget {
                catalog: current_catalog,
                database: current_database.clone(),
                table: (*table).to_string(),
            },
            [database, table] => RoutedTarget {
                catalog: current_catalog,
                database: (*database).to_string(),
                table: (*table).to_string(),
            },
            [catalog, database, table] => RoutedTarget {
                catalog: (*catalog).to_string(),
                database: (*database).to_string(),
                table: (*table).to_string(),
            },
            _ => {
                return Err(ErrorCode::TableOptionInvalid(format!(
                    "Invalid PROXY table target '{}'",
                    target
                )));
            }
        };

        if target_ref.catalog == self.table_info.catalog()
            && target_ref.database == current_database
            && target_ref.table == self.table_info.name
        {
            return Err(ErrorCode::TableOptionInvalid(format!(
                "PROXY table '{}' cannot target itself",
                self.table_info.name
            )));
        }

        Ok(target_ref)
    }

    fn wrap_partitions(
        &self,
        target: &str,
        target_table_info: TableInfo,
        partitions: Partitions,
    ) -> Partitions {
        if partitions.partitions.is_empty() {
            return Partitions::create(partitions.kind, vec![Arc::new(Box::new(ProxyPartInfo {
                target: target.to_string(),
                target_table_info,
                inner: None,
            })
                as Box<dyn PartInfo>)]);
        }

        Partitions::create(
            partitions.kind,
            partitions
                .partitions
                .into_iter()
                .map(|part| {
                    Arc::new(Box::new(ProxyPartInfo {
                        target: target.to_string(),
                        target_table_info: target_table_info.clone(),
                        inner: Some(part),
                    }) as Box<dyn PartInfo>)
                })
                .collect(),
        )
    }

    fn unwrap_partitions(
        &self,
        partitions: &Partitions,
    ) -> Result<(String, TableInfo, Partitions)> {
        let mut target = None;
        let mut target_table_info = None;
        let mut inner_parts = Vec::with_capacity(partitions.partitions.len());

        for part in &partitions.partitions {
            let proxy_part = part
                .as_any()
                .downcast_ref::<ProxyPartInfo>()
                .ok_or_else(|| {
                    ErrorCode::Internal(
                        "PROXY table expected partitions produced by PROXY read_partitions"
                            .to_string(),
                    )
                })?;

            match &target {
                Some(target) if target != &proxy_part.target => {
                    return Err(ErrorCode::Internal(
                        "PROXY table partitions contain multiple targets".to_string(),
                    ));
                }
                None => target = Some(proxy_part.target.clone()),
                _ => {}
            }
            target_table_info.get_or_insert_with(|| proxy_part.target_table_info.clone());
            if let Some(inner) = &proxy_part.inner {
                inner_parts.push(inner.clone());
            }
        }

        let target_table_info = target_table_info.ok_or_else(|| {
            ErrorCode::Internal("PROXY table expected target table info in partitions".to_string())
        })?;

        Ok((
            target.unwrap_or_else(|| self.default_target.clone()),
            target_table_info,
            Partitions::create(partitions.kind.clone(), inner_parts),
        ))
    }
}

#[async_trait::async_trait]
impl Table for ProxyTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn support_column_projection(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        let candidate = self.select_target(ctx, push_downs).await?;
        let partitions = self.wrap_partitions(
            &candidate.target,
            candidate.table.get_table_info().clone(),
            candidate.partitions,
        );

        Ok((candidate.statistics, partitions))
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        put_cache: bool,
    ) -> Result<()> {
        let (_target, target_table_info, partitions) = self.unwrap_partitions(&plan.parts)?;
        let mut target_plan = plan.clone();
        target_plan.source_info = DataSourceInfo::TableSource(target_table_info);
        target_plan.parts = partitions.clone();
        ctx.set_partitions(partitions)?;

        let table = ctx.build_table_from_source_plan(&target_plan)?;
        table.read_data(ctx, &target_plan, pipeline, put_cache)
    }
}

#[typetag::serde(name = "proxy")]
impl PartInfo for ProxyPartInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        info.as_any()
            .downcast_ref::<ProxyPartInfo>()
            .is_some_and(|other| {
                self.target == other.target
                    && self.target_table_info.ident == other.target_table_info.ident
                    && match (&self.inner, &other.inner) {
                        (Some(left), Some(right)) => left.equals(right),
                        (None, None) => true,
                        _ => false,
                    }
            })
    }

    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.target.hash(&mut hasher);
        self.target_table_info.ident.table_id.hash(&mut hasher);
        self.inner
            .as_ref()
            .map(|inner| inner.hash())
            .hash(&mut hasher);
        hasher.finish()
    }

    fn part_type(&self) -> PartInfoType {
        self.inner
            .as_ref()
            .map_or(PartInfoType::BlockLevel, |inner| inner.part_type())
    }
}

fn parse_targets(value: &str) -> Result<Vec<String>> {
    let targets = value
        .split(',')
        .map(normalize_target)
        .collect::<Result<Vec<_>>>()?;

    if targets.is_empty() {
        return Err(ErrorCode::TableOptionInvalid(
            "PROXY table option targets cannot be empty".to_string(),
        ));
    }

    Ok(targets)
}

fn normalize_target(value: &str) -> Result<String> {
    let target = value.trim();
    if target.is_empty() {
        return Err(ErrorCode::TableOptionInvalid(
            "PROXY table target cannot be empty".to_string(),
        ));
    }
    Ok(target.to_string())
}

fn has_filter(push_downs: &Option<PushDownInfo>) -> bool {
    push_downs.as_ref().is_some_and(|push_downs| {
        push_downs.filters.is_some() || push_downs.secure_filters.is_some()
    })
}

fn statistics_cost(statistics: &PartStatistics) -> (usize, usize, usize) {
    (
        statistics.partitions_scanned,
        statistics.read_rows,
        statistics.read_bytes,
    )
}

fn database_from_desc(desc: &str) -> Option<String> {
    let raw = desc.split('.').next()?;
    raw.strip_prefix('\'')
        .and_then(|value| value.strip_suffix('\''))
        .map(ToString::to_string)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use databend_common_catalog::plan::PartitionsShuffleKind;
    use databend_common_expression::TableSchemaRefExt;
    use databend_common_meta_app::schema::TableInfo;
    use databend_common_meta_app::schema::TableMeta;

    use super::*;
    use crate::RandomPartInfo;

    fn proxy_table_info(options: BTreeMap<String, String>) -> TableInfo {
        let mut table_info =
            TableInfo::simple("default", "proxy", TableSchemaRefExt::create(vec![]));
        table_info.meta = TableMeta {
            engine: "PROXY".to_string(),
            options,
            ..Default::default()
        };
        table_info
    }

    #[test]
    fn test_proxy_table_options() -> Result<()> {
        let mut options = BTreeMap::new();
        options.insert(
            PROXY_OPT_KEY_TARGETS.to_string(),
            "spans_by_trace, spans_by_chat".to_string(),
        );
        options.insert(
            PROXY_OPT_KEY_DEFAULT.to_string(),
            "spans_by_chat".to_string(),
        );

        let table = ProxyTable::try_create(proxy_table_info(options))?;
        let proxy = table.as_any().downcast_ref::<ProxyTable>().unwrap();
        assert_eq!(proxy.targets, vec!["spans_by_trace", "spans_by_chat"]);
        assert_eq!(proxy.default_target, "spans_by_chat");

        Ok(())
    }

    #[test]
    fn test_proxy_empty_partition_keeps_target_info() -> Result<()> {
        let mut options = BTreeMap::new();
        options.insert(PROXY_OPT_KEY_TARGETS.to_string(), "target".to_string());
        let table = ProxyTable::try_create(proxy_table_info(options))?;
        let proxy = table.as_any().downcast_ref::<ProxyTable>().unwrap();
        let target_info = TableInfo::simple("default", "target", TableSchemaRefExt::create(vec![]));

        let wrapped = proxy.wrap_partitions(
            "target",
            target_info.clone(),
            Partitions::create(PartitionsShuffleKind::Seq, vec![]),
        );
        let (target, unwrapped_info, unwrapped) = proxy.unwrap_partitions(&wrapped)?;

        assert_eq!(target, "target");
        assert_eq!(unwrapped_info.name, target_info.name);
        assert!(unwrapped.partitions.is_empty());

        let wrapped = proxy.wrap_partitions(
            "target",
            target_info,
            Partitions::create(PartitionsShuffleKind::Seq, vec![RandomPartInfo::create(1)]),
        );
        let (_, _, unwrapped) = proxy.unwrap_partitions(&wrapped)?;
        assert_eq!(unwrapped.partitions.len(), 1);

        Ok(())
    }
}
