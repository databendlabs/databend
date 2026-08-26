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
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartInfo;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::DistributionLevel;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;
use databend_common_pipeline::sources::EmptySource;
use paimon::CatalogFactory;
use paimon::catalog::Identifier;
use serde::Deserialize;
use serde::Serialize;

use super::PaimonSystemTableKind;
use super::read_system_table;
use crate::PAIMON_ENGINE;
use crate::ParsedName;
use crate::error::map_paimon_result;
use crate::parse_system_name;
use crate::table::PaimonTableDescriptor;
use crate::table::catalog_options_from_info;
use crate::table::descriptor::PAIMON_TABLE_DESCRIPTOR_KEY;
use crate::table::options_from_map;
use crate::table::parse_descriptor;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PaimonSystemPartInfo {
    pub kind: PaimonSystemTableKind,
}

#[typetag::serde(name = "paimon_system")]
impl PartInfo for PaimonSystemPartInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, other: &Box<dyn PartInfo>) -> bool {
        other.as_any().downcast_ref::<Self>() == Some(self)
    }

    fn hash(&self) -> u64 {
        self.kind as u64
    }
}

pub struct PaimonSystemTable {
    info: TableInfo,
    kind: PaimonSystemTableKind,
    descriptor: PaimonTableDescriptor,
    catalog_identifier: Identifier,
    catalog_options: HashMap<String, String>,
}

impl PaimonSystemTable {
    pub fn create(
        catalog_info: Arc<CatalogInfo>,
        name: String,
        kind: PaimonSystemTableKind,
        table: &paimon::Table,
    ) -> Result<Arc<dyn Table>> {
        // Persist the base table descriptor so the table can be rebuilt from its
        // `TableInfo` alone (e.g. by `get_table_by_info` during planning); catalog
        // credentials come from `catalog_info` at read time, not from here.
        let descriptor = PaimonTableDescriptor {
            identifier: table.identifier().clone(),
            location: table.location().to_string(),
            schema: table.schema().clone(),
        };
        let catalog_identifier = catalog_identifier(&name, &descriptor)?;
        let descriptor_json = serde_json::to_string(&descriptor).map_err(|err| {
            ErrorCode::Internal(format!("serialize paimon table descriptor failed: {err:?}"))
        })?;
        let mut engine_options = BTreeMap::new();
        engine_options.insert(PAIMON_TABLE_DESCRIPTOR_KEY.to_string(), descriptor_json);

        let info = TableInfo {
            ident: TableIdent::new(0, 0),
            desc: format!(
                "'{}'.'{}'.'{}'",
                catalog_info.name_ident.catalog_name,
                descriptor.identifier.database(),
                name
            ),
            name,
            catalog_info,
            meta: TableMeta {
                schema: kind.schema(),
                engine: PAIMON_ENGINE.to_string(),
                engine_options,
                ..Default::default()
            },
            ..Default::default()
        };
        let catalog_options = catalog_options_from_info(&info)?;
        Ok(Arc::new(Self {
            info,
            kind,
            descriptor,
            catalog_identifier,
            catalog_options,
        }))
    }

    /// Rebuilds a system table from a `TableInfo`, used by `get_table_by_info`.
    pub fn try_create(info: TableInfo) -> Result<Arc<dyn Table>> {
        let kind = match parse_system_name(&info.name) {
            ParsedName::System { kind, .. } => kind,
            _ => {
                return Err(ErrorCode::Internal(format!(
                    "not a paimon system table: {}",
                    info.name
                )));
            }
        };
        let descriptor = parse_descriptor(&info)?;
        let catalog_identifier = catalog_identifier(&info.name, &descriptor)?;
        let catalog_options = catalog_options_from_info(&info)?;
        Ok(Arc::new(Self {
            info,
            kind,
            descriptor,
            catalog_identifier,
            catalog_options,
        }))
    }
}

#[async_trait::async_trait]
impl Table for PaimonSystemTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.info
    }

    fn distribution_level(&self) -> DistributionLevel {
        // Match Databend's own system tables: reading through the cluster path is
        // what applies column projection on top of the full-schema block (and lets
        // the table be joined/unioned). The plan is rebuilt via `get_table_by_info`.
        DistributionLevel::Cluster
    }

    fn is_read_only(&self) -> bool {
        true
    }

    fn support_column_projection(&self) -> bool {
        // The collector builds the full-schema block and the source projects it to
        // the plan's column set, so the emitted block matches the planned output
        // schema (otherwise ORDER BY / projection would read misaligned columns).
        true
    }

    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        Ok((
            PartStatistics::new_exact(1, 0, 1, 1),
            Partitions::create(PartitionsShuffleKind::Seq, vec![Arc::new(Box::new(
                PaimonSystemPartInfo { kind: self.kind },
            ))]),
        ))
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        if plan.parts.partitions.is_empty() {
            pipeline.add_source(EmptySource::create, 1)?;
            return Ok(());
        }
        let projection = plan
            .push_downs
            .as_ref()
            .and_then(|push_downs| push_downs.projection.clone());
        pipeline.add_source(
            |output| {
                PaimonSystemSource::create(
                    ctx.clone(),
                    output,
                    self.kind,
                    self.descriptor.clone(),
                    self.catalog_identifier.clone(),
                    self.catalog_options.clone(),
                    projection.clone(),
                )
            },
            1,
        )?;
        Ok(())
    }
}

struct PaimonSystemSource {
    finished: bool,
    kind: PaimonSystemTableKind,
    descriptor: PaimonTableDescriptor,
    catalog_identifier: Identifier,
    catalog_options: HashMap<String, String>,
    projection: Option<Projection>,
}

impl PaimonSystemSource {
    fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        kind: PaimonSystemTableKind,
        descriptor: PaimonTableDescriptor,
        catalog_identifier: Identifier,
        catalog_options: HashMap<String, String>,
        projection: Option<Projection>,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.get_scan_progress(), output, Self {
            finished: false,
            kind,
            descriptor,
            catalog_identifier,
            catalog_options,
            projection,
        })
    }

    /// Projects the full-schema block down to the plan's column set, preserving
    /// the requested column order so it matches the planned output schema.
    fn apply_projection(&self, block: DataBlock) -> DataBlock {
        match &self.projection {
            Some(Projection::Columns(indices)) => {
                let entries = indices
                    .iter()
                    .map(|index| block.get_by_offset(*index).clone())
                    .collect();
                DataBlock::new(entries, block.num_rows())
            }
            // Inner-column projection is never requested for these flat metadata
            // tables; fall back to the full block.
            _ => block,
        }
    }
}

#[async_trait::async_trait]
impl AsyncSource for PaimonSystemSource {
    const NAME: &'static str = "paimon_system";

    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finished {
            return Ok(None);
        }
        self.finished = true;

        let options = options_from_map(&self.catalog_options)?;
        let catalog = map_paimon_result(CatalogFactory::create(options).await)?;
        let loaded = map_paimon_result(catalog.get_table(&self.catalog_identifier).await)?;
        let table = paimon::Table::new(
            loaded.file_io().clone(),
            self.descriptor.identifier.clone(),
            self.descriptor.location.clone(),
            self.descriptor.schema.clone(),
            loaded.rest_env().cloned(),
        );
        let block =
            read_system_table(self.kind, catalog, self.catalog_identifier.clone(), table).await?;
        Ok(Some(self.apply_projection(block)))
    }
}

fn catalog_identifier(name: &str, descriptor: &PaimonTableDescriptor) -> Result<Identifier> {
    match parse_system_name(name) {
        ParsedName::System { base, .. } => {
            Ok(Identifier::new(descriptor.identifier.database(), base))
        }
        _ => Err(ErrorCode::Internal(format!(
            "not a paimon system table: {name}"
        ))),
    }
}
