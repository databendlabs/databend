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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_sql::InsertInputSource;
use databend_common_sql::MetadataRef;
use databend_common_sql::plans::CopyIntoLocationPlan;
use databend_common_sql::plans::CopyIntoTablePlan;
use databend_common_sql::plans::Insert;
use databend_common_sql::plans::InsertMultiTable;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::TagSetObject;
use log::info;

use crate::interpreters::access_log::log_entry::AccessLogEntry;
use crate::interpreters::access_log::log_entry::AccessObject;
use crate::interpreters::access_log::log_entry::AccessObjectColumn;
use crate::interpreters::access_log::log_entry::DDLOperationType;
use crate::interpreters::access_log::log_entry::ModifyByDDLObject;
use crate::interpreters::access_log::log_entry::ObjectDomain;
use crate::sessions::QueryContext;
use crate::sessions::convert_query_log_timestamp;

pub struct AccessLogger {
    entry: AccessLogEntry,
}

impl AccessLogger {
    pub fn create(ctx: Arc<QueryContext>) -> Self {
        let query_id = ctx.get_id().to_string();
        let query_start = convert_query_log_timestamp(ctx.get_created_time());
        let user_name = ctx.get_current_user().map(|u| u.name).unwrap_or_default();

        Self {
            entry: AccessLogEntry::create(query_id, query_start, user_name),
        }
    }

    #[recursive::recursive]
    pub fn log(&mut self, plan: &Plan) {
        match plan {
            // DQL Operations
            Plan::Query { metadata, .. } => {
                self.log_query(metadata);
            }

            // DML Operations
            Plan::Insert(plan) => {
                self.log_insert(plan);
            }
            Plan::InsertMultiTable(plan) => {
                self.log_insert_multi(plan);
            }
            Plan::CopyIntoTable(plan) => {
                self.log_copy_into_table(plan);
            }
            Plan::CopyIntoLocation(plan) => {
                self.log_copy_into_location(plan);
            }
            Plan::DataMutation { metadata, .. } => {
                let modified_objects = extract_metadata_ref(metadata);
                self.entry.objects_modified.extend(modified_objects);
            }
            Plan::Replace(plan) => {
                let modified_object = AccessObject {
                    object_domain: ObjectDomain::Table,
                    object_name: format!("{}.{}.{}", plan.catalog, plan.database, plan.table),
                    columns: Some(
                        plan.schema
                            .fields
                            .iter()
                            .map(|field| AccessObjectColumn {
                                column_name: field.name.clone(),
                            })
                            .collect::<Vec<_>>(),
                    ),
                    stage_type: None,
                };
                self.entry.objects_modified.push(modified_object);
                // Log the base objects accessed by the replace operation's select part
                match &plan.source {
                    InsertInputSource::SelectPlan(inner) => {
                        self.log(inner);
                    }
                    InsertInputSource::Stage(inner) => {
                        self.log(inner);
                    }
                    _ => {}
                }
            }
            Plan::RemoveStage(plan) => {
                let modified_object = AccessObject {
                    object_domain: ObjectDomain::Stage,
                    object_name: plan.stage.stage_name.clone(),
                    stage_type: Some(plan.stage.stage_type.clone()),
                    columns: None,
                };
                self.entry.objects_modified.push(modified_object);
            }
            Plan::TruncateTable(plan) => {
                let modified_object = AccessObject {
                    object_domain: ObjectDomain::Table,
                    object_name: format!("{}.{}.{}", plan.catalog, plan.database, plan.table),
                    columns: None,
                    stage_type: None,
                };
                self.entry.objects_modified.push(modified_object);
            }

            // DDL Operations
            // Database
            Plan::CreateDatabase(plan) => {
                let object_name = format!("{}.{}", plan.catalog, plan.database);
                let operation_type = DDLOperationType::Create;
                self.entry.object_modified_by_ddl.push(ModifyByDDLObject {
                    object_domain: ObjectDomain::Database,
                    object_name,
                    operation_type,
                    ..Default::default()
                });
            }
            Plan::DropDatabase(plan) => {
                let object_name = format!("{}.{}", plan.catalog, plan.database);
                let operation_type = DDLOperationType::Drop;
                self.entry.object_modified_by_ddl.push(ModifyByDDLObject {
                    object_domain: ObjectDomain::Database,
                    object_name,
                    operation_type,
                    ..Default::default()
                });
            }

            Plan::UndropDatabase(plan) => {
                let object_name = format!("{}.{}", plan.catalog, plan.database);
                let operation_type = DDLOperationType::Undrop;
                self.entry.object_modified_by_ddl.push(ModifyByDDLObject {
                    object_domain: ObjectDomain::Database,
                    object_name,
                    operation_type,
                    ..Default::default()
                });
            }
            Plan::RenameDatabase(plan) => {
                plan.entities.iter().for_each(|entity| {
                    let object_name = format!("{}.{}", entity.catalog, entity.database);
                    let operation_type = DDLOperationType::Alter;
                    self.entry.object_modified_by_ddl.push(ModifyByDDLObject {
                        object_domain: ObjectDomain::Database,
                        object_name,
                        operation_type,
                        properties: HashMap::from([(
                            "new_database".to_string(),
                            serde_json::to_value(entity.new_database.clone()).unwrap(),
                        )]),
                    });
                });
            }

            // Table
            Plan::CreateTable(plan) => {
                let object_name = format!("{}.{}.{}", plan.catalog, plan.database, plan.table);
                let operation_type = DDLOperationType::Create;
                #[derive(serde::Serialize)]
                struct ModifiedColumn {
                    column_name: String,
                    sub_operation_type: String,
                }
                let columns = plan
                    .schema
                    .fields
                    .iter()
                    .map(|field| ModifiedColumn {
                        column_name: field.name.clone(),
                        sub_operation_type: "Add".to_string(),
                    })
                    .collect::<Vec<_>>();
                self.entry.object_modified_by_ddl.push(ModifyByDDLObject {
                    object_domain: ObjectDomain::Table,
                    object_name,
                    operation_type,
                    properties: HashMap::from([
                        (
                            "columns".to_string(),
                            serde_json::to_value(&columns).unwrap(),
                        ),
                        (
                            "create_options".to_string(),
                            serde_json::to_value(&plan.options).unwrap(),
                        ),
                    ]),
                });
                // consider `create table xx as select * from another_table`
                // if the plan has a select statement, we log access operations
                if let Some(inner) = &plan.as_select {
                    self.log(inner)
                }
            }
            Plan::SetOptions(plan) => {
                let object_name = format!("{}.{}.{}", plan.catalog, plan.database, plan.table);
                let operation_type = DDLOperationType::Alter;
                self.entry.object_modified_by_ddl.push(ModifyByDDLObject {
                    object_domain: ObjectDomain::Table,
                    object_name,
                    operation_type,
                    properties: HashMap::from([(
                        "set_options".to_string(),
                        serde_json::to_value(&plan.set_options).unwrap(),
                    )]),
                });
            }
            Plan::UnsetOptions(plan) => {
                let object_name = format!("{}.{}.{}", plan.catalog, plan.database, plan.table);
                let operation_type = DDLOperationType::Alter;
                self.entry.object_modified_by_ddl.push(ModifyByDDLObject {
                    object_domain: ObjectDomain::Table,
                    object_name,
                    operation_type,
                    properties: HashMap::from([(
                        "unset_options".to_string(),
                        serde_json::to_value(&plan.options).unwrap(),
                    )]),
                });
            }
            Plan::DropTable(plan) => {
                let object_name = format!("{}.{}.{}", plan.catalog, plan.database, plan.table);
                let operation_type = DDLOperationType::Drop;
                self.entry.object_modified_by_ddl.push(ModifyByDDLObject {
                    object_domain: ObjectDomain::Table,
                    object_name,
                    operation_type,
                    ..Default::default()
                });
            }
            Plan::UndropTable(plan) => {
                let object_name = format!("{}.{}.{}", plan.catalog, plan.database, plan.table);
                let operation_type = DDLOperationType::Undrop;
                self.entry.object_modified_by_ddl.push(ModifyByDDLObject {
                    object_domain: ObjectDomain::Table,
                    object_name,
                    operation_type,
                    ..Default::default()
                });
            }
            Plan::RenameTable(plan) => {
                let object_name = format!("{}.{}.{}", plan.catalog, plan.database, plan.table);
                let operation_type = DDLOperationType::Alter;
                self.entry.object_modified_by_ddl.push(ModifyByDDLObject {
                    object_domain: ObjectDomain::Table,
                    object_name,
                    operation_type,
                    properties: HashMap::from([(
                        "new_table".to_string(),
                        serde_json::to_value(format!("{}.{}", plan.new_database, plan.new_table))
                            .unwrap(),
                    )]),
                });
            }
            Plan::AddTableColumn(plan) => {
                let object_name = format!("{}.{}.{}", plan.catalog, plan.database, plan.table);
                let operation_type = DDLOperationType::Alter;
                #[derive(serde::Serialize)]
                struct ModifiedColumn {
                    column_name: String,
                    sub_operation_type: String,
                }
                let columns = vec![ModifiedColumn {
                    column_name: plan.field.name.clone(),
                    sub_operation_type: "Add".to_string(),
                }];
                self.entry.object_modified_by_ddl.push(ModifyByDDLObject {
                    object_domain: ObjectDomain::Table,
                    object_name,
                    operation_type,
                    properties: HashMap::from([(
                        "columns".to_string(),
                        serde_json::to_value(&columns).unwrap(),
                    )]),
                });
            }
            Plan::DropTableColumn(plan) => {
                let object_name = format!("{}.{}.{}", plan.catalog, plan.database, plan.table);
                let operation_type = DDLOperationType::Alter;
                #[derive(serde::Serialize)]
                struct ModifiedColumn {
                    column_name: String,
                    sub_operation_type: String,
                }
                let columns = vec![ModifiedColumn {
                    column_name: plan.column.clone(),
                    sub_operation_type: "Drop".to_string(),
                }];
                self.entry.object_modified_by_ddl.push(ModifyByDDLObject {
                    object_domain: ObjectDomain::Table,
                    object_name,
                    operation_type,
                    properties: HashMap::from([(
                        "columns".to_string(),
                        serde_json::to_value(&columns).unwrap(),
                    )]),
                });
            }
            Plan::RenameTableColumn(plan) => {
                let object_name = format!("{}.{}.{}", plan.catalog, plan.database, plan.table);
                let operation_type = DDLOperationType::Alter;
                #[derive(serde::Serialize)]
                struct ModifiedColumn {
                    column_name: String,
                    new_column_name: String,
                    sub_operation_type: String,
                }
                let columns = vec![ModifiedColumn {
                    column_name: plan.old_column.clone(),
                    new_column_name: plan.new_column.clone(),
                    sub_operation_type: "Alter".to_string(),
                }];
                self.entry.object_modified_by_ddl.push(ModifyByDDLObject {
                    object_domain: ObjectDomain::Table,
                    object_name,
                    operation_type,
                    properties: HashMap::from([(
                        "columns".to_string(),
                        serde_json::to_value(&columns).unwrap(),
                    )]),
                });
            }

            // View
            Plan::CreateView(plan) => {
                let object_name = format!("{}.{}.{}", plan.catalog, plan.database, plan.view_name);
                let operation_type = DDLOperationType::Create;
                self.entry.object_modified_by_ddl.push(ModifyByDDLObject {
                    object_domain: ObjectDomain::Table,
                    object_name,
                    operation_type,
                    ..Default::default()
                });
            }
            Plan::DropView(plan) => {
                let object_name = format!("{}.{}.{}", plan.catalog, plan.database, plan.view_name);
                let operation_type = DDLOperationType::Drop;
                self.entry.object_modified_by_ddl.push(ModifyByDDLObject {
                    object_domain: ObjectDomain::Table,
                    object_name,
                    operation_type,
                    ..Default::default()
                });
            }
            Plan::AlterView(plan) => {
                let object_name = format!("{}.{}.{}", plan.catalog, plan.database, plan.view_name);
                let operation_type = DDLOperationType::Alter;
                self.entry.object_modified_by_ddl.push(ModifyByDDLObject {
                    object_domain: ObjectDomain::Table,
                    object_name,
                    operation_type,
                    ..Default::default()
                });
            }

            // Stage
            Plan::CreateStage(plan) => {
                let object_name = plan.stage_info.stage_name.clone();
                let operation_type = DDLOperationType::Create;
                self.entry.object_modified_by_ddl.push(ModifyByDDLObject {
                    object_domain: ObjectDomain::Stage,
                    object_name,
                    operation_type,
                    properties: HashMap::from([(
                        "stage_type".to_string(),
                        serde_json::to_value(plan.stage_info.stage_type.to_string()).unwrap(),
                    )]),
                });
            }
            Plan::AlterStage(plan) => {
                let object_name = plan.stage_name.clone();
                let operation_type = DDLOperationType::Alter;
                self.entry.object_modified_by_ddl.push(ModifyByDDLObject {
                    object_domain: ObjectDomain::Stage,
                    object_name,
                    operation_type,
                    ..Default::default()
                });
            }
            Plan::DropStage(plan) => {
                let object_name = plan.name.clone();
                let operation_type = DDLOperationType::Drop;
                self.entry.object_modified_by_ddl.push(ModifyByDDLObject {
                    object_domain: ObjectDomain::Stage,
                    object_name,
                    operation_type,
                    ..Default::default()
                });
            }
            Plan::SetObjectTags(plan) => {
                self.log_tag_object_modify(&plan.object);
            }
            Plan::UnsetObjectTags(plan) => {
                self.log_tag_object_modify(&plan.object);
            }

            _ => {}
        }
    }

    fn log_tag_object_modify(&mut self, object: &TagSetObject) {
        let (object_domain, object_name) = match object {
            TagSetObject::Database(target) => (
                ObjectDomain::Database,
                format!("{}.{}", target.catalog, target.database),
            ),
            TagSetObject::Table(target) => (
                ObjectDomain::Table,
                format!("{}.{}.{}", target.catalog, target.database, target.table),
            ),
            TagSetObject::Stage(target) => (ObjectDomain::Stage, target.stage_name.clone()),
            TagSetObject::Connection(_) | TagSetObject::UDF(_) | TagSetObject::Procedure(_) => {
                return;
            }
            TagSetObject::View(target) => (
                ObjectDomain::Table,
                format!("{}.{}.{}", target.catalog, target.database, target.view),
            ),
        };
        self.entry.object_modified_by_ddl.push(ModifyByDDLObject {
            object_domain,
            object_name,
            operation_type: DDLOperationType::Alter,
            ..Default::default()
        });
    }

    fn log_query(&mut self, metadata: &MetadataRef) {
        let access_objects = extract_metadata_ref(metadata);
        self.entry.base_objects_accessed.extend(access_objects);
    }

    fn log_insert(&mut self, plan: &Insert) {
        let columns = plan
            .schema
            .fields
            .iter()
            .map(|field| AccessObjectColumn {
                column_name: field.name.clone(),
            })
            .collect::<Vec<_>>();
        let object_name = if let Some(branch) = &plan.branch {
            format!(
                "{}.{}.{}/{}",
                plan.catalog, plan.database, plan.table, branch
            )
        } else {
            format!("{}.{}.{}", plan.catalog, plan.database, plan.table)
        };
        let modified_object = AccessObject {
            object_domain: ObjectDomain::Table,
            object_name,
            columns: Some(columns),
            stage_type: None,
        };
        self.entry.objects_modified.push(modified_object);

        // Log the base objects accessed by the insert operation's select part
        match &plan.source {
            InsertInputSource::SelectPlan(plan) => {
                self.log(plan);
            }
            InsertInputSource::Values(_) => {}
            InsertInputSource::Stage(plan) => {
                self.log(plan);
            }
            InsertInputSource::StreamingLoad { .. } => {}
        }
    }

    fn log_insert_multi(&mut self, plan: &InsertMultiTable) {
        let modified_objects = extract_metadata_ref(&plan.meta_data);
        self.entry.objects_modified.extend(modified_objects);
        // Log the base objects accessed by the insert operation's select part
        self.log(&plan.input_source)
    }

    fn log_copy_into_table(&mut self, plan: &CopyIntoTablePlan) {
        let modified_object = AccessObject {
            object_domain: ObjectDomain::Table,
            object_name: format!(
                "{}.{}.{}",
                plan.catalog_info.name_ident.catalog_name, plan.database_name, plan.table_name
            ),
            columns: Some(
                plan.required_values_schema
                    .fields
                    .iter()
                    .map(|field| AccessObjectColumn {
                        column_name: field.name().clone(),
                    })
                    .collect::<Vec<_>>(),
            ),
            stage_type: None,
        };
        self.entry.objects_modified.push(modified_object);

        // Log the base objects accessed by the copy operation's source part
        if let Some(inner) = &plan.query {
            self.log(inner)
        }
    }

    fn log_copy_into_location(&mut self, plan: &CopyIntoLocationPlan) {
        let partition_columns = plan.partition_by.as_ref().map(|desc| {
            vec![AccessObjectColumn {
                column_name: format!("PARTITION BY ({})", desc.display),
            }]
        });
        let modified_object = AccessObject {
            object_domain: ObjectDomain::Stage,
            object_name: plan.info.stage.stage_name.clone(),
            stage_type: Some(plan.info.stage.stage_type.clone()),
            columns: partition_columns,
        };
        self.entry.objects_modified.push(modified_object);

        // Log the base objects accessed by the copy operation's source part
        self.log(&plan.from);
    }

    /// Serialize the access log entry to JSON and output it
    pub fn output(&self) {
        // No access operations need to be logged
        if self.entry.is_empty() {
            return;
        }

        let json = serde_json::to_string(&self.entry).unwrap();
        info!(target: "databend::log::access", "{}", json);
    }
}

fn extract_metadata_ref(metadata: &MetadataRef) -> Vec<AccessObject> {
    let metadata = metadata.read().clone();
    let mut table_to_columns = HashMap::with_capacity(metadata.tables().len());
    for table in metadata.tables().iter() {
        // Skip views since they are not the object accessed during execution.
        // Their underlying source tables are logged instead.
        if table.table().engine() == "VIEW" {
            continue;
        }
        match table.table().get_data_source_info() {
            DataSourceInfo::StageSource(stage_info) => {
                let access_object = AccessObject {
                    object_domain: ObjectDomain::Stage,
                    object_name: stage_info.stage_info.stage_name.clone(),
                    stage_type: Some(stage_info.stage_info.stage_type.clone()),
                    ..Default::default()
                };
                table_to_columns.insert(table.index(), access_object);
            }
            DataSourceInfo::ParquetSource(stage_info) => {
                let access_object = AccessObject {
                    object_domain: ObjectDomain::Stage,
                    object_name: stage_info.stage_info.stage_name.clone(),
                    stage_type: Some(stage_info.stage_info.stage_type.clone()),
                    ..Default::default()
                };
                table_to_columns.insert(table.index(), access_object);
            }
            DataSourceInfo::ORCSource(stage_info) => {
                let access_object = AccessObject {
                    object_domain: ObjectDomain::Stage,
                    object_name: stage_info.stage_table_info.stage_info.stage_name.clone(),
                    stage_type: Some(stage_info.stage_table_info.stage_info.stage_type.clone()),
                    columns: None,
                };
                table_to_columns.insert(table.index(), access_object);
            }
            DataSourceInfo::TableSource(_) => {
                let access_object = AccessObject {
                    object_domain: ObjectDomain::Table,
                    object_name: format!(
                        "{}.{}.{}",
                        table.catalog(),
                        table.database(),
                        table.name()
                    ),
                    ..Default::default()
                };
                table_to_columns.insert(table.index(), access_object);
            }
            _ => {}
        }
    }
    for column in metadata.columns().iter() {
        let table_index = column.table_index();
        if let Some(index) = table_index {
            if let Some(access_object) = table_to_columns.get_mut(&index) {
                let column_name = column.name().to_string();
                let access_column = AccessObjectColumn { column_name };
                if let Some(columns) = &mut access_object.columns {
                    columns.push(access_column);
                } else {
                    access_object.columns = Some(vec![access_column]);
                }
            }
        }
    }
    table_to_columns.into_values().collect::<Vec<_>>()
}
