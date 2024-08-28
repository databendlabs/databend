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

use std::any::type_name;
use std::fmt::Write;
use std::sync::Arc;

use databend_common_base::get_all_tasks;
use databend_common_base::runtime::Runtime;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::StringType;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use regex::Captures;

use crate::SyncOneBlockSystemTable;
use crate::SyncSystemTable;

pub struct BacktraceTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl SyncSystemTable for BacktraceTable {
    const NAME: &'static str = "system.backtrace";

    // Allow distributed query.
    const DATA_IN_LOCAL: bool = false;

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_full_data(&self, ctx: Arc<dyn TableContext>) -> Result<DataBlock> {
        let local_node = ctx.get_cluster().local_id.clone();
        let (tasks, polling_tasks) = get_all_tasks(false);
        let tasks_size = tasks.len() + polling_tasks.len();

        let mut nodes: Vec<String> = Vec::with_capacity(tasks_size);
        let mut queries_id: Vec<String> = Vec::with_capacity(tasks_size);
        let mut queries_status: Vec<String> = Vec::with_capacity(tasks_size);
        let mut stacks: Vec<String> = Vec::with_capacity(tasks_size);

        for (status, mut tasks) in [
            ("PENDING".to_string(), tasks),
            ("RUNNING".to_string(), polling_tasks),
        ] {
            tasks.sort_by(|l, r| Ord::cmp(&l.stack_frames.len(), &r.stack_frames.len()));

            for item in tasks.into_iter().rev() {
                let mut query_id = String::from("Global");

                let mut stack_frames = String::new();
                let mut frames_iter = item.stack_frames.into_iter();

                if let Some(mut frame) = frames_iter.next() {
                    let matcher = "╼ Running query ";
                    if let Some(_pos) = frame.find(matcher) {
                        let task_matcher = " spawn task";
                        if let Some(pos) = frame.find(task_matcher) {
                            query_id = frame[matcher.len()..pos].to_string();

                            frame = format!(
                                "╼ {}::spawn{}",
                                type_name::<Runtime>(),
                                &frame[pos + task_matcher.len()..]
                            );
                        }
                    } else if let Some(_pos) = frame.find("╼ Global spawn task") {
                        let replaced = format!("{}::spawn", type_name::<Runtime>());
                        frame = frame.replace("╼ Global spawn task", &replaced)
                    }

                    writeln!(stack_frames, "{}", frame).unwrap();
                }

                for mut frame in frames_iter {
                    frame = frame.replace("::{{closure}}", "");

                    let regex = regex::Regex::new("<(.+) as .+>").unwrap();
                    let frame = regex
                        .replace(&frame, |caps: &Captures| caps[1].to_string())
                        .to_string();

                    writeln!(stack_frames, "{}", frame).unwrap();
                }

                nodes.push(local_node.clone());
                stacks.push(stack_frames);
                queries_id.push(query_id);
                queries_status.push(status.clone());
            }
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(nodes),
            StringType::from_data(queries_id),
            StringType::from_data(queries_status),
            StringType::from_data(stacks),
        ]))
    }
}

impl BacktraceTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("node", TableDataType::String),
            TableField::new("query_id", TableDataType::String),
            TableField::new("status", TableDataType::String),
            TableField::new("stack", TableDataType::String),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'backtrace'".to_string(),
            name: "backtrace".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemBacktrace".to_string(),

                ..Default::default()
            },
            ..Default::default()
        };

        SyncOneBlockSystemTable::create(BacktraceTable { table_info })
    }
}
