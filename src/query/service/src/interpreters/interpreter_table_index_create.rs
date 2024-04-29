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
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::LazyLock;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::get_license_manager;
use databend_common_meta_app::schema::CreateTableIndexReq;
use databend_common_sql::plans::CreateTableIndexPlan;
use databend_common_storages_fuse::TableContext;
use databend_enterprise_inverted_index::get_inverted_index_handler;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

// valid values for inverted index option tokenizer
static INDEX_TOKENIZER_VALUES: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut r = HashSet::new();
    r.insert("english");
    r.insert("chinese");
    r
});

// valid values for inverted index option filter
static INDEX_FILTER_VALUES: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut r = HashSet::new();
    r.insert("english_stop");
    r.insert("english_stemmer");
    r.insert("chinese_stop");
    r
});

// valid values for inverted index record option
static INDEX_RECORD_VALUES: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut r = HashSet::new();
    r.insert("basic");
    r.insert("freq");
    r.insert("position");
    r
});

fn is_valid_tokenizer_values<S: AsRef<str>>(opt_val: S) -> bool {
    INDEX_TOKENIZER_VALUES.contains(opt_val.as_ref())
}

fn is_valid_filter_values<S: AsRef<str>>(opt_val: S) -> bool {
    INDEX_FILTER_VALUES.contains(opt_val.as_ref())
}

fn is_valid_index_record_values<S: AsRef<str>>(opt_val: S) -> bool {
    INDEX_RECORD_VALUES.contains(opt_val.as_ref())
}

pub struct CreateTableIndexInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateTableIndexPlan,
}

impl CreateTableIndexInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateTableIndexPlan) -> Result<Self> {
        Ok(CreateTableIndexInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateTableIndexInterpreter {
    fn name(&self) -> &str {
        "CreateTableIndexInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let license_manager = get_license_manager();
        license_manager
            .manager
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::InvertedIndex)?;

        let index_name = self.plan.index_name.clone();
        let column_ids = self.plan.column_ids.clone();
        let sync_creation = self.plan.sync_creation;
        let table_id = self.plan.table_id;
        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;

        let mut options = BTreeMap::new();
        for (opt, val) in self.plan.index_options.iter() {
            let key = opt.to_lowercase();
            let value = val.to_lowercase();
            match key.as_str() {
                "tokenizer" => {
                    if !is_valid_tokenizer_values(&value) {
                        return Err(ErrorCode::IndexOptionInvalid(format!(
                            "value `{value}` is invalid index tokenizer",
                        )));
                    }
                    options.insert("tokenizer".to_string(), value.to_string());
                }
                "filters" => {
                    let raw_filters: Vec<&str> = value.split(',').collect();
                    let mut filters = Vec::with_capacity(raw_filters.len());
                    for raw_filter in raw_filters {
                        let filter = raw_filter.trim();
                        if !is_valid_filter_values(filter) {
                            return Err(ErrorCode::IndexOptionInvalid(format!(
                                "value `{filter}` is invalid index filters",
                            )));
                        }
                        filters.push(filter);
                    }
                    options.insert("filters".to_string(), filters.join(",").to_string());
                }
                "index_record" => {
                    if !is_valid_index_record_values(&value) {
                        return Err(ErrorCode::IndexOptionInvalid(format!(
                            "value `{value}` is invalid index record option",
                        )));
                    }
                    // convert to a JSON string, for `IndexRecordOption` deserialize
                    let index_record_val = format!("\"{}\"", value);
                    options.insert("index_record".to_string(), index_record_val);
                }
                _ => {
                    return Err(ErrorCode::IndexOptionInvalid(format!(
                        "index option `{key}` is invalid key for create inverted index statement",
                    )));
                }
            }
        }

        let create_index_req = CreateTableIndexReq {
            create_option: self.plan.create_option,
            table_id,
            name: index_name,
            column_ids,
            sync_creation,
            options,
        };

        let handler = get_inverted_index_handler();
        let _ = handler
            .do_create_table_index(catalog, create_index_req)
            .await?;

        Ok(PipelineBuildResult::create())
    }
}
