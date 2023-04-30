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
use std::collections::HashMap;
use std::sync::Arc;

use common_arrow::arrow::bitmap::MutableBitmap;
use common_base::base::tokio::sync::Notify;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::RemoteExpr;
use common_functions::BUILTIN_FUNCTIONS;
use common_sql::plans::RuntimeFilterId;
use parking_lot::Mutex;
use parking_lot::RwLock;
use storages_common_index::filters::Filter;
use storages_common_index::filters::FilterBuilder;
use storages_common_index::filters::Xor8Builder;
use storages_common_index::filters::Xor8Filter;

use crate::pipelines::processors::transforms::runtime_filter::RuntimeFilterConnector;
use crate::sessions::QueryContext;

pub struct RuntimeFilterState {
    pub(crate) ctx: Arc<QueryContext>,
    pub(crate) channel_filter_builders: RwLock<HashMap<RuntimeFilterId, Xor8Builder>>,
    pub(crate) channel_filters: RwLock<HashMap<RuntimeFilterId, Xor8Filter>>,
    pub(crate) left_runtime_filters: BTreeMap<RuntimeFilterId, RemoteExpr>,
    pub(crate) right_runtime_filters: BTreeMap<RuntimeFilterId, RemoteExpr>,
    pub(crate) sinker_count: Mutex<usize>,
    pub(crate) finished_notify: Arc<Notify>,
    pub(crate) finished: Mutex<bool>,
}

impl RuntimeFilterState {
    pub fn new(
        ctx: Arc<QueryContext>,
        left_runtime_filters: BTreeMap<RuntimeFilterId, RemoteExpr>,
        right_runtime_filters: BTreeMap<RuntimeFilterId, RemoteExpr>,
    ) -> Self {
        RuntimeFilterState {
            ctx,
            channel_filter_builders: Default::default(),
            channel_filters: Default::default(),
            left_runtime_filters,
            right_runtime_filters,
            sinker_count: Mutex::new(0),
            finished_notify: Arc::new(Default::default()),
            finished: Default::default(),
        }
    }
}

#[async_trait::async_trait]
impl RuntimeFilterConnector for RuntimeFilterState {
    fn attach(&self) {
        let mut sinker_count = self.sinker_count.lock();
        *sinker_count += 1;
    }

    fn detach(&self) -> Result<()> {
        let mut sinker_count = self.sinker_count.lock();
        *sinker_count -= 1;
        if *sinker_count == 0 {
            let mut channel_filters = self.channel_filters.write();
            let mut channel_filter_builders = self.channel_filter_builders.write();
            for (id, filter_builder) in channel_filter_builders.iter_mut() {
                channel_filters.insert(id.clone(), filter_builder.build()?);
            }
            let mut finished = self.finished.lock();
            *finished = true;
            self.finished_notify.notify_waiters();
        }
        Ok(())
    }

    fn is_finished(&self) -> Result<bool> {
        Ok(*self.finished.lock())
    }

    #[async_backtrace::framed]
    async fn wait_finish(&self) -> Result<()> {
        if !self.is_finished()? {
            self.finished_notify.notified().await;
        }
        Ok(())
    }

    fn consume(&self, data: &DataBlock) -> Result<Vec<DataBlock>> {
        let channel_filters = self.channel_filters.read();
        if channel_filters.is_empty() {
            return Ok(vec![data.clone()]);
        }
        // Create a bitmap to filter data
        let mut bitmap = MutableBitmap::from_len_set(data.num_rows());
        let func_ctx = self.ctx.get_function_context()?;
        for (id, remote_expr) in self.left_runtime_filters.iter() {
            let expr = remote_expr.as_expr(&BUILTIN_FUNCTIONS);
            let evaluator = Evaluator::new(data, &func_ctx, &BUILTIN_FUNCTIONS);
            let column = evaluator
                .run(&expr)?
                .convert_to_full_column(expr.data_type(), data.num_rows());
            let channel_filter = channel_filters.get(id).unwrap();
            for (idx, val) in column.iter().enumerate() {
                if !channel_filter.contains(&val) {
                    bitmap.set(idx, false);
                }
            }
        }
        Ok(vec![data.clone().filter_with_bitmap(&bitmap.into())?])
    }

    fn collect(&self, data: &DataBlock) -> Result<()> {
        let func_ctx = self.ctx.get_function_context()?;
        for (id, remote_expr) in self.right_runtime_filters.iter() {
            let expr = remote_expr.as_expr(&BUILTIN_FUNCTIONS);
            // expr represents equi condition in join build side
            // Such as: `select * from t1 inner join t2 on t1.a + 1 = t2.a + 2`
            // expr is `t2.a + 2`
            // First we need get expected values from data by expr
            let evaluator = Evaluator::new(data, &func_ctx, &BUILTIN_FUNCTIONS);
            let column = evaluator
                .run(&expr)?
                .convert_to_full_column(expr.data_type(), data.num_rows());

            // Generate Xor8 filter by column
            let mut channel_filter_builders = self.channel_filter_builders.write();
            if let Some(filter_builder) = channel_filter_builders.get_mut(id) {
                for val in column.iter() {
                    filter_builder.add_key(&val);
                }
            } else {
                let mut filter_builder = Xor8Builder::create();
                for val in column.iter() {
                    filter_builder.add_key(&val);
                }
                channel_filter_builders.insert(id.clone(), filter_builder);
            }
        }
        Ok(())
    }
}
