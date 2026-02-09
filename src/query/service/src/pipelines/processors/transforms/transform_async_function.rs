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
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use databend_common_catalog::catalog::Catalog;
use databend_common_exception::Result;
use databend_common_expression::AutoIncrementExpr;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::types::UInt64Type;
use databend_common_meta_app::principal::AutoIncrementKey;
use databend_common_meta_app::schema::GetAutoIncrementNextValueReq;
use databend_common_meta_app::schema::GetSequenceNextValueReq;
use databend_common_meta_app::schema::GetSequenceReply;
use databend_common_meta_app::schema::GetSequenceReq;
use databend_common_meta_app::schema::SequenceIdent;
use databend_common_pipeline_transforms::processors::AsyncTransform;
use databend_common_sql::binder::AsyncFunctionDesc;
use databend_common_storages_fuse::TableContext;
use databend_common_users::GrantObjectVisibilityChecker;
use databend_common_users::Object;
use tokio::sync::Mutex;

use crate::pipelines::processors::transforms::transform_dictionary::DictionaryOperator;
use crate::sessions::QueryContext;
use crate::sql::plans::AsyncFunctionArgument;

// Structure to manage sequence numbers in batches
pub struct SequenceCounter {
    // Current sequence number
    current: AtomicU64,
    // Maximum sequence number in the current batch
    max: AtomicU64,
    // Serialize slow-path meta fetch / refill.
    refill_lock: Mutex<()>,
}

impl SequenceCounter {
    fn new() -> Self {
        Self {
            current: AtomicU64::new(0),
            max: AtomicU64::new(0),
            refill_lock: Mutex::new(()),
        }
    }

    // Try to reserve a range of sequence numbers
    fn try_reserve(&self, count: u64) -> Option<(u64, u64)> {
        if self.current.load(Ordering::Relaxed) == 0 {
            return None;
        }

        let current = self.current.load(Ordering::Relaxed);
        let max = self.max.load(Ordering::Relaxed);

        // Check if we have enough sequence numbers in the current batch
        if current + count <= max {
            let new_current = current + count;
            if self
                .current
                .compare_exchange(current, new_current, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                // Successfully reserved the range
                return Some((current, new_current));
            }
        }

        // Failed to reserve
        None
    }

    // Update the counter with a new batch of sequence numbers
    fn update_batch(&self, current: u64, max: u64) {
        self.current.store(current, Ordering::SeqCst);
        self.max.store(max, Ordering::SeqCst);
    }
}

// Shared sequence counters type
pub type SequenceCounters = Vec<Arc<SequenceCounter>>;

pub struct TransformAsyncFunction {
    ctx: Arc<QueryContext>,
    // key is the index of async_func_desc
    pub(crate) operators: BTreeMap<usize, Arc<DictionaryOperator>>,
    async_func_descs: Vec<AsyncFunctionDesc>,
    // Shared map of sequence name to sequence counter
    pub(crate) sequence_counters: SequenceCounters,
}

impl TransformAsyncFunction {
    // New constructor that accepts a shared sequence counters map
    pub(crate) fn new(
        ctx: Arc<QueryContext>,
        async_func_descs: Vec<AsyncFunctionDesc>,
        operators: BTreeMap<usize, Arc<DictionaryOperator>>,
        sequence_counters: SequenceCounters,
    ) -> Self {
        Self {
            ctx,
            async_func_descs,
            operators,
            sequence_counters,
        }
    }

    // Create a new shared sequence counters map
    pub(crate) fn create_sequence_counters(size: usize) -> SequenceCounters {
        (0..size)
            .map(|_| Arc::new(SequenceCounter::new()))
            .collect()
    }

    // transform add sequence nextval column.
    pub async fn transform<T: NextValFetcher>(
        ctx: Arc<QueryContext>,
        data_block: &mut DataBlock,
        counter: Arc<SequenceCounter>,
        fetcher: T,
    ) -> Result<()> {
        let count = data_block.num_rows() as u64;
        let column = if count == 0 {
            UInt64Type::from_data(vec![])
        } else {
            let fn_range_collect = |start: u64, end: u64, step: i64| {
                (0..end - start)
                    .map(|num| start + num * step as u64)
                    .collect::<Vec<_>>()
            };

            let catalog = ctx.get_default_catalog()?;

            // Try to reserve sequence numbers from the counter
            if let Some((start, _end)) = counter.try_reserve(count) {
                let step = fetcher.step(&ctx, &catalog).await?;
                UInt64Type::from_data(fn_range_collect(start, start + count, step))
            } else {
                // Slow path: serialize refill, but do not hold a RW lock while awaiting.
                let _guard = counter.refill_lock.lock().await;

                // try reserve again
                if let Some((start, _end)) = counter.try_reserve(count) {
                    drop(_guard);
                    let step = fetcher.step(&ctx, &catalog).await?;
                    UInt64Type::from_data(fn_range_collect(start, start + count, step))
                } else {
                    // Claim the remaining numbers in the current batch (if any).
                    let (remaining_start, remaining) = loop {
                        let current = counter.current.load(Ordering::Relaxed);
                        let max = counter.max.load(Ordering::Relaxed);

                        if current == 0 {
                            break (0, 0);
                        }

                        let remaining = max.saturating_sub(current);
                        if remaining == 0 {
                            break (current, 0);
                        }

                        if counter
                            .current
                            .compare_exchange(current, max, Ordering::SeqCst, Ordering::Relaxed)
                            .is_ok()
                        {
                            break (current, remaining);
                        }
                    };

                    let to_fetch = count.saturating_sub(remaining);
                    let NextValFetchResult {
                        start,
                        batch_size,
                        step,
                    } = fetcher.fetch(&ctx, &catalog, to_fetch).await?;

                    if remaining > 0 {
                        let mut numbers = Vec::with_capacity(count as usize);
                        let remaining_to_use = remaining.min(count);
                        numbers.extend(fn_range_collect(
                            remaining_start,
                            remaining_start + remaining_to_use,
                            step,
                        ));

                        if remaining_to_use < count {
                            let new_needed = count - remaining_to_use;
                            numbers.extend(fn_range_collect(start, start + new_needed, step));

                            // Reserve the consumed part before publishing the new max.
                            counter.update_batch(start + new_needed, start + batch_size);
                        } else {
                            // Unreachable due to the slow-path condition, keep it safe.
                            counter.update_batch(start, start + batch_size);
                        }

                        UInt64Type::from_data(numbers)
                    } else {
                        let numbers = fn_range_collect(start, start + count, step);
                        counter.update_batch(start + count, start + batch_size);
                        UInt64Type::from_data(numbers)
                    }
                }
            }
        };

        data_block.add_column(column);
        Ok(())
    }
}

pub trait NextValFetcher {
    async fn fetch(
        self,
        ctx: &QueryContext,
        catalog: &Arc<dyn Catalog>,
        to_fetch: u64,
    ) -> Result<NextValFetchResult>;

    async fn step(&self, ctx: &QueryContext, catalog: &Arc<dyn Catalog>) -> Result<i64>;
}

pub struct NextValFetchResult {
    start: u64,
    batch_size: u64,
    step: i64,
}

pub struct SequenceNextValFetcher {
    pub(crate) sequence_ident: SequenceIdent,
}

impl NextValFetcher for SequenceNextValFetcher {
    async fn fetch(
        self,
        ctx: &QueryContext,
        catalog: &Arc<dyn Catalog>,
        to_fetch: u64,
    ) -> Result<NextValFetchResult> {
        let (resp, visibility_checker) = self.get_sequence(ctx, catalog).await?;
        let step_size = resp.meta.step as u64;

        // Calculate batch size - take the larger of count or step_size
        let batch_size = to_fetch.max(step_size);

        // Calculate batch size - take the larger of count or step_size
        let req = GetSequenceNextValueReq {
            ident: self.sequence_ident,
            count: batch_size,
        };

        let resp = catalog
            .get_sequence_next_value(req, &visibility_checker)
            .await?;
        Ok(NextValFetchResult {
            start: resp.start,
            batch_size,
            step: resp.step,
        })
    }

    async fn step(&self, ctx: &QueryContext, catalog: &Arc<dyn Catalog>) -> Result<i64> {
        self.get_sequence(ctx, catalog)
            .await
            .map(|(resp, _)| resp.meta.step)
    }
}

impl SequenceNextValFetcher {
    async fn get_sequence(
        &self,
        ctx: &QueryContext,
        catalog: &Arc<dyn Catalog>,
    ) -> Result<(GetSequenceReply, Option<GrantObjectVisibilityChecker>)> {
        let visibility_checker = if ctx
            .get_settings()
            .get_enable_experimental_sequence_privilege_check()?
        {
            Some(ctx.get_visibility_checker(false, Object::Sequence).await?)
        } else {
            None
        };

        let req = GetSequenceReq {
            ident: self.sequence_ident.clone(),
        };
        catalog
            .get_sequence(req, &visibility_checker)
            .await
            .map(|reply| (reply, visibility_checker))
    }
}

pub struct AutoIncrementNextValFetcher {
    pub(crate) key: AutoIncrementKey,
    pub(crate) expr: AutoIncrementExpr,
}

impl NextValFetcher for AutoIncrementNextValFetcher {
    async fn fetch(
        self,
        ctx: &QueryContext,
        catalog: &Arc<dyn Catalog>,
        to_fetch: u64,
    ) -> Result<NextValFetchResult> {
        let step_size = self.expr.step as u64;

        // Calculate batch size - take the larger of count or step_size
        let batch_size = to_fetch.max(step_size);
        let step = self.expr.step;

        // Calculate batch size - take the larger of count or step_size
        let req = GetAutoIncrementNextValueReq {
            tenant: ctx.get_tenant(),
            key: self.key,
            expr: self.expr,
            count: batch_size,
        };

        let resp = catalog.get_autoincrement_next_value(req).await?;
        Ok(NextValFetchResult {
            start: resp.start,
            batch_size,
            step,
        })
    }

    async fn step(&self, _ctx: &QueryContext, _catalog: &Arc<dyn Catalog>) -> Result<i64> {
        Ok(self.expr.step)
    }
}

#[async_trait::async_trait]
impl AsyncTransform for TransformAsyncFunction {
    const NAME: &'static str = "AsyncFunction";

    #[async_backtrace::framed]
    async fn transform(&mut self, mut data_block: DataBlock) -> Result<DataBlock> {
        for (i, async_func_desc) in self.async_func_descs.iter().enumerate() {
            match &async_func_desc.func_arg {
                AsyncFunctionArgument::SequenceFunction(sequence_name) => {
                    Self::transform(
                        self.ctx.clone(),
                        &mut data_block,
                        self.sequence_counters[i].clone(),
                        SequenceNextValFetcher {
                            sequence_ident: SequenceIdent::new(
                                self.ctx.get_tenant(),
                                sequence_name,
                            ),
                        },
                    )
                    .await?;
                }
                AsyncFunctionArgument::AutoIncrement { key, expr } => {
                    Self::transform(
                        self.ctx.clone(),
                        &mut data_block,
                        self.sequence_counters[i].clone(),
                        AutoIncrementNextValFetcher {
                            key: key.clone(),
                            expr: expr.clone(),
                        },
                    )
                    .await?;
                }
                AsyncFunctionArgument::DictGetFunction(dict_arg) => {
                    self.transform_dict_get(
                        i,
                        &mut data_block,
                        dict_arg,
                        &async_func_desc.arg_indices,
                        &async_func_desc.data_type,
                    )
                    .await?;
                }
            }
        }
        Ok(data_block)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::time::Duration;
    use tokio::time::sleep;
    use tokio::time::timeout;

    use super::SequenceCounter;

    #[tokio::test]
    async fn test_no_stall_when_refill_lock_waiting() {
        let counter = Arc::new(SequenceCounter::new());
        counter.update_batch(1, 1000);

        let holder = {
            let counter = counter.clone();
            databend_common_base::runtime::spawn(async move {
                let _guard = counter.refill_lock.lock().await;
                sleep(Duration::from_millis(200)).await;
            })
        };

        // Ensure the lock is held.
        sleep(Duration::from_millis(20)).await;

        let waiter = {
            let counter = counter.clone();
            databend_common_base::runtime::spawn(async move {
                let _guard = counter.refill_lock.lock().await;
            })
        };

        // While a slow-path refill is holding the lock and another task is
        // waiting for it, the fast-path reservation should still make progress.
        timeout(Duration::from_millis(50), async {
            for _ in 0..10 {
                assert!(counter.try_reserve(1).is_some());
            }
        })
        .await
        .expect("fast-path reservation should not be stalled");

        holder.await.unwrap();
        waiter.await.unwrap();
    }
}
