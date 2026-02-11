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
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::execute_futures_in_parallel;
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AutoIncrementExpr;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::StringColumn;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_meta_app::principal::AutoIncrementKey;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::principal::StageType;
use databend_common_meta_app::schema::GetAutoIncrementNextValueReq;
use databend_common_meta_app::schema::GetSequenceNextValueReq;
use databend_common_meta_app::schema::GetSequenceReply;
use databend_common_meta_app::schema::GetSequenceReq;
use databend_common_meta_app::schema::SequenceIdent;
use databend_common_pipeline_transforms::processors::AsyncTransform;
use databend_common_settings::Settings;
use databend_common_sql::binder::AsyncFunctionDesc;
use databend_common_sql::binder::parse_stage_location;
use databend_common_sql::binder::parse_stage_name;
use databend_common_storage::init_stage_operator;
use databend_common_users::GrantObjectVisibilityChecker;
use databend_common_users::Object;
use databend_common_users::UserApiProvider;
use log::LevelFilter;
use opendal::Operator;
use tokio::sync::RwLock;

use crate::pipelines::processors::transforms::transform_dictionary::DictionaryOperator;
use crate::sessions::QueryContext;
use crate::sql::IndexType;
use crate::sql::plans::AsyncFunctionArgument;
use crate::sql::plans::ReadFileFunctionArgument;

// Structure to manage sequence numbers in batches
pub struct SequenceCounter {
    // Current sequence number
    current: AtomicU64,
    // Maximum sequence number in the current batch
    max: AtomicU64,
}

impl SequenceCounter {
    fn new() -> Self {
        Self {
            current: AtomicU64::new(0),
            max: AtomicU64::new(0),
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
    fn update_batch(&self, start: u64, count: u64) {
        self.current.store(start, Ordering::SeqCst);
        self.max.store(start + count, Ordering::SeqCst);
    }
}

// Shared sequence counters type
pub type SequenceCounters = Vec<Arc<RwLock<SequenceCounter>>>;

enum VisibilityCheckerState {
    Disabled,
    Pending,
    Ready(GrantObjectVisibilityChecker),
}

impl VisibilityCheckerState {
    fn try_new(settings: &Settings) -> Result<Self> {
        if settings.get_enable_experimental_rbac_check()? {
            Ok(Self::Pending)
        } else {
            Ok(Self::Disabled)
        }
    }

    async fn get(&mut self, ctx: &QueryContext) -> Result<Option<&GrantObjectVisibilityChecker>> {
        match self {
            Self::Disabled => Ok(None),
            Self::Pending => {
                let checker = ctx.get_visibility_checker(false, Object::Stage).await?;
                *self = Self::Ready(checker);
                match self {
                    Self::Ready(checker) => Ok(Some(checker)),
                    _ => Ok(None),
                }
            }
            Self::Ready(checker) => Ok(Some(checker)),
        }
    }
}

pub struct ReadFileContext {
    thread_num: usize,
    permit_num: usize,
    visibility_checker: VisibilityCheckerState,
    stage_infos: HashMap<String, StageInfo>,
    stage_operators: HashMap<String, Operator>,
}

impl ReadFileContext {
    pub(crate) fn try_new(ctx: &QueryContext) -> Result<Self> {
        let settings = ctx.get_settings();
        let permit_num = 2.max(
            settings.get_max_storage_io_requests()? as usize / settings.get_max_threads()? as usize,
        );
        Ok(Self {
            thread_num: 2,
            permit_num,
            visibility_checker: VisibilityCheckerState::try_new(settings.as_ref())?,
            stage_infos: HashMap::new(),
            stage_operators: HashMap::new(),
        })
    }

    fn thread_num(&self) -> usize {
        self.thread_num
    }

    fn permit_num(&self) -> usize {
        self.permit_num
    }

    fn apply_credential_chain(stage_info: &mut StageInfo) {
        if ThreadTracker::capture_log_settings()
            .is_some_and(|settings| settings.level == LevelFilter::Off)
        {
            stage_info.allow_credential_chain = true;
        }
    }

    fn cache_stage_info(&mut self, stage_name: &str, mut stage_info: StageInfo) {
        if self.stage_infos.contains_key(stage_name) {
            return;
        }
        Self::apply_credential_chain(&mut stage_info);
        self.stage_infos.insert(stage_name.to_string(), stage_info);
    }

    async fn visibility_checker(
        &mut self,
        ctx: &QueryContext,
    ) -> Result<Option<&GrantObjectVisibilityChecker>> {
        self.visibility_checker.get(ctx).await
    }

    fn operator_for_stage(&mut self, stage_info: &StageInfo) -> Result<Operator> {
        let key = if stage_info.stage_type == StageType::User {
            "~".to_string()
        } else {
            stage_info.stage_name.clone()
        };
        if let Some(operator) = self.stage_operators.get(&key) {
            return Ok(operator.clone());
        }

        let operator = init_stage_operator(stage_info)?;
        self.stage_operators.insert(key, operator.clone());
        Ok(operator)
    }

    async fn read_files_in_parallel<K, I>(&self, tasks_info: I) -> Result<HashMap<K, Vec<u8>>>
    where
        K: Eq + std::hash::Hash + Send + 'static,
        I: IntoIterator<Item = (K, Operator, String)>,
    {
        let thread_num = self.thread_num();
        let permit_num = self.permit_num();
        let tasks = tasks_info
            .into_iter()
            .map(|(key, operator, path)| async move {
                let buffer = operator.read(&path).await?;
                Ok::<(K, Vec<u8>), ErrorCode>((key, buffer.to_bytes().to_vec()))
            });
        let results = execute_futures_in_parallel(
            tasks,
            thread_num,
            permit_num,
            "read-file-worker".to_string(),
        )
        .await?
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        Ok(results.into_iter().collect())
    }

    fn normalize_relative_path(path: &str) -> Result<String> {
        let path = path.trim_start_matches('/');
        let path = if path.is_empty() { "/" } else { path };
        if path == "/" || path.ends_with('/') {
            return Err(ErrorCode::BadArguments(format!(
                "read_file location must be a file, but got {}",
                path
            )));
        }
        Ok(path.to_string())
    }

    async fn stage_info_for(&mut self, ctx: &QueryContext, stage_name: &str) -> Result<StageInfo> {
        if let Some(stage_info) = self.stage_infos.get(stage_name) {
            return Ok(stage_info.clone());
        }

        let mut stage_info = if stage_name == "~" {
            StageInfo::new_user_stage(&ctx.get_current_user()?.name)
        } else {
            UserApiProvider::instance()
                .get_stage(&ctx.get_tenant(), stage_name)
                .await?
        };

        if ThreadTracker::capture_log_settings()
            .is_some_and(|settings| settings.level == LevelFilter::Off)
        {
            stage_info.allow_credential_chain = true;
        }

        if let Some(visibility_checker) = self.visibility_checker(ctx).await? {
            if !(stage_info.is_temporary
                || visibility_checker.check_stage_read_visibility(&stage_info.stage_name)
                || stage_info.stage_type == StageType::User
                    && stage_info.stage_name == ctx.get_current_user()?.name)
            {
                return Err(ErrorCode::PermissionDenied(format!(
                    "Permission denied: privilege READ is required on stage {} for user {}",
                    stage_info.stage_name.clone(),
                    &ctx.get_current_user()?.identity().display(),
                )));
            }
        }

        self.stage_infos
            .insert(stage_name.to_string(), stage_info.clone());
        Ok(stage_info)
    }

    #[async_backtrace::framed]
    async fn resolve_stage_file(
        &mut self,
        ctx: &QueryContext,
        location: &str,
    ) -> Result<(Operator, String)> {
        if !location.starts_with('@') {
            return Err(ErrorCode::BadArguments(format!(
                "stage path must start with @, but got {}",
                location
            )));
        }

        let stage_location = &location[1..];
        let (stage_name, path) = parse_stage_location(stage_location)?;
        if path == "/" || path.ends_with('/') {
            return Err(ErrorCode::BadArguments(format!(
                "read_file location must be a file, but got {}",
                location
            )));
        }

        let stage_info = self.stage_info_for(ctx, &stage_name).await?;
        let operator = self.operator_for_stage(&stage_info)?;
        Ok((operator, path))
    }

    #[async_backtrace::framed]
    async fn resolve_stage_file_with_stage_name(
        &mut self,
        ctx: &QueryContext,
        stage_name: &str,
        path: &str,
    ) -> Result<(Operator, String)> {
        let path = Self::normalize_relative_path(path)?;
        let stage_info = self.stage_info_for(ctx, stage_name).await?;
        let operator = self.operator_for_stage(&stage_info)?;
        Ok((operator, path))
    }

    #[async_backtrace::framed]
    async fn read_stage_file(&mut self, ctx: &QueryContext, location: &str) -> Result<Vec<u8>> {
        let (operator, path) = self.resolve_stage_file(ctx, location).await?;
        let buffer = operator.read(&path).await?;
        Ok(buffer.to_bytes().to_vec())
    }

    pub async fn transform_read_file_column(
        &mut self,
        ctx: Arc<QueryContext>,
        values: Vec<Value<AnyType>>,
        allow_nullable: bool,
        read_file_arg: &ReadFileFunctionArgument,
    ) -> Result<Value<AnyType>> {
        let null_scalar = || -> Result<Value<AnyType>> { Ok(Value::Scalar(Scalar::Null)) };

        if values.iter().any(|v| v.is_scalar_null()) {
            return null_scalar();
        }

        let require_string = |scalar: &Scalar| -> Result<String> {
            if let Scalar::String(value) = scalar {
                Ok(value.clone())
            } else {
                Err(ErrorCode::BadArguments(
                    "Bug: read_file args must be a string",
                ))
            }
        };

        fn output_from_cache<K, I>(
            rows: I,
            cache: &HashMap<K, Vec<u8>>,
            result_is_nullable: bool,
        ) -> Result<Value<AnyType>>
        where
            K: Eq + std::hash::Hash + std::fmt::Debug,
            I: IntoIterator<Item = Option<K>>,
        {
            let mut output = Vec::new();
            for key in rows {
                let Some(key) = key else {
                    output.push(None);
                    continue;
                };
                let bytes = cache
                    .get(&key)
                    .ok_or_else(|| {
                        ErrorCode::Internal(format!("read_file missing cached value for {:?}", key))
                    })?
                    .clone();
                output.push(Some(bytes));
            }
            let result = Value::Column(BinaryType::from_opt_data(output));
            if result_is_nullable {
                Ok(result)
            } else {
                let (result, has_null) = result.remove_nullable();
                if has_null {
                    Err(ErrorCode::Internal("Bug: read_file got null".to_owned()))
                } else {
                    Ok(result)
                }
            }
        }

        let resolve_stage_name = |stage: &str| -> Result<String> {
            if let Some(stage_name) = &read_file_arg.stage_name {
                Ok(stage_name.clone())
            } else {
                parse_stage_name(stage)
            }
        };

        match values.len() {
            1 => match &values[0] {
                Value::Scalar(scalar) => {
                    let location = require_string(scalar)?;
                    let bytes = self
                        .read_stage_file(ctx.as_ref(), location.as_str())
                        .await?;
                    Ok(Value::Scalar(Scalar::Binary(bytes)))
                }
                Value::Column(column) => {
                    let (is_all_null, validity) = column.validity();
                    if is_all_null {
                        null_scalar()
                    } else {
                        let string_col: StringColumn =
                            StringType::try_downcast_column(&column.remove_nullable())?;
                        let mut row_locations = Vec::with_capacity(string_col.len());
                        let mut unique_locations = HashSet::new();
                        for location in string_col.option_iter(validity) {
                            let Some(location) = location else {
                                row_locations.push(None);
                                continue;
                            };
                            let location = location.to_string();
                            unique_locations.insert(location.clone());
                            row_locations.push(Some(location));
                        }

                        let mut tasks_info = Vec::with_capacity(unique_locations.len());
                        for location in unique_locations {
                            let (operator, path) =
                                self.resolve_stage_file(ctx.as_ref(), &location).await?;
                            tasks_info.push((location, operator, path));
                        }
                        let cache = self.read_files_in_parallel(tasks_info).await?;
                        output_from_cache(row_locations, &cache, allow_nullable)
                    }
                }
            },
            2 => match (&values[0], &values[1]) {
                (Value::Scalar(stage_scalar), Value::Scalar(path_scalar)) => {
                    let stage = require_string(stage_scalar)?;
                    let path = require_string(path_scalar)?;
                    let stage_name = resolve_stage_name(stage.as_str())?;
                    let (operator, path) = self
                        .resolve_stage_file_with_stage_name(
                            ctx.as_ref(),
                            &stage_name,
                            path.as_str(),
                        )
                        .await?;
                    let buffer = operator.read(&path).await?;
                    Ok(Value::Scalar(Scalar::Binary(buffer.to_bytes().to_vec())))
                }
                (Value::Scalar(stage_scalar), Value::Column(path_column)) => {
                    let stage = require_string(stage_scalar)?;
                    let stage_name = resolve_stage_name(stage.as_str())?;
                    let (is_all_null, validity) = path_column.validity();
                    if is_all_null {
                        null_scalar()
                    } else {
                        let string_col: StringColumn =
                            StringType::try_downcast_column(&path_column.remove_nullable())?;
                        let mut row_paths = Vec::with_capacity(string_col.len());
                        let mut unique_paths = HashSet::new();
                        for path in string_col.option_iter(validity) {
                            let Some(path) = path else {
                                row_paths.push(None);
                                continue;
                            };
                            let normalized = Self::normalize_relative_path(path)?;
                            unique_paths.insert(normalized.clone());
                            row_paths.push(Some(normalized));
                        }

                        let stage_info = self.stage_info_for(ctx.as_ref(), &stage_name).await?;
                        let operator = self.operator_for_stage(&stage_info)?;
                        let tasks_info = unique_paths
                            .into_iter()
                            .map(|path| (path.clone(), operator.clone(), path))
                            .collect::<Vec<_>>();
                        let cache = self.read_files_in_parallel(tasks_info).await?;
                        output_from_cache(row_paths, &cache, allow_nullable)
                    }
                }
                (Value::Column(stage_column), Value::Scalar(path_scalar)) => {
                    let path = require_string(path_scalar)?;
                    let (is_all_null, validity) = stage_column.validity();
                    if is_all_null {
                        null_scalar()
                    } else {
                        let string_col: StringColumn =
                            StringType::try_downcast_column(&stage_column.remove_nullable())?;
                        let normalized_path = Self::normalize_relative_path(path.as_str())?;
                        let mut row_stages = Vec::with_capacity(string_col.len());
                        let mut unique_stages = HashSet::new();
                        for stage in string_col.option_iter(validity) {
                            let Some(stage) = stage else {
                                row_stages.push(None);
                                continue;
                            };
                            let stage_name = parse_stage_name(stage)?;
                            unique_stages.insert(stage_name.clone());
                            row_stages.push(Some(stage_name));
                        }

                        let mut tasks_info = Vec::with_capacity(unique_stages.len());
                        for stage_name in unique_stages {
                            let (operator, path) = self
                                .resolve_stage_file_with_stage_name(
                                    ctx.as_ref(),
                                    &stage_name,
                                    &normalized_path,
                                )
                                .await?;
                            tasks_info.push((stage_name, operator, path));
                        }
                        let cache = self.read_files_in_parallel(tasks_info).await?;
                        output_from_cache(row_stages, &cache, allow_nullable)
                    }
                }
                (Value::Column(stage_column), Value::Column(path_column)) => {
                    let (stage_all_null, stage_validity) = stage_column.validity();
                    let (path_all_null, path_validity) = path_column.validity();
                    if stage_all_null || path_all_null {
                        null_scalar()
                    } else {
                        let stage_col: StringColumn =
                            StringType::try_downcast_column(&stage_column.remove_nullable())?;
                        let path_col: StringColumn =
                            StringType::try_downcast_column(&path_column.remove_nullable())?;
                        let mut row_keys = Vec::with_capacity(stage_col.len());
                        let mut unique_keys = HashSet::new();
                        for (stage, path) in stage_col
                            .option_iter(stage_validity)
                            .zip(path_col.option_iter(path_validity))
                        {
                            let (Some(stage), Some(path)) = (stage, path) else {
                                row_keys.push(None);
                                continue;
                            };
                            let stage_name = parse_stage_name(stage)?;
                            let normalized_path = Self::normalize_relative_path(path)?;
                            let key = (stage_name, normalized_path);
                            unique_keys.insert(key.clone());
                            row_keys.push(Some(key));
                        }

                        let mut tasks_info = Vec::with_capacity(unique_keys.len());
                        for (stage_name, path) in unique_keys {
                            let (operator, resolved_path) = self
                                .resolve_stage_file_with_stage_name(
                                    ctx.as_ref(),
                                    &stage_name,
                                    &path,
                                )
                                .await?;
                            tasks_info.push(((stage_name, path), operator, resolved_path));
                        }
                        let cache = self.read_files_in_parallel(tasks_info).await?;
                        output_from_cache(row_keys, &cache, allow_nullable)
                    }
                }
            },
            _ => Err(ErrorCode::Internal(
                "read_file expects one or two arguments".to_string(),
            )),
        }
    }

    #[async_backtrace::framed]
    pub async fn transform_read_file(
        &mut self,
        ctx: Arc<QueryContext>,
        data_block: &mut DataBlock,
        arg_indices: &[IndexType],
        data_type: &DataType,
        read_file_arg: &ReadFileFunctionArgument,
    ) -> Result<()> {
        if let (Some(stage_name), Some(stage_info)) =
            (&read_file_arg.stage_name, &read_file_arg.stage_info)
        {
            if !self.stage_infos.contains_key(stage_name) {
                self.cache_stage_info(stage_name, stage_info.as_ref().clone());
            }
        }
        let allow_nullable = data_type.is_nullable_or_null();
        let entries = arg_indices
            .iter()
            .map(|i| data_block.get_by_offset(*i).clone())
            .collect::<Vec<_>>();
        let is_nullable = entries.iter().any(|b| b.data_type().is_nullable_or_null());
        let values = entries.into_iter().map(|e| e.value()).collect::<Vec<_>>();
        if !allow_nullable && is_nullable {
            return Err(ErrorCode::BadArguments(
                "Bug: read_file args should not be null".to_string(),
            ));
        }
        let output = self
            .transform_read_file_column(ctx, values, allow_nullable, read_file_arg)
            .await?;
        let entry = BlockEntry::new(output, || (data_type.clone(), data_block.num_rows()));
        data_block.add_entry(entry);
        Ok(())
    }
}

pub struct TransformAsyncFunction {
    ctx: Arc<QueryContext>,
    // key is the index of async_func_desc
    pub(crate) operators: BTreeMap<usize, Arc<DictionaryOperator>>,
    async_func_descs: Vec<AsyncFunctionDesc>,
    // Shared map of sequence name to sequence counter
    pub(crate) sequence_counters: SequenceCounters,
    pub(crate) read_file_ctx: Option<ReadFileContext>,
}

impl TransformAsyncFunction {
    // New constructor that accepts a shared sequence counters map
    pub(crate) fn new(
        ctx: Arc<QueryContext>,
        async_func_descs: Vec<AsyncFunctionDesc>,
        operators: BTreeMap<usize, Arc<DictionaryOperator>>,
        sequence_counters: SequenceCounters,
    ) -> Result<Self> {
        let read_file_ctx = if async_func_descs
            .iter()
            .any(|desc| matches!(desc.func_arg, AsyncFunctionArgument::ReadFile(_)))
        {
            Some(ReadFileContext::try_new(&ctx)?)
        } else {
            None
        };
        Ok(Self {
            ctx,
            async_func_descs,
            operators,
            sequence_counters,
            read_file_ctx,
        })
    }

    // Create a new shared sequence counters map
    pub(crate) fn create_sequence_counters(size: usize) -> SequenceCounters {
        (0..size)
            .map(|_| Arc::new(RwLock::new(SequenceCounter::new())))
            .collect()
    }

    // transform add sequence nextval column.
    pub async fn transform<T: NextValFetcher>(
        ctx: Arc<QueryContext>,
        data_block: &mut DataBlock,
        counter_lock: Arc<RwLock<SequenceCounter>>,
        fetcher: T,
    ) -> Result<()> {
        let count = data_block.num_rows() as u64;
        let column = if count == 0 {
            UInt64Type::from_data(vec![])
        } else {
            // Get or create the sequence counter
            let counter = counter_lock.read().await;
            let fn_range_collect = |start: u64, end: u64, step: i64| {
                (0..end - start)
                    .map(|num| start + num * step as u64)
                    .collect::<Vec<_>>()
            };
            // We need to fetch more sequence numbers
            let catalog = ctx.get_default_catalog()?;

            // Try to reserve sequence numbers from the counter
            if let Some((start, _end)) = counter.try_reserve(count) {
                let step = fetcher.step(&ctx, &catalog).await?;
                // We have enough sequence numbers in the current batch
                UInt64Type::from_data(fn_range_collect(start, start + count, step))
            } else {
                // drop the read lock and get the write lock
                drop(counter);
                let counter = counter_lock.write().await;
                {
                    // try reserve again
                    if let Some((start, _end)) = counter.try_reserve(count) {
                        let step = fetcher.step(&ctx, &catalog).await?;
                        // We have enough sequence numbers in the current batch
                        UInt64Type::from_data(fn_range_collect(start, count, step))
                    } else {
                        // Get current state of the counter
                        let current = counter.current.load(Ordering::Relaxed);
                        let max = counter.max.load(Ordering::Relaxed);
                        // Calculate how many sequence numbers we need to fetch
                        // If there are remaining numbers, we'll use them first
                        let remaining = max.saturating_sub(current);
                        let to_fetch = count.saturating_sub(remaining);

                        let NextValFetchResult {
                            start,
                            batch_size,
                            step,
                        } = fetcher.fetch(&ctx, &catalog, to_fetch).await?;

                        // If we have remaining numbers, use them first
                        if remaining > 0 {
                            // Then add the new batch after the remaining numbers
                            counter.update_batch(start, batch_size);

                            // Return a combined range: first the remaining numbers, then the new ones
                            let mut numbers = Vec::with_capacity(count as usize);

                            // Add the remaining numbers
                            let remaining_to_use = remaining.min(count);
                            numbers.extend(fn_range_collect(
                                current,
                                current + remaining_to_use,
                                step,
                            ));

                            // Add numbers from the new batch if needed
                            if remaining_to_use < count {
                                let new_needed = count - remaining_to_use;
                                numbers.extend(fn_range_collect(start, start + new_needed, step));
                                // Update the counter to reflect that we've used some of the new batch
                                counter.current.store(start + new_needed, Ordering::SeqCst);
                            }

                            UInt64Type::from_data(numbers)
                        } else {
                            // No remaining numbers, just use the new batch
                            counter.update_batch(start + count, batch_size - count);
                            // Return the sequence numbers needed for this request
                            UInt64Type::from_data(fn_range_collect(start, start + count, step))
                        }
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
                AsyncFunctionArgument::ReadFile(read_file_arg) => {
                    let read_file_ctx = self.read_file_ctx.as_mut().ok_or_else(|| {
                        ErrorCode::Internal("read_file context is not initialized".to_string())
                    })?;
                    read_file_ctx
                        .transform_read_file(
                            self.ctx.clone(),
                            &mut data_block,
                            &async_func_desc.arg_indices,
                            &async_func_desc.data_type,
                            read_file_arg,
                        )
                        .await?;
                }
            }
        }
        Ok(data_block)
    }
}
