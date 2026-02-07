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
use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_column::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::RemoteExpr;
use databend_common_expression::local_block_meta_serde;
use databend_common_expression::types::DataType;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::string::StringColumn;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;

const NULL_PARTITION_DIR: &str = "_NULL_";

#[derive(Debug)]
pub struct CopyPartitionMeta {
    partition: Arc<str>,
}

impl CopyPartitionMeta {
    pub fn new(partition: Arc<str>) -> Self {
        Self { partition }
    }

    pub fn partition_arc(&self) -> Arc<str> {
        self.partition.clone()
    }
}

local_block_meta_serde!(CopyPartitionMeta);

#[typetag::serde(name = "copy_partition_meta")]
impl BlockMetaInfo for CopyPartitionMeta {
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        CopyPartitionMeta::downcast_ref_from(info).is_some_and(|other| {
            Arc::ptr_eq(&self.partition, &other.partition)
                || self.partition.as_ref() == other.partition.as_ref()
        })
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(Self {
            partition: self.partition.clone(),
        })
    }
}

pub fn partition_from_block(block: &DataBlock) -> Option<Arc<str>> {
    block
        .get_meta()
        .and_then(|meta| CopyPartitionMeta::downcast_ref_from(meta).map(|m| m.partition_arc()))
}

#[derive(Clone)]
pub struct PartitionByRuntime {
    expr: Expr,
    expr_type: DataType,
    func_ctx: Arc<FunctionContext>,
}

impl PartitionByRuntime {
    pub fn try_create(remote_expr: RemoteExpr, func_ctx: FunctionContext) -> Result<Self> {
        let expr = remote_expr.as_expr(&BUILTIN_FUNCTIONS);
        let expr_type = expr.data_type().clone();
        Ok(Self {
            expr,
            expr_type,
            func_ctx: Arc::new(func_ctx),
        })
    }

    pub fn split_block(&self, block: DataBlock) -> Result<Vec<DataBlock>> {
        if block.num_rows() == 0 {
            return Ok(vec![block]);
        }

        let evaluator = Evaluator::new(&block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        let value = evaluator.run(&self.expr).map_err(|err| {
            ErrorCode::Internal(format!(
                "Failed to evaluate PARTITION BY expression: {}",
                err.message()
            ))
        })?;

        let column = value.into_full_column(&self.expr_type, block.num_rows());
        let (strings, validity) = extract_string_column(column)?;

        let mut groups: HashMap<Option<String>, Vec<u32>> = HashMap::new();
        let mut order: Vec<Option<String>> = Vec::new();
        for row in 0..block.num_rows() {
            let key = partition_key(&strings, row, validity.as_ref())?;
            let entry = groups.entry(key.clone()).or_insert_with(|| {
                order.push(key.clone());
                Vec::new()
            });
            entry.push(row as u32);
        }

        let mut result = Vec::with_capacity(groups.len());
        for key in order {
            if let Some(indices) = groups.remove(&key) {
                let partition_arc = key.as_ref().map(|k| Arc::<str>::from(k.as_str()));
                let mut taken = block.clone().take(indices.as_slice())?;
                if let Some(partition) = partition_arc.clone() {
                    let meta = CopyPartitionMeta::new(partition);
                    taken = taken.add_meta(Some(Box::new(meta)))?;
                } else {
                    // ensure we don't leak previous meta from source block
                    let _ = taken.take_meta();
                }
                result.push(taken);
            }
        }
        Ok(result)
    }
}

fn extract_string_column(column: Column) -> Result<(StringColumn, Option<Bitmap>)> {
    match column {
        Column::String(col) => Ok((col, None)),
        Column::Nullable(box NullableColumn { column, validity }) => {
            let inner = column.as_string().cloned().ok_or_else(|| {
                ErrorCode::Internal("PARTITION BY expression must evaluate to STRING".to_string())
            })?;
            Ok((inner, Some(validity)))
        }
        other => Err(ErrorCode::Internal(format!(
            "Unexpected column type for PARTITION BY expression: {}",
            other.data_type()
        ))),
    }
}

fn partition_key(
    strings: &StringColumn,
    row: usize,
    validity: Option<&Bitmap>,
) -> Result<Option<String>> {
    let is_null = validity.map(|bitmap| !bitmap.get_bit(row)).unwrap_or(false);
    if is_null {
        return Ok(Some(NULL_PARTITION_DIR.to_string()));
    }

    let raw = unsafe { strings.value_unchecked(row) };
    sanitize_partition_path(raw)
}

pub fn sanitize_partition_path(raw: &str) -> Result<Option<String>> {
    let normalized = raw.replace('\\', "/");
    let mut parts = Vec::new();
    for segment in normalized.split('/') {
        if segment.is_empty() || segment == "." {
            continue;
        }
        if segment == ".." {
            return Err(ErrorCode::InvalidArgument(
                "PARTITION BY path cannot reference parent directory",
            ));
        }
        parts.push(segment.to_string());
    }

    if parts.is_empty() {
        Ok(None)
    } else {
        Ok(Some(parts.join("/")))
    }
}

pub struct TransformPartitionBy {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    runtime: Arc<PartitionByRuntime>,
    input_data: Option<DataBlock>,
    pending: VecDeque<DataBlock>,
}

impl TransformPartitionBy {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        runtime: Arc<PartitionByRuntime>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(Self {
            input,
            output,
            runtime,
            input_data: None,
            pending: VecDeque::new(),
        })))
    }
}

impl Processor for TransformPartitionBy {
    fn name(&self) -> String {
        "TransformPartitionBy".to_string()
    }

    fn as_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(block) = self.pending.pop_front() {
            self.output.push_data(Ok(block));
            return Ok(Event::NeedConsume);
        }

        if self.input_data.is_some() {
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.input.has_data() {
            self.input_data = Some(self.input.pull_data().unwrap()?);
            return Ok(Event::Sync);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(block) = self.input_data.take() {
            if block.num_rows() == 0 {
                self.pending.push_back(block);
                return Ok(());
            }

            let mut parts = self.runtime.split_block(block)?;
            self.pending.extend(parts.drain(..));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_partition_path() {
        assert_eq!(sanitize_partition_path("").unwrap(), None);
        assert_eq!(
            sanitize_partition_path("foo//bar").unwrap(),
            Some("foo/bar".to_string())
        );
        assert!(sanitize_partition_path("..").is_err());
        assert!(sanitize_partition_path("./a/../b").is_err());
        assert!(sanitize_partition_path("../../evil").is_err());
    }
}
