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

use std::sync::Arc;

use bumpalo::Bump;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::DataBlock;
use databend_common_expression::HashTableConfig;
use databend_common_expression::ProbeState;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_expression::aggregate_function::AggregateFunctionRef;
use databend_common_expression::aggregate_function::AggregateFunctionRequest;
use databend_common_expression::is_stream_column;
use databend_common_expression::types::DataType;
use databend_common_functions::aggregates::AGGR_REGISTRY;
use databend_common_meta_app::schema::MATERIALIZED_VIEW_SOURCE_ROW_ID_COLUMN;
use databend_common_meta_app::schema::is_materialized_view_engine;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_pipeline_transforms::processors::Transform;

/// Merge equal group keys inside one newly written physical block.
///
/// Aggregating materialized views, including `GROUP BY` without aggregate
/// functions, do not persist `_mv_source_row_id`. Compact/recluster already
/// gathers neighboring rows into the same block. Re-aggregating that block
/// collapses duplicate groups without rewriting the whole materialized view.
/// Non-aggregate MVs keep `_mv_source_row_id` and skip this transform.
///
/// The output preserves the input column layout: the materialized view
/// physical schema always stores aggregate state columns before group
/// columns, matching the `[states..., groups...]` layout produced by
/// `Payload::aggregate_flush`, and evaluated cluster key columns stay
/// appended after the table columns as trailing group keys.
///
/// The output row order is unspecified: rows are flushed in hash-table
/// order. Write paths serializing clustered data must sort each block
/// afterwards, as compact does inside `cluster_gen_for_append` and recluster
/// does with an explicit partial sort before `TransformSerializeBlock`.
#[derive(Clone)]
pub struct TransformReaggregateAggregateStateBlock {
    table_column_count: usize,
    state_indices: Vec<usize>,
    group_indices: Vec<usize>,
    functions: Vec<AggregateFunctionRef>,
    group_types: Vec<DataType>,
}

impl TransformReaggregateAggregateStateBlock {
    pub fn try_create(table_schema: &TableSchema) -> Result<Option<Self>> {
        if table_schema.fields().iter().any(|field| {
            is_stream_column(field.name()) || field.name() == MATERIALIZED_VIEW_SOURCE_ROW_ID_COLUMN
        }) {
            return Ok(None);
        }

        let mut state_indices = Vec::new();
        let mut group_indices = Vec::new();
        let mut functions = Vec::new();
        let mut group_types = Vec::new();

        for (idx, field) in table_schema.fields().iter().enumerate() {
            let data_type = field.data_type();
            match data_type.remove_nullable() {
                TableDataType::AggregateState {
                    function_name,
                    params,
                    argument_types,
                    ..
                } => {
                    let params = &params.into_iter().map(Scalar::from).collect::<Vec<_>>();
                    let args_type = &argument_types
                        .iter()
                        .map(DataType::from)
                        .collect::<Vec<_>>();
                    let function = AGGR_REGISTRY.resolve(AggregateFunctionRequest {
                        name: &function_name,
                        params,
                        args_type,
                        distinct: false,
                        order_by: &[],
                    })?;
                    state_indices.push(idx);
                    functions.push(function);
                }
                _ => {
                    group_indices.push(idx);
                    group_types.push(DataType::from(data_type));
                }
            }
        }
        if group_indices.is_empty() && state_indices.is_empty() {
            return Ok(None);
        }

        Ok(Some(Self {
            table_column_count: table_schema.num_fields(),
            state_indices,
            group_indices,
            functions,
            group_types,
        }))
    }
}

impl Transform for TransformReaggregateAggregateStateBlock {
    const NAME: &'static str = "TransformReaggregateAggregateStateBlock";
    const SKIP_EMPTY_DATA_BLOCK: bool = false;

    fn transform(&mut self, mut block: DataBlock) -> Result<DataBlock> {
        if block.is_empty() || block.num_rows() <= 1 {
            return Ok(block);
        }
        if block.num_columns() < self.table_column_count {
            return Err(ErrorCode::Internal(format!(
                "aggregate state reaggregate expected at least {} columns, got {}",
                self.table_column_count,
                block.num_columns()
            )));
        }

        let extra_key_indices: Vec<usize> =
            (self.table_column_count..block.num_columns()).collect();
        let mut grouping = self.group_indices.clone();
        grouping.extend_from_slice(&extra_key_indices);

        let group_columns = grouping
            .iter()
            .map(|&index| block.get_by_offset(index).clone())
            .collect::<Vec<_>>();
        let state_columns = self
            .state_indices
            .iter()
            .map(|&index| block.get_by_offset(index).clone())
            .collect::<Vec<_>>();

        let mut group_types = self.group_types.clone();
        for &index in &extra_key_indices {
            group_types.push(block.get_by_offset(index).data_type());
        }

        let mut hashtable = AggregateHashTable::new(
            group_types,
            self.functions.clone(),
            HashTableConfig::default(),
            Arc::new(Bump::new()),
        );
        let mut probe = ProbeState::default();
        hashtable.add_groups(
            &mut probe,
            (&group_columns).into(),
            &[],
            (&state_columns).into(),
            block.num_rows(),
        )?;
        hashtable
            .payload
            .aggregate_flush_all()?
            .add_meta(block.take_meta())
    }
}

/// Compact and recluster already gather neighboring rows into one physical block.
/// Re-aggregate that block so equal group keys collapse before serialization.
///
/// Non-aggregate materialized views keep `_mv_source_row_id` and are left unchanged.
///
/// Returns whether the transform was added. Re-aggregation flushes rows in
/// hash-table order, so write paths that do not already sort each block
/// before serialization (recluster: serialize-time cluster statistics read
/// the first and last rows of a sorted block) must re-establish the
/// cluster-key order afterwards.
pub fn add_aggregate_state_reaggregate_transform(
    pipeline: &mut Pipeline,
    engine: &str,
    table_schema: &TableSchema,
) -> Result<bool> {
    if !is_materialized_view_engine(engine) {
        return Ok(false);
    }
    let Some(transform) = TransformReaggregateAggregateStateBlock::try_create(table_schema)? else {
        return Ok(false);
    };
    pipeline.add_transformer(move || transform.clone());
    Ok(true)
}

#[cfg(test)]
mod tests {
    use databend_common_expression::FromData;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::infer_schema_type;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::StringType;
    use databend_common_pipeline_transforms::processors::Transform;

    use super::*;

    fn sum_state_block(groups: &[&str], values: &[i32]) -> Result<DataBlock> {
        let function = AGGR_REGISTRY.resolve(AggregateFunctionRequest {
            name: "sum_state",
            params: &[],
            args_type: &[DataType::Number(NumberDataType::Int32)],
            distinct: false,
            order_by: &[],
        })?;
        let input = DataBlock::new_from_columns(vec![
            Int32Type::from_data(values.to_vec()),
            StringType::from_data(
                groups
                    .iter()
                    .map(|group| group.to_string())
                    .collect::<Vec<_>>(),
            ),
        ]);
        let mut hashtable = AggregateHashTable::new(
            vec![DataType::String],
            vec![function],
            HashTableConfig::default(),
            Arc::new(Bump::new()),
        );
        let mut probe = ProbeState::default();
        hashtable.add_groups(
            &mut probe,
            (&[input.get_by_offset(1).clone()]).into(),
            &[(&[input.get_by_offset(0).clone()]).into()],
            (&[]).into(),
            values.len(),
        )?;
        hashtable.payload.aggregate_flush_all()
    }

    #[test]
    fn reaggregate_merges_duplicate_group_keys() -> Result<()> {
        let function = AGGR_REGISTRY.resolve(AggregateFunctionRequest {
            name: "sum_state",
            params: &[],
            args_type: &[DataType::Number(NumberDataType::Int32)],
            distinct: false,
            order_by: &[],
        })?;
        let schema = TableSchema::new(vec![
            TableField::new(
                "total",
                infer_schema_type(&function.signature().return_type)?,
            ),
            TableField::new("category", TableDataType::String),
        ]);
        let mut transform = TransformReaggregateAggregateStateBlock::try_create(&schema)?
            .expect("aggregate state schema should build a reaggregate transform");
        let first = sum_state_block(&["a"], &[1])?;
        let second = sum_state_block(&["a"], &[2])?;
        let input = DataBlock::concat(&[first, second])?;
        assert_eq!(input.num_rows(), 2);
        let output = transform.transform(input)?;
        assert_eq!(output.num_rows(), 1);
        assert_eq!(output.num_columns(), 2);
        Ok(())
    }

    #[test]
    fn reaggregate_preserves_column_layout() -> Result<()> {
        // Materialized view physical schemas store aggregate state columns
        // before group columns; the transform output must keep that layout.
        let function = AGGR_REGISTRY.resolve(AggregateFunctionRequest {
            name: "sum_state",
            params: &[],
            args_type: &[DataType::Number(NumberDataType::Int32)],
            distinct: false,
            order_by: &[],
        })?;
        let schema = TableSchema::new(vec![
            TableField::new(
                "total",
                infer_schema_type(&function.signature().return_type)?,
            ),
            TableField::new("category", TableDataType::String),
        ]);
        let mut transform = TransformReaggregateAggregateStateBlock::try_create(&schema)?
            .expect("aggregate state schema should build a reaggregate transform");

        // Two runs of the same group keys, concatenated the way a
        // recluster-merged block interleaves rows from different source blocks.
        let groups: Vec<String> = (0..64).map(|i| format!("key_{i:03}")).collect();
        let group_refs: Vec<&str> = groups.iter().map(|group| group.as_str()).collect();
        let values_a: Vec<i32> = (0..64).collect();
        let values_b: Vec<i32> = (0..64).map(|i| i * 10).collect();
        let first = sum_state_block(&group_refs, &values_a)?;
        let second = sum_state_block(&group_refs, &values_b)?;
        let input = DataBlock::concat(&[first, second])?;
        assert_eq!(input.num_rows(), 128);
        // Flushed state columns use the state's physical serialization type.
        let state_type = input.get_by_offset(0).data_type();

        let output = transform.transform(input)?;
        assert_eq!(output.num_rows(), 64);
        assert_eq!(output.num_columns(), 2);
        // Column layout preserved: state column first, group column second.
        assert_eq!(output.get_by_offset(0).data_type(), state_type);
        assert_eq!(output.get_by_offset(1).data_type(), DataType::String);
        // The flushed row order is unspecified; compare the group key set.
        let group_column = output.get_by_offset(1).clone().into_column().unwrap();
        let mut flushed: Vec<String> = (0..output.num_rows())
            .map(|row| group_column.index(row).unwrap().to_string())
            .collect();
        flushed.sort();
        let expected: Vec<String> = groups.iter().map(|group| format!("'{group}'")).collect();
        assert_eq!(flushed, expected);
        Ok(())
    }

    #[test]
    fn non_aggregate_schema_skips_reaggregate() -> Result<()> {
        let schema = TableSchema::new(vec![
            TableField::new("item_id", TableDataType::Number(NumberDataType::Int32)),
            TableField::new(
                "doubled_amount",
                TableDataType::Number(NumberDataType::Int32),
            ),
            TableField::new(
                "_mv_source_row_id",
                TableDataType::Number(NumberDataType::UInt64),
            ),
        ]);
        assert!(TransformReaggregateAggregateStateBlock::try_create(&schema)?.is_none());
        Ok(())
    }

    #[test]
    fn group_only_schema_reaggregates_duplicate_keys() -> Result<()> {
        let schema = TableSchema::new(vec![TableField::new("category", TableDataType::String)]);
        let mut transform = TransformReaggregateAggregateStateBlock::try_create(&schema)?
            .expect("group-only schema should build a reaggregate transform");
        let input = DataBlock::new_from_columns(vec![StringType::from_data(vec![
            "a".to_string(),
            "a".to_string(),
            "b".to_string(),
        ])]);
        let output = transform.transform(input)?;
        assert_eq!(output.num_rows(), 2);
        assert_eq!(output.num_columns(), 1);
        Ok(())
    }

    #[test]
    fn nullable_group_schema_preserves_group_type() -> Result<()> {
        let schema = TableSchema::new(vec![TableField::new(
            "category",
            TableDataType::Nullable(Box::new(TableDataType::String)),
        )]);
        let mut transform = TransformReaggregateAggregateStateBlock::try_create(&schema)?
            .expect("nullable group schema should build a reaggregate transform");
        let input = DataBlock::new_from_columns(vec![StringType::from_opt_data(vec![
            Some("a"),
            Some("a"),
            None,
            None,
        ])]);
        let output = transform.transform(input)?;
        assert_eq!(output.num_rows(), 2);
        assert_eq!(
            output.get_by_offset(0).data_type(),
            DataType::Nullable(Box::new(DataType::String))
        );
        Ok(())
    }
}
