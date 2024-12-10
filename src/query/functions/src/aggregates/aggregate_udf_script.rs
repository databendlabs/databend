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
use std::fmt;

use databend_common_exception::Result;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::InputColumns;
use databend_common_expression::StateAddr;

use crate::aggregates::AggregateFunction;

pub struct AggregateUdf;

impl AggregateFunction for AggregateUdf {
    fn name(&self) -> &str {
        "udf" // todo
    }

    fn return_type(&self) -> Result<DataType> {
        todo!()
    }

    fn init_state(&self, place: StateAddr) {
        todo!()
    }

    fn state_layout(&self) -> std::alloc::Layout {
        todo!()
    }

    fn accumulate(
        &self,
        _place: StateAddr,
        _columns: InputColumns,
        _validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        todo!()
    }

    fn accumulate_row(&self, _place: StateAddr, _columns: InputColumns, _row: usize) -> Result<()> {
        todo!()
    }

    fn serialize(&self, _place: StateAddr, _writer: &mut Vec<u8>) -> Result<()> {
        todo!()
    }

    fn merge(&self, _place: StateAddr, _reader: &mut &[u8]) -> Result<()> {
        todo!()
    }

    fn merge_states(&self, _place: StateAddr, _rhs: StateAddr) -> Result<()> {
        todo!()
    }

    fn merge_result(&self, _place: StateAddr, _builder: &mut ColumnBuilder) -> Result<()> {
        todo!()
    }
}

impl fmt::Display for AggregateUdf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "udf")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::ArrayRef;
    use arrow_array::Int32Array;
    use arrow_array::RecordBatch;
    use arrow_cast::pretty::pretty_format_columns;
    use arrow_schema::DataType;
    use arrow_schema::Field;
    use arrow_schema::Schema;
    use arrow_udf_js::CallMode;

    use super::*;

    #[test]
    fn test_weighted_avg() {
        let mut runtime = arrow_udf_js::Runtime::new().unwrap();
        runtime
            .add_aggregate(
                "weighted_avg",
                DataType::Struct(
                    vec![
                        Field::new("sum", DataType::Int32, false),
                        Field::new("weight", DataType::Int32, false),
                    ]
                    .into(),
                ),
                DataType::Float32,
                CallMode::ReturnNullOnNullInput,
                r#"
                export function create_state() {
                    return {sum: 0, weight: 0};
                }
                export function accumulate(state, value, weight) {
                    state.sum += value * weight;
                    state.weight += weight;
                    return state;
                }
                export function retract(state, value, weight) {
                    state.sum -= value * weight;
                    state.weight -= weight;
                    return state;
                }
                export function merge(state1, state2) {
                    state1.sum += state2.sum;
                    state1.weight += state2.weight;
                    return state1;
                }
                export function finish(state) {
                    return state.sum / state.weight;
                }
    "#,
            )
            .unwrap();

        let schema = Schema::new(vec![
            Field::new("value", DataType::Int32, true),
            Field::new("weight", DataType::Int32, true),
        ]);
        let arg0 = Int32Array::from(vec![Some(1), None, Some(3), Some(5)]);
        let arg1 = Int32Array::from(vec![Some(2), None, Some(4), Some(6)]);
        let input =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

        let state = runtime.create_state("weighted_avg").unwrap();
        check_array(
            std::slice::from_ref(&state),
            r#"+---------------------+
| array               |
+---------------------+
| {sum: 0, weight: 0} |
+---------------------+"#,
        );

        let state = runtime.accumulate("weighted_avg", &state, &input).unwrap();
        check_array(
            std::slice::from_ref(&state),
            r#"+-----------------------+
| array                 |
+-----------------------+
| {sum: 44, weight: 12} |
+-----------------------+"#,
        );

        let states = arrow_select::concat::concat(&[&state, &state]).unwrap();
        let state = runtime.merge("weighted_avg", &states).unwrap();
        check_array(
            std::slice::from_ref(&state),
            r#"+-----------------------+
| array                 |
+-----------------------+
| {sum: 88, weight: 24} |
+-----------------------+"#,
        );

        let output = runtime.finish("weighted_avg", &state).unwrap();
        check_array(
            &[output],
            r#"+-----------+
| array     |
+-----------+
| 3.6666667 |
+-----------+"#,
        );
    }

    /// Compare the actual output with the expected output.
    #[track_caller]
    fn check_array(actual: &[ArrayRef], expect: &str) {
        assert_eq!(
            expect,
            &pretty_format_columns("array", actual).unwrap().to_string(),
        );
    }
}
