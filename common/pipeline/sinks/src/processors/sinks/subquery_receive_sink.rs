// Copyright 2022 Datafuse Labs.
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

use common_base::base::tokio::sync::broadcast::Sender;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataValue;
use common_exception::Result;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;

use crate::processors::sinks::Sink;
use crate::processors::sinks::Sinker;

pub struct SubqueryReceiveSink {
    input_columns: Vec<Vec<DataValue>>,
    sender: Sender<DataValue>,
}

impl SubqueryReceiveSink {
    pub fn try_create(
        input: Arc<InputPort>,
        schema: DataSchemaRef,
        sender: Sender<DataValue>,
    ) -> Result<ProcessorPtr> {
        let mut input_columns = Vec::with_capacity(schema.fields().len());

        for _index in 0..schema.fields().len() {
            input_columns.push(vec![]);
        }

        Ok(Sinker::create(input, SubqueryReceiveSink {
            sender,
            input_columns,
        }))
    }
}

impl Sink for SubqueryReceiveSink {
    const NAME: &'static str = "SubqueryReceiveSink";

    fn on_finish(&mut self) -> Result<()> {
        let input_columns = std::mem::take(&mut self.input_columns);
        let mut struct_fields = Vec::with_capacity(input_columns.len());

        for values in input_columns {
            struct_fields.push(DataValue::Array(values))
        }

        let subquery_data_values = match struct_fields.len() {
            1 => struct_fields.remove(0),
            _ => DataValue::Struct(struct_fields),
        };

        self.sender.send(subquery_data_values).ok();

        Ok(())
    }

    fn consume(&mut self, data_block: DataBlock) -> Result<()> {
        for column_index in 0..data_block.num_columns() {
            let column = data_block.column(column_index);
            let mut values = column.to_values();
            self.input_columns[column_index].append(&mut values)
        }

        Ok(())
    }
}
