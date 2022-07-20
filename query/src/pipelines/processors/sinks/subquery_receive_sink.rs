use std::any::Any;
use std::sync::Arc;
use common_base::base::tokio::sync::broadcast::Sender;
use common_datablocks::DataBlock;
use common_datavalues::{DataSchemaRef, DataValue};
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::{Processor, Sink, Sinker};
use common_exception::Result;
use crate::pipelines::processors::processor::{Event, ProcessorPtr};

pub struct SubqueryReceiveSink {
    input_columns: Vec<Vec<DataValue>>,
    sender: Sender<DataValue>,
}

impl SubqueryReceiveSink {
    pub fn try_create(input: Arc<InputPort>, schema: DataSchemaRef, sender: Sender<DataValue>) -> Result<ProcessorPtr> {
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

        if let Err(cause) = self.sender.send(subquery_data_values) {
            println!("Send failure : {:?}", cause);
        }

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
//
// #[async_trait::async_trait]
// impl Processor for SubqueryReceiveSink {
//     fn name(&self) -> &'static str {
//         "SubqueryReceiveSink"
//     }
//
//     fn as_any(&mut self) -> &mut dyn Any {
//         self
//     }
//
//     fn event(&mut self) -> common_exception::Result<Event> {
//         if self.input_data.is_some() {
//             return Ok(Event::Sync);
//         }
//
//         if self.input.is_finished() {
//             return Ok(Event::Async);
//         }
//
//         match self.input.has_data() {
//             true => {
//                 self.input_data = Some(self.input.pull_data().unwrap()?);
//                 self.input.set_need_data();
//                 Ok(Event::Sync)
//             }
//             false => {
//                 self.input.set_need_data();
//                 Ok(Event::NeedData)
//             }
//         }
//     }
//
//     fn process(&mut self) -> Result<()> {
//         if let Some(data_block) = self.input_data.take() {
//             for column_index in 0..data_block.num_columns() {
//                 let column = data_block.column(column_index);
//                 let mut values = column.to_values();
//                 self.input_columns[column_index].append(&mut values)
//             }
//         }
//
//         Ok(())
//     }
//
//     async fn async_process(&mut self) -> Result<()> {
//         let input_columns = std::mem::take(&mut self.input_columns);
//         let mut struct_fields = Vec::with_capacity(input_columns.len());
//
//         for values in input_columns {
//             struct_fields.push(DataValue::Array(values))
//         }
//
//         let subquery_data_values = match struct_fields.len() {
//             1 => struct_fields.remove(0),
//             _ => DataValue::Struct(struct_fields),
//         };
//
//         self.sender.send(subquery_data_values);
//         Ok(())
//     }
// }
