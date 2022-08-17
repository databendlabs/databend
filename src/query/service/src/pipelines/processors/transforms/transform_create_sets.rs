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

use std::any::Any;
use std::sync::Arc;

use common_base::base::tokio::sync::broadcast::Receiver;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::Result;

use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::Processor;

pub enum SubqueryReceiver {
    Subquery(Receiver<DataValue>),
    ScalarSubquery(Receiver<DataValue>),
}

impl SubqueryReceiver {
    pub fn subscribe(&mut self) -> SubqueryReceiver {
        match self {
            SubqueryReceiver::Subquery(rx) => SubqueryReceiver::Subquery(rx.resubscribe()),
            SubqueryReceiver::ScalarSubquery(rx) => {
                SubqueryReceiver::ScalarSubquery(rx.resubscribe())
            }
        }
    }
}

pub struct TransformCreateSets {
    initialized: bool,
    schema: DataSchemaRef,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    input_data: Option<DataBlock>,
    output_data: Option<DataBlock>,

    sub_queries_result: Vec<DataValue>,
    sub_queries_receiver: Vec<SubqueryReceiver>,
}

impl TransformCreateSets {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        schema: DataSchemaRef,
        sub_queries_receiver: Vec<SubqueryReceiver>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(TransformCreateSets {
            schema,
            input,
            output,
            input_data: None,
            initialized: false,
            sub_queries_receiver,
            sub_queries_result: vec![],
            output_data: None,
        })))
    }
}

#[async_trait::async_trait]
impl Processor for TransformCreateSets {
    fn name(&self) -> &'static str {
        "TransformCreateSets"
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if !self.initialized {
            return Ok(Event::Async);
        }

        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(output_data) = self.output_data.take() {
            self.output.push_data(Ok(output_data));
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
        if let Some(input_data) = self.input_data.take() {
            let num_rows = input_data.num_rows();
            let mut new_columns = input_data.columns().to_vec();
            let start_index = self.schema.fields().len() - self.sub_queries_result.len();

            for (index, result) in self.sub_queries_result.iter().enumerate() {
                let data_type = self.schema.field(start_index + index).data_type();
                let col = data_type.create_constant_column(result, num_rows)?;
                new_columns.push(col);
            }

            self.output_data = Some(DataBlock::create(self.schema.clone(), new_columns));
        }

        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        if !self.initialized {
            self.initialized = true;

            let sub_queries_receiver = std::mem::take(&mut self.sub_queries_receiver);
            let mut async_get = Vec::with_capacity(sub_queries_receiver.len());

            for subquery_receiver in sub_queries_receiver.into_iter() {
                async_get.push(async move {
                    match subquery_receiver {
                        SubqueryReceiver::Subquery(mut rx) => rx.recv().await,
                        SubqueryReceiver::ScalarSubquery(mut rx) => rx.recv().await,
                    }
                });
            }

            if let Ok(sub_queries_result) = futures::future::try_join_all(async_get).await {
                self.sub_queries_result = sub_queries_result;
            }
        }

        Ok(())
    }
}
