//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.


use std::any::Any;
use std::ops::Not;
use std::sync::Arc;

use common_catalog::plan::PartInfoPtr;
use common_catalog::table_context::TableContext;
use common_datablocks::serialize_data_blocks;
use common_datablocks::DataBlock;
use common_datavalues::BooleanColumn;
use common_datavalues::ColumnRef;
use common_datavalues::DataSchemaRef;
use common_datavalues::Series;
use common_exception::ErrorCode;
use common_exception::Result;
use common_sql::evaluator::EvalNode;
use common_storages_table_meta::meta::BlockMeta;
use common_storages_table_meta::meta::ClusterStatistics;
use opendal::Operator;

use crate::io::write_data;
use crate::io::BlockReader;
use crate::io::TableMetaLocationGenerator;
use crate::operations::util;
use crate::operations::BloomIndexState;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::Processor;
use crate::pruning::BlockIndex;
use crate::statistics::gen_columns_statistics;
use crate::statistics::ClusterStatsGenerator;
use crate::FuseTable;
use crate::Table;

enum State {
    ReadData(Option<PartInfoPtr>),
    Output(Option<PartInfoPtr>, DataBlock),
    Finish,
}

pub struct UpdateSource {
    state: State,
    ctx: Arc<dyn TableContext>,
    output: Arc<OutputPort>,
}


#[async_trait::async_trait]
impl Processor for UpdateSource {
    fn name(&self) -> String {
        "UpdateSource".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(self.state, State::ReadData(None)) {
            self.state = match self.ctx.try_get_part() {
                None => State::Finish,
                Some(part) => State::ReadData(Some(part)),
            }
        }

        if matches!(self.state, State::Finish) {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if matches!(self.state, State::Output(_, _)) {
            if let State::Output(part, data_block) =
                std::mem::replace(&mut self.state, State::Finish)
            {
                self.state = match part {
                    None => State::Finish,
                    Some(part) => State::ReadData(Some(part)),
                };

                self.output.push_data(Ok(data_block));
                return Ok(Event::NeedConsume);
            }
        }

        todo!()
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }
}
