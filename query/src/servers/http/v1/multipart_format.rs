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

use std::mem::replace;
use std::sync::Arc;

use common_base::base::tokio::io::AsyncReadExt;
use common_base::base::tokio::sync::mpsc::Receiver;
use common_base::base::tokio::sync::mpsc::Sender;
use common_base::base::Progress;
use common_base::base::ProgressValues;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use poem::web::Multipart;

use crate::formats::FormatFactory;
use crate::formats::InputFormat;
use crate::formats::InputState;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::Event;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::Processor;
use crate::servers::http::v1::sequential_format_source::{MultipartWorker, SequentialInputFormatSource};
use crate::sessions::QueryContext;

pub struct MultipartFormat;

impl MultipartFormat {
    pub fn input_sources(
        name: &str,
        ctx: Arc<QueryContext>,
        multipart: Multipart,
        schema: DataSchemaRef,
        settings: FormatSettings,
        ports: Vec<Arc<OutputPort>>,
    ) -> Result<(MultipartWorker, Vec<ProcessorPtr>)> {
        let input_format = FormatFactory::instance().get_input(name, schema, settings)?;

        if ports.len() != 1 || input_format.support_parallel() {
            return Err(ErrorCode::UnImplement(
                "Unimplemented parallel input format.",
            ));
        }

        let (tx, rx) = common_base::base::tokio::sync::mpsc::channel(2);

        Ok((
            MultipartWorker::create(multipart, tx),
            vec![SequentialInputFormatSource::create(
                ports[0].clone(),
                input_format,
                rx,
                ctx.get_scan_progress(),
            )?],
        ))
    }
}


