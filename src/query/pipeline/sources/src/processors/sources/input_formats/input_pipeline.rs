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

use std::fmt::Debug;
use std::sync::Arc;

use common_base::base::tokio;
use common_base::base::tokio::sync::mpsc::Receiver;
use common_base::base::tokio::sync::mpsc::Sender;
use common_base::base::GlobalIORuntime;
use common_base::base::TrySpawn;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::Pipeline;
use common_pipeline_core::SourcePipeBuilder;
use futures_util::stream::FuturesUnordered;
use futures_util::AsyncReadExt;
use futures_util::StreamExt;

use crate::processors::sources::input_formats::input_context::InputContext;
use crate::processors::sources::input_formats::input_context::InputPlan;
use crate::processors::sources::input_formats::input_format::SplitInfo;
use crate::processors::sources::input_formats::source_aligner::Aligner;
use crate::processors::sources::input_formats::source_deserializer::DeserializeSource;
use crate::processors::sources::input_formats::transform_deserializer::DeserializeTransformer;

pub struct Split<I: InputFormatPipe> {
    pub(crate) info: SplitInfo,
    pub(crate) rx: Receiver<Result<I::ReadBatch>>,
}

#[allow(unused)]
pub struct StreamingSplit {
    path: String,
    data_tx: Sender<StreamingReadBatch>,
}

pub struct StreamingReadBatch {
    data: Vec<u8>,
    pub(crate) path: String,
    pub(crate) is_start: bool,
}

pub trait AligningStateTrait: Sized {
    type Pipe: InputFormatPipe<AligningState = Self>;

    fn try_create(ctx: &Arc<InputContext>, split_info: &SplitInfo) -> Result<Self>;

    fn align(
        &mut self,
        read_batch: Option<<Self::Pipe as InputFormatPipe>::ReadBatch>,
    ) -> Result<Vec<<Self::Pipe as InputFormatPipe>::RowBatch>>;
}

pub trait BlockBuilderTrait {
    type Pipe: InputFormatPipe<BlockBuilder = Self>;
    fn create(ctx: Arc<InputContext>) -> Self;

    fn deserialize(
        &mut self,
        batch: Option<<Self::Pipe as InputFormatPipe>::RowBatch>,
    ) -> Result<Vec<DataBlock>>;
}

#[async_trait::async_trait]
pub trait InputFormatPipe: Sized + Send + 'static {
    type ReadBatch: From<Vec<u8>> + Send + Debug;
    type RowBatch: Send;
    type AligningState: AligningStateTrait<Pipe = Self> + Send;
    type BlockBuilder: BlockBuilderTrait<Pipe = Self> + Send;

    fn execute_stream(
        ctx: Arc<InputContext>,
        pipeline: &mut Pipeline,
        mut input: Receiver<StreamingReadBatch>,
    ) -> Result<()> {
        let (split_tx, split_rx) = async_channel::bounded(ctx.num_prefetch_splits()?);
        Self::build_pipeline_with_aligner(&ctx, split_rx, pipeline)?;

        tokio::spawn(async move {
            let mut sender: Option<Sender<Result<Self::ReadBatch>>> = None;
            while let Some(batch) = input.recv().await {
                if batch.is_start {
                    let (data_tx, data_rx) = tokio::sync::mpsc::channel(1);
                    sender = Some(data_tx);
                    let split_info = SplitInfo::from_stream_split(batch.path.clone());
                    split_tx
                        .send(Split {
                            info: split_info,
                            rx: data_rx,
                        })
                        .await
                        .expect("fail to send split from stream");
                }
                if let Some(s) = sender.as_mut() {
                    s.send(Ok(batch.data.into()))
                        .await
                        .expect("fail to send read batch from stream");
                }
            }
        });
        Ok(())
    }

    fn execute_copy_with_aligner(ctx: Arc<InputContext>, pipeline: &mut Pipeline) -> Result<()> {
        let (split_tx, split_rx) = async_channel::bounded(ctx.num_prefetch_splits()?);
        Self::build_pipeline_with_aligner(&ctx, split_rx, pipeline)?;

        let ctx_clone = ctx.clone();
        GlobalIORuntime::instance().spawn(async move {
            tracing::debug!("start copy splits feeder");
            for s in &ctx_clone.splits {
                let (data_tx, data_rx) = tokio::sync::mpsc::channel(ctx.num_prefetch_per_split());
                let split_clone = s.clone();
                let ctx_clone2 = ctx_clone.clone();
                tokio::spawn(async move {
                    if let Err(e) =
                        Self::copy_reader_with_aligner(ctx_clone2, split_clone, data_tx).await
                    {
                        tracing::error!("copy split reader error: {:?}", e);
                    } else {
                        tracing::debug!("copy split reader stopped");
                    }
                });
                if split_tx
                    .send(Split {
                        info: s.clone(),
                        rx: data_rx,
                    })
                    .await
                    .is_err()
                {
                    break;
                };
            }
            tracing::info!("end copy splits feeder");
        });

        Ok(())
    }

    fn execute_copy_aligned(ctx: Arc<InputContext>, pipeline: &mut Pipeline) -> Result<()> {
        let (data_tx, data_rx) = async_channel::bounded(ctx.num_prefetch_splits()?);
        Self::build_pipeline_aligned(&ctx, data_rx, pipeline)?;

        let ctx_clone = ctx.clone();
        let p = 3;
        tokio::spawn(async move {
            let mut futs = FuturesUnordered::new();
            for s in &ctx_clone.splits {
                let fut = Self::read_split(ctx_clone.clone(), s.clone());
                futs.push(fut);
                if futs.len() >= p {
                    let row_batch = futs.next().await.unwrap().unwrap();
                    data_tx.send(row_batch).await.unwrap();
                }
            }

            while let Some(row_batch) = futs.next().await {
                data_tx.send(row_batch.unwrap()).await.unwrap();
            }
        });
        Ok(())
    }

    fn build_pipeline_aligned(
        ctx: &Arc<InputContext>,
        row_batch_rx: async_channel::Receiver<Self::RowBatch>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let mut builder = SourcePipeBuilder::create();
        for _ in 0..ctx.settings.get_max_threads()? {
            let output = OutputPort::create();
            let source = DeserializeSource::<Self>::create(
                ctx.clone(),
                output.clone(),
                row_batch_rx.clone(),
            )?;
            builder.add_source(output, source);
        }
        pipeline.add_pipe(builder.finalize());
        Ok(())
    }

    fn build_pipeline_with_aligner(
        ctx: &Arc<InputContext>,
        split_rx: async_channel::Receiver<Split<Self>>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let mut builder = SourcePipeBuilder::create();
        let n_threads = ctx.settings.get_max_threads()? as usize;
        let max_aligner = match ctx.plan {
            InputPlan::CopyInto(_) => ctx.splits.len(),
            InputPlan::StreamingLoad => 3,
        };
        let (row_batch_tx, row_batch_rx) = crossbeam_channel::bounded(n_threads);
        for _ in 0..std::cmp::min(max_aligner, n_threads) {
            let output = OutputPort::create();
            let source = Aligner::<Self>::try_create(
                output.clone(),
                ctx.clone(),
                split_rx.clone(),
                row_batch_tx.clone(),
            )?;
            builder.add_source(output, source);
        }
        pipeline.add_pipe(builder.finalize());
        pipeline.resize(n_threads)?;
        pipeline.add_transform(|input, output| {
            DeserializeTransformer::<Self>::create(ctx.clone(), input, output, row_batch_rx.clone())
        })?;
        Ok(())
    }

    async fn read_split(_ctx: Arc<InputContext>, _split_info: SplitInfo) -> Result<Self::RowBatch> {
        unimplemented!()
    }

    #[tracing::instrument(level = "debug", skip(ctx, batch_tx))]
    async fn copy_reader_with_aligner(
        ctx: Arc<InputContext>,
        split_info: SplitInfo,
        batch_tx: Sender<Result<Self::ReadBatch>>,
    ) -> Result<()> {
        tracing::debug!("start");
        let object = ctx.operator.object(&split_info.file_info.path);
        let offset = split_info.offset as u64;
        let mut reader = object.range_reader(offset..).await?;
        loop {
            let mut batch = vec![0u8; ctx.read_batch_size];
            let n = read_full(&mut reader, &mut batch[0..]).await?;
            if n == 0 {
                break;
            } else {
                batch.truncate(n);
                tracing::debug!("read {} bytes", n);
                batch_tx
                    .send(Ok(batch.into()))
                    .await
                    .map_err(|_| ErrorCode::UnexpectedError("fail to send ReadBatch"))?;
            }
        }
        tracing::debug!("finished");
        Ok(())
    }
}

pub async fn read_full<R: AsyncReadExt + Unpin>(reader: &mut R, buf: &mut [u8]) -> Result<usize> {
    let mut buf = &mut buf[0..];
    let mut n = 0;
    while !buf.is_empty() {
        let read = reader.read(buf).await?;
        if read == 0 {
            break;
        }
        n += read;
        buf = &mut buf[read..]
    }
    Ok(n)
}
