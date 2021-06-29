use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_infallible::RwLock;
use common_runtime::tokio::sync::*;
use common_streams::AbortStream;
use common_streams::SendableDataBlockStream;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use crate::api::rpc::flight_actions::ShuffleAction;
use crate::api::rpc::flight_scatter::FlightScatterByHash;
use crate::pipelines::processors::Pipeline;
use crate::pipelines::processors::PipelineBuilder;
use crate::sessions::FuseQueryContext;
use crate::sessions::FuseQueryContextRef;
use crate::sessions::SessionRef;

struct StreamInfo {
    schema: DataSchemaRef,
    tx: mpsc::Sender<Result<DataBlock>>,
    rx: mpsc::Receiver<Result<DataBlock>>,
}

pub struct FuseQueryFlightDispatcher {
    streams: Arc<RwLock<HashMap<String, StreamInfo>>>,
    stages_notify: Arc<RwLock<HashMap<String, Arc<Notify>>>>,
}

impl FuseQueryFlightDispatcher {
    pub fn create() -> FuseQueryFlightDispatcher {
        FuseQueryFlightDispatcher {
            streams: Arc::new(RwLock::new(HashMap::new())),
            stages_notify: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn get_stream(
        &self,
        query_id: String,
        stage_id: String,
        stream: String,
    ) -> Result<mpsc::Receiver<Result<DataBlock>>> {
        let stage_name = format!("{}/{}", query_id, stage_id);
        if let Some(notify) = self.stages_notify.write().remove(&stage_name) {
            notify.notify_waiters();
        }

        let stream_name = format!("{}/{}", stage_name, stream);
        match self.streams.write().remove(&stream_name) {
            None => Err(ErrorCode::NotFoundStream(format!(
                "Stream {} is not found",
                stream_name
            ))),
            Some(mut stream_info) => Ok(stream_info.rx),
        }
    }

    // TODO: run_broadcast_action

    pub fn run_shuffle_action(&self, session: SessionRef, action: ShuffleAction) -> Result<()> {
        let schema = action.plan.schema();
        self.create_stage_streams(&action.query_id, &action.stage_id, &schema, &action.sinks);

        match action.sinks.len() {
            0 => Err(ErrorCode::LogicalError("")),
            1 => self.run_action_without_scatters(session, &action),
            _ => self.run_action(
                session,
                &action,
                FlightScatterByHash::try_create(
                    schema.clone(),
                    action.scatters_expression.clone(),
                    action.sinks.len(),
                )?,
            ),
        }
    }

    fn run_action_without_scatters(
        &self,
        session: SessionRef,
        action: &ShuffleAction,
    ) -> Result<()> {
        let query_context = session.try_create_context()?;
        let action_context = FuseQueryContext::new(query_context.clone());
        let mut pipeline =
            PipelineBuilder::create(action_context.clone(), action.plan.clone()).build()?;

        let (stage_notify, tx) = {
            assert_eq!(action.sinks.len(), 1);

            let stage_name = format!("{}/{}", action.query_id, action.stage_id);
            let stage_notify = self.stages_notify.read().get(&stage_name).map(Arc::clone);

            let stream_name = format!("{}/{}", stage_name, action.sinks[0]);
            let tx = self.streams.read().get(&stream_name).map(|x| x.tx.clone());

            match (stage_notify, tx) {
                (Some(stage_notify), Some(tx)) => Ok((stage_notify, tx)),
                _ => Err(ErrorCode::NotFoundStream(format!(
                    "Not found stream {}",
                    stream_name
                ))),
            }
        }?;

        query_context
            .execute_task(async move {
                stage_notify.notified().await;

                let abortable_stream = Self::execute(pipeline, action_context).await;

                match abortable_stream {
                    Err(error) => {
                        tx.send(Err(error)).await.ok();
                    }
                    Ok(mut abortable_stream) => {
                        while let Some(item) = abortable_stream.next().await {
                            if let Err(error) = tx.send(item).await {
                                log::error!(
                                    "Cannot push data when run_action_without_scatters. {}",
                                    error
                                );
                                break;
                            }
                        }
                    }
                };

                drop(session);
            })
            .map(|_| ())
    }

    fn run_action(
        &self,
        session: SessionRef,
        action: &ShuffleAction,
        scatter: FlightScatterByHash,
    ) -> Result<()> {
        let query_context = session.try_create_context()?;
        let action_context = FuseQueryContext::new(query_context.clone());
        let mut pipeline =
            PipelineBuilder::create(action_context.clone(), action.plan.clone()).build()?;

        let (stage_notify, sinks_tx) = {
            assert!(action.sinks.len() > 1);

            let mut sinks_tx = Vec::with_capacity(action.sinks.len());
            let stage_name = format!("{}/{}", action.query_id, action.stage_id);
            let stage_notify = self.stages_notify.write().get(&stage_name).map(Arc::clone);

            for sink in &action.sinks {
                let stream_name = format!("{}/{}", stage_name, sink);
                match self.streams.read().get(&stream_name) {
                    Some(stream) => sinks_tx.push(stream.tx.clone()),
                    None => {
                        return Err(ErrorCode::NotFoundStream(format!(
                            "Not found stream {}",
                            stream_name
                        )))
                    }
                }
            }

            match stage_notify {
                Some(stage_notify) => Ok((stage_notify, sinks_tx)),
                _ => Err(ErrorCode::NotFoundSession(format!(
                    "Not found stream {}",
                    stage_name
                ))),
            }
        }?;

        query_context
            .execute_task(async move {
                stage_notify.notified().await;

                let sinks_tx_ref = &sinks_tx;
                let forward_blocks = async move {
                    let mut abortable_stream = Self::execute(pipeline, action_context).await?;
                    while let Some(item) = abortable_stream.next().await {
                        let forward_blocks = scatter.execute(&item?)?;

                        assert_eq!(forward_blocks.len(), sinks_tx_ref.len());

                        for (index, forward_block) in forward_blocks.iter().enumerate() {
                            let tx = &sinks_tx_ref[index];
                            tx.send(Ok(forward_block.clone()))
                                .await
                                .map_err_to_code(ErrorCode::LogicalError, || {
                                    "Cannot push data when run_action"
                                })?;
                        }
                    }

                    Result::Ok(())
                };

                if let Err(error) = forward_blocks.await {
                    for tx in &sinks_tx {
                        tx.send(Err(ErrorCode::create(
                            error.code(),
                            error.message(),
                            error.backtrace(),
                        )))
                        .await
                        .ok();
                    }
                }

                drop(session);
            })
            .map(|_| ())
    }

    async fn execute(mut pipeline: Pipeline, context: FuseQueryContextRef) -> Result<AbortStream> {
        let data_stream = pipeline.execute().await?;
        context.try_create_abortable(data_stream)
    }

    fn create_stage_streams(
        &self,
        query_id: &str,
        stage_id: &str,
        schema: &DataSchemaRef,
        streams_name: &[String],
    ) {
        let stage_name = format!("{}/{}", query_id, stage_id);
        self.stages_notify
            .write()
            .insert(stage_name.clone(), Arc::new(Notify::new()));

        let mut streams = self.streams.write();

        for stream_name in streams_name {
            let (tx, rx) = mpsc::channel(5);
            let stream_name = format!("{}/{}", stage_name, stream_name);

            streams.insert(stream_name, StreamInfo {
                schema: schema.clone(),
                tx,
                rx,
            });
        }
    }
}
