// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::Arc;

use common_arrow::arrow::record_batch::RecordBatch;
use common_arrow::arrow_flight::FlightData;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::Expression;
use common_planners::PlanNode;
use common_runtime::tokio;
use common_runtime::tokio::sync::mpsc::channel;
use common_runtime::tokio::sync::mpsc::Receiver;
use common_runtime::tokio::sync::mpsc::Sender;
use log::error;
use tokio_stream::StreamExt;

use crate::api::rpc::flight_scatter::FlightScatterByHash;
use crate::configs::Config;
use crate::pipelines::processors::Pipeline;
use crate::pipelines::processors::PipelineBuilder;
use crate::sessions::FuseQueryContextRef;
use crate::sessions::SessionMgrRef;

#[derive(Debug)]
pub struct PrepareStageInfo {
    pub query_id: String,
    pub stage_id: String,
    pub plan: PlanNode,
    pub scatters: Vec<String>,
    pub scatters_expression: Expression,
    pub subquery_res_map: HashMap<String, bool>,
}

#[derive(Debug)]
pub struct StreamInfo {
    pub schema: DataSchemaRef,
    pub stream_name: String,
}

pub enum Request {
    GetSchema(String, Sender<Result<DataSchemaRef>>),
    GetStream(String, Sender<Result<Receiver<Result<FlightData>>>>),
    PrepareQueryStage(Box<PrepareStageInfo>, Sender<Result<()>>),
    GetStreamInfo(String, Sender<Result<StreamInfo>>),
    TerminalStage(FuseQueryContextRef, String, String),
}

#[derive(Debug)]
pub struct FlightStreamInfo {
    schema: DataSchemaRef,
    data_receiver: Option<Receiver<Result<FlightData>>>,
    launcher_sender: Sender<()>,
}

pub struct DispatcherState {
    streams: HashMap<String, FlightStreamInfo>,
}

struct ServerState {
    conf: Config,
    session_manager: SessionMgrRef,
}

pub struct FlightDispatcher {
    state: Arc<ServerState>,
}

type DataReceiver = Receiver<Result<FlightData>>;

impl FlightDispatcher {
    pub fn run(&self) -> Sender<Request> {
        let state = self.state.clone();
        let (sender, receiver) = channel::<Request>(100);
        let dispatch_sender = sender.clone();
        tokio::spawn(async move { Self::dispatch(state.clone(), receiver, dispatch_sender).await });
        sender
    }

    #[inline(always)]
    async fn dispatch(
        state: Arc<ServerState>,
        mut receiver: Receiver<Request>,
        request_sender: Sender<Request>,
    ) {
        let mut dispatcher_state = DispatcherState::create();
        while let Some(request) = receiver.recv().await {
            match request {
                Request::GetStream(id, stream_receiver) => {
                    let receiver = Self::do_get_stream(&mut dispatcher_state, &id).await;
                    if let Err(error) = stream_receiver.send(receiver).await {
                        error!("Cannot push: {}", error);
                    }
                }
                Request::GetSchema(id, schema_receiver) => {
                    let schema = Self::get_schema(&mut dispatcher_state, &id).await;
                    if let Err(error) = schema_receiver.send(schema).await {
                        error!("Cannot push: {}", error);
                    }
                }
                Request::PrepareQueryStage(info, response_sender) => {
                    let pipeline = Self::create_plan_pipeline(
                        &*state,
                        &info.plan,
                        info.subquery_res_map.clone(),
                    );
                    let prepared_query = Self::prepare_stage(
                        &mut dispatcher_state,
                        &info,
                        pipeline,
                        request_sender.clone(),
                    );
                    if let Err(error) = response_sender.send(prepared_query).await {
                        error!("Cannot push: {}", error);
                    }
                }
                Request::GetStreamInfo(id, stream_info_receiver) => {
                    let stream_info = Self::get_stream_info(&mut dispatcher_state, &id).await;
                    if let Err(error) = stream_info_receiver.send(stream_info).await {
                        error!("Cannot push: {}", error);
                    }
                }
                Request::TerminalStage(context, query_id, stage_id) => {
                    let stage_stream_prefix = format!("{}/{}", query_id, stage_id);
                    dispatcher_state
                        .streams
                        .retain(|name, _| !name.starts_with(&stage_stream_prefix));

                    if let Err(error) = state.session_manager.try_remove_context(context) {
                        error!("Terminal Stage error: {}", error);
                    }
                }
            };
        }
        // TODO: shutdown
    }

    async fn get_stream_info(state: &mut DispatcherState, id: &str) -> Result<StreamInfo> {
        match state.streams.get(id) {
            Some(info) => Ok(StreamInfo {
                schema: info.schema.clone(),
                stream_name: id.to_string(),
            }),
            None => Err(ErrorCode::NotFoundStream(format!(
                "Stream {} is not found",
                id
            ))),
        }
    }

    async fn get_schema(state: &mut DispatcherState, id: &str) -> Result<DataSchemaRef> {
        match state.streams.get(&id.to_string()) {
            Some(stream_info) => Ok(stream_info.schema.clone()),
            None => Err(ErrorCode::NotFoundStream(format!(
                "Stream {} is not found",
                id
            ))),
        }
    }

    async fn do_get_stream(state: &mut DispatcherState, id: &str) -> Result<DataReceiver> {
        match state.streams.get_mut(&id.to_string()) {
            None => Err(ErrorCode::NotFoundStream(format!(
                "Stream {} is not found",
                id
            ))),
            Some(stream_info) => match stream_info.data_receiver.take() {
                None => Err(ErrorCode::DuplicateGetStream(format!(
                    "Stream {} has been fetched once",
                    id
                ))),
                Some(receiver) => match stream_info.launcher_sender.send(()).await {
                    Ok(_) => Ok(receiver),
                    Err(error) => Err(ErrorCode::TokioError(format!(
                        "Cannot launch query stage: {}",
                        error
                    ))),
                },
            },
        }
    }

    fn prepare_stage(
        state: &mut DispatcherState,
        info: &PrepareStageInfo,
        pipeline: Result<(FuseQueryContextRef, Pipeline)>,
        request_sender: Sender<Request>,
    ) -> Result<()> {
        let scattered_to = &info.scatters;
        let mut streams_data_sender = vec![];
        let (launcher_sender, mut launcher_receiver) = channel(scattered_to.len());
        for stream_name in scattered_to {
            let stream_full_name = format!("{}/{}/{}", info.query_id, info.stage_id, stream_name);
            let (sender, stream_info) =
                FlightStreamInfo::create(&info.plan.schema(), &launcher_sender);
            streams_data_sender.push(sender);
            state.streams.insert(stream_full_name, stream_info);
        }

        let query_id = info.query_id.clone();
        let stage_id = info.stage_id.clone();
        let (context, pipeline) = pipeline?;
        let flight_scatter = FlightScatterByHash::try_create(
            info.plan.schema(),
            info.scatters_expression.clone(),
            streams_data_sender.len(),
        )?;

        let stage_context = context.clone();
        stage_context.execute_task(async move {
            let _ = launcher_receiver.recv().await;

            if let Err(error) =
                Self::receive_data_and_push(pipeline, flight_scatter, streams_data_sender.clone())
                    .await
            {
                for sender in &streams_data_sender {
                    let clone_error =
                        ErrorCode::create(error.code(), error.message(), error.backtrace());

                    if sender.send(Err(clone_error)).await.is_err() {
                        error!("Cannot push: {}", error);
                    }
                }
            }

            for _ in 0..(streams_data_sender.len() - 1) {
                if launcher_receiver.recv().await.is_none() {
                    break;
                }
            }

            // Destroy Query Stage
            let _ = request_sender
                .send(Request::TerminalStage(context, query_id, stage_id))
                .await;
        });

        Ok(())
    }

    fn create_plan_pipeline(
        state: &ServerState,
        plan: &PlanNode,
        subquery_res_map: HashMap<String, bool>,
    ) -> Result<(FuseQueryContextRef, Pipeline)> {
        state
            .session_manager
            .clone()
            .try_create_context()
            .and_then(|ctx| {
                ctx.set_max_threads(state.conf.num_cpus)?;
                PipelineBuilder::create(ctx.clone(), subquery_res_map, plan.clone())
                    .build()
                    .map(move |pipeline| (ctx, pipeline))
            })
    }

    // We need to always use the inline function to ensure that async/await state machine is simple enough
    #[inline(always)]
    async fn receive_data_and_push(
        mut pipeline: Pipeline,
        flight_scatter: FlightScatterByHash,
        senders: Vec<Sender<Result<FlightData>>>,
    ) -> Result<()> {
        use common_arrow::arrow::ipc::writer::IpcWriteOptions;
        use common_arrow::arrow_flight::utils::flight_data_from_arrow_batch;

        let options = IpcWriteOptions::default();
        let mut pipeline_stream = pipeline.execute().await?;

        if senders.len() == 1 {
            while let Some(item) = pipeline_stream.next().await {
                let block = item?;

                let record_batch = block.try_into()?;
                let (dicts, values) = flight_data_from_arrow_batch(&record_batch, &options);
                let normalized_flight_data = dicts.into_iter().chain(std::iter::once(values));
                for flight_data in normalized_flight_data {
                    if let Err(error) = (&senders[0]).send(Ok(flight_data)).await {
                        return Err(ErrorCode::TokioError(format!(
                            "Cannot push data to sender: {}",
                            error
                        )));
                    }
                }
            }
        } else {
            while let Some(item) = pipeline_stream.next().await {
                let block = item?;
                let scattered_data = flight_scatter.execute(&block)?;

                for index in 0..scattered_data.len() {
                    if !scattered_data[index].is_empty() {
                        let scattered_columns = scattered_data[index]
                            .columns()
                            .iter()
                            .map(|column| column.get_array_ref())
                            .collect::<Result<Vec<_>>>()?;

                        let schema = scattered_data[index].schema().clone();
                        let record_batch =
                            RecordBatch::try_new(Arc::new(schema.to_arrow()), scattered_columns)?;
                        let (dicts, values) = flight_data_from_arrow_batch(&record_batch, &options);
                        let normalized_flight_data =
                            dicts.into_iter().chain(std::iter::once(values));

                        for flight_data in normalized_flight_data {
                            if let Err(error) = (&senders[index]).send(Ok(flight_data)).await {
                                return Err(ErrorCode::TokioError(format!(
                                    "Cannot push data to sender: {}",
                                    error
                                )));
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub fn new(conf: Config, session_manager: SessionMgrRef) -> FlightDispatcher {
        FlightDispatcher {
            state: Arc::new(ServerState {
                conf,
                session_manager,
            }),
        }
    }
}

impl DispatcherState {
    pub fn create() -> DispatcherState {
        DispatcherState {
            streams: HashMap::new(),
        }
    }
}

impl FlightStreamInfo {
    pub fn create(
        schema: &DataSchemaRef,
        launcher_sender: &Sender<()>,
    ) -> (Sender<Result<FlightData>>, FlightStreamInfo) {
        // TODO: Back pressure buffer size
        let (sender, receive) = channel(5);
        (sender, FlightStreamInfo {
            schema: schema.clone(),
            data_receiver: Some(receive),
            launcher_sender: launcher_sender.clone(),
        })
    }
}

impl PrepareStageInfo {
    pub fn create(
        query_id: String,
        stage_id: String,
        plan: PlanNode,
        scatters: Vec<String>,
        scatters_expression: Expression,
        subquery_res_map: HashMap<String, bool>,
    ) -> Box<PrepareStageInfo> {
        Box::new(PrepareStageInfo {
            query_id,
            stage_id,
            plan,
            scatters,
            scatters_expression,
            subquery_res_map,
        })
    }
}
