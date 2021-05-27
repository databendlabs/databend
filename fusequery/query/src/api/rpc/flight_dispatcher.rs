// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::Arc;

use common_arrow::arrow::datatypes::SchemaRef;
use common_arrow::arrow::record_batch::RecordBatch;
use common_arrow::arrow_flight::FlightData;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCodes;
use common_exception::Result;
use common_planners::ExpressionAction;
use common_planners::PlanNode;
use log::error;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio_stream::StreamExt;

use crate::api::rpc::flight_scatter::FlightScatter;
use crate::clusters::ClusterRef;
use crate::configs::Config;
use crate::pipelines::processors::Pipeline;
use crate::pipelines::processors::PipelineBuilder;
use crate::sessions::SessionManagerRef;

#[derive(Debug)]
pub struct PrepareStageInfo(
    pub String,
    pub String,
    pub PlanNode,
    pub Vec<String>,
    pub ExpressionAction
);

#[derive(Debug)]
pub struct StreamInfo(pub DataSchemaRef, pub String, pub Vec<String>);

pub enum Request {
    GetSchema(String, Sender<Result<DataSchemaRef>>),
    GetStream(String, Sender<Result<Receiver<Result<FlightData>>>>),
    PrepareQueryStage(Box<PrepareStageInfo>, Sender<Result<()>>),
    GetStreamInfo(String, Sender<Result<StreamInfo>>),
    TerminalStage(String, String),
}

pub struct QueryInfo {
    pub runtime: Runtime
}

#[derive(Debug)]
pub struct FlightStreamInfo {
    schema: DataSchemaRef,
    data_receiver: Option<Receiver<Result<FlightData>>>,
    launcher_sender: Sender<()>
}

pub struct DispatcherState {
    queries: HashMap<String, QueryInfo>,
    streams: HashMap<String, FlightStreamInfo>
}

struct ServerState {
    conf: Config,
    cluster: ClusterRef,
    session_manager: SessionManagerRef,
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
        request_sender: Sender<Request>
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
                    let pipeline = Self::create_plan_pipeline(&*state, &info.2);
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
                Request::TerminalStage(_, _) => {
                    // let stage_stream_prefix = format!("{}/{}", query_id, stage_id);
                    //
                    // let completed_streams = &dispatcher_state.streams
                    //     .iter()
                    //     .filter(|(id, _)| id.starts_with(&stage_stream_prefix))
                    //     .collect::<Vec<_>>();
                    //
                    // for (stream_id, _) in completed_streams {
                    //     dispatcher_state.streams.remove(stream_id);
                    // }
                    //
                    // let query_stream_prefix = format!("{}/", query_id);
                    // if let None = dispatcher_state.streams
                    //     .iter()
                    //     .filter(|(id, _)| id.starts_with(&query_stream_prefix))
                    //     .next() {
                    //     // Destroy runtime
                    //     println!("Destroy runtime when : {:?}", &dispatcher_state.streams);
                    //
                    //     match dispatcher_state.queries.remove(&query_id) {
                    //         None => {}
                    //         Some(query_info) => {
                    //             query_info.runtime.shutdown_background();
                    //         }
                    //     };
                    // }
                }
            };
        }
        // TODO: shutdown
    }

    async fn get_stream_info(state: &mut DispatcherState, id: &str) -> Result<StreamInfo> {
        match state.streams.get(id) {
            Some(info) => Ok(StreamInfo(info.schema.clone(), id.to_string(), vec![])),
            None => Err(ErrorCodes::NotFoundStream(format!(
                "Stream {} is not found",
                id
            )))
        }
    }

    async fn get_schema(state: &mut DispatcherState, id: &str) -> Result<DataSchemaRef> {
        match state.streams.get(&id.to_string()) {
            Some(stream_info) => Ok(stream_info.schema.clone()),
            None => Err(ErrorCodes::NotFoundStream(format!(
                "Stream {} is not found",
                id
            )))
        }
    }

    async fn do_get_stream(state: &mut DispatcherState, id: &str) -> Result<DataReceiver> {
        match state.streams.get_mut(&id.to_string()) {
            None => Err(ErrorCodes::NotFoundStream(format!(
                "Stream {} is not found",
                id
            ))),
            Some(stream_info) => match stream_info.data_receiver.take() {
                None => Err(ErrorCodes::DuplicateGetStream(format!(
                    "Stream {} has been fetched once",
                    id
                ))),
                Some(receiver) => match stream_info.launcher_sender.send(()).await {
                    Ok(_) => Ok(receiver),
                    Err(error) => Err(ErrorCodes::TokioError(format!(
                        "Cannot launch query stage: {}",
                        error
                    )))
                }
            }
        }
    }

    fn prepare_stage(
        state: &mut DispatcherState,
        info: &PrepareStageInfo,
        pipeline: Result<Pipeline>,
        request_sender: Sender<Request>,
    ) -> Result<()> {
        if !state.queries.contains_key(&info.0) {
            fn build_runtime(max_threads: u64) -> Result<Runtime> {
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(max_threads as usize)
                    .build()
                    .map_err(|tokio_error| ErrorCodes::TokioError(format!("{}", tokio_error)))
            }

            // TODO: max_threads
            match build_runtime(8) {
                Err(e) => return Err(e),
                Ok(runtime) => state.queries.insert(info.0.clone(), QueryInfo { runtime })
            };
        }

        let mut streams_data_sender = vec![];
        let (launcher_sender, mut launcher_receiver) = channel(info.3.len());
        for stream_name in &info.3 {
            let stream_full_name = format!("{}/{}/{}", info.0, info.1, stream_name);
            let (sender, stream_info) =
                FlightStreamInfo::create(&info.2.schema(), &launcher_sender);
            streams_data_sender.push(sender);
            state.streams.insert(stream_full_name, stream_info);
        }

        let query_id = info.0.clone();
        let stage_id = info.1.clone();
        let pipeline = pipeline?;
        let flight_scatter =
            FlightScatter::try_create(info.2.schema(), info.4.clone(), streams_data_sender.len())?;
        let query_info = state
            .queries
            .get_mut(&info.0)
            .expect("No exists query info");

        query_info.runtime.spawn(async move {
            let _ = launcher_receiver.recv().await;

            if let Err(error) =
                Self::receive_data_and_push(pipeline, flight_scatter, streams_data_sender.clone())
                    .await
            {
                for sender in &streams_data_sender {
                    // TODO: backtrace
                    let clone_error = ErrorCodes::create(error.code(), error.message(), None);
                    if sender.send(Err(clone_error)).await.is_err() {
                        // TODO: log to error
                    }
                }
            }

            for _ in 0..(streams_data_sender.len() - 1) {
                if launcher_receiver.recv().await.is_none() {
                    println!("launcher receiver.");
                    break;
                }
            }

            // Destroy Query Stage
            let _ = request_sender.send(Request::TerminalStage(query_id, stage_id)).await;
        });

        Ok(())
    }

    fn create_plan_pipeline(state: &ServerState, plan: &PlanNode) -> Result<Pipeline> {
        state
            .session_manager
            .clone()
            .try_create_context()
            .and_then(|ctx| ctx.with_cluster(state.cluster.clone()))
            .and_then(|ctx| {
                ctx.set_max_threads(state.conf.num_cpus)?;
                PipelineBuilder::create(ctx, plan.clone()).build()
            })
    }

    // We need to always use the inline function to ensure that async/await state machine is simple enough
    #[inline(always)]
    async fn receive_data_and_push(
        mut pipeline: Pipeline,
        flight_scatter: FlightScatter,
        senders: Vec<Sender<Result<FlightData>>>
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
                        return Err(ErrorCodes::TokioError(format!(
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
                            .map(|column| column.to_array())
                            .collect::<Result<Vec<_>>>()?;

                        let schema = scattered_data[index].schema().clone();
                        let record_batch = RecordBatch::try_new(schema, scattered_columns)?;
                        let (dicts, values) = flight_data_from_arrow_batch(&record_batch, &options);
                        let normalized_flight_data =
                            dicts.into_iter().chain(std::iter::once(values));

                        for flight_data in normalized_flight_data {
                            if let Err(error) = (&senders[index]).send(Ok(flight_data)).await {
                                return Err(ErrorCodes::TokioError(format!(
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

    pub fn new(
        conf: Config,
        cluster: ClusterRef,
        session_manager: SessionManagerRef
    ) -> FlightDispatcher {
        FlightDispatcher {
            state: Arc::new(ServerState {
                conf,
                cluster,
                session_manager
            })
        }
    }
}

impl DispatcherState {
    pub fn create() -> DispatcherState {
        DispatcherState {
            queries: HashMap::new(),
            streams: HashMap::new()
        }
    }
}

impl FlightStreamInfo {
    pub fn create(
        schema: &SchemaRef,
        launcher_sender: &Sender<()>
    ) -> (Sender<Result<FlightData>>, FlightStreamInfo) {
        // TODO: Back pressure buffer size
        let (sender, receive) = channel(5);
        (sender, FlightStreamInfo {
            schema: schema.clone(),
            data_receiver: Some(receive),
            launcher_sender: launcher_sender.clone()
        })
    }
}

impl PrepareStageInfo {
    pub fn create(
        query_id: String,
        stage_id: String,
        plan_node: PlanNode,
        scatters: Vec<String>,
        scatters_action: ExpressionAction,
    ) -> Box<PrepareStageInfo> {
        Box::new(PrepareStageInfo(
            query_id,
            stage_id,
            plan_node,
            scatters,
            scatters_action,
        ))
    }
}
