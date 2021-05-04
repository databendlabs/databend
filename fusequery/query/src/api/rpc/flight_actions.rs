// use common_arrow::arrow_flight::Action;
use common_flights::{QueryDoAction, ExecutePlanWithShuffleAction};
use tonic::{Response, Status};
use crate::api::rpc::{FlightStream, QueryInfo, QueryInfoPtr, Queries, StageInfo};
use crate::pipelines::processors::PipelineBuilder;
use crate::api::rpc::flight_service_new::FuseQueryService;
use common_infallible::RwLock;
use std::sync::Arc;
use std::collections::HashMap;
use tokio::runtime::Runtime;
use common_exception::ErrorCodes;
use futures::FutureExt;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use common_arrow::arrow_flight::FlightData;

pub struct ActionService<T>;

type SResult = Result<Response<FlightStream<FlightResult>>, Status>;

impl ActionService<ExecutePlanWithShuffleAction> {
    fn init_stage(senders: &Vec<FlightDataSender>, stage: &mut StageInfo) {}

    pub fn process(queries: Queries, action: ExecutePlanWithShuffleAction) -> SResult {
        let mut queries_map = queries.write();

        fn build_runtime(max_threads: u64) -> common_exception::Result<Runtime> {
            tokio::runtime::Builder::new_multi_thread()
                .enable_io()
                .worker_threads(max_threads as usize)
                .build()
                .map_err(|tokio_error| ErrorCodes::TokioError(format!("{}", tokio_error)))
        }

        let mut query_info = queries_map
            .entry(action.query_id.clone())
            .or_insert_with(|| {
                Arc::new(QueryInfo {
                    stages: Arc::new(HashMap::new()),
                    runtime: build_runtime(8),
                })
            });

        let (sender, mut receiver) = tokio::sync::mpsc::channel(2);
        type FlightDataSender = tokio::sync::mpsc::Sender<Result<FlightData, Status>>;
        match query_info.stages.entry(action.stage_id.clone()) {
            Occupied(_) => return Err(Status::already_exists(format!("Stage {} already exists.", action.stage_id))),
            Vacant(entry) => {
                let shuffle_senders: Vec<FlightDataSender> = vec![];
                let stage_info = entry.insert(Arc::new(StageInfo { flights: HashMap::new() }));
                Self::init_stage(&shuffle_senders, stage_info);

                query_info.runtime.unwrap().spawn(async move {
                    // wait stage run.
                    let _ = receiver.recv().await;

                    // run stage plan.
                });
            }
        };

        Err(Status::invalid_argument(""))
        // let ctx = session_manager
        //     .try_create_context()
        //     .map_err(|e| Status::internal(e.to_string()))?
        //     .with_cluster(cluster.clone())
        //     .map_err(|e| Status::internal(e.to_string()))?;
        // ctx.set_max_threads(cpus)
        //     .map_err(|e| Status::internal(e.to_string()))?;

        // let cpus = self.conf.num_cpus;
        // let cluster = self.cluster.clone();
        // let session_manager = self.session_manager.clone();


        // info!("Executor[{:?}] received action, job_id: {:?}", self.conf.flight_api_address, action.job_id);


        // Pipeline.
        // let mut pipeline = PipelineBuilder::create(ctx.clone(), plan.clone())
        //     .build()
        //     .map_err(|e| Status::internal(e.to_string()))?;

        // let mut stream = pipeline.execute().await.map_err(|e| Status::internal(e.to_string()))?;

        // tokio::spawn(async move {
        //     let options = arrow::ipc::writer::IpcWriteOptions::default();
        //     let mut has_send = false;
        //     let start = Instant::now();
        //
        //     // Get the batch from the stream and send to one channel.
        //     while let Some(item) = stream.next().await {
        //         let block = match_async_result!(item, sender);
        //         if !has_send {
        //             let schema_flight_data =
        //                 arrow_flight::utils::flight_data_from_arrow_schema(
        //                     block.schema(),
        //                     &options,
        //                 );
        //             sender.send(Ok(schema_flight_data)).await.ok();
        //             has_send = true;
        //         }
        //
        //         // Check block is empty.
        //         if !block.is_empty() {
        //             // Convert batch to flight data.
        //             let batch = match_async_result!(block.try_into(), sender);
        //             let (flight_dicts, flight_batch) =
        //                 arrow_flight::utils::flight_data_from_arrow_batch(&batch, &options);
        //             let batch_flight_data = flight_dicts
        //                 .into_iter()
        //                 .chain(std::iter::once(flight_batch))
        //                 .map(Ok);
        //
        //             for batch in batch_flight_data {
        //                 send_response(&sender, batch.clone()).await.ok();
        //             }
        //         }
        //     }

        // Cost.
        // let delta = start.elapsed();
        // histogram!(super::metrics::METRIC_FLIGHT_EXECUTE_COST, delta);
        //
        // info!("Executor executed cost: {:?}", delta);
        //
        // // Remove the context from the manager.
        // session_manager.try_remove_context(ctx.clone()).ok();
    }
}
