// use std::collections::hash_map::Entry::{Occupied, Vacant};
// use std::collections::HashMap;
// use std::convert::TryInto;
// use std::marker::PhantomData;
// use std::sync::Arc;
//
// use tokio::runtime::Runtime;
// use tokio::sync::mpsc::{Receiver, Sender};
// use tokio_stream::StreamExt;
// use tonic::Status;
//
// use common_arrow::arrow_flight::FlightData;
// use common_exception::ErrorCodes;
// use common_exception::Result;
// use common_flights::ExecutePlanWithShuffleAction;
// use common_planners::PlanNode;
//
// use crate::api::rpc::{QueryInfo, QueryInfoPtr, StageInfo};
// use crate::api::rpc::flight_service_new::FuseQueryService;
// use crate::pipelines::processors::{Pipeline, PipelineBuilder};
//
// use super::StagePtr;
// use common_infallible::RwLock;
//
// pub struct ActionService<T> {
//     ss: PhantomData<T>
// }
//
// type SResult = Result<()>;
//
// impl ActionService<ExecutePlanWithShuffleAction> {
//     pub fn process(service: &FuseQueryService, action: ExecutePlanWithShuffleAction) -> SResult {
//         let mut queries_map = service.queries.write();
//
//         fn build_runtime(max_threads: u64) -> Result<Runtime> {
//             tokio::runtime::Builder::new_multi_thread()
//                 .enable_io()
//                 .worker_threads(max_threads as usize)
//                 .build()
//                 .map_err(|tokio_error| ErrorCodes::TokioError(format!("{}", tokio_error)))
//         }
//
//         let query_info = queries_map
//             .entry(action.query_id.clone())
//             .or_insert_with(|| {
//                 Arc::new(QueryInfo {
//                     stages: Arc::new(RwLock::new(HashMap::new())),
//                     runtime: build_runtime(8),
//                 })
//             });
//
//         match query_info.stages.write().entry(action.stage_id.clone()) {
//             Occupied(_) => return Err(ErrorCodes::StageExists(format!("Stage {} already exists.", action.stage_id))),
//             Vacant(entry) => {
//                 let (sender, mut receiver) = tokio::sync::mpsc::channel::<()>(action.shuffle_to.len());
//                 let stage_info = entry.insert(StageInfo::create(sender));
//                 Self::dispatch_action(service, query_info, stage_info, receiver, action)
//             }
//         }
//     }
//
//     fn create_plan_pipeline(service: &FuseQueryService, plan: &PlanNode) -> Result<Pipeline> {
//         let ctx = service.session_manager.try_create_context()?;
//
//         // .with_cluster(cluster.clone())
//         // .map_err(|e| Status::internal(e.to_string()))?;
//         let cpus = service.server_config.num_cpus;
//
//         ctx.set_max_threads(cpus)?;
//
//         // let cluster = self.cluster.clone();
//         // let session_manager = self.session_manager.clone();
//
//
//         // info!("Executor[{:?}] received action, job_id: {:?}", self.conf.flight_api_address, action.job_id);
//
//
//         // Pipeline.
//         PipelineBuilder::create(ctx.clone(), plan.clone()).build()
//     }
//
//     // We need to always use the inline function to ensure that async/await state machine is simple enough
//     #[inline(always)]
//     async fn receive_data_and_push(action_pipeline: Result<Pipeline>) -> Result<()> {
//         use common_arrow::arrow::ipc::writer::IpcWriteOptions;
//         use common_arrow::arrow_flight::utils::flight_data_from_arrow_batch;
//
//         let options = IpcWriteOptions::default();
//         let mut pipeline = action_pipeline?;
//         let mut pipeline_stream = pipeline.execute().await?;
//         while let Some(item) = pipeline_stream.next().await {
//             let block = item?;
//
//             // TODO: split block and push to sender
//             let record_batch = block.try_into()?;
//             let (dicts, values) = flight_data_from_arrow_batch(&record_batch, &options);
//             let normalized_flight_data = dicts.into_iter().chain(std::iter::once(values));
//             for flight_data in normalized_flight_data {
//                 // TODO: push to sender
//             }
//         }
//
//         Ok(())
//     }
//
//     fn dispatch_action(
//         service: &FuseQueryService,
//         query_info: &QueryInfoPtr,
//         stage_info: &mut StagePtr,
//         mut receiver: Receiver<()>,
//         action: ExecutePlanWithShuffleAction,
//     ) -> SResult {
//         let action_pipeline = Self::create_plan_pipeline(service, &action.plan);
//
//         let mut streams_data_sender: Vec<Sender<std::result::Result<FlightData, Status>>> = vec![];
//         for stream_location in action.shuffle_to {
//             let (sender, rr) = tokio::sync::mpsc::channel(2);
//             streams_data_sender.push(sender.clone());
//             stage_info.flights.insert(stream_location, rr);
//         }
//
//         query_info.runtime.unwrap().spawn(async move {
//             // wait stage run.
//             let _ = receiver.recv().await;
//
//             match Self::receive_data_and_push(action_pipeline).await {
//                 Ok(_) => Ok(()),
//                 Err(error_code) => Err(Status::internal(error_code.to_string()))
//             };
//
//             // for stream_data_sender in streams_data_sender {
//             //     stream_data_sender.send(last_result.clone()).await;
//             // }
//         });
//
//         Ok(())
//     }
// }
