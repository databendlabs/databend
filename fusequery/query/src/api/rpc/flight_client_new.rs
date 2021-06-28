// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow::datatypes::SchemaRef;
use common_arrow::arrow_flight::flight_service_client::FlightServiceClient;
use common_arrow::arrow_flight::Action;
use common_arrow::arrow_flight::Ticket;
use common_exception::ErrorCode;
use common_exception::Result;
use common_runtime::tokio::time::Duration;
use common_streams::SendableDataBlockStream;
use tonic::transport::channel::Channel;
use tonic::Request;

use crate::api::rpc::actions::ExecutePlanWithShuffleAction;
use crate::api::rpc::flight_data_stream::FlightDataStream;
use crate::api::rpc::from_status;

pub struct FlightClient {
    inner: FlightServiceClient<Channel>,
}

// TODO: Integration testing required
impl FlightClient {
    pub fn new(inner: FlightServiceClient<Channel>) -> FlightClient {
        FlightClient { inner }
    }

    pub async fn fetch_stream(
        &mut self,
        name: String,
        schema: SchemaRef,
        timeout: u64,
    ) -> Result<SendableDataBlockStream> {
        self.do_get(
            Ticket {
                ticket: name.as_bytes().to_vec(),
            },
            schema,
            timeout,
        )
        .await
    }

    pub async fn prepare_query_stage(
        &mut self,
        action: ExecutePlanWithShuffleAction,
        timeout: u64,
    ) -> Result<()> {
        self.do_action(
            Action {
                r#type: "PrepareQueryStage".to_string(),
                body: serde_json::to_string(&action)?.as_bytes().to_vec(),
            },
            timeout,
        )
        .await?;

        Ok(())
    }

    // Execute do_get.
    async fn do_get(
        &mut self,
        ticket: Ticket,
        schema: SchemaRef,
        timeout: u64,
    ) -> Result<SendableDataBlockStream> {
        let mut request = Request::new(ticket);
        request.set_timeout(Duration::from_secs(timeout));

        let response = self.inner.do_get(request).await.map_err(from_status);

        Ok(Box::pin(FlightDataStream::from_remote(
            schema,
            response?.into_inner(),
        )))
    }

    // Execute do_action.
    async fn do_action(&mut self, action: Action, timeout: u64) -> Result<Vec<u8>> {
        let action_type = action.r#type.clone();
        let mut request = Request::new(action);
        request.set_timeout(Duration::from_secs(timeout));

        let response = self.inner.do_action(request).await.map_err(from_status);

        match response?
            .into_inner()
            .message()
            .await
            .map_err(from_status)?
        {
            Some(response) => Ok(response.body),
            None => Result::Err(ErrorCode::EmptyDataFromServer(format!(
                "Can not receive data from flight server, action: {:?}",
                action_type
            ))),
        }
    }

    //async fn do_put(&self, request: StreamRequest<FlightData>) -> Response<Self::DoPutStream> {
    //    Result::Err(Status::unimplemented(
    //        "FuseQuery does not implement do_put.",
    //    ))
    //}
    /*
    type DoPutStream = FlightStream<PutResult>;
    async fn do_put_subquery_res(
        &self,
        scheme_ref: SchemaRef,
        mut block_stream: SendableDataBlockStream,
    ) -> Result<Vec<u8>> {

       let ipc_write_opt = IpcWriteOptions::default();
        let flight_schema = flight_data_from_arrow_schema(&scheme_ref, &ipc_write_opt);
        let (mut tx, flight_stream) = futures::channel::mpsc::channel(100);

        tx.send(flight_schema).await?;

        tokio::spawn(async move {
            while let Some(block) = block_stream.next().await {
                info!("next data block");
                match RecordBatch::try_from(block) {
                    Ok(batch) => {
                        if let Err(_e) = tx
                            .send(flight_data_from_arrow_batch(&batch, &ipc_write_opt).1)
                            .await
                        {
                            log::error!("failed to send flight-data to downstream, breaking out");
                            break;
                        }
                    }
                    Err(e) => {
                        log::error!(
                            "failed to convert DataBlock to RecordBatch , breaking out, {:?}",
                            e
                        );
                        break;
                    }
                }
            }
        });

        let mut req = Request::new(flight_stream);
        //let meta = req.metadata_mut();
        //store_do_put::set_do_put_meta(meta, &db_name, &tbl_name);
        //let res = self.inner.do_put(req).await?;

        let response = self.inner.do_put(req).await.map_err(from_status);

        match response?
            .into_inner()
            .message()
            .await
            .map_err(from_status)?
        {
            Some(response) => Ok(response.body),
            None => Result::Err(ErrorCode::EmptyDataFromServer(format!(
                "Can not receive data from flight server, action: {:?}",
                action_type
            ))),
        }

        //use anyhow::Context;
        //let put_result = res.into_inner().next().await.context("empty response")??;
        //let vec = serde_json::from_slice(&put_result.app_metadata)?;
        //Ok(put_result)
    } */
}
