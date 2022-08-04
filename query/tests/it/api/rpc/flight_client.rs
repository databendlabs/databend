use std::error::Error;
use std::io::ErrorKind;
use std::pin::Pin;
use std::time::Duration;
use byteorder::{BigEndian, ReadBytesExt};
use futures::Stream;
use futures_util::StreamExt;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, Streaming};
use tonic::transport::Server;
use common_arrow::arrow_format::flight::data::{Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket};
use common_arrow::arrow_format::flight::service::flight_service_client::FlightServiceClient;
use common_arrow::arrow_format::flight::service::flight_service_server::{FlightService, FlightServiceServer};
use common_base::base::tokio::net::TcpListener;
use common_exception::Result;
use common_grpc::ConnectionFactory;
use common_base::base::{ProgressValues, tokio};
use common_arrow::arrow_format::flight::data::Result as FlightResult;
use common_arrow::parquet::FallibleStreamingIterator;
use common_base::base::tokio::sync::Notify;
use common_tracing::set_panic_hook;
use databend_query::api::{DataPacket, FlightClient, ClientFlightExchange, FragmentData, ServerFlightExchange};

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_flight_exchange() -> Result<()> {
    set_panic_hook();
    let listener = TcpListener::bind("127.0.0.1:10023").await.unwrap();
    let listener_stream = TcpListenerStream::new(listener);

    let mut builder = Server::builder();
    let abort_notify = Notify::new();

    let server = builder
        .add_service(FlightServiceServer::new(TestFlightService {}))
        .serve_with_incoming_shutdown(listener_stream, async move { abort_notify.notified().await });

    tokio::spawn(server);

    let mut flight_client = FlightClient::new(
        FlightServiceClient::new(ConnectionFactory::create_rpc_channel(
            "127.0.0.1:10023",
            None,
            None,
        ).await.unwrap())
    );

    let mut exchange_channel_1 = flight_client.do_exchange("", "").await?;
    let mut exchange_channel_2 = flight_client.do_exchange("", "").await?;

    let join1 = tokio::spawn(async move {
        for index in 0..100 {
            exchange_channel_1.send(DataPacket::FragmentData(FragmentData::End(index))).await.unwrap();
            let data_packet = exchange_channel_1.recv().await.unwrap();
            assert!(matches!(&data_packet, Some(DataPacket::FragmentData(FragmentData::End(v))) if *v == index + 1));
        }

        drop(exchange_channel_1);
    });

    let join2 = tokio::spawn(async move {
        for index in 0..100 {
            exchange_channel_2.send(DataPacket::FragmentData(FragmentData::End(index))).await.unwrap();
            let data_packet = exchange_channel_2.recv().await.unwrap();
            assert!(matches!(&data_packet, Some(DataPacket::FragmentData(FragmentData::End(v))) if *v == index + 1));
        }

        drop(exchange_channel_2);
    });

    join1.await.unwrap();
    join2.await.unwrap();

    Ok(())
}

struct TestFlightService {}

type FlightStream<T> = Pin<Box<dyn Stream<Item=std::result::Result<T, Status>> + Send + Sync + 'static>>;

#[async_trait::async_trait]
impl FlightService for TestFlightService {
    type HandshakeStream = FlightStream<HandshakeResponse>;

    async fn handshake(&self, request: Request<Streaming<HandshakeRequest>>) -> std::result::Result<Response<Self::HandshakeStream>, Status> {
        unimplemented!()
    }

    type ListFlightsStream = FlightStream<FlightInfo>;

    async fn list_flights(&self, request: Request<Criteria>) -> std::result::Result<Response<Self::ListFlightsStream>, Status> {
        unimplemented!()
    }

    async fn get_flight_info(&self, request: Request<FlightDescriptor>) -> std::result::Result<Response<FlightInfo>, Status> {
        unimplemented!()
    }

    async fn get_schema(&self, request: Request<FlightDescriptor>) -> std::result::Result<Response<SchemaResult>, Status> {
        unimplemented!()
    }

    type DoGetStream = FlightStream<FlightData>;

    async fn do_get(&self, request: Request<Ticket>) -> std::result::Result<Response<Self::DoGetStream>, Status> {
        unimplemented!()
    }

    type DoPutStream = FlightStream<PutResult>;

    async fn do_put(&self, request: Request<Streaming<FlightData>>) -> std::result::Result<Response<Self::DoPutStream>, Status> {
        unimplemented!()
    }

    type DoExchangeStream = FlightStream<FlightData>;

    async fn do_exchange(&self, request: Request<Streaming<FlightData>>) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
        let mut req_stream = request.into_inner();
        let (tx, rx) = async_channel::bounded(1);

        tokio::spawn(async move {
            let mut flight_exchange = ServerFlightExchange::try_create(tx, req_stream).unwrap();

            while let Ok(Some(message)) = flight_exchange.recv().await {
                match message {
                    DataPacket::FragmentData(FragmentData::End(index)) => {
                        let next_message = DataPacket::FragmentData(FragmentData::End(index + 1));
                        flight_exchange.send(next_message).await.expect("working rx");
                    }
                    _ => {
                        break;
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(rx)))
    }

    type DoActionStream = FlightStream<FlightResult>;

    async fn do_action(&self, request: Request<Action>) -> std::result::Result<Response<Self::DoActionStream>, Status> {
        unimplemented!()
    }

    type ListActionsStream = FlightStream<ActionType>;

    async fn list_actions(&self, request: Request<Empty>) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
        unimplemented!()
    }
}

// fn match_for_io_error(err_status: &Status) -> Option<&std::io::Error> {
//     let mut err: &(dyn Error + 'static) = err_status;
//
//     loop {
//         if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
//             return Some(io_err);
//         }
//
//         // h2::Error do not expose std::io::Error with `source()`
//         // https://github.com/hyperium/h2/pull/462
//
//         // if let Some(h2_err) = err.downcast_ref::<h2::Error>() {
//         //     if let Some(io_err) = h2_err.get_io() {
//         //         return Some(io_err);
//         //     }
//         // }
//
//         err = match err.source() {
//             Some(err) => err,
//             None => return None,
//         };
//     }
// }

