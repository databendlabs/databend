// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::BufMut;
use bytes::BytesMut;
use databend_common_base::runtime::spawn;
use databend_common_meta_app::schema::CatalogIdIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_meta_client::kvapi::KVApi;
use databend_meta_client::kvapi::KVStream;
use databend_meta_client::kvapi::ListOptions;
use databend_meta_client::types::Change;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::SeqV;
use databend_meta_client::types::TxnReply;
use databend_meta_client::types::TxnRequest;
use databend_meta_client::types::UpsertKV;
use databend_meta_client::types::protobuf::RaftRequest;
use databend_meta_client::types::protobuf::StreamItem;
use databend_meta_client::types::protobuf::meta_service_client::MetaServiceClient;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::future::poll_fn;
use futures::stream::BoxStream;
use http::HeaderMap;
use http::HeaderValue;
use prost::Message;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tonic::Code;
use tonic::Streaming;

use crate::kv_pb_api::KVPbApi;

const TEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, PartialEq)]
enum Event {
    Cancelled(h2::Reason),
    TrailersSent,
}

/// A real HTTP/2 peer that withholds grpc-status until the test releases it.
/// Observe resets at the peer rather than inferring them from a dropped Body.
struct ResponsePeer {
    address: String,
    finish: Option<oneshot::Sender<Code>>,
    events: mpsc::UnboundedReceiver<Event>,
    channel: Option<tonic::transport::Channel>,
    task: JoinHandle<()>,
}

impl ResponsePeer {
    async fn start(items: Vec<StreamItem>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let (finish, done) = oneshot::channel::<Code>();
        let (events_tx, events) = mpsc::unbounded_channel();
        let task = spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut connection = h2::server::handshake(socket).await.unwrap();
            let (_, mut respond) = connection.accept().await.unwrap().unwrap();
            let headers = http::Response::builder()
                .header("content-type", "application/grpc")
                .body(())
                .unwrap();
            let mut response = respond.send_response(headers, false).unwrap();
            let mut data = BytesMut::new();
            for item in items {
                data.put_u8(0); // Uncompressed gRPC message.
                data.put_u32(item.encoded_len() as u32);
                item.encode(&mut data).unwrap();
            }
            if !data.is_empty() {
                // Coalesce messages as tonic does: many tiny DATA frames
                // would exercise h2's unrelated framing-overhead protection.
                response.send_data(data.freeze(), false).unwrap();
            }

            let control = async move {
                tokio::select! {
                    reason = poll_fn(|cx| response.poll_reset(cx)) => {
                        let _ = events_tx.send(Event::Cancelled(reason.unwrap()));
                    }
                    code = done => {
                        let code = code.unwrap();
                        let mut trailers = HeaderMap::new();
                        trailers.insert("grpc-status", HeaderValue::from_str(&(code as i32).to_string()).unwrap());
                        response.send_trailers(trailers).unwrap();
                        let _ = events_tx.send(Event::TrailersSent);
                    }
                }
            };
            // send_data/send_trailers only queue frames. Keep driving the
            // connection while waiting for the release signal or peer reset.
            let drive = poll_fn(|cx| connection.poll_closed(cx));
            tokio::pin!(drive);
            tokio::select! {
                _ = control => { let _ = drive.await; }
                result = &mut drive => { result.unwrap(); }
            }
        });
        Self {
            address,
            finish: Some(finish),
            events,
            channel: None,
            task,
        }
    }

    async fn stream(&mut self) -> Streaming<StreamItem> {
        let channel = tonic::transport::Endpoint::from_shared(self.address.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        // Keep the connection alive after the response stream is dropped, so
        // the peer observes a stream reset rather than a connection teardown.
        self.channel = Some(channel.clone());
        MetaServiceClient::new(channel)
            .kv_read_v1(RaftRequest::default())
            .await
            .unwrap()
            .into_inner()
    }

    fn finish(&mut self, code: Code) {
        self.finish.take().unwrap().send(code).unwrap();
    }

    async fn event(&mut self) -> Event {
        self.events.recv().await.unwrap()
    }
}

impl Drop for ResponsePeer {
    fn drop(&mut self) {
        // Also terminate on assertion failure or timeout; no detached listener
        // or connection task may survive a failed test.
        self.task.abort();
    }
}

/// Exercise the actual KVPbApi adapter with a real tonic response stream.
struct StreamKV {
    streams: Mutex<VecDeque<KVStream<MetaError>>>,
    calls: AtomicUsize,
    read: Arc<Notify>,
}

impl StreamKV {
    fn new(streams: Vec<Streaming<StreamItem>>) -> Self {
        let read = Arc::new(Notify::new());
        let streams = streams
            .into_iter()
            .map(|stream| {
                let read = read.clone();
                stream
                    .inspect(move |_| read.notify_one())
                    .map_err(MetaError::from)
                    .boxed()
            })
            .collect();
        Self {
            streams: Mutex::new(streams),
            calls: AtomicUsize::new(0),
            read,
        }
    }
}

#[async_trait::async_trait]
impl KVApi for StreamKV {
    type Error = MetaError;

    async fn get_many_kv(
        &self,
        keys: BoxStream<'static, Result<String, MetaError>>,
    ) -> Result<KVStream<MetaError>, MetaError> {
        let _ = keys.try_collect::<Vec<_>>().await?;
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.streams.lock().unwrap().pop_front().unwrap())
    }

    async fn list_kv(&self, _: ListOptions<'_, str>) -> Result<KVStream<MetaError>, MetaError> {
        unimplemented!()
    }
    async fn upsert_kv(&self, _: UpsertKV) -> Result<Change<Vec<u8>>, MetaError> {
        unimplemented!()
    }
    async fn transaction(&self, _: TxnRequest) -> Result<TxnReply, MetaError> {
        unimplemented!()
    }
}

fn item(id: u64) -> StreamItem {
    StreamItem::new(format!("__fd_catalog_by_id/{id}"), None)
}

fn key(id: u64) -> CatalogIdIdent {
    CatalogIdIdent::new(Tenant::new_literal("dummy"), id)
}

#[tokio::test]
async fn test_pb_vec_waits_for_trailers() {
    timeout(TEST_TIMEOUT, async {
        for code in [Code::Ok, Code::Internal] {
            let mut peer = ResponsePeer::start(vec![item(1)]).await;
            let kv = StreamKV::new(vec![peer.stream().await]);
            let result = kv.get_pb_vec([key(1)]);
            tokio::pin!(result);
            tokio::select! {
                biased;
                result = &mut result => panic!("completed before grpc-status: {result:?}"),
                _ = kv.read.notified() => {}
            }
            assert!(futures::poll!(&mut result).is_pending());
            peer.finish(code);
            let result = result.await;
            if code == Code::Ok {
                assert_eq!(result.unwrap().len(), 1);
            } else {
                assert!(result.is_err(), "final grpc-status must not be discarded");
            }
            assert_eq!(peer.event().await, Event::TrailersSent);
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_pb_vec_checks_item_count() {
    timeout(TEST_TIMEOUT, async {
        for count in [0, 2] {
            let mut peer = ResponsePeer::start((1..=count).map(item).collect()).await;
            let kv = StreamKV::new(vec![peer.stream().await]);
            peer.finish(Code::Ok);
            let error = kv.get_pb_vec([key(1)]).await.unwrap_err();
            assert!(
                error.to_string().contains(if count == 0 {
                    "StreamReadEOF"
                } else {
                    "StreamReadOverflow"
                }),
                "{error}"
            );
        }
        let kv = StreamKV::new(vec![]);
        let stream = kv
            .get_pb_stream_low(Vec::<CatalogIdIdent>::new())
            .await
            .unwrap();
        assert!(stream.collect::<Vec<_>>().await.is_empty());
        assert_eq!(kv.calls.load(Ordering::SeqCst), 0);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_pb_vec_propagates_disconnect_and_cancellation() {
    timeout(TEST_TIMEOUT, async {
        let mut peer = ResponsePeer::start(vec![item(1)]).await;
        let kv = StreamKV::new(vec![peer.stream().await]);
        let result = kv.get_pb_vec([key(1)]);
        tokio::pin!(result);
        tokio::select! {
            biased;
            result = &mut result => panic!("completed before grpc-status: {result:?}"),
            _ = kv.read.notified() => {}
        }
        peer.task.abort();
        assert!(result.await.is_err());

        let mut peer = ResponsePeer::start(vec![item(1)]).await;
        let kv = StreamKV::new(vec![peer.stream().await]);
        let mut stream = kv.get_pb_stream([key(1), key(2)]).await.unwrap();
        assert!(stream.next().await.unwrap().is_ok());
        drop(stream);
        assert_eq!(peer.event().await, Event::Cancelled(h2::Reason::CANCEL));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_pb_stream_retains_decode_errors_and_terminal_status() {
    timeout(TEST_TIMEOUT, async {
        let mut invalid = item(1);
        invalid.value = Some(SeqV::new(1, b"b".to_vec()).into());
        for code in [Code::Ok, Code::Internal] {
            let mut peer = ResponsePeer::start(vec![invalid.clone(), item(2)]).await;
            let kv = StreamKV::new(vec![peer.stream().await]);
            peer.finish(code);
            let stream = kv.get_pb_stream_low([key(1), key(2)]).await.unwrap();
            let results = stream.collect::<Vec<_>>().await;
            assert_eq!(results.len(), 2);
            assert!(matches!(
                results[0],
                Err(crate::kv_pb_api::errors::PbApiReadError::PbDecodeError(_))
            ));
            assert_eq!(results[1].is_ok(), code == Code::Ok);
            // Existing table/GC consumers zip results with requested keys;
            // the terminal error must replace the last item, not follow it.
            assert_eq!(
                results
                    .into_iter()
                    .zip([key(1), key(2)])
                    .filter(|(result, _)| result.is_err())
                    .count(),
                if code == Code::Ok { 1 } else { 2 }
            );
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_pb_vec_timeout_cancels_pending_trailers() {
    timeout(TEST_TIMEOUT, async {
        let mut peer = ResponsePeer::start(vec![item(1)]).await;
        let kv = StreamKV::new(vec![peer.stream().await]);
        {
            let result = kv.get_pb_vec([key(1)]);
            tokio::pin!(result);
            tokio::select! {
                biased;
                result = &mut result => panic!("completed before grpc-status: {result:?}"),
                _ = kv.read.notified() => {}
            }
            assert!(futures::poll!(&mut result).is_pending());
            assert!(timeout(Duration::ZERO, &mut result).await.is_err());
            // The timeout only borrows `result`. Leave this scope to drop the
            // read and its response stream before waiting for the peer reset.
        }
        assert_eq!(peer.event().await, Event::Cancelled(h2::Reason::CANCEL));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_pb_vec_checks_final_status_of_each_chunk() {
    timeout(TEST_TIMEOUT, async {
        let n = StreamKV::CHUNK_SIZE as u64;
        let mut first = ResponsePeer::start((1..=n).map(item).collect()).await;
        let mut second = ResponsePeer::start(vec![item(n + 1)]).await;
        let kv = StreamKV::new(vec![first.stream().await, second.stream().await]);
        first.finish(Code::Ok);
        second.finish(Code::Internal);
        assert!(kv.get_pb_vec((1..=n + 1).map(key)).await.is_err());
        assert_eq!(kv.calls.load(Ordering::SeqCst), 2);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_grpc_take_cancels_before_trailers() {
    timeout(TEST_TIMEOUT, async {
        let mut peer = ResponsePeer::start(vec![StreamItem::new("key".to_string(), None)]).await;
        let stream = peer.stream().await;
        let items = stream.take(1).try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(peer.event().await, Event::Cancelled(h2::Reason::CANCEL));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_grpc_full_consumption_checks_trailers() {
    timeout(TEST_TIMEOUT, async {
        for code in [Code::Ok, Code::Internal] {
            let mut peer =
                ResponsePeer::start(vec![StreamItem::new("key".to_string(), None)]).await;
            let mut stream = peer.stream().await;
            assert_eq!(stream.next().await.unwrap().unwrap().key, "key");
            let tail = stream.next();
            tokio::pin!(tail);
            assert!(futures::poll!(&mut tail).is_pending());
            peer.finish(code);
            match code {
                Code::Ok => assert!(tail.await.is_none()),
                _ => assert_eq!(tail.await.unwrap().unwrap_err().code(), code),
            }
            assert_eq!(peer.event().await, Event::TrailersSent);
        }
    })
    .await
    .unwrap();
}
