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

use std::time::Duration;

use bytes::BufMut;
use bytes::BytesMut;
use databend_common_base::runtime::spawn;
use databend_meta_client::types::protobuf::RaftRequest;
use databend_meta_client::types::protobuf::StreamItem;
use databend_meta_client::types::protobuf::meta_service_client::MetaServiceClient;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::future::poll_fn;
use http::HeaderMap;
use http::HeaderValue;
use prost::Message;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tonic::Code;
use tonic::Streaming;

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
            for item in items {
                let mut data = BytesMut::new();
                data.put_u8(0); // Uncompressed gRPC message.
                data.put_u32(item.encoded_len() as u32);
                item.encode(&mut data).unwrap();
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
