// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use common_base::tokio;
use common_base::tokio::sync::mpsc;
use common_base::tokio::sync::mpsc::UnboundedReceiver;
use common_base::tokio::sync::mpsc::UnboundedSender;
use common_meta_api::KVApi;
use common_meta_grpc::MetaGrpcClient;
use common_meta_types::protobuf::event::EventType;
use common_meta_types::protobuf::watch_request::RequestUnion;
use common_meta_types::protobuf::Event;
use common_meta_types::protobuf::WatchCreateRequest;
use common_meta_types::protobuf::WatchRequest;
use common_meta_types::MatchSeq;
use common_meta_types::Operation;
use common_meta_types::UpsertKVAction;
use futures::stream::iter;

use crate::init_meta_ut;

async fn watcher_client_main(
    addr: String,
    key: String,
    key_end: String,
    start_tx: UnboundedSender<()>,
    mut events_rx: UnboundedReceiver<Vec<Event>>,
    mut shutdown_rx: UnboundedReceiver<()>,
) -> anyhow::Result<()> {
    let client = MetaGrpcClient::try_create(addr.as_str(), "root", "xxx", None, None).await?;
    let mut grpc_client = client.make_client().await?;

    let watch = WatchRequest {
        request_union: Some(RequestUnion::CreateRequest(WatchCreateRequest {
            key,
            key_end,
        })),
    };

    let request = tonic::Request::new(iter(vec![watch]));

    let mut client_stream = grpc_client.watch(request).await?.into_inner();

    let mut watch_events = Vec::<Event>::new();

    // notify client stream has started
    let _ = start_tx.send(());

    loop {
        tokio::select! {
            resp = client_stream.message() => {
                if let Ok(Some(resp)) = resp {
                    assert!(!watch_events.is_empty());
                    assert_eq!(watch_events.get(0), resp.events.get(0));
                    watch_events.pop();
                    if watch_events.is_empty() {
                        // notify has recv all the notify events
                        let _ = start_tx.send(());
                    }
                }
            },
            _ = shutdown_rx.recv() => {
                break;
            }
            events = events_rx.recv() => {
                assert!(watch_events.is_empty());
                if let Some(events) = events {
                    for event in events {
                        watch_events.push(event);
                    }
                }
                // notify has recv evens
                let _ = start_tx.send(());
            }
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_watch() -> anyhow::Result<()> {
    // - Start a metasrv server.
    // - Watch some key.
    // - Write some data.
    // - Assert watcher get all the update.

    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();
    let (_tc, addr) = crate::tests::start_metasrv().await?;

    let (start_tx, mut start_rx) = mpsc::unbounded_channel::<()>();
    let (events_tx, events_rx) = mpsc::unbounded_channel::<Vec<Event>>();
    let (shutdown_tx, shutdown_rx) = mpsc::unbounded_channel::<()>();

    let _h = tokio::spawn(watcher_client_main(
        addr.clone(),
        "a".to_string(),
        "z".to_string(),
        start_tx,
        events_rx,
        shutdown_rx,
    ));

    // wait for client stream start up
    let _ = start_rx.recv();

    //let client = MetaGrpcClient::try_create(addr.as_str(), "root", "xxx", None, None).await?;
    //let mut grpc_client = client.make_client().await?;
    let client = MetaGrpcClient::try_create(addr.as_str(), "root", "xxx", None, None).await?;

    // update some events
    let key_a = "a".to_string();
    let key_b = "z".to_string();
    let key_z = "z".to_string();

    let empty = "".as_bytes().to_vec();
    let val_a = "a".as_bytes().to_vec();
    let val_b = "b".as_bytes().to_vec();
    let val_new = "new".as_bytes().to_vec();
    let val_z = "z".as_bytes().to_vec();

    let events = vec![
        // set a->a
        Event {
            key: key_a.clone(),
            event: EventType::UpInsert.into(),
            current: val_a.clone(),
            prev: empty.clone(),
        },
        // set z->z
        Event {
            key: key_z.clone(),
            event: EventType::UpInsert.into(),
            current: val_z.clone(),
            prev: empty.clone(),
        },
        // set b->b
        Event {
            key: key_b.clone(),
            event: EventType::UpInsert.into(),
            current: val_b.clone(),
            prev: empty.clone(),
        },
        // update b->new
        Event {
            key: key_b.clone(),
            event: EventType::UpInsert.into(),
            current: val_new.clone(),
            prev: val_b.clone(),
        },
        // delete b
        Event {
            key: key_b.clone(),
            event: EventType::Delete.into(),
            current: empty.clone(),
            prev: val_new.clone(),
        },
    ];

    let _ = events_tx.send(events);

    // wait client stream recv events
    let _ = start_rx.recv();

    // update kv
    let updates = vec![
        UpsertKVAction::new("a", MatchSeq::Any, Operation::Update(val_a), None),
        UpsertKVAction::new("z", MatchSeq::Any, Operation::Update(val_z), None),
        UpsertKVAction::new("b", MatchSeq::Any, Operation::Update(val_b), None),
        UpsertKVAction::new("b", MatchSeq::Any, Operation::Update(val_new), None),
        UpsertKVAction::new("b", MatchSeq::Any, Operation::Delete, None),
    ];
    for update in updates.iter() {
        let _ = client.upsert_kv(update.clone()).await;
    }

    // wait client stream recv notify
    let _ = start_rx.recv();

    // ok, shutdown client main
    shutdown_tx.send(()).expect("shutdown client stream error");

    Ok(())
}
