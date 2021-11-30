//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::io::Error;
use std::io::SeekFrom;
use std::io::Write;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;

use azure_core_mirror::prelude::*;
use azure_storage_mirror::blob::blob::responses::GetBlobPropertiesResponse;
use azure_storage_mirror::blob::blob::responses::GetBlobResponse;
use azure_storage_mirror::clients::BlobClient;
use common_base::tokio;
use futures::ready;
use futures::AsyncSeek;
use futures::Future;
use futures::FutureExt;

type GetBlobFuture =
    Pin<Box<dyn Future<Output = std::result::Result<GetBlobResponse, Error>> + Send>>;

type GetBlobPropertiesFuture =
    Pin<Box<dyn Future<Output = std::result::Result<GetBlobPropertiesResponse, Error>> + Send>>;

enum State<Fut, Resp> {
    Init,
    Running(Fut),
    Done(std::result::Result<Resp, Error>),
}

struct ReadState {
    state: State<GetBlobFuture, GetBlobResponse>,
}

struct SeekState {
    state: State<GetBlobPropertiesFuture, GetBlobPropertiesResponse>,
}

pub struct AzureBlobInputStream {
    blob_client: Arc<BlobClient>,
    cursor: u64,
    content_length: Option<u64>,
    read_state: Arc<Mutex<ReadState>>,
    seek_state: Arc<Mutex<SeekState>>,
}

impl AzureBlobInputStream {
    pub fn create(client: Arc<BlobClient>) -> Self {
        Self {
            blob_client: client,
            cursor: 0_u64,
            content_length: None,
            read_state: Arc::new(Mutex::new(ReadState { state: State::Init })),
            seek_state: Arc::new(Mutex::new(SeekState { state: State::Init })),
        }
    }
}

impl futures::AsyncRead for AzureBlobInputStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        if self.content_length.is_none() {
            // Seek to current position, this is just for fetching content length
            return match ready!(self.poll_seek(cx, SeekFrom::Current(0))) {
                Ok(_) => Poll::Pending,
                Err(e) => Poll::Ready(Err(e)),
            };
        }

        let mut instance = self.get_mut();

        let mut read_state = instance.read_state.lock().unwrap();

        let poll_result = match &mut read_state.state {
            State::Init => {
                let start = instance.cursor;
                let end = start + buf.len() as u64;

                // start offset is beyond the size of file, return Ok(0)
                if instance.content_length.is_some() && start >= instance.content_length.unwrap()
                    || start == end
                {
                    return Poll::Ready(Ok(0));
                }

                let client = instance.blob_client.clone();

                let fut = async move {
                    client
                        .get()
                        .range(Range::new(start, end))
                        .execute()
                        .await
                        .map_err(|e| {
                            Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("Failed to read blob with range {}-{}, {}", start, end, e),
                            )
                        })
                };
                read_state.state = State::Running(fut.boxed());
                Poll::Pending
            }
            State::Running(fut) => {
                if let Poll::Ready(res) = fut.as_mut().poll(cx) {
                    read_state.state = State::Done(res);
                }
                Poll::Pending
            }
            State::Done(res) => {
                let poll_result = match res {
                    Ok(response) => {
                        // set cursor, content_length, write data to buf
                        let len = response.data.len();
                        instance.cursor += len as u64;

                        let mut buf_mut = buf;
                        if let Err(err) = buf_mut.write_all(response.data.as_slice()) {
                            return Poll::Ready(Err(Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("Failed to write buffer {}", err),
                            )));
                        }
                        Poll::Ready(Ok(len))
                    }
                    Err(err) => Poll::Ready(Err(Error::new(
                        std::io::ErrorKind::InvalidData,
                        err.to_string(),
                    ))),
                };

                read_state.state = State::Init;
                poll_result
            }
        };

        if poll_result.is_pending() {
            let waker = cx.waker().clone();
            tokio::spawn(async move {
                tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                waker.wake();
            });
        }
        poll_result
    }
}

impl futures::AsyncSeek for AzureBlobInputStream {
    fn poll_seek(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<std::io::Result<u64>> {
        let mut instance = self.get_mut();

        let mut seek_state = instance.seek_state.lock().unwrap();

        let poll_result = match &mut seek_state.state {
            State::Init => {
                if instance.content_length.is_none() {
                    // content length is unknown, send request to get blob properties

                    let client = instance.blob_client.clone();

                    let fut = async move {
                        client.get_properties().execute().await.map_err(|e| {
                            Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("Failed to get blob properties, {}", e),
                            )
                        })
                    };

                    seek_state.state = State::Running(fut.boxed());
                    Poll::Pending
                } else {
                    let file_size = instance.content_length.unwrap();
                    let res = Self::calculate_cursor(pos, instance.cursor, file_size);
                    if let Ok(cursor) = res {
                        instance.cursor = cursor;
                    }
                    Poll::Ready(res)
                }
            }
            State::Running(fut) => {
                if let Poll::Ready(res) = fut.as_mut().poll(cx) {
                    seek_state.state = State::Done(res);
                }
                Poll::Pending
            }
            State::Done(res) => {
                let poll_result = match res {
                    Ok(response) => {
                        let file_size = response.blob.properties.content_length;
                        instance.content_length = Some(file_size);
                        let res = Self::calculate_cursor(pos, instance.cursor, file_size);
                        if let Ok(cursor) = res {
                            instance.cursor = cursor;
                        }
                        Poll::Ready(res)
                    }
                    Err(err) => Poll::Ready(Err(Error::new(
                        std::io::ErrorKind::InvalidData,
                        err.to_string(),
                    ))),
                };

                seek_state.state = State::Init;
                poll_result
            }
        };

        if poll_result.is_pending() {
            let waker = cx.waker().clone();
            tokio::spawn(async move {
                tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                waker.wake();
            });
        }
        poll_result
    }
}

impl AzureBlobInputStream {
    fn calculate_cursor(pos: SeekFrom, current: u64, file_size: u64) -> std::io::Result<u64> {
        let err = Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "Seeking {:?} is out of range of file size {}.",
                pos, file_size
            ),
        );

        if let SeekFrom::Start(offset) = pos {
            return Ok(offset);
        }

        let base;
        let offset;
        match pos {
            SeekFrom::End(offset_) => {
                base = file_size;
                offset = offset_;
            }
            SeekFrom::Current(offset_) => {
                base = current;
                offset = offset_;
            }
            _ => unreachable!(),
        };

        // According to Rust doc, "It is possible to seek beyond the end of an object, but it’s an error to seek before byte 0.",
        // we allow seeking beyond the file size and only return error when seeking to negative or overflow u64 max.
        let new_pos: Option<u64>;
        if offset < 0 {
            new_pos = base.checked_sub(offset.abs() as u64);
            // case of seeking to negative
            if new_pos.is_none() {
                return Err(err);
            }
        } else {
            new_pos = base.checked_add(offset as u64);
            // case of overflow u64 max
            if new_pos.is_none() {
                return Err(err);
            }
        }
        let cursor = new_pos.unwrap();
        Ok(cursor)
    }
}
