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
use std::task::Context;
use std::task::Poll;

use bytes::BufMut;
use common_runtime::tokio::io::ErrorKind;
use futures::ready;
use futures::stream::Fuse;
use futures::Future;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use rusoto_s3::GetObjectRequest;
use rusoto_s3::HeadObjectRequest;
use rusoto_s3::S3Client;
use rusoto_s3::StreamingBody;
use rusoto_s3::S3;

type StreamLenFuture = Pin<Box<dyn Future<Output = Result<i64, Error>> + Send>>;

enum State {
    Bare,
    GettingBody(Pin<Box<dyn Future<Output = Result<Fuse<StreamingBody>, Error>> + Send>>),
    GotBody(Fuse<StreamingBody>),
    Seeking(StreamLenFuture),
}

pub struct S3InputStream {
    client: S3Client,
    bucket: String,
    key: String,

    state: State,

    buffer: bytes::BytesMut,
    /// where reading begins
    cursor_pos: u64,
    /// total length of target object
    stream_len: Option<u64>,
}

impl S3InputStream {
    pub fn new(client: &S3Client, bucket: &str, key: &str, len_hint: Option<u64>) -> Self {
        Self {
            client: client.clone(),
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            state: State::Bare,
            buffer: bytes::BytesMut::new(),
            cursor_pos: 0,
            stream_len: len_hint,
        }
    }
}

impl futures::AsyncRead for S3InputStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        loop {
            let empty = { self.buffer.is_empty() };
            match &mut self.state {
                State::Bare => {
                    let req = GetObjectRequest {
                        range: Some(format!("bytes={}-", self.cursor_pos)),
                        key: self.key.clone(),
                        bucket: self.bucket.clone(),
                        ..Default::default()
                    };
                    let client = self.client.clone();
                    let resp = async move {
                        let reply = client
                            .get_object(req)
                            .await
                            .map_err(|e| Error::new(ErrorKind::Other, e))?;
                        reply
                            .body
                            .map(|s| s.fuse())
                            .ok_or_else(|| Error::new(ErrorKind::Other, "empty stream"))
                    };
                    self.state = State::GettingBody(resp.boxed());
                }
                State::GettingBody(resp) => {
                    let resp = Pin::new(resp);
                    match ready!(resp.poll(cx)) {
                        Ok(v) => {
                            self.state = State::GotBody(v);
                        }
                        Err(e) => return Poll::Ready(Err(Error::new(ErrorKind::Other, e))),
                    }
                }
                State::GotBody(stream) => {
                    if !empty {
                        return self.do_read(buf);
                    }

                    let v = Pin::new(stream);
                    let next = v.poll_next(cx);
                    match ready!(next) {
                        Some(Ok(buffer)) => {
                            self.buffer.put(buffer);
                            return self.do_read(buf);
                        }
                        Some(Err(e)) => {
                            return Poll::Ready(Err(Error::new(ErrorKind::Other, e)));
                        }

                        None => {
                            return Poll::Ready(Ok(0));
                        }
                    }
                }
                State::Seeking(_f) => {
                    // read while seeking is NOT allowed
                    return Poll::Ready(Err(Error::new(
                        ErrorKind::Other,
                        "read while seeking NOT allowed",
                    )));
                }
            }
        }
    }
}

impl futures::AsyncSeek for S3InputStream {
    fn poll_seek(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<std::io::Result<u64>> {
        self.do_poll_seek(cx, pos)
    }
}

impl S3InputStream {
    fn do_poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<std::io::Result<u64>> {
        match self.stream_len {
            Some(len) => Poll::Ready(self.seek_with_stream_len(cx, pos, len)),
            None => loop {
                match &mut self.state {
                    State::Seeking(f) => match ready!(Pin::new(f).poll(cx)) {
                        Ok(v) => {
                            let len = v as u64;
                            self.stream_len = Some(len);
                            return Poll::Ready(self.seek_with_stream_len(cx, pos, len));
                        }
                        Err(e) => return Poll::Ready(Err(e)),
                    },
                    State::Bare => {
                        let head_req = HeadObjectRequest {
                            key: self.key.clone(),
                            bucket: self.bucket.clone(),
                            ..Default::default()
                        };
                        //jhead_req.key = self.key.clone();
                        //head_req.bucket = self.bucket.clone();
                        let cli = self.client.clone();
                        let res = async move {
                            let result = cli
                                .head_object(head_req)
                                .await
                                .map_err(|e| Error::new(ErrorKind::Other, e))?;
                            result.content_length.ok_or_else(|| {
                                Error::new(ErrorKind::Other, "expects content-length")
                            })
                        };
                        self.state = State::Seeking(res.boxed());
                    }
                    State::GettingBody(_) | State::GotBody(_) => self.state = State::Bare,
                };
            },
        }
    }

    fn seek_with_stream_len(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        pos: SeekFrom,
        stream_len: u64,
    ) -> std::io::Result<u64> {
        let (base, offset) = match pos {
            SeekFrom::Start(start) => (start, 0),
            SeekFrom::End(end) => (stream_len, end),
            SeekFrom::Current(current) => (self.cursor_pos, current),
        };

        let new_pos = if offset >= 0 {
            base.checked_add(offset as u64)
        } else {
            base.checked_sub(offset.wrapping_neg() as u64)
        };

        // invalid position
        let new_pos = new_pos.ok_or_else(|| {
            Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "invalid seeking operation, current offset {}, SeekFrom {:?}",
                    self.cursor_pos, pos
                ),
            )
        })?;

        // For this read-only stream, we treat a seeking beyonds end of stream as an error
        if new_pos > stream_len {
            return Err(Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid seeking operation",
            ));
        }
        if self.cursor_pos != new_pos {
            // stop pending read
            self.state = State::Bare;
        }
        self.cursor_pos = new_pos;
        Ok(self.cursor_pos)
    }

    fn do_read(mut self: Pin<&mut Self>, buf: &mut [u8]) -> Poll<std::io::Result<usize>> {
        let available = std::cmp::min(buf.remaining_mut(), self.buffer.len());
        let bytes = self.buffer.split_to(available);
        let mut new_buf = buf;
        new_buf.write_all(bytes.as_ref())?;
        let new_pos = self.cursor_pos.checked_add(available as u64);
        match new_pos {
            Some(v) => self.cursor_pos = v,
            None => {
                return Poll::Ready(Err(Error::new(
                    ErrorKind::Other,
                    "u64 addition overflow detected while moving cursor forward",
                )))
            }
        }
        Poll::Ready(Ok(available))
    }
}
