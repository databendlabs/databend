// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::ops::Range;

use bytes::Bytes;
use futures::{future::BoxFuture, FutureExt, TryFutureExt};

use lance_core::Result;

pub mod buffer;
pub mod compression;
pub mod compression_config;
pub mod constants;
pub mod data;
pub mod decoder;
pub mod encoder;
pub mod encodings;
pub mod format;
pub mod previous;
pub mod repdef;
pub mod statistics;
#[cfg(test)]
pub mod testing;
pub mod utils;
pub mod version;

// We can definitely add support for big-endian machines someday.  However, it's not a priority and
// would involve extensive testing (probably through emulation) to ensure that the encodings are
// correct.
#[cfg(not(target_endian = "little"))]
compile_error!("Lance encodings only support little-endian systems.");

/// A trait for an I/O service
///
/// This represents the I/O API that the encoders and decoders need in order to operate.
/// We specify this as a trait so that lance-encodings does not need to depend on lance-io
///
/// In general, it is assumed that this trait will be implemented by some kind of "file reader"
/// or "file scheduler".  The encodings here are all limited to accessing a single file.
pub trait EncodingsIo: std::fmt::Debug + Send + Sync {
    /// Submit an I/O request
    ///
    /// The response must contain a `Bytes` object for each range requested even if the underlying
    /// I/O was coalesced into fewer actual requests.
    ///
    /// # Arguments
    ///
    /// * `ranges` - the byte ranges to request
    /// * `priority` - the priority of the request
    ///
    /// Priority should be set to the lowest row number that this request is delivering data for.
    /// This is important in cases where indirect I/O causes high priority requests to be submitted
    /// after low priority requests.  We want to fulfill the indirect I/O more quickly so that we
    /// can decode as quickly as possible.
    ///
    /// The implementation should be able to handle empty ranges, and should return an empty
    /// byte buffer for each empty range.
    fn submit_request(
        &self,
        range: Vec<Range<u64>>,
        priority: u64,
    ) -> BoxFuture<'static, Result<Vec<Bytes>>>;

    /// Submit an I/O request with a single range
    ///
    /// This is just a utitliy function that wraps [`EncodingsIo::submit_request`] for the common
    /// case of a single range request.
    fn submit_single(
        &self,
        range: std::ops::Range<u64>,
        priority: u64,
    ) -> BoxFuture<'static, lance_core::Result<bytes::Bytes>> {
        self.submit_request(vec![range], priority)
            .map_ok(|mut v| v.pop().unwrap())
            .boxed()
    }
}

/// An implementation of EncodingsIo that serves data from an in-memory buffer
#[derive(Debug)]
pub struct BufferScheduler {
    data: Bytes,
}

impl BufferScheduler {
    pub fn new(data: Bytes) -> Self {
        Self { data }
    }

    fn satisfy_request(&self, req: Range<u64>) -> Bytes {
        self.data.slice(req.start as usize..req.end as usize)
    }
}

impl EncodingsIo for BufferScheduler {
    fn submit_request(
        &self,
        ranges: Vec<Range<u64>>,
        _priority: u64,
    ) -> BoxFuture<'static, Result<Vec<Bytes>>> {
        std::future::ready(Ok(ranges
            .into_iter()
            .map(|range| self.satisfy_request(range))
            .collect::<Vec<_>>()))
        .boxed()
    }
}
