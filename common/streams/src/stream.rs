// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use anyhow::Result;
use common_datablocks::DataBlock;
use common_progress::ProgressCallback;

pub type SendableDataBlockStream =
    std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<DataBlock>> + Sync + Send>>;

pub trait IStream {
    /// Set progress callback to the stream.
    /// By default, it is called for leaf sources, after each block
    /// Note that the callback can be called from different threads.
    fn set_progress_callback(&mut self, callback: ProgressCallback);
}
