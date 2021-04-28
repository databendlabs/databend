// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;
use common_datablocks::DataBlock;

pub type SendableDataBlockStream =
    std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<DataBlock>> + Sync + Send>>;
