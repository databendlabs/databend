// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::datablocks::DataBlock;
use crate::error::Result;

pub type SendableDataBlockStream =
    std::pin::Pin<Box<dyn async_std::stream::Stream<Item = Result<DataBlock>> + Sync + Send>>;
