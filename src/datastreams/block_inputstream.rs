// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::datablocks::DataBlock;
use crate::error::Result;

pub trait IBlockInputStream: Iterator<Item = Result<DataBlock>> {}
