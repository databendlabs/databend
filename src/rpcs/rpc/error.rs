// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::error::FuseQueryError;

pub fn fuse_to_tonic_err(e: FuseQueryError) -> tonic::Status {
    tonic::Status::internal(format!("{:?}", e))
}

pub fn tonic_to_fuse_err(e: tonic::Status) -> FuseQueryError {
    FuseQueryError::build_internal_error(format!("{:?}", e))
}
