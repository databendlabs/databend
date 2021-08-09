// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
pub mod prelude;

mod binary_de;
mod binary_read;
mod binary_ser;
mod binary_write;
mod marshal;
mod stat_buffer;
mod unmarshal;

#[cfg(test)]
mod binary_read_test;
#[cfg(test)]
mod binary_write_test;
#[cfg(test)]
mod marshal_test;
