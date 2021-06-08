// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

pub enum ActiveSession {
    Rejected(Option<tokio::task::JoinHandle<()>>),
    Accepted(Option<std::thread::JoinHandle<()>>),
}