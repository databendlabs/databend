// Copyright 2020 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use core::fmt;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::io::BufWriter;
use std::io::Read;
use std::net::ToSocketAddrs;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use clickhouse_rs::Pool;
use common_base::tokio;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_infallible::RwLock;
use crossbeam_queue::ArrayQueue;
use futures::future::try_join_all;
use futures::StreamExt;
use rand::Rng;
use structopt::StructOpt;
use structopt_toml::StructOptToml;