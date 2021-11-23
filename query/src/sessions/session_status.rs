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

use std::net::SocketAddr;
use std::sync::Arc;

use common_macros::MallocSizeOf;
use futures::channel::oneshot::Sender;

use crate::sessions::context_shared::DatabendQueryContextShared;
use crate::sessions::Settings;

#[derive(MallocSizeOf)]
pub(in crate::sessions) struct MutableStatus {
    pub(in crate::sessions) abort: bool,
    pub(in crate::sessions) current_database: String,
    pub(in crate::sessions) session_settings: Arc<Settings>,
    #[ignore_malloc_size_of = "insignificant"]
    pub(in crate::sessions) client_host: Option<SocketAddr>,
    #[ignore_malloc_size_of = "insignificant"]
    pub(in crate::sessions) io_shutdown_tx: Option<Sender<Sender<()>>>,
    #[ignore_malloc_size_of = "insignificant"]
    pub(in crate::sessions) context_shared: Option<Arc<DatabendQueryContextShared>>,
}
