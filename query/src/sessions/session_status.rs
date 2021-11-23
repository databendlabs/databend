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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use common_infallible::RwLock;
use common_macros::MallocSizeOf;
use futures::channel::oneshot::Sender;

use crate::sessions::context_shared::DatabendQueryContextShared;
use crate::sessions::Settings;

#[derive(MallocSizeOf, Default)]
pub struct MutableStatus {
    abort: AtomicBool,
    current_database: RwLock<String>,
    session_settings: RwLock<Settings>,
    #[ignore_malloc_size_of = "insignificant"]
    client_host: RwLock<Option<SocketAddr>>,
    #[ignore_malloc_size_of = "insignificant"]
    io_shutdown_tx: RwLock<Option<Sender<Sender<()>>>>,
    #[ignore_malloc_size_of = "insignificant"]
    context_shared: RwLock<Option<Arc<DatabendQueryContextShared>>>,
}

impl MutableStatus {
    pub fn get_abort(&self) -> bool {
        self.abort.load(Ordering::Relaxed) as bool
    }

    pub fn set_abort(&self, v: bool) {
        self.abort.fetch_and(v, Ordering::Relaxed);
    }

    pub fn get_current_database(&self) -> String {
        let lock = self.current_database.read();
        lock.clone()
    }

    pub fn set_current_database(&self, db: String) {
        let mut lock = self.current_database.write();
        *lock = db
    }

    pub fn get_settings(&self) -> Arc<Settings> {
        let lock = self.session_settings.read();
        Arc::new(lock.clone())
    }

    pub fn get_client_host(&self) -> Option<SocketAddr> {
        let lock = self.client_host.read();
        *lock
    }

    pub fn set_client_host(&self, sock: Option<SocketAddr>) {
        let mut lock = self.client_host.write();
        *lock = sock
    }

    pub fn set_io_shutdown_tx(&self, tx: Option<Sender<Sender<()>>>) {
        let mut lock = self.io_shutdown_tx.write();
        *lock = tx
    }

    //  Take the io_shutdown_tx.
    pub fn take_io_shutdown_tx(&self) -> Option<Sender<Sender<()>>> {
        let mut lock = self.io_shutdown_tx.write();
        lock.take()
    }

    pub fn context_shared_is_none(&self) -> bool {
        let lock = self.context_shared.read();
        lock.is_none()
    }

    pub fn get_context_shared(&self) -> Option<Arc<DatabendQueryContextShared>> {
        let lock = self.context_shared.read();
        lock.clone()
    }

    pub fn set_context_shared(&self, ctx: Option<Arc<DatabendQueryContextShared>>) {
        let mut lock = self.context_shared.write();
        *lock = ctx
    }

    //  Take the context_shared.
    pub fn take_context_shared(&self) -> Option<Arc<DatabendQueryContextShared>> {
        let mut lock = self.context_shared.write();
        lock.take()
    }
}
