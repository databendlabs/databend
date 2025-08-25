// Copyright 2021 Datafuse Labs
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

use std::io;
use std::io::Error;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use futures_util::StreamExt;
use futures_util::TryStreamExt;
use map_api::mvcc;
use map_api::mvcc::ViewReadonly;
use map_api::IOResultStream;
use seq_marked::InternalSeq;
use seq_marked::SeqMarked;
use state_machine_api::ExpireKey;
use state_machine_api::MetaValue;
use state_machine_api::UserKey;

use crate::leveled_store::leveled_map::applier_acquirer::ApplierPermit;
use crate::leveled_store::leveled_map::LeveledMapData;
use crate::leveled_store::types::Key;
use crate::leveled_store::types::Namespace;
use crate::leveled_store::types::Value;
use crate::sm_v003::OnChange;

pub(crate) type StateMachineView = mvcc::View<Namespace, Key, Value, Arc<LeveledMapData>>;

pub(crate) struct ApplierData {
    /// Hold a unique permit to serialize all apply operations to the state machine.
    pub(crate) _permit: ApplierPermit,

    pub(crate) view: StateMachineView,

    /// Since when to start cleaning expired keys.
    pub(crate) cleanup_start_time: Arc<Mutex<Duration>>,

    pub(crate) on_change_applied: Arc<Option<OnChange>>,
}

#[async_trait::async_trait]
impl mvcc::ScopedViewReadonly<UserKey, MetaValue> for ApplierData {
    fn base_seq(&self) -> InternalSeq {
        self.view.base_seq()
    }

    async fn get(&self, key: UserKey) -> Result<SeqMarked<MetaValue>, io::Error> {
        let got = self.view.get(Namespace::User, Key::User(key)).await?;
        Ok(got.map(|x| x.into_user()))
    }

    async fn range<R>(
        &self,
        range: R,
    ) -> Result<IOResultStream<(UserKey, SeqMarked<MetaValue>)>, io::Error>
    where
        R: RangeBounds<UserKey> + Send + Sync + Clone + 'static,
    {
        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();

        let start = start.map(Key::User);
        let end = end.map(Key::User);

        let strm = self.view.range(Namespace::User, (start, end)).await?;

        Ok(strm
            .map_ok(|(k, v)| (k.into_user(), v.map(|x| x.into_user())))
            .boxed())
    }
}

#[async_trait::async_trait]
impl mvcc::ScopedView<UserKey, MetaValue> for ApplierData {
    fn set(&mut self, key: UserKey, value: Option<MetaValue>) -> SeqMarked<()> {
        self.view
            .set(Namespace::User, Key::User(key), value.map(Value::User))
    }
}

pub struct Scoped<T>(pub T);

#[async_trait::async_trait]
impl mvcc::ScopedViewReadonly<UserKey, MetaValue> for Scoped<StateMachineView> {
    fn base_seq(&self) -> InternalSeq {
        self.0.base_seq()
    }

    async fn get(&self, key: UserKey) -> Result<SeqMarked<MetaValue>, io::Error> {
        let got = self.0.get(Namespace::User, Key::User(key)).await?;
        Ok(got.map(|x| x.into_user()))
    }

    async fn range<R>(
        &self,
        range: R,
    ) -> Result<IOResultStream<(UserKey, SeqMarked<MetaValue>)>, io::Error>
    where
        R: RangeBounds<UserKey> + Send + Sync + Clone + 'static,
    {
        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();

        let start = start.map(Key::User);
        let end = end.map(Key::User);

        let strm = self.0.range(Namespace::User, (start, end)).await?;

        Ok(strm
            .map_ok(|(k, v)| (k.into_user(), v.map(|x| x.into_user())))
            .boxed())
    }
}

#[async_trait::async_trait]
impl mvcc::ScopedView<UserKey, MetaValue> for Scoped<StateMachineView> {
    fn set(&mut self, key: UserKey, value: Option<MetaValue>) -> SeqMarked<()> {
        self.0
            .set(Namespace::User, Key::User(key), value.map(Value::User))
    }
}

#[async_trait::async_trait]
impl mvcc::ScopedViewReadonly<ExpireKey, String> for Scoped<StateMachineView> {
    fn base_seq(&self) -> InternalSeq {
        self.0.base_seq()
    }

    async fn get(&self, key: ExpireKey) -> Result<SeqMarked<String>, io::Error> {
        let got = self.0.get(Namespace::Expire, Key::Expire(key)).await?;
        Ok(got.map(|x| x.into_expire()))
    }

    async fn range<R>(
        &self,
        range: R,
    ) -> Result<IOResultStream<(ExpireKey, SeqMarked<String>)>, io::Error>
    where
        R: RangeBounds<ExpireKey> + Send + Sync + Clone + 'static,
    {
        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();

        let start = start.map(Key::Expire);
        let end = end.map(Key::Expire);

        let strm = self.0.range(Namespace::Expire, (start, end)).await?;

        Ok(strm
            .map_ok(|(k, v)| (k.into_expire(), v.map(|x| x.into_expire())))
            .boxed())
    }
}

#[async_trait::async_trait]
impl mvcc::ScopedViewReadonly<ExpireKey, String> for ApplierData {
    fn base_seq(&self) -> InternalSeq {
        self.view.base_seq()
    }

    async fn get(&self, key: ExpireKey) -> Result<SeqMarked<String>, io::Error> {
        let got = self.view.get(Namespace::Expire, Key::Expire(key)).await?;
        Ok(got.map(|x| x.into_expire()))
    }

    async fn range<R>(
        &self,
        range: R,
    ) -> Result<IOResultStream<(ExpireKey, SeqMarked<String>)>, Error>
    where
        R: RangeBounds<ExpireKey> + Send + Sync + Clone + 'static,
    {
        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();

        let start = start.map(Key::Expire);
        let end = end.map(Key::Expire);

        let strm = self.view.range(Namespace::Expire, (start, end)).await?;

        Ok(strm
            .map_ok(|(k, v)| (k.into_expire(), v.map(|x| x.into_expire())))
            .boxed())
    }
}

#[async_trait::async_trait]
impl mvcc::ScopedView<ExpireKey, String> for ApplierData {
    fn set(&mut self, key: ExpireKey, value: Option<String>) -> SeqMarked<()> {
        self.view.set(
            Namespace::Expire,
            Key::Expire(key),
            value.map(Value::Expire),
        )
    }
}
