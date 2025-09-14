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
use std::ops::Deref;
use std::ops::DerefMut;
use std::ops::RangeBounds;

use futures_util::StreamExt;
use futures_util::TryStreamExt;
use map_api::mvcc;
use map_api::IOResultStream;
use seq_marked::SeqMarked;
use state_machine_api::ExpireKey;
use state_machine_api::MetaValue;
use state_machine_api::UserKey;

use crate::leveled_store::leveled_map::LeveledMap;
use crate::leveled_store::types::Key;
use crate::leveled_store::types::Namespace;
use crate::leveled_store::types::Value;

pub(crate) type MvccView = mvcc::View<Namespace, Key, Value, LeveledMap>;

pub(crate) struct StateMachineView {
    inner: MvccView,
}

impl Deref for StateMachineView {
    type Target = MvccView;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for StateMachineView {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl StateMachineView {
    pub fn new(inner: MvccView) -> Self {
        Self { inner }
    }

    pub fn from_leveled_map(leveled_map: &LeveledMap) -> Self {
        let view = mvcc::View::new(leveled_map.to_snapshot());
        StateMachineView::new(view)
    }

    pub fn into_inner(self) -> MvccView {
        self.inner
    }

    pub async fn commit(self) -> Result<(), io::Error> {
        self.into_inner().commit().await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl mvcc::ScopedGet<UserKey, MetaValue> for StateMachineView {
    async fn get(&self, key: UserKey) -> Result<SeqMarked<MetaValue>, io::Error> {
        let got = self.inner.get(Namespace::User, Key::User(key)).await?;
        Ok(got.map(|x| x.into_user()))
    }
}

#[async_trait::async_trait]
impl mvcc::ScopedRange<UserKey, MetaValue> for StateMachineView {
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

        let strm = self.inner.range(Namespace::User, (start, end)).await?;

        Ok(strm
            .map_ok(|(k, v)| (k.into_user(), v.map(|x| x.into_user())))
            .boxed())
    }
}

#[async_trait::async_trait]
impl mvcc::ScopedSet<UserKey, MetaValue> for StateMachineView {
    fn set(&mut self, key: UserKey, value: Option<MetaValue>) -> SeqMarked<()> {
        let mvcc_view: &mut MvccView = self.deref_mut();
        mvcc_view.set(Namespace::User, Key::User(key), value.map(Value::User))
    }
}

#[async_trait::async_trait]
impl mvcc::ScopedGet<ExpireKey, String> for StateMachineView {
    async fn get(&self, key: ExpireKey) -> Result<SeqMarked<String>, io::Error> {
        let got = self
            .deref()
            .get(Namespace::Expire, Key::Expire(key))
            .await?;

        Ok(got.map(|x| x.into_expire()))
    }
}

#[async_trait::async_trait]
impl mvcc::ScopedSet<ExpireKey, String> for StateMachineView {
    fn set(&mut self, key: ExpireKey, value: Option<String>) -> SeqMarked<()> {
        let t: &mut MvccView = self.deref_mut();
        t.set(
            Namespace::Expire,
            Key::Expire(key),
            value.map(Value::Expire),
        )
    }
}

#[async_trait::async_trait]
impl mvcc::ScopedRange<ExpireKey, String> for StateMachineView {
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

        let strm = self.inner.range(Namespace::Expire, (start, end)).await?;

        Ok(strm
            .map_ok(|(k, v)| (k.into_expire(), v.map(|x| x.into_expire())))
            .boxed())
    }
}
