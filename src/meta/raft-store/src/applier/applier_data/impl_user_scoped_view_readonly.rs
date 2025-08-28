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
use std::ops::RangeBounds;

use futures_util::StreamExt;
use futures_util::TryStreamExt;
use map_api::mvcc;
use map_api::mvcc::ViewReadonly;
use map_api::IOResultStream;
use seq_marked::InternalSeq;
use seq_marked::SeqMarked;
use state_machine_api::MetaValue;
use state_machine_api::UserKey;

use crate::applier::applier_data::ApplierData;
use crate::leveled_store::types::Key;
use crate::leveled_store::types::Namespace;

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
