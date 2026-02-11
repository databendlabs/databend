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

use databend_meta_kvapi::kvapi;
use databend_meta_types::MetaError;

#[async_trait::async_trait]
pub trait RefApi
where
    Self: Send + Sync,
    Self: kvapi::KVApi<Error = MetaError>,
{
    #[logcall::logcall]
    #[fastrace::trace]
    async fn create_table_tag() {

    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn drop_table_tag() {

    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_table_tag() {

    }

    #[logcall::logcall]
    #[fastrace::trace]
    #[logcall::logcall]
    #[fastrace::trace]
    async fn create_table_branch() {

    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn drop_table_branch() {

    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn undrop_table_branch() {
        // 
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_table_branch() {
        // get branch需要先拿到table id,然后再拿到branch id,再拿到branch的tablemeta
    }
}

#[async_trait::async_trait]
impl<KV> RefApi for KV
where
    KV: Send + Sync,
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
}
