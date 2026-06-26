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

use databend_common_proto_conv::FromToProto;
use databend_meta_client::kvapi::kvapi;

use super::target::FetchedRecordTarget;

/// A fetched record asserted to be absent.
pub struct AbsentRecord<'t, 'a, KV: ?Sized, K: kvapi::Key> {
    pub(crate) target: FetchedRecordTarget<'t, 'a, KV, K>,
}

impl<'t, 'a, KV, K> AbsentRecord<'t, 'a, KV, K>
where
    KV: ?Sized,
    K: kvapi::Key,
    K::ValueType: FromToProto + 'static,
{
    /// Stage a put to the read key.
    pub fn stage_put(self, value: &K::ValueType) {
        self.target.stage_put(value)
    }
}
