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

use crate::protobuf::meta_service_client::MetaServiceClient;

impl<T> MetaServiceClient<T> {
    pub fn inner_mut(&mut self) -> &mut tonic::client::Grpc<T> {
        // assert the size of self is the same as tonic::client::Grpc<T>;
        assert_eq!(
            std::mem::size_of::<Self>(),
            std::mem::size_of::<tonic::client::Grpc<T>>()
        );
        let p = self as *mut MetaServiceClient<T>;
        unsafe { &mut *(p as *mut tonic::client::Grpc<T>) }
    }
}
