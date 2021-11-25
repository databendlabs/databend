// Copyright 2021 Datafuse Labs.
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

use crate::scalars::function_factory::FunctionFactory;
use crate::scalars::Md5HashFunction;
use crate::scalars::Sha1HashFunction;
use crate::scalars::Sha2HashFunction;
use crate::scalars::SipHashFunction;

#[derive(Clone)]
pub struct HashesFunction;

impl HashesFunction {
    pub fn register(factory: &mut FunctionFactory) {
        factory.register("siphash", SipHashFunction::desc());
        factory.register("siphash64", SipHashFunction::desc());
        factory.register("md5", Md5HashFunction::desc());
        factory.register("sha", Sha1HashFunction::desc());
        factory.register("sha1", Sha1HashFunction::desc());
        factory.register("sha2", Sha2HashFunction::desc());
    }
}
