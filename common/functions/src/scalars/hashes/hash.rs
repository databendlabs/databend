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

use std::collections::hash_map::DefaultHasher;

use twox_hash::XxHash32;
use twox_hash::XxHash64;

use super::BaseHashFunction;
use crate::scalars::Blake3HashFunction;
use crate::scalars::City64WithSeedFunction;
use crate::scalars::FunctionFactory;
use crate::scalars::Md5HashFunction;
use crate::scalars::Sha1HashFunction;
use crate::scalars::Sha2HashFunction;

#[derive(Clone)]
pub struct HashesFunction;

pub type XxHash32Function = BaseHashFunction<XxHash32, u32>;
pub type XxHash64Function = BaseHashFunction<XxHash64, u64>;
pub type SipHash64Function = BaseHashFunction<DefaultHasher, u64>;

impl HashesFunction {
    pub fn register2(factory: &mut FunctionFactory) {
        factory.register("md5", Md5HashFunction::desc());
        factory.register("sha", Sha1HashFunction::desc());
        factory.register("sha1", Sha1HashFunction::desc());
        factory.register("sha2", Sha2HashFunction::desc());

        factory.register("blake3", Blake3HashFunction::desc());
        factory.register("xxhash32", XxHash32Function::desc());
        factory.register("xxhash64", XxHash64Function::desc());
        factory.register("siphash64", SipHash64Function::desc());
        factory.register("siphash", SipHash64Function::desc());
        factory.register("city64WithSeed", City64WithSeedFunction::desc());
    }
}
