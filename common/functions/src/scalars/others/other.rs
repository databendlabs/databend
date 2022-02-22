// Copyright 2022 Datafuse Labs.
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

use super::inet_aton::InetAtonFunction;
use super::inet_aton::TryInetAtonFunction;
use super::inet_ntoa::InetNtoaFunction;
use super::inet_ntoa::TryInetNtoaFunction;
use super::running_difference_function::RunningDifferenceFunction;
use super::IgnoreFunction;
use crate::scalars::FunctionFactory;

#[derive(Clone)]
pub struct OtherFunction {}

impl OtherFunction {
    pub fn register(factory: &mut FunctionFactory) {
        factory.register("runningDifference", RunningDifferenceFunction::desc());
        factory.register("ignore", IgnoreFunction::desc());

        // inet_aton
        factory.register("inet_aton", InetAtonFunction::desc());
        factory.register("IPv4StringToNum", InetAtonFunction::desc());

        // try_inet_aton
        factory.register("try_inet_aton", TryInetAtonFunction::desc());
        factory.register("TryIPv4StringToNum", TryInetAtonFunction::desc());

        // inet_ntoa
        factory.register("inet_ntoa", InetNtoaFunction::desc());
        factory.register("IPv4NumToString", InetNtoaFunction::desc());

        // try_inet_ntoa
        factory.register("try_inet_ntoa", TryInetNtoaFunction::desc());
        factory.register("TryIPv4NumToString", TryInetNtoaFunction::desc());
    }
}
