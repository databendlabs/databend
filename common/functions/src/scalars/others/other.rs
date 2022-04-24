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

use super::humanize::HumanizeFunction;
use super::inet_aton::InetAtonFunction;
use super::inet_aton::TryInetAtonFunction;
use super::inet_ntoa::InetNtoaFunction;
use super::inet_ntoa::TryInetNtoaFunction;
use super::running_difference_function::RunningDifferenceFunction;
use super::ExistsFunction;
use super::IgnoreFunction;
use super::SleepFunction;
use super::TypeOfFunction;
use crate::scalars::FunctionFactory;

#[derive(Clone)]
pub struct OtherFunction {}

impl OtherFunction {
    pub fn register(factory: &mut FunctionFactory) {
        factory.register("exists", ExistsFunction::desc());
        factory.register("typeof", TypeOfFunction::desc());
        factory.register("sleep", SleepFunction::desc());

        factory.register("running_difference", RunningDifferenceFunction::desc());
        factory.register("ignore", IgnoreFunction::desc());
        factory.register("humanize", HumanizeFunction::desc());

        // INET string to number.
        factory.register("ipv4_string_to_num", InetAtonFunction::desc());
        factory.register("try_ipv4_string_to_num", TryInetAtonFunction::desc());
        factory.register("inet_aton", InetAtonFunction::desc());
        factory.register("try_inet_aton", TryInetAtonFunction::desc());

        // INET number to string.
        factory.register("ipv4_num_to_string", InetNtoaFunction::desc());
        factory.register("try_ipv4_num_to_string", TryInetNtoaFunction::desc());
        factory.register("inet_ntoa", InetNtoaFunction::desc());
        factory.register("try_inet_ntoa", TryInetNtoaFunction::desc());
    }
}
