//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

mod api;
mod key;
mod test_suite;

pub use api::get_start_and_end_of_prefix;
pub use api::prefix_of_string;
pub use api::ApiBuilder;
pub use api::AsKVApi;
pub use api::KVApi;
pub use key::check_segment;
pub use key::check_segment_absent;
pub use key::check_segment_present;
pub use key::decode_id;
pub use key::escape;
pub use key::unescape;
pub use key::Key;
pub use key::KeyError;
pub use test_suite::TestSuite;
