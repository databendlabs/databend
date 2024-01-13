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

use std::collections::BTreeMap;

use crate::ast::UriLocation;
use crate::input::Input;
use crate::parser::expr::*;
use crate::rule;
use crate::util::*;
use crate::ErrorKind;

pub fn share_endpoint_uri_location(i: Input) -> IResult<UriLocation> {
    map_res(
        rule! {
            #literal_string
        },
        |location| {
            UriLocation::from_uri(location, "".to_string(), BTreeMap::new())
                .map_err(|_| nom::Err::Failure(ErrorKind::Other("invalid uri")))
        },
    )(i)
}
