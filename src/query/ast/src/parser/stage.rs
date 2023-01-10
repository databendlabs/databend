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
use std::collections::BTreeMap;

use nom::branch::alt;
use nom::combinator::map;
use url::Url;

use crate::ast::StageLocation;
use crate::ast::UriLocation;
use crate::input::Input;
use crate::parser::expr::*;
use crate::parser::token::*;
use crate::rule;
use crate::util::*;
use crate::ErrorKind;

pub fn ident_to_string(i: Input) -> IResult<String> {
    map_res(ident, |ident| Ok(ident.name))(i)
}

pub fn u64_to_string(i: Input) -> IResult<String> {
    map(literal_u64, |v| v.to_string())(i)
}

pub fn parameter_to_string(i: Input) -> IResult<String> {
    map(
        rule! { ( #literal_string | #ident_to_string | #u64_to_string ) },
        |parameter| parameter,
    )(i)
}

// parse: (k = v ...)* into a map
pub fn options(i: Input) -> IResult<BTreeMap<String, String>> {
    let ident_with_format = alt((
        ident_to_string,
        map(rule! { FORMAT }, |_| "FORMAT".to_string()),
    ));

    map(
        rule! {
            "(" ~ ( #ident_with_format ~ "=" ~ #parameter_to_string )* ~ ")"
        },
        |(_, opts, _)| {
            BTreeMap::from_iter(opts.iter().map(|(k, _, v)| (k.to_lowercase(), v.clone())))
        },
    )(i)
}

pub fn stage_location(i: Input) -> IResult<StageLocation> {
    map_res(at_string, |location| {
        let parsed = location.splitn(2, '/').collect::<Vec<_>>();
        let name = parsed[0].to_string();
        let path = if parsed.len() == 1 {
            "/".to_string()
        } else {
            format!("/{}", parsed[1])
        };
        Ok(StageLocation { name, path })
    })(i)
}

/// Parse input into `UriLocation`
pub fn uri_location(i: Input) -> IResult<UriLocation> {
    map_res(
        rule! {
            #literal_string
            ~ (CONNECTION ~ "=" ~ #options)?
            ~ (CREDENTIALS ~ "=" ~ #options)?
            ~ (ENCRYPTION ~ "=" ~ #options)?
            ~ (LOCATION_PREFIX ~ "=" ~ #literal_string)?
        },
        |(location, connection_opt, credentials_opt, encryption_opt, location_prefix)| {
            let part_prefix = if let Some((_, _, p)) = location_prefix {
                p
            } else {
                "".to_string()
            };
            // fs location is not a valid url, let's check it in advance.
            if let Some(path) = location.strip_prefix("fs://") {
                return Ok(UriLocation::new(
                    "fs".to_string(),
                    "".to_string(),
                    path.to_string(),
                    part_prefix,
                    BTreeMap::default(),
                ));
            }

            let parsed =
                Url::parse(&location).map_err(|_| ErrorKind::Other("invalid uri location"))?;

            // TODO: We will use `CONNECTION` to replace `CREDENTIALS` and `ENCRYPTION`.
            let mut conns = connection_opt.map(|v| v.2).unwrap_or_default();
            conns.extend(credentials_opt.map(|v| v.2).unwrap_or_default());
            conns.extend(encryption_opt.map(|v| v.2).unwrap_or_default());

            let protocol = parsed.scheme().to_string();

            let name = parsed
                .host_str()
                .map(|hostname| {
                    if let Some(port) = parsed.port() {
                        format!("{}:{}", hostname, port)
                    } else {
                        hostname.to_string()
                    }
                })
                .ok_or(ErrorKind::Other("invalid uri location"))?;

            let path = if parsed.path().is_empty() {
                "/".to_string()
            } else {
                parsed.path().to_string()
            };

            Ok(UriLocation::new(protocol, name, path, part_prefix, conns))
        },
    )(i)
}
