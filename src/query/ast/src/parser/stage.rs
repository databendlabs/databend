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

use nom::branch::alt;
use nom::combinator::map;

use crate::ast::SelectStageOption;
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

fn connection_opt(sep: &'static str) -> impl FnMut(Input) -> IResult<(String, String)> {
    move |i| {
        let sep1 = match_text(sep);
        let sep2 = match_text(sep);
        let string_options = map(
            rule! {
                ( #ident ) ~ #sep1 ~ #literal_string
            },
            |(k, _, v)| (k.to_string().to_lowercase(), v),
        );

        let bool_options = map(
            rule! {
                (ENABLE_VIRTUAL_HOST_STYLE) ~ #sep2 ~ #literal_bool
            },
            |(k, _, v)| (k.text().to_string().to_lowercase(), v.to_string()),
        );

        alt((string_options, bool_options))(i)
    }
}

pub fn connection_options(i: Input) -> IResult<BTreeMap<String, String>> {
    let connection_opt = connection_opt("=");
    map(rule! { "(" ~ (#connection_opt)* ~ ")"}, |(_, opts, _)| {
        BTreeMap::from_iter(opts.iter().map(|(k, v)| (k.to_lowercase(), v.clone())))
    })(i)
}

pub fn format_options(i: Input) -> IResult<BTreeMap<String, String>> {
    let option_type = map(
        rule! {
        (TYPE ~ "=" ~ (TSV| CSV | NDJSON | PARQUET | JSON | XML) )
        },
        |(_, _, v)| ("type".to_string(), v.text().to_string()),
    );

    let option_compression = map(
        rule! {
        (COMPRESSION ~ "=" ~ (AUTO | NONE | GZIP | BZ2 | BROTLI | ZSTD | DEFLATE | RAWDEFLATE | XZ ) )
        },
        |(_, _, v)| ("COMPRESSION".to_string(), v.text().to_string()),
    );

    let string_options = map(
        rule! {
            (TYPE
                | FORMAT_NAME
                | COMPRESSION
                | RECORD_DELIMITER
                | FIELD_DELIMITER
                | QUOTE
                | NON_DISPLAY
                | ESCAPE
                | ROW_TAG) ~ "=" ~ #literal_string
        },
        |(k, _, v)| (k.text().to_string(), v),
    );

    let int_options = map(
        rule! {
            SKIP_HEADER ~ "=" ~ #u64_to_string
        },
        |(k, _, v)| (k.text().to_string(), v),
    );

    let none_options = map(
        rule! {
            (RECORD_DELIMITER | FIELD_DELIMITER | QUOTE | SKIP_HEADER | NON_DISPLAY | ESCAPE ) ~ "=" ~ NONE
        },
        |(k, _, v)| (k.text().to_string(), v.text().to_string()),
    );

    map(
        rule! { (#option_type | #option_compression | #string_options | #int_options | #none_options)* },
        |opts| BTreeMap::from_iter(opts.iter().map(|(k, v)| (k.to_lowercase(), v.clone()))),
    )(i)
}

pub fn file_format_clause(i: Input) -> IResult<BTreeMap<String, String>> {
    map(
        rule! { FILE_FORMAT ~ "=" ~ "(" ~ #format_options ~ ")" },
        |(_, _, _, opts, _)| opts,
    )(i)
}

// parse: (k = v ...)* into a map
pub fn options(i: Input) -> IResult<BTreeMap<String, String>> {
    map(
        rule! {
        "(" ~ ( #ident_to_string ~ "=" ~ #parameter_to_string )* ~ ")"
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
            ~ (CONNECTION ~ "=" ~ #connection_options)?
            ~ (CREDENTIALS ~ "=" ~ #connection_options)?
            ~ (LOCATION_PREFIX ~ "=" ~ #literal_string)?
        },
        |(location, connection_opt, credentials_opt, location_prefix)| {
            let part_prefix = if let Some((_, _, p)) = location_prefix {
                p
            } else {
                "".to_string()
            };
            // fs location is not a valid url, let's check it in advance.

            // TODO: We will use `CONNECTION` to replace `CREDENTIALS`.
            let mut conns = connection_opt.map(|v| v.2).unwrap_or_default();
            conns.extend(credentials_opt.map(|v| v.2).unwrap_or_default());

            UriLocation::from_uri(location, part_prefix, conns)
                .map_err(|_| ErrorKind::Other("invalid uri"))
        },
    )(i)
}

pub fn select_stage_option(i: Input) -> IResult<SelectStageOption> {
    let connection_opt = connection_opt("=>");
    alt((
        map(
            rule! { FILES ~ "=>" ~ "(" ~ #comma_separated_list0(literal_string) ~ ")" },
            |(_, _, _, files, _)| SelectStageOption::Files(files),
        ),
        map(
            rule! { PATTERN ~ "=>" ~ #literal_string },
            |(_, _, pattern)| SelectStageOption::Pattern(pattern),
        ),
        map(
            rule! { FILE_FORMAT ~ "=>" ~ #literal_string },
            |(_, _, file_format)| SelectStageOption::FileFormat(file_format),
        ),
        map(connection_opt, SelectStageOption::Connection),
    ))(i)
}
