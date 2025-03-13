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

use nom::branch::alt;
use nom::combinator::map;
use nom_rule::rule;

use super::query::with;
use crate::ast::CopyIntoLocationOption;
use crate::ast::CopyIntoLocationSource;
use crate::ast::CopyIntoLocationStmt;
use crate::ast::CopyIntoTableOption;
use crate::ast::CopyIntoTableSource;
use crate::ast::CopyIntoTableStmt;
use crate::ast::LiteralStringOrVariable;
use crate::ast::Statement;
use crate::ast::Statement::CopyIntoLocation;
use crate::parser::common::comma_separated_list0;
use crate::parser::common::comma_separated_list1;
use crate::parser::common::ident;
use crate::parser::common::table_ref;
use crate::parser::common::IResult;
use crate::parser::common::*;
use crate::parser::expr::literal_bool;
use crate::parser::expr::literal_string;
use crate::parser::expr::literal_u64;
use crate::parser::query::query;
use crate::parser::stage::file_format_clause;
use crate::parser::stage::file_location;
use crate::parser::statement::hint;
use crate::parser::token::TokenKind::COPY;
use crate::parser::token::TokenKind::*;
use crate::parser::ErrorKind;
use crate::parser::Input;

pub fn copy_into_table(i: Input) -> IResult<Statement> {
    let copy_into_table_source = alt((
        map(file_location, CopyIntoTableSource::Location),
        map(rule! { "(" ~ #query ~ ")" }, |(_, query, _)| {
            CopyIntoTableSource::Query(Box::new(query))
        }),
    ));

    map_res(
        rule! {
            #with? ~ COPY
            ~ #hint?
            ~ INTO ~ #table_ref ~ ( "(" ~ #comma_separated_list1(ident) ~ ")" )?
            ~ ^FROM ~ ^#copy_into_table_source
            ~ #copy_into_table_option*
        },
        |(with, _copy, opt_hints, _into, dst, dst_columns, _from, src, opts)| {
            let mut copy_stmt = CopyIntoTableStmt {
                with,
                hints: opt_hints,
                src,
                dst,
                dst_columns: dst_columns.map(|(_, columns, _)| columns),
                files: Default::default(),
                pattern: Default::default(),
                file_format: Default::default(),

                options: Default::default(),
            };
            for opt in opts {
                copy_stmt
                    .apply_option(opt)
                    .map_err(|e| nom::Err::Failure(ErrorKind::Other(e)))?;
            }
            Ok(Statement::CopyIntoTable(copy_stmt))
        },
    )(i)
}

fn copy_into_location(i: Input) -> IResult<Statement> {
    let copy_into_location_source = alt((
        map(table_ref, CopyIntoLocationSource::Table),
        map(rule! { "(" ~ #query ~ ")" }, |(_, query, _)| {
            CopyIntoLocationSource::Query(Box::new(query))
        }),
    ));

    map(
        rule! {
            #with? ~ COPY
            ~ #hint?
            ~ INTO ~ #file_location
            ~ ^FROM ~ ^#copy_into_location_source
            ~ #copy_into_location_option*
        },
        |(with, _copy, opt_hints, _into, dst, _from, src, opts)| {
            let mut copy_stmt = CopyIntoLocationStmt {
                with,
                hints: opt_hints,
                src,
                dst,
                file_format: Default::default(),
                options: Default::default(),
            };
            for opt in opts {
                copy_stmt.apply_option(opt);
            }
            CopyIntoLocation(copy_stmt)
        },
    )(i)
}
pub fn copy_into(i: Input) -> IResult<Statement> {
    rule!(
         #copy_into_location:"`COPY
                INTO { internalStage | externalStage | externalLocation }
                FROM { [<database_name>.]<table_name> | ( <query> ) }
                [ FILE_FORMAT = ( { TYPE = { CSV | NDJSON | PARQUET | TSV | AVRO } [ formatTypeOptions ] } ) ]
                [ copyOptions ]`"
         | #copy_into_table: "`COPY
                INTO { [<database_name>.]<table_name> { ( <columns> ) } }
                FROM { internalStage | externalStage | externalLocation | ( <query> ) }
                [ FILE_FORMAT = ( { TYPE = { CSV | NDJSON | PARQUET | TSV | AVRO } [ formatTypeOptions ] } ) ]
                [ FILES = ( '<file_name>' [ , '<file_name>' ] [ , ... ] ) ]
                [ PATTERN = '<regex_pattern>' ]
                [ VALIDATION_MODE = RETURN_ROWS ]
                [ copyOptions ]`"
    )(i)
}

pub fn literal_string_or_variable(i: Input) -> IResult<LiteralStringOrVariable> {
    alt((
        map(literal_string, LiteralStringOrVariable::Literal),
        map(variable_ident, LiteralStringOrVariable::Variable),
    ))(i)
}

fn copy_into_table_option(i: Input) -> IResult<CopyIntoTableOption> {
    alt((
        map(
            rule! { FILES ~ "=" ~ "(" ~ #comma_separated_list0(literal_string) ~ ")" },
            |(_, _, _, files, _)| CopyIntoTableOption::Files(files),
        ),
        map(
            rule! { PATTERN ~ ^"=" ~ ^#literal_string_or_variable },
            |(_, _, pattern)| CopyIntoTableOption::Pattern(pattern),
        ),
        map(rule! { #file_format_clause }, |options| {
            CopyIntoTableOption::FileFormat(options)
        }),
        map(
            rule! { SIZE_LIMIT ~ "=" ~ #literal_u64 },
            |(_, _, size_limit)| CopyIntoTableOption::SizeLimit(size_limit as usize),
        ),
        map(
            rule! { MAX_FILES ~ "=" ~ #literal_u64 },
            |(_, _, max_files)| CopyIntoTableOption::MaxFiles(max_files as usize),
        ),
        map(
            rule! { SPLIT_SIZE ~ "=" ~ #literal_u64 },
            |(_, _, split_size)| CopyIntoTableOption::SplitSize(split_size as usize),
        ),
        map(rule! { PURGE ~ "=" ~ #literal_bool }, |(_, _, purge)| {
            CopyIntoTableOption::Purge(purge)
        }),
        map(rule! { FORCE ~ "=" ~ #literal_bool }, |(_, _, force)| {
            CopyIntoTableOption::Force(force)
        }),
        map(rule! { ON_ERROR ~ "=" ~ #ident }, |(_, _, on_error)| {
            CopyIntoTableOption::OnError(on_error.to_string())
        }),
        map(
            rule! { COLUMN_MATCH_MODE ~ "=" ~ #ident },
            |(_, _, mode)| CopyIntoTableOption::ColumnMatchMode(mode.to_string()),
        ),
        map(
            rule! { DISABLE_VARIANT_CHECK ~ "=" ~ #literal_bool },
            |(_, _, disable_variant_check)| {
                CopyIntoTableOption::DisableVariantCheck(disable_variant_check)
            },
        ),
        map(
            rule! { RETURN_FAILED_ONLY ~ "=" ~ #literal_bool },
            |(_, _, return_failed_only)| CopyIntoTableOption::ReturnFailedOnly(return_failed_only),
        ),
    ))(i)
}

fn copy_into_location_option(i: Input) -> IResult<CopyIntoLocationOption> {
    alt((
        map(rule! { SINGLE ~ "=" ~ #literal_bool }, |(_, _, single)| {
            CopyIntoLocationOption::Single(single)
        }),
        map(
            rule! { MAX_FILE_SIZE ~ "=" ~ #literal_u64 },
            |(_, _, max_file_size)| CopyIntoLocationOption::MaxFileSize(max_file_size as usize),
        ),
        map(
            rule! { DETAILED_OUTPUT ~ "=" ~ #literal_bool },
            |(_, _, detailed_output)| CopyIntoLocationOption::DetailedOutput(detailed_output),
        ),
        map(
            rule! { USE_RAW_PATH ~ "=" ~ #literal_bool },
            |(_, _, use_raw_path)| CopyIntoLocationOption::UseRawPath(use_raw_path),
        ),
        map(
            rule! {  INCLUDE_QUERY_ID ~ "=" ~ #literal_bool },
            |(_, _, include_query_id)| CopyIntoLocationOption::IncludeQueryID(include_query_id),
        ),
        map(
            rule! {  OVERWRITE ~ "=" ~ #literal_bool },
            |(_, _, include_query_id)| CopyIntoLocationOption::OverWrite(include_query_id),
        ),
        map(rule! { #file_format_clause }, |options| {
            CopyIntoLocationOption::FileFormat(options)
        }),
    ))(i)
}
