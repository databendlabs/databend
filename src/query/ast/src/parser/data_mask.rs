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

use nom::combinator::map;
use nom_rule::rule;

use crate::ast::DataMaskArg;
use crate::ast::DataMaskPolicy;
use crate::ast::Expr;
use crate::ast::TypeName;
use crate::parser::common::*;
use crate::parser::expr::*;
use crate::parser::input::Input;
use crate::parser::token::*;

fn data_mask_arg(i: Input) -> IResult<DataMaskArg> {
    map(rule! { #ident ~ #type_name }, |(arg_name, arg_type)| {
        DataMaskArg {
            arg_name: arg_name.name,
            arg_type,
        }
    })(i)
}

fn data_mask_more_arg(i: Input) -> IResult<DataMaskArg> {
    map(rule! { "," ~ #data_mask_arg }, |(_, arg)| arg)(i)
}

fn data_mask_arg_list(i: Input) -> IResult<Vec<DataMaskArg>> {
    map(rule! { #data_mask_more_arg* }, |args| args)(i)
}

fn data_mask_args(i: Input) -> IResult<Vec<DataMaskArg>> {
    map(
        rule! { AS ~ "(" ~ #data_mask_arg ~ #data_mask_arg_list ~ ")" },
        |(_, _, arg_0, more_args, _)| {
            let mut args = vec![];
            args.push(arg_0);
            for arg in more_args {
                args.push(arg);
            }
            args
        },
    )(i)
}

fn data_mask_body(i: Input) -> IResult<Expr> {
    map(rule! { #expr }, |expr| expr)(i)
}

fn data_mask_return_type(i: Input) -> IResult<TypeName> {
    map(rule! { RETURNS ~ #type_name }, |(_, type_name)| type_name)(i)
}

pub fn data_mask_policy(i: Input) -> IResult<DataMaskPolicy> {
    map(
        rule! { #data_mask_args ~ #data_mask_return_type ~ "->" ~ #data_mask_body ~ ( COMMENT ~ "=" ~ #literal_string)? },
        |(args, return_type, _, body, comment_opt)| DataMaskPolicy {
            args,
            return_type,
            body,
            comment: match comment_opt {
                Some(opt) => Some(opt.2),
                None => None,
            },
        },
    )(i)
}
