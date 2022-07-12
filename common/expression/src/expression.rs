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

use std::sync::Arc;

use crate::function::Function;
use crate::function::FunctionID;
use crate::property::ValueProperty;
use crate::types::DataType;

#[derive(Debug, Clone)]
pub enum RawExpr {
    Literal(Literal),
    ColumnRef {
        id: usize,
        data_type: DataType,
        property: ValueProperty,
    },
    // Cast {
    //     is_try: bool,
    //     expr: Box<Expr>,
    //     dest_type: DataType,
    // },
    FunctionCall {
        name: String,
        params: Vec<usize>,
        args: Vec<RawExpr>,
    },
}

#[derive(Debug, Clone)]
pub enum Expr {
    Literal(Literal),
    ColumnRef {
        id: usize,
    },
    Cast {
        // is_try: bool,
        expr: Box<Expr>,
        dest_type: DataType,
    },
    FunctionCall {
        id: FunctionID,
        function: Arc<Function>,
        generics: Vec<DataType>,
        args: Vec<(Expr, ValueProperty)>,
    },
}

#[derive(Debug, Clone)]
pub enum Literal {
    Null,
    Int8(i8),
    Int16(i16),
    UInt8(u8),
    UInt16(u16),
    Boolean(bool),
    String(Vec<u8>),
}
