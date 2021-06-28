// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::marker::PhantomData;
use std::ops::Add;
use std::ops::Div;
use std::ops::Mul;
use std::ops::Sub;

use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::array::PrimitiveArray;
use common_exception::ErrorCode;
use common_exception::Result;
use num::Num;
use num::NumCast;
use num::One;
use num::ToPrimitive;
use num::Zero;

use crate::DFNumericType;
use crate::DataColumn;
use crate::*;
