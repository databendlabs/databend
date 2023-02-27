// Copyright 2023 Datafuse Labs.
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

use common_expression::types::array::ArrayColumnBuilder;
use common_expression::types::ArrayType;
use common_expression::types::GenericType;
use common_expression::FunctionDomain;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use common_expression::Value;
use common_expression::ValueRef;

pub fn register(registry: &mut FunctionRegistry) {
    registry
        .register_passthrough_nullable_1_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>, _, _>(
            "unnest",
            FunctionProperty::default(),
            |_| FunctionDomain::Full,
            |val, ctx| {
                match val {
                    ValueRef::Scalar(array) => {
                        let array_col = ArrayColumnBuilder::<GenericType<0>>::repeat(&array, ctx.num_rows);
                        Value::Column(array_col.build())
                    },
                    ValueRef::Column(array_col) => {
                        Value::Column(array_col)
                    }
                }
            }
        );
}
