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

use common_expression::types::array::ArrayColumnBuilder;
use common_expression::types::AnyType;
use common_expression::types::DataType;
use common_expression::Column;
use common_expression::Scalar;
use common_expression::Value;
use common_expression::ValueRef;

use crate::srfs::SetReturningFunctionRegistry;

pub fn builtin_set_returning_functions() -> SetReturningFunctionRegistry {
    let mut registry = SetReturningFunctionRegistry::default();
    register_unnest_functions(&mut registry);
    registry
}

pub fn register_unnest_functions(registry: &mut SetReturningFunctionRegistry) {
    let unnest_impl = |args: &[ValueRef<AnyType>], num_rows| {
        debug_assert_eq!(args.len(), 1);
        let unnest_array = match args[0].clone().to_owned() {
            Value::Scalar(Scalar::Array(col)) => {
                ArrayColumnBuilder::<AnyType>::repeat(&col, num_rows).build()
            }
            Value::Column(Column::Array(col)) => *col,
            _ => unreachable!(),
        };
        debug_assert_eq!(unnest_array.len(), num_rows);
        let result = unnest_array
            .iter()
            .map(|v| {
                let column = v.unnest();
                let len = column.len();
                (vec![Value::Column(column)], len)
            })
            .collect::<Vec<_>>();

        result
    };

    {
        // Unnest 10d-array
        let arg_types = vec![DataType::Array(Box::new(DataType::Array(Box::new(
            DataType::Array(Box::new(DataType::Array(Box::new(DataType::Array(
                Box::new(DataType::Array(Box::new(DataType::Array(Box::new(
                    DataType::Array(Box::new(DataType::Array(Box::new(DataType::Array(
                        Box::new(DataType::Nullable(Box::new(DataType::Generic(0)))),
                    ))))),
                ))))),
            ))))),
        ))))];
        registry.register(
            "unnest",
            &arg_types,
            &[DataType::Nullable(Box::new(DataType::Generic(0)))],
            unnest_impl,
        );
    }

    {
        // Unnest 9d-array
        let arg_types = vec![DataType::Array(Box::new(DataType::Array(Box::new(
            DataType::Array(Box::new(DataType::Array(Box::new(DataType::Array(
                Box::new(DataType::Array(Box::new(DataType::Array(Box::new(
                    DataType::Array(Box::new(DataType::Array(Box::new(DataType::Nullable(
                        Box::new(DataType::Generic(0)),
                    ))))),
                ))))),
            ))))),
        ))))];
        registry.register(
            "unnest",
            &arg_types,
            &[DataType::Nullable(Box::new(DataType::Generic(0)))],
            unnest_impl,
        );
    }

    {
        // Unnest 8d-array
        let arg_types = vec![DataType::Array(Box::new(DataType::Array(Box::new(
            DataType::Array(Box::new(DataType::Array(Box::new(DataType::Array(
                Box::new(DataType::Array(Box::new(DataType::Array(Box::new(
                    DataType::Array(Box::new(DataType::Nullable(Box::new(DataType::Generic(0))))),
                ))))),
            ))))),
        ))))];
        registry.register(
            "unnest",
            &arg_types,
            &[DataType::Nullable(Box::new(DataType::Generic(0)))],
            unnest_impl,
        );
    }

    {
        // Unnest 7d-array
        let arg_types = vec![DataType::Array(Box::new(DataType::Array(Box::new(
            DataType::Array(Box::new(DataType::Array(Box::new(DataType::Array(
                Box::new(DataType::Array(Box::new(DataType::Array(Box::new(
                    DataType::Nullable(Box::new(DataType::Generic(0))),
                ))))),
            ))))),
        ))))];
        registry.register(
            "unnest",
            &arg_types,
            &[DataType::Nullable(Box::new(DataType::Generic(0)))],
            unnest_impl,
        );
    }

    {
        // Unnest 6d-array
        let arg_types = vec![DataType::Array(Box::new(DataType::Array(Box::new(
            DataType::Array(Box::new(DataType::Array(Box::new(DataType::Array(
                Box::new(DataType::Array(Box::new(DataType::Nullable(Box::new(
                    DataType::Generic(0),
                ))))),
            ))))),
        ))))];
        registry.register(
            "unnest",
            &arg_types,
            &[DataType::Nullable(Box::new(DataType::Generic(0)))],
            unnest_impl,
        );
    }

    {
        // Unnest 5d-array
        let arg_types = vec![DataType::Array(Box::new(DataType::Array(Box::new(
            DataType::Array(Box::new(DataType::Array(Box::new(DataType::Array(
                Box::new(DataType::Nullable(Box::new(DataType::Generic(0)))),
            ))))),
        ))))];
        registry.register(
            "unnest",
            &arg_types,
            &[DataType::Nullable(Box::new(DataType::Generic(0)))],
            unnest_impl,
        );
    }

    {
        // Unnest 4d-array
        let arg_types = vec![DataType::Array(Box::new(DataType::Array(Box::new(
            DataType::Array(Box::new(DataType::Array(Box::new(DataType::Nullable(
                Box::new(DataType::Generic(0)),
            ))))),
        ))))];
        registry.register(
            "unnest",
            &arg_types,
            &[DataType::Nullable(Box::new(DataType::Generic(0)))],
            unnest_impl,
        );
    }

    {
        // Unnest 3d-array
        let arg_types = vec![DataType::Array(Box::new(DataType::Array(Box::new(
            DataType::Array(Box::new(DataType::Nullable(Box::new(DataType::Generic(0))))),
        ))))];
        registry.register(
            "unnest",
            &arg_types,
            &[DataType::Nullable(Box::new(DataType::Generic(0)))],
            unnest_impl,
        );
    }

    {
        // Unnest 2d-array
        let arg_types = vec![DataType::Array(Box::new(DataType::Array(Box::new(
            DataType::Nullable(Box::new(DataType::Generic(0))),
        ))))];
        registry.register(
            "unnest",
            &arg_types,
            &[DataType::Nullable(Box::new(DataType::Generic(0)))],
            unnest_impl,
        );
    }

    {
        // Unnest 1d-array
        let arg_types = vec![DataType::Array(Box::new(DataType::Nullable(Box::new(
            DataType::Generic(0),
        ))))];
        registry.register(
            "unnest",
            &arg_types,
            &[DataType::Nullable(Box::new(DataType::Generic(0)))],
            unnest_impl,
        );
    }
}
