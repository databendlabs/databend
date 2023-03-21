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

use common_arrow::arrow::bitmap::Bitmap;
use common_expression::types::nullable::NullableColumn;
use common_expression::types::AnyType;
use common_expression::types::DataType;
use common_expression::Column;
use common_expression::ScalarRef;
use common_expression::Value;

use crate::srfs::SetReturningFunctionRegistry;

pub fn builtin_set_returning_functions() -> SetReturningFunctionRegistry {
    let mut registry = SetReturningFunctionRegistry::default();
    register_unnest_functions(&mut registry);
    registry
}

pub fn register_unnest_functions(registry: &mut SetReturningFunctionRegistry) {
    let unnest_impl = |args: &[ScalarRef], arg_types: &[DataType]| {
        let arg = Value::<AnyType>::Scalar(args[0].to_owned());
        let mut result = arg.convert_to_full_column(&arg_types[0], 1).unnest();
        let num_rows = result.len();
        if !result.data_type().is_nullable() {
            result = Column::Nullable(Box::new(NullableColumn {
                column: result,
                validity: Bitmap::new_zeroed(num_rows),
            }));
        }

        (vec![Value::<AnyType>::Column(result)], num_rows)
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
