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
use common_expression::ColumnBuilder;
use common_expression::Function;
use common_expression::FunctionEval;
use common_expression::FunctionKind;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use common_expression::FunctionSignature;
use common_expression::Scalar;
use common_expression::Value;
use common_expression::ValueRef;

pub fn register(registry: &mut FunctionRegistry) {
    registry.properties.insert(
        "unnest".to_string(),
        FunctionProperty::default().kind(FunctionKind::SRF),
    );

    let unnest_impl = |args: &[ValueRef<AnyType>], num_rows| {
        debug_assert_eq!(args.len(), 1);
        let unnest_array = match args[0].clone().to_owned() {
            Value::Scalar(Scalar::Array(col)) => {
                ArrayColumnBuilder::<AnyType>::repeat(&col, num_rows).build()
            }
            Value::Column(Column::Array(col)) => *col,
            Value::Column(Column::Nullable(box nullable_column)) => match nullable_column.column {
                Column::Array(col) => *col,
                _ => unreachable!(),
            },
            _ => unreachable!(),
        };
        debug_assert_eq!(unnest_array.len(), num_rows);
        unnest_array
            .iter()
            .map(|v| {
                let column = v.unnest();
                let len = column.len();
                (Value::Column(Column::Tuple(vec![column])), len)
            })
            .collect::<Vec<_>>()
    };

    {
        // Unnest 10d-array
        let args_type = vec![DataType::Array(Box::new(DataType::Array(Box::new(
            DataType::Array(Box::new(DataType::Array(Box::new(DataType::Array(
                Box::new(DataType::Array(Box::new(DataType::Array(Box::new(
                    DataType::Array(Box::new(DataType::Array(Box::new(DataType::Array(
                        Box::new(DataType::Nullable(Box::new(DataType::Generic(0)))),
                    ))))),
                ))))),
            ))))),
        ))))];
        registry.register_function(Function {
            signature: FunctionSignature {
                name: "unnest".to_string(),
                args_type: vec![args_type[0].wrap_nullable()],
                return_type: DataType::Tuple(vec![DataType::Nullable(Box::new(
                    DataType::Generic(0),
                ))]),
            },
            eval: FunctionEval::SRF {
                eval: Box::new(unnest_impl),
            },
        });
        registry.register_function(Function {
            signature: FunctionSignature {
                name: "unnest".to_string(),
                args_type,
                return_type: DataType::Tuple(vec![DataType::Nullable(Box::new(
                    DataType::Generic(0),
                ))]),
            },
            eval: FunctionEval::SRF {
                eval: Box::new(unnest_impl),
            },
        });
    }

    {
        // Unnest 9d-array
        let args_type = vec![DataType::Array(Box::new(DataType::Array(Box::new(
            DataType::Array(Box::new(DataType::Array(Box::new(DataType::Array(
                Box::new(DataType::Array(Box::new(DataType::Array(Box::new(
                    DataType::Array(Box::new(DataType::Array(Box::new(DataType::Nullable(
                        Box::new(DataType::Generic(0)),
                    ))))),
                ))))),
            ))))),
        ))))];
        registry.register_function(Function {
            signature: FunctionSignature {
                name: "unnest".to_string(),
                args_type: vec![args_type[0].wrap_nullable()],
                return_type: DataType::Tuple(vec![DataType::Nullable(Box::new(
                    DataType::Generic(0),
                ))]),
            },
            eval: FunctionEval::SRF {
                eval: Box::new(unnest_impl),
            },
        });
        registry.register_function(Function {
            signature: FunctionSignature {
                name: "unnest".to_string(),
                args_type,
                return_type: DataType::Tuple(vec![DataType::Nullable(Box::new(
                    DataType::Generic(0),
                ))]),
            },
            eval: FunctionEval::SRF {
                eval: Box::new(unnest_impl),
            },
        });
    }

    {
        // Unnest 8d-array
        let args_type = vec![DataType::Array(Box::new(DataType::Array(Box::new(
            DataType::Array(Box::new(DataType::Array(Box::new(DataType::Array(
                Box::new(DataType::Array(Box::new(DataType::Array(Box::new(
                    DataType::Array(Box::new(DataType::Nullable(Box::new(DataType::Generic(0))))),
                ))))),
            ))))),
        ))))];
        registry.register_function(Function {
            signature: FunctionSignature {
                name: "unnest".to_string(),
                args_type: vec![args_type[0].wrap_nullable()],
                return_type: DataType::Tuple(vec![DataType::Nullable(Box::new(
                    DataType::Generic(0),
                ))]),
            },
            eval: FunctionEval::SRF {
                eval: Box::new(unnest_impl),
            },
        });
        registry.register_function(Function {
            signature: FunctionSignature {
                name: "unnest".to_string(),
                args_type,
                return_type: DataType::Tuple(vec![DataType::Nullable(Box::new(
                    DataType::Generic(0),
                ))]),
            },
            eval: FunctionEval::SRF {
                eval: Box::new(unnest_impl),
            },
        });
    }

    {
        // Unnest 7d-array
        let args_type = vec![DataType::Array(Box::new(DataType::Array(Box::new(
            DataType::Array(Box::new(DataType::Array(Box::new(DataType::Array(
                Box::new(DataType::Array(Box::new(DataType::Array(Box::new(
                    DataType::Nullable(Box::new(DataType::Generic(0))),
                ))))),
            ))))),
        ))))];
        registry.register_function(Function {
            signature: FunctionSignature {
                name: "unnest".to_string(),
                args_type: vec![args_type[0].wrap_nullable()],
                return_type: DataType::Tuple(vec![DataType::Nullable(Box::new(
                    DataType::Generic(0),
                ))]),
            },
            eval: FunctionEval::SRF {
                eval: Box::new(unnest_impl),
            },
        });
        registry.register_function(Function {
            signature: FunctionSignature {
                name: "unnest".to_string(),
                args_type,
                return_type: DataType::Tuple(vec![DataType::Nullable(Box::new(
                    DataType::Generic(0),
                ))]),
            },
            eval: FunctionEval::SRF {
                eval: Box::new(unnest_impl),
            },
        });
    }

    {
        // Unnest 6d-array
        let args_type = vec![DataType::Array(Box::new(DataType::Array(Box::new(
            DataType::Array(Box::new(DataType::Array(Box::new(DataType::Array(
                Box::new(DataType::Array(Box::new(DataType::Nullable(Box::new(
                    DataType::Generic(0),
                ))))),
            ))))),
        ))))];
        registry.register_function(Function {
            signature: FunctionSignature {
                name: "unnest".to_string(),
                args_type: vec![args_type[0].wrap_nullable()],
                return_type: DataType::Tuple(vec![DataType::Nullable(Box::new(
                    DataType::Generic(0),
                ))]),
            },
            eval: FunctionEval::SRF {
                eval: Box::new(unnest_impl),
            },
        });
        registry.register_function(Function {
            signature: FunctionSignature {
                name: "unnest".to_string(),
                args_type,
                return_type: DataType::Tuple(vec![DataType::Nullable(Box::new(
                    DataType::Generic(0),
                ))]),
            },
            eval: FunctionEval::SRF {
                eval: Box::new(unnest_impl),
            },
        });
    }

    {
        // Unnest 5d-array
        let args_type = vec![DataType::Array(Box::new(DataType::Array(Box::new(
            DataType::Array(Box::new(DataType::Array(Box::new(DataType::Array(
                Box::new(DataType::Nullable(Box::new(DataType::Generic(0)))),
            ))))),
        ))))];
        registry.register_function(Function {
            signature: FunctionSignature {
                name: "unnest".to_string(),
                args_type: vec![args_type[0].wrap_nullable()],
                return_type: DataType::Tuple(vec![DataType::Nullable(Box::new(
                    DataType::Generic(0),
                ))]),
            },
            eval: FunctionEval::SRF {
                eval: Box::new(unnest_impl),
            },
        });
        registry.register_function(Function {
            signature: FunctionSignature {
                name: "unnest".to_string(),
                args_type,
                return_type: DataType::Tuple(vec![DataType::Nullable(Box::new(
                    DataType::Generic(0),
                ))]),
            },
            eval: FunctionEval::SRF {
                eval: Box::new(unnest_impl),
            },
        });
    }

    {
        // Unnest 4d-array
        let args_type = vec![DataType::Array(Box::new(DataType::Array(Box::new(
            DataType::Array(Box::new(DataType::Array(Box::new(DataType::Nullable(
                Box::new(DataType::Generic(0)),
            ))))),
        ))))];
        registry.register_function(Function {
            signature: FunctionSignature {
                name: "unnest".to_string(),
                args_type: vec![args_type[0].wrap_nullable()],
                return_type: DataType::Tuple(vec![DataType::Nullable(Box::new(
                    DataType::Generic(0),
                ))]),
            },
            eval: FunctionEval::SRF {
                eval: Box::new(unnest_impl),
            },
        });
        registry.register_function(Function {
            signature: FunctionSignature {
                name: "unnest".to_string(),
                args_type,
                return_type: DataType::Tuple(vec![DataType::Nullable(Box::new(
                    DataType::Generic(0),
                ))]),
            },
            eval: FunctionEval::SRF {
                eval: Box::new(unnest_impl),
            },
        });
    }

    {
        // Unnest 3d-array
        let args_type = vec![DataType::Array(Box::new(DataType::Array(Box::new(
            DataType::Array(Box::new(DataType::Nullable(Box::new(DataType::Generic(0))))),
        ))))];
        registry.register_function(Function {
            signature: FunctionSignature {
                name: "unnest".to_string(),
                args_type: vec![args_type[0].wrap_nullable()],
                return_type: DataType::Tuple(vec![DataType::Nullable(Box::new(
                    DataType::Generic(0),
                ))]),
            },
            eval: FunctionEval::SRF {
                eval: Box::new(unnest_impl),
            },
        });
        registry.register_function(Function {
            signature: FunctionSignature {
                name: "unnest".to_string(),
                args_type,
                return_type: DataType::Tuple(vec![DataType::Nullable(Box::new(
                    DataType::Generic(0),
                ))]),
            },
            eval: FunctionEval::SRF {
                eval: Box::new(unnest_impl),
            },
        });
    }

    {
        // Unnest 2d-array
        let args_type = vec![DataType::Array(Box::new(DataType::Array(Box::new(
            DataType::Nullable(Box::new(DataType::Generic(0))),
        ))))];
        registry.register_function(Function {
            signature: FunctionSignature {
                name: "unnest".to_string(),
                args_type: vec![args_type[0].wrap_nullable()],
                return_type: DataType::Tuple(vec![DataType::Nullable(Box::new(
                    DataType::Generic(0),
                ))]),
            },
            eval: FunctionEval::SRF {
                eval: Box::new(unnest_impl),
            },
        });
        registry.register_function(Function {
            signature: FunctionSignature {
                name: "unnest".to_string(),
                args_type,
                return_type: DataType::Tuple(vec![DataType::Nullable(Box::new(
                    DataType::Generic(0),
                ))]),
            },
            eval: FunctionEval::SRF {
                eval: Box::new(unnest_impl),
            },
        });
    }

    {
        // Unnest 1d-array
        let args_type = vec![DataType::Array(Box::new(DataType::Nullable(Box::new(
            DataType::Generic(0),
        ))))];
        registry.register_function(Function {
            signature: FunctionSignature {
                name: "unnest".to_string(),
                args_type: vec![args_type[0].wrap_nullable()],
                return_type: DataType::Tuple(vec![DataType::Nullable(Box::new(
                    DataType::Generic(0),
                ))]),
            },
            eval: FunctionEval::SRF {
                eval: Box::new(unnest_impl),
            },
        });
        registry.register_function(Function {
            signature: FunctionSignature {
                name: "unnest".to_string(),
                args_type,
                return_type: DataType::Tuple(vec![DataType::Nullable(Box::new(
                    DataType::Generic(0),
                ))]),
            },
            eval: FunctionEval::SRF {
                eval: Box::new(unnest_impl),
            },
        });
    }

    {
        // Unnest NULL
        let args_type = vec![DataType::Null];
        registry.register_function(Function {
            signature: FunctionSignature {
                name: "unnest".to_string(),
                args_type,
                return_type: DataType::Tuple(vec![DataType::Null]),
            },
            eval: FunctionEval::SRF {
                eval: Box::new(|_, num_rows| {
                    let mut columns = Vec::with_capacity(num_rows);
                    (0..num_rows).for_each(|_| {
                        let column = ColumnBuilder::with_capacity(&DataType::Null, 0).build();
                        columns.push((Value::Column(Column::Tuple(vec![column])), 0));
                    });
                    columns
                }),
            },
        });
    }
}
