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

use common_constraint::mir::MirBinaryOperator;
use common_constraint::mir::MirConstant;
use common_constraint::mir::MirDataType;
use common_constraint::mir::MirExpr;
use common_constraint::mir::MirUnaryOperator;
use common_constraint::problem::variable_must_not_null;

#[test]
fn test_assert_int_not_null() {
    // a is not null => a is not null
    assert!(variable_must_not_null("a", &MirExpr::UnaryOperator {
        op: MirUnaryOperator::Not,
        arg: Box::new(MirExpr::UnaryOperator {
            op: MirUnaryOperator::IsNull,
            arg: Box::new(MirExpr::Variable {
                name: "a".to_string(),
                data_type: MirDataType::Int
            })
        })
    }));

    // a > 0 => a is not null
    assert!(variable_must_not_null("a", &MirExpr::BinaryOperator {
        op: MirBinaryOperator::Gt,
        left: Box::new(MirExpr::Variable {
            name: "a".to_string(),
            data_type: MirDataType::Int
        }),
        right: Box::new(MirExpr::Constant(MirConstant::Int(0)))
    }));

    // a > 0 or true => a may be null
    assert!(!variable_must_not_null("a", &MirExpr::BinaryOperator {
        op: MirBinaryOperator::Or,
        left: Box::new(MirExpr::BinaryOperator {
            op: MirBinaryOperator::Gt,
            left: Box::new(MirExpr::Variable {
                name: "a".to_string(),
                data_type: MirDataType::Int
            }),
            right: Box::new(MirExpr::Constant(MirConstant::Int(0)))
        }),
        right: Box::new(MirExpr::Constant(MirConstant::Bool(true)))
    }));

    // a > 0 or a < 0 => a is not null
    assert!(variable_must_not_null("a", &MirExpr::BinaryOperator {
        op: MirBinaryOperator::Or,
        left: Box::new(MirExpr::BinaryOperator {
            op: MirBinaryOperator::Gt,
            left: Box::new(MirExpr::Variable {
                name: "a".to_string(),
                data_type: MirDataType::Int
            }),
            right: Box::new(MirExpr::Constant(MirConstant::Int(0)))
        }),
        right: Box::new(MirExpr::BinaryOperator {
            op: MirBinaryOperator::Lt,
            left: Box::new(MirExpr::Variable {
                name: "a".to_string(),
                data_type: MirDataType::Int
            }),
            right: Box::new(MirExpr::Constant(MirConstant::Int(0)))
        })
    }));

    // a > 0 and a < 1 => a is not null
    assert!(variable_must_not_null("a", &MirExpr::BinaryOperator {
        op: MirBinaryOperator::And,
        left: Box::new(MirExpr::BinaryOperator {
            op: MirBinaryOperator::Gt,
            left: Box::new(MirExpr::Variable {
                name: "a".to_string(),
                data_type: MirDataType::Int
            }),
            right: Box::new(MirExpr::Constant(MirConstant::Int(0)))
        }),
        right: Box::new(MirExpr::BinaryOperator {
            op: MirBinaryOperator::Lt,
            left: Box::new(MirExpr::Variable {
                name: "a".to_string(),
                data_type: MirDataType::Int
            }),
            right: Box::new(MirExpr::Constant(MirConstant::Int(1)))
        })
    }));
}

#[test]
fn test_assert_int_is_not_null_multiple_variable() {
    // a > 0 and b > 0 => a is not null
    assert!(variable_must_not_null("a", &MirExpr::BinaryOperator {
        op: MirBinaryOperator::And,
        left: Box::new(MirExpr::BinaryOperator {
            op: MirBinaryOperator::Gt,
            left: Box::new(MirExpr::Variable {
                name: "a".to_string(),
                data_type: MirDataType::Int
            }),
            right: Box::new(MirExpr::Constant(MirConstant::Int(0)))
        }),
        right: Box::new(MirExpr::BinaryOperator {
            op: MirBinaryOperator::Gt,
            left: Box::new(MirExpr::Variable {
                name: "b".to_string(),
                data_type: MirDataType::Int
            }),
            right: Box::new(MirExpr::Constant(MirConstant::Int(0)))
        })
    }));

    // a > 0 and b > 0 => b is not null
    assert!(variable_must_not_null("b", &MirExpr::BinaryOperator {
        op: MirBinaryOperator::And,
        left: Box::new(MirExpr::BinaryOperator {
            op: MirBinaryOperator::Gt,
            left: Box::new(MirExpr::Variable {
                name: "a".to_string(),
                data_type: MirDataType::Int
            }),
            right: Box::new(MirExpr::Constant(MirConstant::Int(0)))
        }),
        right: Box::new(MirExpr::BinaryOperator {
            op: MirBinaryOperator::Gt,
            left: Box::new(MirExpr::Variable {
                name: "b".to_string(),
                data_type: MirDataType::Int
            }),
            right: Box::new(MirExpr::Constant(MirConstant::Int(0)))
        })
    }));
}
