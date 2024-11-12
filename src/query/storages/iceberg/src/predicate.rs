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

use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use iceberg::expr::Predicate;
use iceberg::expr::Reference;
use iceberg::spec::Datum;

#[derive(Default, Copy, Clone, Debug)]
pub struct PredicateBuilder {
    uncertain: bool,
}

impl PredicateBuilder {
    pub fn build(&mut self, expr: &RemoteExpr<String>) -> Predicate {
        match expr {
            RemoteExpr::Constant {
                span: _,
                scalar,
                data_type,
            } if data_type.remove_nullable() == DataType::Boolean => {
                let value = scalar.as_boolean();
                let is_true = value.copied().unwrap_or(false);
                if is_true {
                    Predicate::AlwaysTrue
                } else {
                    Predicate::AlwaysFalse
                }
            }

            // is_true
            RemoteExpr::FunctionCall {
                span: _,
                id,
                generics: _,
                args,
                return_type: _,
            } if args.len() == 1 && id.name().as_ref() == "is_true" => {
                let predicate = self.build(&args[0]);
                if self.uncertain {
                    return Predicate::AlwaysTrue;
                }
                match predicate {
                    Predicate::AlwaysTrue => Predicate::AlwaysTrue,
                    Predicate::AlwaysFalse => Predicate::AlwaysFalse,
                    _ => predicate,
                }
            }

            // unary
            RemoteExpr::FunctionCall {
                span: _,
                id,
                generics: _,
                args,
                return_type: _,
            } if args.len() == 1 && matches!(args[0], RemoteExpr::ColumnRef { .. }) => {
                let (_, name, _, _) = args[0].as_column_ref().unwrap();
                let r = Reference::new(name);
                if let Some(op) = build_unary(r, id.name().as_ref()) {
                    return op;
                }
                self.uncertain = true;
                Predicate::AlwaysTrue
            }

            // not
            RemoteExpr::FunctionCall {
                span: _,
                id,
                generics: _,
                args,
                return_type: _,
            } if args.len() == 1 && id.name().as_ref() == "not" => {
                let predicate = self.build(&args[0]);
                if self.uncertain {
                    return Predicate::AlwaysTrue;
                }
                match predicate {
                    Predicate::AlwaysTrue => Predicate::AlwaysFalse,
                    Predicate::AlwaysFalse => Predicate::AlwaysTrue,
                    _ => predicate.negate(),
                }
            }

            // binary {a op datum}
            RemoteExpr::FunctionCall {
                span: _,
                id,
                generics: _,
                args,
                return_type: _,
            } if args.len() == 2 && ["and", "and_filters", "or"].contains(&id.name().as_ref()) => {
                let left = self.build(&args[0]);
                let right = self.build(&args[1]);
                if self.uncertain {
                    return Predicate::AlwaysTrue;
                }
                match id.name().as_ref() {
                    "and" | "and_filters" => left.and(right),
                    "or" => left.or(right),
                    _ => unreachable!(),
                }
            }

            // binary {a op datum}
            RemoteExpr::FunctionCall {
                span: _,
                id,
                generics: _,
                args,
                return_type: _,
            } if args.len() == 2
                && matches!(args[0], RemoteExpr::ColumnRef { .. })
                && matches!(args[1], RemoteExpr::Constant { .. }) =>
            {
                let val = args[1].as_constant().unwrap();
                let val = scalar_to_datatum(val.1);
                if let Some(datum) = val {
                    let (_, name, _, _) = args[0].as_column_ref().unwrap();
                    let r = Reference::new(name);
                    let p = build_binary(r, id.name().as_ref(), datum);
                    if let Some(op) = p {
                        return op;
                    }
                }
                self.uncertain = true;
                Predicate::AlwaysTrue
            }

            // binary {datum op a}
            RemoteExpr::FunctionCall {
                span: _,
                id,
                generics: _,
                args,
                return_type: _,
            } if args.len() == 2
                && matches!(args[1], RemoteExpr::ColumnRef { .. })
                && matches!(args[0], RemoteExpr::Constant { .. }) =>
            {
                let val = args[0].as_constant().unwrap();
                let val = scalar_to_datatum(val.1);
                if let Some(datum) = val {
                    let (_, name, _, _) = args[1].as_column_ref().unwrap();
                    let r = Reference::new(name);
                    let p = build_reverse_binary(r, id.name().as_ref(), datum);
                    if let Some(op) = p {
                        return op;
                    }
                }
                self.uncertain = true;
                Predicate::AlwaysTrue
            }

            _ => {
                self.uncertain = true;
                Predicate::AlwaysTrue
            }
        }
    }
}

fn build_unary(r: Reference, op: &str) -> Option<Predicate> {
    let op = match op {
        "is_null" => r.is_null(),
        "is_not_null" => r.is_not_null(),
        _ => return None,
    };
    Some(op)
}

// a op datum
fn build_binary(r: Reference, op: &str, datum: Datum) -> Option<Predicate> {
    let op = match op {
        "lt" | "<" => r.less_than(datum),
        "le" | "<=" => r.less_than_or_equal_to(datum),
        "gt" | ">" => r.greater_than(datum),
        "ge" | ">=" => r.greater_than_or_equal_to(datum),
        "eq" | "=" => r.equal_to(datum),
        "ne" | "!=" => r.not_equal_to(datum),
        _ => return None,
    };
    Some(op)
}

// datum op a  to  a op_v datum
fn build_reverse_binary(r: Reference, op: &str, datum: Datum) -> Option<Predicate> {
    let op = match op {
        "lt" | "<" => r.greater_than(datum),
        "le" | "<=" => r.greater_than_or_equal_to(datum),
        "gt" | ">" => r.less_than(datum),
        "ge" | ">=" => r.less_than_or_equal_to(datum),
        "eq" | "=" => r.equal_to(datum),
        "ne" | "!=" => r.not_equal_to(datum),
        _ => return None,
    };
    Some(op)
}

fn scalar_to_datatum(scalar: &Scalar) -> Option<Datum> {
    let val = match scalar {
        Scalar::Number(n) => match n {
            NumberScalar::Int8(i) => Datum::int(*i as i32),
            NumberScalar::Int16(i) => Datum::int(*i as i32),
            NumberScalar::Int32(i) => Datum::int(*i),
            NumberScalar::Int64(i) => Datum::long(*i),
            NumberScalar::UInt8(i) => Datum::int(*i as i32),
            NumberScalar::UInt16(i) => Datum::int(*i as i32),
            NumberScalar::UInt32(i) if *i <= i32::MAX as u32 => Datum::int(*i as i32),
            NumberScalar::UInt64(i) if *i <= i64::MAX as u64 => Datum::long(*i as i64), /* Potential loss of precision */
            NumberScalar::Float32(f) => Datum::float(*f),
            NumberScalar::Float64(f) => Datum::double(*f),
            _ => return None,
        },
        Scalar::Timestamp(ts) => Datum::timestamp_micros(*ts),
        Scalar::Date(d) => Datum::date(*d),
        Scalar::Boolean(b) => Datum::bool(*b),
        Scalar::Binary(b) => Datum::binary(b.clone()),
        Scalar::String(s) => Datum::string(s),
        _ => return None,
    };
    Some(val)
}
