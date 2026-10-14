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

use std::collections::HashMap;

use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use iceberg::expr::Predicate;
use iceberg::expr::Reference;
use iceberg::spec::Datum;
use iceberg::spec::Schema as IcebergSchema;
use log::debug;

pub struct PredicateBuilder {
    references: HashMap<String, Option<String>>,
}

impl PredicateBuilder {
    pub fn new(schema: &TableSchema) -> Self {
        let mut references = HashMap::new();
        for field in schema.fields() {
            collect_references(
                field.data_type(),
                field.name(),
                field.name(),
                &mut references,
            );
        }
        Self { references }
    }

    fn reference(&self, name: &str) -> Option<Reference> {
        self.references.get(name)?.as_ref().map(Reference::new)
    }

    /// iceberg-rust binds predicates against the scanned snapshot's schema,
    /// which can differ from the current schema after renames, added columns or
    /// type promotion. Translate each current reference through its field ID.
    /// Fields absent from the snapshot, or whose type changed, are left
    /// unpushed so binding can never fail or compare against a different type.
    pub fn bind_to_snapshot(mut self, current: &IcebergSchema, snapshot: &IcebergSchema) -> Self {
        for reference in self.references.values_mut() {
            *reference = reference.take().and_then(|name| {
                let id = current.field_id_by_name(&name)?;
                let current_field = current.field_by_id(id)?;
                let snapshot_field = snapshot.field_by_id(id)?;
                (current_field.field_type == snapshot_field.field_type)
                    .then(|| snapshot.name_by_field_id(id).map(str::to_owned))
                    .flatten()
            });
        }
        self
    }

    pub fn build(&self, expr: &RemoteExpr<String>) -> (bool, Predicate) {
        match expr {
            RemoteExpr::Constant {
                span: _,
                scalar,
                data_type,
            } if data_type.remove_nullable() == DataType::Boolean => {
                let value = scalar.as_boolean();
                let is_true = value.copied().unwrap_or(false);
                if is_true {
                    (false, Predicate::AlwaysTrue)
                } else {
                    (false, Predicate::AlwaysFalse)
                }
            }

            // is_true
            RemoteExpr::FunctionCall {
                span: _,
                id,
                generics: _,
                args,
                return_type: _,
            } if args.len() == 1 && id.name().as_ref() == "is_true" => self.build(&args[0]),

            // unary
            RemoteExpr::FunctionCall {
                span: _,
                id,
                generics: _,
                args,
                return_type: _,
            } if args.len() == 1 && matches!(args[0], RemoteExpr::ColumnRef { .. }) => {
                let (_, name, _, _) = args[0].as_column_ref().unwrap();
                let Some(r) = self.reference(name) else {
                    return (true, Predicate::AlwaysTrue);
                };
                if let Some(op) = build_unary(r, id.name().as_ref()) {
                    return (false, op);
                }
                (true, Predicate::AlwaysTrue)
            }

            // not
            RemoteExpr::FunctionCall {
                span: _,
                id,
                generics: _,
                args,
                return_type: _,
            } if args.len() == 1 && id.name().as_ref() == "not" => {
                let (uncertain, predicate) = self.build(&args[0]);
                if uncertain {
                    return (true, Predicate::AlwaysTrue);
                }

                let predicate = match predicate {
                    Predicate::AlwaysTrue => Predicate::AlwaysFalse,
                    Predicate::AlwaysFalse => Predicate::AlwaysTrue,
                    _ => predicate.negate(),
                };

                (false, predicate)
            }

            // logical operators
            RemoteExpr::FunctionCall {
                span: _,
                id,
                generics: _,
                args,
                return_type: _,
            } if args.len() >= 2
                && ["and", "and_filters", "or", "or_filters"].contains(&id.name().as_ref()) =>
            {
                let is_and = matches!(id.name().as_ref(), "and" | "and_filters");
                let initial = if is_and {
                    Predicate::AlwaysTrue
                } else {
                    Predicate::AlwaysFalse
                };

                let mut uncertain = false;
                let mut predicate = initial;
                for arg in args {
                    let (arg_uncertain, arg_predicate) = self.build(arg);
                    if !is_and && arg_uncertain {
                        return (true, Predicate::AlwaysTrue);
                    }
                    uncertain |= arg_uncertain;
                    predicate = if is_and {
                        predicate.and(arg_predicate)
                    } else {
                        predicate.or(arg_predicate)
                    };
                }

                (uncertain, predicate)
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
                    let Some(r) = self.reference(name) else {
                        return (true, Predicate::AlwaysTrue);
                    };
                    let p = build_binary(r, id.name().as_ref(), datum);
                    if let Some(op) = p {
                        return (false, op);
                    }
                }
                (true, Predicate::AlwaysTrue)
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
                    let Some(r) = self.reference(name) else {
                        return (true, Predicate::AlwaysTrue);
                    };
                    let p = build_reverse_binary(r, id.name().as_ref(), datum);
                    if let Some(op) = p {
                        return (false, op);
                    }
                }
                (true, Predicate::AlwaysTrue)
            }

            v => {
                debug!("predicate build for {v:?} is nit supported yet");
                (true, Predicate::AlwaysTrue)
            }
        }
    }
}

/// Construct paths from actual field boundaries, not separator replacement.
/// Literal ':' characters belong to field names. If flattened names collide,
/// omit pruning rather than binding a predicate to a different column.
fn collect_references(
    data_type: &TableDataType,
    flat_name: &str,
    reference: &str,
    references: &mut HashMap<String, Option<String>>,
) {
    references
        .entry(flat_name.to_owned())
        .and_modify(|existing| {
            if existing.as_deref() != Some(reference) {
                *existing = None;
            }
        })
        .or_insert_with(|| Some(reference.to_owned()));
    match data_type {
        TableDataType::Nullable(inner) => {
            collect_references(inner, flat_name, reference, references)
        }
        TableDataType::Tuple {
            fields_name,
            fields_type,
        } => {
            for (name, ty) in fields_name.iter().zip(fields_type) {
                collect_references(
                    ty,
                    &format!("{flat_name}:{name}"),
                    &format!("{reference}.{name}"),
                    references,
                );
            }
        }
        _ => (),
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
        "le" | "lte" | "<=" => r.less_than_or_equal_to(datum),
        "gt" | ">" => r.greater_than(datum),
        "ge" | "gte" | ">=" => r.greater_than_or_equal_to(datum),
        "eq" | "=" => r.equal_to(datum),
        "ne" | "noteq" | "!=" => r.not_equal_to(datum),
        _ => return None,
    };
    Some(op)
}

// datum op a  to  a op_v datum
fn build_reverse_binary(r: Reference, op: &str, datum: Datum) -> Option<Predicate> {
    let op = match op {
        "lt" | "<" => r.greater_than(datum),
        "le" | "lte" | "<=" => r.greater_than_or_equal_to(datum),
        "gt" | ">" => r.less_than(datum),
        "ge" | "gte" | ">=" => r.less_than_or_equal_to(datum),
        "eq" | "=" => r.equal_to(datum),
        "ne" | "noteq" | "!=" => r.not_equal_to(datum),
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use databend_common_expression::FunctionID;
    use databend_common_expression::types::NumberDataType;
    use iceberg::expr::Bind;

    use super::*;

    fn builder() -> PredicateBuilder {
        use databend_common_expression::TableField;
        PredicateBuilder::new(&TableSchema::new(vec![
            TableField::new("report_time", TableDataType::Timestamp),
            TableField::new("event_name", TableDataType::String),
            TableField::new("source_flag", TableDataType::String),
            TableField::new("page", TableDataType::String),
            TableField::new("item", TableDataType::Tuple {
                fields_name: vec!["value".to_owned()],
                fields_type: vec![TableDataType::String],
            }),
        ]))
    }

    fn column(name: &str, data_type: DataType) -> RemoteExpr<String> {
        RemoteExpr::ColumnRef {
            span: None,
            id: name.to_string(),
            data_type,
            display_name: name.to_string(),
        }
    }

    fn constant(scalar: Scalar, data_type: DataType) -> RemoteExpr<String> {
        RemoteExpr::Constant {
            span: None,
            scalar,
            data_type,
        }
    }

    fn function(name: &str, args: Vec<RemoteExpr<String>>) -> RemoteExpr<String> {
        RemoteExpr::FunctionCall {
            span: None,
            id: Box::new(FunctionID::Builtin {
                name: name.to_string(),
                id: 0,
            }),
            generics: vec![],
            args,
            return_type: DataType::Boolean,
        }
    }

    fn string_eq(column_name: &str, value: &str) -> RemoteExpr<String> {
        function("eq", vec![
            column(column_name, DataType::String),
            constant(Scalar::String(value.to_string()), DataType::String),
        ])
    }

    #[test]
    fn test_build_databend_comparison_names() {
        let timestamp = 1_784_197_641_000_000;
        let expr = function("gte", vec![
            column("report_time", DataType::Timestamp),
            constant(Scalar::Timestamp(timestamp), DataType::Timestamp),
        ]);

        let (uncertain, predicate) = builder().build(&expr);

        assert!(!uncertain);
        assert_eq!(
            predicate,
            Reference::new("report_time")
                .greater_than_or_equal_to(Datum::timestamp_micros(timestamp))
        );
    }

    #[test]
    fn test_nested_field_paths_use_iceberg_separator() {
        let (uncertain, predicate) = builder().build(&string_eq("item:value", "test"));
        assert!(!uncertain);
        assert_eq!(
            predicate,
            Reference::new("item.value").equal_to(Datum::string("test"))
        );
    }

    #[test]
    fn literal_colons_in_field_names_are_not_path_separators() {
        use databend_common_expression::TableField;
        let schema = TableSchema::new(vec![
            TableField::new("a:b", TableDataType::String),
            TableField::new("parent:x", TableDataType::Tuple {
                fields_name: vec!["child:y".to_owned()],
                fields_type: vec![TableDataType::String],
            }),
        ]);
        let builder = PredicateBuilder::new(&schema);
        for (name, reference) in [("a:b", "a:b"), ("parent:x:child:y", "parent:x.child:y")] {
            let (uncertain, predicate) = builder.build(&string_eq(name, "test"));
            assert!(!uncertain);
            assert_eq!(
                predicate,
                Reference::new(reference).equal_to(Datum::string("test"))
            );
        }
        let (_, unary) = builder.build(&function("is_null", vec![column("a:b", DataType::String)]));
        assert_eq!(unary, Reference::new("a:b").is_null());
        let (_, reverse) = builder.build(&function("eq", vec![
            constant(Scalar::String("test".to_owned()), DataType::String),
            column("a:b", DataType::String),
        ]));
        assert_eq!(
            reverse,
            Reference::new("a:b").equal_to(Datum::string("test"))
        );
    }

    #[test]
    fn ambiguous_flattened_paths_do_not_prune_a_different_field() {
        use databend_common_expression::TableField;
        let schema = TableSchema::new(vec![
            TableField::new("item:value", TableDataType::String),
            TableField::new("item", TableDataType::Tuple {
                fields_name: vec!["value".to_owned()],
                fields_type: vec![TableDataType::String],
            }),
        ]);
        let (uncertain, predicate) =
            PredicateBuilder::new(&schema).build(&string_eq("item:value", "test"));
        assert!(uncertain);
        assert_eq!(predicate, Predicate::AlwaysTrue);
    }

    #[test]
    fn snapshot_binding_follows_field_ids_and_skips_unbindable_fields() {
        use databend_common_expression::TableField;
        use iceberg::spec::NestedField;
        use iceberg::spec::PrimitiveType;
        use iceberg::spec::StructType;
        use iceberg::spec::Type;

        let iceberg_schema = |fields: Vec<NestedField>| {
            IcebergSchema::builder()
                .with_fields(fields.into_iter().map(Arc::new))
                .build()
                .unwrap()
        };
        let long = || Type::Primitive(PrimitiveType::Long);
        let string = || Type::Primitive(PrimitiveType::String);
        let nested = |child: &str| {
            Type::Struct(StructType::new(vec![Arc::new(NestedField::optional(
                5,
                child,
                string(),
            ))]))
        };
        // Snapshot: old_k long, s {old_v string}, p int. Current schema renamed
        // old_k -> k and old_v -> v, promoted p to long and added new_c.
        let snapshot = iceberg_schema(vec![
            NestedField::optional(1, "old_k", long()),
            NestedField::optional(2, "p", Type::Primitive(PrimitiveType::Int)),
            NestedField::optional(4, "s", nested("old_v")),
        ]);
        let current = iceberg_schema(vec![
            NestedField::optional(1, "k", long()),
            NestedField::optional(2, "p", long()),
            NestedField::optional(3, "new_c", long()),
            NestedField::optional(4, "s", nested("v")),
        ]);
        let table = TableSchema::new(vec![
            TableField::new("k", TableDataType::Number(NumberDataType::Int64)),
            TableField::new("p", TableDataType::Number(NumberDataType::Int64)),
            TableField::new("new_c", TableDataType::Number(NumberDataType::Int64)),
            TableField::new("s", TableDataType::Tuple {
                fields_name: vec!["v".to_owned()],
                fields_type: vec![TableDataType::String],
            }),
        ]);
        let builder = PredicateBuilder::new(&table).bind_to_snapshot(&current, &snapshot);
        let int_eq = |name: &str| {
            function("eq", vec![
                column(name, DataType::Number(NumberDataType::Int64)),
                constant(
                    Scalar::Number(NumberScalar::Int64(1)),
                    DataType::Number(NumberDataType::Int64),
                ),
            ])
        };

        let (uncertain, predicate) = builder.build(&int_eq("k"));
        assert!(!uncertain);
        assert_eq!(predicate, Reference::new("old_k").equal_to(Datum::long(1)));
        let (uncertain, predicate) = builder.build(&string_eq("s:v", "x"));
        assert!(!uncertain);
        assert_eq!(
            predicate,
            Reference::new("s.old_v").equal_to(Datum::string("x"))
        );
        // Added after the snapshot, or promoted since: never bound.
        for name in ["new_c", "p"] {
            let (uncertain, predicate) = builder.build(&int_eq(name));
            assert!(uncertain, "{name}");
            assert_eq!(predicate, Predicate::AlwaysTrue);
        }
        // Unbindable conjuncts drop out of AND without losing the others.
        let (uncertain, predicate) =
            builder.build(&function("and_filters", vec![int_eq("new_c"), int_eq("k")]));
        assert!(uncertain);
        assert_eq!(predicate, Reference::new("old_k").equal_to(Datum::long(1)));
        assert!(predicate.bind(Arc::new(snapshot), true).is_ok());
    }

    #[test]
    fn test_and_keeps_supported_conjuncts() {
        let unsupported = function("unsupported", vec![column(
            "report_time",
            DataType::Timestamp,
        )]);
        let event_name = string_eq("event_name", "game_end");
        let expr = function("is_true", vec![function("and_filters", vec![
            unsupported,
            event_name,
        ])]);

        let (uncertain, predicate) = builder().build(&expr);

        assert!(uncertain);
        assert_eq!(
            predicate,
            Reference::new("event_name").equal_to(Datum::string("game_end"))
        );
    }

    #[test]
    fn test_variadic_and_builds_all_conjuncts() {
        let expr = function("is_true", vec![function("and_filters", vec![
            string_eq("event_name", "game_end"),
            string_eq("source_flag", "p7"),
            string_eq("page", "home"),
        ])]);

        let (uncertain, predicate) = builder().build(&expr);

        assert!(!uncertain);
        assert_ne!(predicate, Predicate::AlwaysTrue);
    }

    #[test]
    fn test_or_drops_partial_predicate() {
        let unsupported = function("unsupported", vec![column(
            "report_time",
            DataType::Timestamp,
        )]);
        let event_name = string_eq("event_name", "game_end");
        let expr = function("or_filters", vec![unsupported, event_name]);

        let (uncertain, predicate) = builder().build(&expr);

        assert!(uncertain);
        assert_eq!(predicate, Predicate::AlwaysTrue);
    }
}
