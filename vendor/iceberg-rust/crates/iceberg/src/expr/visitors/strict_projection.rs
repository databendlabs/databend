// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;

use fnv::FnvHashSet;

use crate::expr::visitors::bound_predicate_visitor::{BoundPredicateVisitor, visit};
use crate::expr::{BoundPredicate, BoundReference, Predicate};
use crate::spec::{Datum, PartitionField, PartitionSpecRef};
use crate::{Error, ErrorKind};

// # TODO
// Remove this after delete support
#[allow(dead_code)]
pub(crate) struct StrictProjection {
    partition_spec: PartitionSpecRef,
    cached_parts: HashMap<i32, Vec<PartitionField>>,
}

#[allow(dead_code)]
impl StrictProjection {
    pub(crate) fn new(partition_spec: PartitionSpecRef) -> Self {
        Self {
            partition_spec,
            cached_parts: HashMap::new(),
        }
    }

    fn get_parts_for_field_id(&mut self, field_id: i32) -> &Vec<PartitionField> {
        if let std::collections::hash_map::Entry::Vacant(e) = self.cached_parts.entry(field_id) {
            let mut parts: Vec<PartitionField> = vec![];
            for partition_spec_field in self.partition_spec.fields() {
                if partition_spec_field.source_id == field_id {
                    parts.push(partition_spec_field.clone())
                }
            }

            e.insert(parts);
        }

        &self.cached_parts[&field_id]
    }

    pub(crate) fn strict_project(
        &mut self,
        predicate: &BoundPredicate,
    ) -> crate::Result<Predicate> {
        visit(self, predicate)
    }

    fn get_parts(
        &mut self,
        reference: &BoundReference,
        predicate: &BoundPredicate,
    ) -> Result<Predicate, Error> {
        let field_id = reference.field().id;

        // This could be made a bit neater if `try_reduce` ever becomes stable
        self.get_parts_for_field_id(field_id).iter().try_fold(
            Predicate::AlwaysFalse,
            |res, part| {
                // consider (ts > 2019-01-01T01:00:00) with day(ts) and hour(ts)
                // projections: d >= 2019-01-02 and h >= 2019-01-01-02 (note the inclusive bounds).
                // any timestamp where either projection predicate is true must match the original
                // predicate. For example, ts = 2019-01-01T03:00:00 matches the hour projection but not
                // the day, but does match the original predicate.
                Ok(
                    if let Some(pred_for_part) =
                        part.transform.strict_project(&part.name, predicate)?
                    {
                        if res == Predicate::AlwaysFalse {
                            pred_for_part
                        } else {
                            res.or(pred_for_part)
                        }
                    } else {
                        res
                    },
                )
            },
        )
    }
}

impl BoundPredicateVisitor for StrictProjection {
    type T = Predicate;

    fn always_true(&mut self) -> crate::Result<Self::T> {
        Ok(Predicate::AlwaysTrue)
    }

    fn always_false(&mut self) -> crate::Result<Self::T> {
        Ok(Predicate::AlwaysFalse)
    }

    fn and(&mut self, lhs: Self::T, rhs: Self::T) -> crate::Result<Self::T> {
        Ok(lhs.and(rhs))
    }

    fn or(&mut self, lhs: Self::T, rhs: Self::T) -> crate::Result<Self::T> {
        Ok(lhs.or(rhs))
    }

    fn not(&mut self, _inner: Self::T) -> crate::Result<Self::T> {
        Err(Error::new(
            ErrorKind::Unexpected,
            "StrictProjection should not be performed against Predicates that contain a Not operator. Ensure that \"Rewrite Not\" gets applied to the originating Predicate before binding it.",
        ))
    }

    fn is_null(
        &mut self,
        reference: &BoundReference,
        predicate: &BoundPredicate,
    ) -> crate::Result<Self::T> {
        self.get_parts(reference, predicate)
    }

    fn not_null(
        &mut self,
        reference: &BoundReference,
        predicate: &BoundPredicate,
    ) -> crate::Result<Self::T> {
        self.get_parts(reference, predicate)
    }

    fn is_nan(
        &mut self,
        reference: &BoundReference,
        predicate: &BoundPredicate,
    ) -> crate::Result<Self::T> {
        self.get_parts(reference, predicate)
    }

    fn not_nan(
        &mut self,
        reference: &BoundReference,
        predicate: &BoundPredicate,
    ) -> crate::Result<Self::T> {
        self.get_parts(reference, predicate)
    }

    fn less_than(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        predicate: &BoundPredicate,
    ) -> crate::Result<Self::T> {
        self.get_parts(reference, predicate)
    }

    fn less_than_or_eq(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        predicate: &BoundPredicate,
    ) -> crate::Result<Self::T> {
        self.get_parts(reference, predicate)
    }

    fn greater_than(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        predicate: &BoundPredicate,
    ) -> crate::Result<Self::T> {
        self.get_parts(reference, predicate)
    }

    fn greater_than_or_eq(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        predicate: &BoundPredicate,
    ) -> crate::Result<Self::T> {
        self.get_parts(reference, predicate)
    }

    fn eq(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        predicate: &BoundPredicate,
    ) -> crate::Result<Self::T> {
        self.get_parts(reference, predicate)
    }

    fn not_eq(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        predicate: &BoundPredicate,
    ) -> crate::Result<Self::T> {
        self.get_parts(reference, predicate)
    }

    fn starts_with(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        predicate: &BoundPredicate,
    ) -> crate::Result<Self::T> {
        self.get_parts(reference, predicate)
    }

    fn not_starts_with(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        predicate: &BoundPredicate,
    ) -> crate::Result<Self::T> {
        self.get_parts(reference, predicate)
    }

    fn r#in(
        &mut self,
        reference: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        predicate: &BoundPredicate,
    ) -> crate::Result<Self::T> {
        self.get_parts(reference, predicate)
    }

    fn not_in(
        &mut self,
        reference: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        predicate: &BoundPredicate,
    ) -> crate::Result<Self::T> {
        self.get_parts(reference, predicate)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use uuid::Uuid;

    use crate::expr::visitors::strict_projection::StrictProjection;
    use crate::expr::{Bind, Reference};
    use crate::spec::{
        Datum, NestedField, PartitionSpec, PrimitiveLiteral, PrimitiveType, Schema, Transform, Type,
    };

    #[tokio::test]
    async fn test_strict_projection_month_epoch() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        1,
                        "col1",
                        Type::Primitive(PrimitiveType::Date),
                    )),
                    Arc::new(NestedField::required(
                        2,
                        "col2",
                        Type::Primitive(PrimitiveType::Timestamp),
                    )),
                    Arc::new(NestedField::required(
                        3,
                        "col3",
                        Type::Primitive(PrimitiveType::Timestamptz),
                    )),
                    Arc::new(NestedField::required(
                        4,
                        "col4",
                        Type::Primitive(PrimitiveType::TimestampNs),
                    )),
                    Arc::new(NestedField::required(
                        5,
                        "col5",
                        Type::Primitive(PrimitiveType::TimestamptzNs),
                    )),
                ])
                .build()
                .unwrap(),
        );
        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(1)
                .add_partition_field("col1", "pcol1", Transform::Month)
                .unwrap()
                .add_partition_field("col2", "pcol2", Transform::Month)
                .unwrap()
                .add_partition_field("col3", "pcol3", Transform::Month)
                .unwrap()
                .add_partition_field("col4", "pcol4", Transform::Month)
                .unwrap()
                .add_partition_field("col5", "pcol5", Transform::Month)
                .unwrap()
                .build()
                .unwrap(),
        );

        let mut strict_projection = StrictProjection::new(partition_spec.clone());

        // test eq: (col1 = 1970-01-01) AND (col2 = 1970-01-01) AND (col3 = 1970-01-01) AND (col4 = 1970-01-01) AND (col5 = 1970-01-01)
        let predicate = Reference::new("col1")
            .equal_to(Datum::date(0))
            .and(Reference::new("col2").equal_to(Datum::timestamp_micros(0)))
            .and(Reference::new("col3").equal_to(Datum::timestamptz_micros(0)))
            .and(Reference::new("col4").equal_to(Datum::timestamp_nanos(0)))
            .and(Reference::new("col5").equal_to(Datum::timestamptz_nanos(0)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "FALSE".to_string());

        // test not eq: (col1 != 1970-01-01) AND (col2 != 1970-01-01) AND (col3 != 1970-01-01) AND (col4 != 1970-01-01) AND (col5 != 1970-01-01)
        let predicate = Reference::new("col1")
            .not_equal_to(Datum::date(0))
            .and(Reference::new("col2").not_equal_to(Datum::timestamp_micros(0)))
            .and(Reference::new("col3").not_equal_to(Datum::timestamptz_micros(0)))
            .and(Reference::new("col4").not_equal_to(Datum::timestamp_nanos(0)))
            .and(Reference::new("col5").not_equal_to(Datum::timestamptz_nanos(0)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "((((pcol1 != 0) AND (pcol2 != 0)) AND (pcol3 != 0)) AND (pcol4 != 0)) AND (pcol5 != 0)".to_string());

        // test less: (col1 < 1970-01-01) AND (col2 < 1970-01-01) AND (col3 < 1970-01-01) AND (col4 < 1970-01-01) AND (col5 < 1970-01-01)
        let predicate = Reference::new("col1")
            .less_than(Datum::date(0))
            .and(Reference::new("col2").less_than(Datum::timestamp_micros(0)))
            .and(Reference::new("col3").less_than(Datum::timestamptz_micros(0)))
            .and(Reference::new("col4").less_than(Datum::timestamp_nanos(0)))
            .and(Reference::new("col5").less_than(Datum::timestamptz_nanos(0)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 < 0) AND (pcol2 < 0)) AND (pcol3 < 0)) AND (pcol4 < 0)) AND (pcol5 < 0)"
                .to_string()
        );

        // test less or eq: (col1 <= 1970-01-01) AND (col2 <= 1970-01-01) AND (col3 <= 1970-01-01) AND (col4 <= 1970-01-01) AND (col5 <= 1970-01-01)
        let predicate = Reference::new("col1")
            .less_than_or_equal_to(Datum::date(0))
            .and(Reference::new("col2").less_than_or_equal_to(Datum::timestamp_micros(0)))
            .and(Reference::new("col3").less_than_or_equal_to(Datum::timestamptz_micros(0)))
            .and(Reference::new("col4").less_than_or_equal_to(Datum::timestamp_nanos(0)))
            .and(Reference::new("col5").less_than_or_equal_to(Datum::timestamptz_nanos(0)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 < 0) AND (pcol2 < 0)) AND (pcol3 < 0)) AND (pcol4 < 0)) AND (pcol5 < 0)"
                .to_string()
        );

        // test greater: (col1 > 1970-01-01) AND (col2 > 1970-01-01) AND (col3 > 1970-01-01) AND (col4 > 1970-01-01) AND (col5 > 1970-01-01)
        let predicate = Reference::new("col1")
            .greater_than(Datum::date(0))
            .and(Reference::new("col2").greater_than(Datum::timestamp_micros(0)))
            .and(Reference::new("col3").greater_than(Datum::timestamptz_micros(0)))
            .and(Reference::new("col4").greater_than(Datum::timestamp_nanos(0)))
            .and(Reference::new("col5").greater_than(Datum::timestamptz_nanos(0)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 > 0) AND (pcol2 > 0)) AND (pcol3 > 0)) AND (pcol4 > 0)) AND (pcol5 > 0)"
                .to_string()
        );

        // test greater or eq: (col1 >= 1970-01-01) AND (col2 >= 1970-01-01) AND (col3 >= 1970-01-01) AND (col4 >= 1970-01-01) AND (col5 >= 1970-01-01)
        let predicate = Reference::new("col1")
            .greater_than_or_equal_to(Datum::date(0))
            .and(Reference::new("col2").greater_than_or_equal_to(Datum::timestamp_micros(0)))
            .and(Reference::new("col3").greater_than_or_equal_to(Datum::timestamptz_micros(0)))
            .and(Reference::new("col4").greater_than_or_equal_to(Datum::timestamp_nanos(0)))
            .and(Reference::new("col5").greater_than_or_equal_to(Datum::timestamptz_nanos(0)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 > -1) AND (pcol2 > -1)) AND (pcol3 > -1)) AND (pcol4 > -1)) AND (pcol5 > -1)"
                .to_string()
        );

        // test not in
        let predicate =
            Reference::new("col1")
                .is_not_in(
                    vec![
                        Datum::date_from_str("1970-01-01").unwrap(),
                        Datum::date_from_str("1969-12-31").unwrap(),
                    ]
                    .into_iter(),
                )
                .and(Reference::new("col2").is_not_in(
                    vec![Datum::timestamp_micros(0), Datum::timestamp_micros(-1)].into_iter(),
                ))
                .and(Reference::new("col3").is_not_in(
                    vec![Datum::timestamptz_micros(0), Datum::timestamptz_micros(-1)].into_iter(),
                ))
                .and(Reference::new("col4").is_not_in(
                    vec![Datum::timestamp_nanos(0), Datum::timestamp_nanos(-1)].into_iter(),
                ))
                .and(Reference::new("col5").is_not_in(
                    vec![Datum::timestamptz_nanos(0), Datum::timestamptz_nanos(-1)].into_iter(),
                ))
                .bind(schema.clone(), false)
                .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "((((pcol1 NOT IN (0, -1)) AND (pcol2 NOT IN (0, -1))) AND (pcol3 NOT IN (0, -1))) AND (pcol4 NOT IN (0, -1))) AND (pcol5 NOT IN (0, -1))".to_string());

        // test in
        let predicate =
            Reference::new("col1")
                .is_in(
                    vec![
                        Datum::date_from_str("1970-01-01").unwrap(),
                        Datum::date_from_str("1969-12-31").unwrap(),
                    ]
                    .into_iter(),
                )
                .and(Reference::new("col2").is_in(
                    vec![Datum::timestamp_micros(0), Datum::timestamp_micros(-1)].into_iter(),
                ))
                .bind(schema.clone(), false)
                .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "FALSE".to_string());
    }

    #[tokio::test]
    async fn test_strict_projection_month_lower_bound() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        1,
                        "col1",
                        Type::Primitive(PrimitiveType::Date),
                    )),
                    Arc::new(NestedField::required(
                        2,
                        "col2",
                        Type::Primitive(PrimitiveType::Timestamp),
                    )),
                    Arc::new(NestedField::required(
                        3,
                        "col3",
                        Type::Primitive(PrimitiveType::Timestamptz),
                    )),
                    Arc::new(NestedField::required(
                        4,
                        "col4",
                        Type::Primitive(PrimitiveType::TimestampNs),
                    )),
                    Arc::new(NestedField::required(
                        5,
                        "col5",
                        Type::Primitive(PrimitiveType::TimestamptzNs),
                    )),
                ])
                .build()
                .unwrap(),
        );
        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(1)
                .add_partition_field("col1", "pcol1", Transform::Month)
                .unwrap()
                .add_partition_field("col2", "pcol2", Transform::Month)
                .unwrap()
                .add_partition_field("col3", "pcol3", Transform::Month)
                .unwrap()
                .add_partition_field("col4", "pcol4", Transform::Month)
                .unwrap()
                .add_partition_field("col5", "pcol5", Transform::Month)
                .unwrap()
                .build()
                .unwrap(),
        );

        let mut strict_projection = StrictProjection::new(partition_spec.clone());

        // test eq: (col1 = 2017-01-01) AND (col2 = 2017-01-01) AND (col3 = 2017-01-01) AND (col4 = 2017-01-01) AND (col5 = 2017-01-01)
        let predicate = Reference::new("col1")
            .equal_to(Datum::date_from_str("2017-01-01").unwrap())
            .and(Reference::new("col2").equal_to(Datum::timestamp_micros(1483228800000000)))
            .and(Reference::new("col3").equal_to(Datum::timestamptz_micros(1483228800000000)))
            .and(Reference::new("col4").equal_to(Datum::timestamp_nanos(1483228800000000000)))
            .and(Reference::new("col5").equal_to(Datum::timestamptz_nanos(1483228800000000000)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "FALSE".to_string());

        // test not eq: (col1 != 2017-01-01) AND (col2 != 2017-01-01) AND (col3 != 2017-01-01) AND (col4 != 2017-01-01) AND (col5 != 2017-01-01)
        let predicate = Reference::new("col1")
            .not_equal_to(Datum::date_from_str("2017-01-01").unwrap())
            .and(Reference::new("col2").not_equal_to(Datum::timestamp_micros(1483228800000000)))
            .and(Reference::new("col3").not_equal_to(Datum::timestamptz_micros(1483228800000000)))
            .and(Reference::new("col4").not_equal_to(Datum::timestamp_nanos(1483228800000000000)))
            .and(Reference::new("col5").not_equal_to(Datum::timestamptz_nanos(1483228800000000000)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "((((pcol1 != 564) AND (pcol2 != 564)) AND (pcol3 != 564)) AND (pcol4 != 564)) AND (pcol5 != 564)".to_string());

        // test less: (col1 < 2017-01-01) AND (col2 < 2017-01-01) AND (col3 < 2017-01-01) AND (col4 < 2017-01-01) AND (col5 < 2017-01-01)
        let predicate = Reference::new("col1")
            .less_than(Datum::date_from_str("2017-01-01").unwrap())
            .and(Reference::new("col2").less_than(Datum::timestamp_micros(1483228800000000)))
            .and(Reference::new("col3").less_than(Datum::timestamptz_micros(1483228800000000)))
            .and(Reference::new("col4").less_than(Datum::timestamp_nanos(1483228800000000000)))
            .and(Reference::new("col5").less_than(Datum::timestamptz_nanos(1483228800000000000)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 < 564) AND (pcol2 < 564)) AND (pcol3 < 564)) AND (pcol4 < 564)) AND (pcol5 < 564)"
                .to_string()
        );

        // test less or eq: (col1 <= 2017-01-01) AND (col2 <= 2017-01-01) AND (col3 <= 2017-01-01) AND (col4 <= 2017-01-01) AND (col5 <= 2017-01-01)
        let predicate = Reference::new("col1")
            .less_than_or_equal_to(Datum::date_from_str("2017-01-01").unwrap())
            .and(
                Reference::new("col2")
                    .less_than_or_equal_to(Datum::timestamp_micros(1483228800000000)),
            )
            .and(
                Reference::new("col3")
                    .less_than_or_equal_to(Datum::timestamptz_micros(1483228800000000)),
            )
            .and(
                Reference::new("col4")
                    .less_than_or_equal_to(Datum::timestamp_nanos(1483228800000000000)),
            )
            .and(
                Reference::new("col5")
                    .less_than_or_equal_to(Datum::timestamptz_nanos(1483228800000000000)),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 < 564) AND (pcol2 < 564)) AND (pcol3 < 564)) AND (pcol4 < 564)) AND (pcol5 < 564)"
                .to_string()
        );

        // test greater: (col1 > 2017-01-01) AND (col2 > 2017-01-01) AND (col3 > 2017-01-01) AND (col4 > 2017-01-01) AND (col5 > 2017-01-01)
        let predicate = Reference::new("col1")
            .greater_than(Datum::date_from_str("2017-01-01").unwrap())
            .and(Reference::new("col2").greater_than(Datum::timestamp_micros(1483228800000000)))
            .and(Reference::new("col3").greater_than(Datum::timestamptz_micros(1483228800000000)))
            .and(Reference::new("col4").greater_than(Datum::timestamp_nanos(1483228800000000000)))
            .and(Reference::new("col5").greater_than(Datum::timestamptz_nanos(1483228800000000000)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 > 564) AND (pcol2 > 564)) AND (pcol3 > 564)) AND (pcol4 > 564)) AND (pcol5 > 564)"
                .to_string()
        );

        // test greater or eq: (col1 >= 2017-01-01) AND (col2 >= 2017-01-01) AND (col3 >= 2017-01-01) AND (col4 >= 2017-01-01) AND (col5 >= 2017-01-01)
        let predicate = Reference::new("col1")
            .greater_than_or_equal_to(Datum::date_from_str("2017-01-01").unwrap())
            .and(
                Reference::new("col2")
                    .greater_than_or_equal_to(Datum::timestamp_micros(1483228800000000)),
            )
            .and(
                Reference::new("col3")
                    .greater_than_or_equal_to(Datum::timestamptz_micros(1483228800000000)),
            )
            .and(
                Reference::new("col4")
                    .greater_than_or_equal_to(Datum::timestamp_nanos(1483228800000000000)),
            )
            .and(
                Reference::new("col5")
                    .greater_than_or_equal_to(Datum::timestamptz_nanos(1483228800000000000)),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 > 563) AND (pcol2 > 563)) AND (pcol3 > 563)) AND (pcol4 > 563)) AND (pcol5 > 563)"
                .to_string()
        );

        // test not in (col1 NOT IN (2017-01-01, 2017-12-02)) AND (col2 NOT IN (2017-01-01, 2017-12-02)) AND (col3 NOT IN (2017-01-01, 2017-12-02)) AND (col4 NOT IN (2017-01-01, 2017-12-02)) AND (col5 NOT IN (2017-01-01, 2017-12-02))
        let predicate = Reference::new("col1")
            .is_not_in(
                vec![
                    Datum::date_from_str("2017-01-01").unwrap(),
                    Datum::date_from_str("2017-12-02").unwrap(),
                ]
                .into_iter(),
            )
            .and(
                Reference::new("col2").is_not_in(
                    vec![
                        Datum::timestamp_micros(1483228800000000),
                        Datum::timestamp_micros(1512182400000000),
                    ]
                    .into_iter(),
                ),
            )
            .and(
                Reference::new("col3").is_not_in(
                    vec![
                        Datum::timestamptz_micros(1483228800000000),
                        Datum::timestamptz_micros(1512182400000000),
                    ]
                    .into_iter(),
                ),
            )
            .and(
                Reference::new("col4").is_not_in(
                    vec![
                        Datum::timestamp_nanos(1483228800000000000),
                        Datum::timestamp_nanos(1512182400000000000),
                    ]
                    .into_iter(),
                ),
            )
            .and(
                Reference::new("col5").is_not_in(
                    vec![
                        Datum::timestamptz_nanos(1483228800000000000),
                        Datum::timestamptz_nanos(1512182400000000000),
                    ]
                    .into_iter(),
                ),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "((((pcol1 NOT IN (575, 564)) AND (pcol2 NOT IN (575, 564))) AND (pcol3 NOT IN (575, 564))) AND (pcol4 NOT IN (575, 564))) AND (pcol5 NOT IN (575, 564))".to_string());

        // test in
        let predicate = Reference::new("col1")
            .is_in(
                vec![
                    Datum::date_from_str("2017-01-01").unwrap(),
                    Datum::date_from_str("2017-01-02").unwrap(),
                ]
                .into_iter(),
            )
            .and(
                Reference::new("col2").is_in(
                    vec![
                        Datum::timestamp_micros(1483228800000000),
                        Datum::timestamp_micros(1483315200000000),
                    ]
                    .into_iter(),
                ),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "FALSE".to_string());
    }

    #[tokio::test]
    async fn test_strict_projection_negative_month_lower_bound() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        1,
                        "col1",
                        Type::Primitive(PrimitiveType::Date),
                    )),
                    Arc::new(NestedField::required(
                        2,
                        "col2",
                        Type::Primitive(PrimitiveType::Timestamp),
                    )),
                    Arc::new(NestedField::required(
                        3,
                        "col3",
                        Type::Primitive(PrimitiveType::Timestamptz),
                    )),
                    Arc::new(NestedField::required(
                        4,
                        "col4",
                        Type::Primitive(PrimitiveType::TimestampNs),
                    )),
                    Arc::new(NestedField::required(
                        5,
                        "col5",
                        Type::Primitive(PrimitiveType::TimestamptzNs),
                    )),
                ])
                .build()
                .unwrap(),
        );

        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(1)
                .add_partition_field("col1", "pcol1", Transform::Month)
                .unwrap()
                .add_partition_field("col2", "pcol2", Transform::Month)
                .unwrap()
                .add_partition_field("col3", "pcol3", Transform::Month)
                .unwrap()
                .add_partition_field("col4", "pcol4", Transform::Month)
                .unwrap()
                .add_partition_field("col5", "pcol5", Transform::Month)
                .unwrap()
                .build()
                .unwrap(),
        );

        let mut strict_projection = StrictProjection::new(partition_spec.clone());

        // test less: (col1 < 1969-01-01) AND (col2 < 1969-01-01) AND (col3 < 1969-01-01) AND (col4 < 1969-01-01) AND (col5 < 1969-01-01)
        let predicate = Reference::new("col1")
            .less_than(Datum::date_from_str("1969-01-01").unwrap())
            .and(Reference::new("col2").less_than(Datum::timestamp_micros(-31536000000000)))
            .and(Reference::new("col3").less_than(Datum::timestamptz_micros(-31536000000000)))
            .and(Reference::new("col4").less_than(Datum::timestamp_nanos(-31536000000000000)))
            .and(Reference::new("col5").less_than(Datum::timestamptz_nanos(-31536000000000000)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 < -12) AND (pcol2 < -12)) AND (pcol3 < -12)) AND (pcol4 < -12)) AND (pcol5 < -12)"
                .to_string()
        );

        // test less or eq: (col1 <= 1969-01-01) AND (col2 <= 1969-01-01) AND (col3 <= 1969-01-01) AND (col4 <= 1969-01-01) AND (col5 <= 1969-01-01)
        let predicate = Reference::new("col1")
            .less_than_or_equal_to(Datum::date_from_str("1969-01-01").unwrap())
            .and(
                Reference::new("col2")
                    .less_than_or_equal_to(Datum::timestamp_micros(-31536000000000)),
            )
            .and(
                Reference::new("col3")
                    .less_than_or_equal_to(Datum::timestamptz_micros(-31536000000000)),
            )
            .and(
                Reference::new("col4")
                    .less_than_or_equal_to(Datum::timestamp_nanos(-31536000000000000)),
            )
            .and(
                Reference::new("col5")
                    .less_than_or_equal_to(Datum::timestamptz_nanos(-31536000000000000)),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 < -12) AND (pcol2 < -12)) AND (pcol3 < -12)) AND (pcol4 < -12)) AND (pcol5 < -12)"
                .to_string()
        );

        // test greater: (col1 > 1969-01-01) AND (col2 > 1969-01-01) AND (col3 > 1969-01-01) AND (col4 > 1969-01-01) AND (col5 > 1969-01-01)
        let predicate = Reference::new("col1")
            .greater_than(Datum::date_from_str("1969-01-01").unwrap())
            .and(Reference::new("col2").greater_than(Datum::timestamp_micros(-31536000000000)))
            .and(Reference::new("col3").greater_than(Datum::timestamptz_micros(-31536000000000)))
            .and(Reference::new("col4").greater_than(Datum::timestamp_nanos(-31536000000000000)))
            .and(Reference::new("col5").greater_than(Datum::timestamptz_nanos(-31536000000000000)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 > -12) AND (pcol2 > -12)) AND (pcol3 > -12)) AND (pcol4 > -12)) AND (pcol5 > -12)"
                .to_string()
        );

        // test greater or eq: (col1 >= 1969-01-01) AND (col2 >= 1969-01-01) AND (col3 >= 1969-01-01) AND (col4 >= 1969-01-01) AND (col5 >= 1969-01-01)
        let predicate = Reference::new("col1")
            .greater_than_or_equal_to(Datum::date_from_str("1969-01-01").unwrap())
            .and(
                Reference::new("col2")
                    .greater_than_or_equal_to(Datum::timestamp_micros(-31536000000000)),
            )
            .and(
                Reference::new("col3")
                    .greater_than_or_equal_to(Datum::timestamptz_micros(-31536000000000)),
            )
            .and(
                Reference::new("col4")
                    .greater_than_or_equal_to(Datum::timestamp_nanos(-31536000000000000)),
            )
            .and(
                Reference::new("col5")
                    .greater_than_or_equal_to(Datum::timestamptz_nanos(-31536000000000000)),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 > -13) AND (pcol2 > -13)) AND (pcol3 > -13)) AND (pcol4 > -13)) AND (pcol5 > -13)"
                .to_string()
        );

        // test not eq: (col1 != 1969-01-01) AND (col2 != 1969-01-01) AND (col3 != 1969-01-01) AND (col4 != 1969-01-01) AND (col5 != 1969-01-01)
        let predicate = Reference::new("col1")
            .not_equal_to(Datum::date_from_str("1969-01-01").unwrap())
            .and(Reference::new("col2").not_equal_to(Datum::timestamp_micros(-31536000000000)))
            .and(Reference::new("col3").not_equal_to(Datum::timestamptz_micros(-31536000000000)))
            .and(Reference::new("col4").not_equal_to(Datum::timestamp_nanos(-31536000000000000)))
            .and(Reference::new("col5").not_equal_to(Datum::timestamptz_nanos(-31536000000000000)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "((((pcol1 != -12) AND (pcol2 != -12)) AND (pcol3 != -12)) AND (pcol4 != -12)) AND (pcol5 != -12)".to_string());

        // test not in: (col1 NOT IN (1969-01-01, 1969-12-31)) AND (col2 NOT IN (1969-01-01, 1969-12-31)) AND (col3 NOT IN (1969-01-01, 1969-12-31)) AND (col4 NOT IN (1969-01-01, 1969-12-31)) AND (col5 NOT IN (1969-01-01, 1969-12-31))
        let predicate = Reference::new("col1")
            .is_not_in(
                vec![
                    Datum::date_from_str("1969-01-01").unwrap(),
                    Datum::date_from_str("1969-12-31").unwrap(),
                ]
                .into_iter(),
            )
            .and(
                Reference::new("col2").is_not_in(
                    vec![
                        Datum::timestamp_micros(-31536000000000),
                        Datum::timestamp_micros(-86400000000),
                    ]
                    .into_iter(),
                ),
            )
            .and(
                Reference::new("col3").is_not_in(
                    vec![
                        Datum::timestamptz_micros(-86400000000),
                        Datum::timestamptz_micros(-31536000000000),
                    ]
                    .into_iter(),
                ),
            )
            .and(
                Reference::new("col4").is_not_in(
                    vec![
                        Datum::timestamp_nanos(-86400000000000),
                        Datum::timestamp_nanos(-31536000000000000),
                    ]
                    .into_iter(),
                ),
            )
            .and(
                Reference::new("col5").is_not_in(
                    vec![
                        Datum::timestamptz_nanos(-86400000000000),
                        Datum::timestamptz_nanos(-31536000000000000),
                    ]
                    .into_iter(),
                ),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "((((pcol1 NOT IN (-1, -12)) AND (pcol2 NOT IN (-1, -12))) AND (pcol3 NOT IN (-1, -12))) AND (pcol4 NOT IN (-1, -12))) AND (pcol5 NOT IN (-1, -12))".to_string());

        // test in
        let predicate = Reference::new("col1")
            .is_in(
                vec![
                    Datum::date_from_str("1969-01-01").unwrap(),
                    Datum::date_from_str("1969-12-31").unwrap(),
                ]
                .into_iter(),
            )
            .and(
                Reference::new("col2").is_in(
                    vec![
                        Datum::timestamp_micros(-315619200000000),
                        Datum::timestamp_micros(-86400000000),
                    ]
                    .into_iter(),
                ),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "FALSE".to_string());
    }

    #[tokio::test]
    async fn test_strict_projection_month_upper_bound() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        1,
                        "col1",
                        Type::Primitive(PrimitiveType::Date),
                    )),
                    Arc::new(NestedField::required(
                        2,
                        "col2",
                        Type::Primitive(PrimitiveType::Timestamp),
                    )),
                    Arc::new(NestedField::required(
                        3,
                        "col3",
                        Type::Primitive(PrimitiveType::Timestamptz),
                    )),
                    Arc::new(NestedField::required(
                        4,
                        "col4",
                        Type::Primitive(PrimitiveType::TimestampNs),
                    )),
                    Arc::new(NestedField::required(
                        5,
                        "col5",
                        Type::Primitive(PrimitiveType::TimestamptzNs),
                    )),
                ])
                .build()
                .unwrap(),
        );

        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(1)
                .add_partition_field("col1", "pcol1", Transform::Month)
                .unwrap()
                .add_partition_field("col2", "pcol2", Transform::Month)
                .unwrap()
                .add_partition_field("col3", "pcol3", Transform::Month)
                .unwrap()
                .add_partition_field("col4", "pcol4", Transform::Month)
                .unwrap()
                .add_partition_field("col5", "pcol5", Transform::Month)
                .unwrap()
                .build()
                .unwrap(),
        );

        let mut strict_projection = StrictProjection::new(partition_spec.clone());

        // test less: (col1 < 2017-12-31) AND (col2 < 2017-12-31) AND (col3 < 2017-12-31) AND (col4 < 2017-12-31) AND (col5 < 2017-12-31)
        let predicate = Reference::new("col1")
            .less_than(Datum::date_from_str("2017-12-31").unwrap())
            .and(Reference::new("col2").less_than(Datum::timestamp_micros(1514764799000000)))
            .and(Reference::new("col3").less_than(Datum::timestamptz_micros(1514764799000000)))
            .and(Reference::new("col4").less_than(Datum::timestamp_nanos(1514764799000000000)))
            .and(Reference::new("col5").less_than(Datum::timestamptz_nanos(1514764799000000000)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 < 575) AND (pcol2 < 575)) AND (pcol3 < 575)) AND (pcol4 < 575)) AND (pcol5 < 575)"
                .to_string()
        );

        // test less or eq: (col1 <= 2017-12-31) AND (col2 <= 2017-12-31) AND (col3 <= 2017-12-31) AND (col4 <= 2017-12-31) AND (col5 <= 2017-12-31)
        let predicate = Reference::new("col1")
            .less_than_or_equal_to(Datum::date_from_str("2017-12-31").unwrap())
            .and(
                Reference::new("col2")
                    .less_than_or_equal_to(Datum::timestamp_micros(1514764799000000)),
            )
            .and(
                Reference::new("col3")
                    .less_than_or_equal_to(Datum::timestamptz_micros(1514764799000000)),
            )
            .and(
                Reference::new("col4")
                    .less_than_or_equal_to(Datum::timestamp_nanos(1514764799000000000)),
            )
            .and(
                Reference::new("col5")
                    .less_than_or_equal_to(Datum::timestamptz_nanos(1514764799000000000)),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 < 576) AND (pcol2 < 575)) AND (pcol3 < 575)) AND (pcol4 < 575)) AND (pcol5 < 575)"
                .to_string()
        );

        // test greater: (col1 > 2017-12-31) AND (col2 > 2017-12-31) AND (col3 > 2017-12-31) AND (col4 > 2017-12-31) AND (col5 > 2017-12-31)
        let predicate = Reference::new("col1")
            .greater_than(Datum::date_from_str("2017-12-31").unwrap())
            .and(Reference::new("col2").greater_than(Datum::timestamp_micros(1514764799000000)))
            .and(Reference::new("col3").greater_than(Datum::timestamptz_micros(1514764799000000)))
            .and(Reference::new("col4").greater_than(Datum::timestamp_nanos(1514764799000000000)))
            .and(Reference::new("col5").greater_than(Datum::timestamptz_nanos(1514764799000000000)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 > 575) AND (pcol2 > 575)) AND (pcol3 > 575)) AND (pcol4 > 575)) AND (pcol5 > 575)"
                .to_string()
        );

        // test greater or eq: (col1 >= 2017-12-31) AND (col2 >= 2017-12-31) AND (col3 >= 2017-12-31) AND (col4 >= 2017-12-31) AND (col5 >= 2017-12-31)
        let predicate = Reference::new("col1")
            .greater_than_or_equal_to(Datum::date_from_str("2017-12-31").unwrap())
            .and(
                Reference::new("col2")
                    .greater_than_or_equal_to(Datum::timestamp_micros(1514764799000000)),
            )
            .and(
                Reference::new("col3")
                    .greater_than_or_equal_to(Datum::timestamptz_micros(1514764799000000)),
            )
            .and(
                Reference::new("col4")
                    .greater_than_or_equal_to(Datum::timestamp_nanos(1514764799000000000)),
            )
            .and(
                Reference::new("col5")
                    .greater_than_or_equal_to(Datum::timestamptz_nanos(1514764799000000000)),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 > 575) AND (pcol2 > 575)) AND (pcol3 > 575)) AND (pcol4 > 575)) AND (pcol5 > 575)"
                .to_string()
        );

        // test not eq: (col1 != 2017-12-31) AND (col2 != 2017-12-31) AND (col3 != 2017-12-31) AND (col4 != 2017-12-31) AND (col5 != 2017-12-31)
        let predicate = Reference::new("col1")
            .not_equal_to(Datum::date_from_str("2017-12-31").unwrap())
            .and(Reference::new("col2").not_equal_to(Datum::timestamp_micros(1514764799000000)))
            .and(Reference::new("col3").not_equal_to(Datum::timestamptz_micros(1514764799000000)))
            .and(Reference::new("col4").not_equal_to(Datum::timestamp_nanos(1514764799000000000)))
            .and(Reference::new("col5").not_equal_to(Datum::timestamptz_nanos(1514764799000000000)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "((((pcol1 != 575) AND (pcol2 != 575)) AND (pcol3 != 575)) AND (pcol4 != 575)) AND (pcol5 != 575)".to_string());

        // test not in: (col1 NOT IN (2017-12-31, 2017-01-01)) AND (col2 NOT IN (2017-12-31, 2017-01-01)) AND (col3 NOT IN (2017-12-31, 2017-01-01)) AND (col4 NOT IN (2017-12-31, 2017-01-01)) AND (col5 NOT IN (2017-12-31, 2017-01-01))
        let predicate = Reference::new("col1")
            .is_not_in(
                vec![
                    Datum::date_from_str("2017-12-31").unwrap(),
                    Datum::date_from_str("2017-01-01").unwrap(),
                ]
                .into_iter(),
            )
            .and(
                Reference::new("col2").is_not_in(
                    vec![
                        Datum::timestamp_micros(1514764799000000),
                        Datum::timestamp_micros(1483228800000000),
                    ]
                    .into_iter(),
                ),
            )
            .and(
                Reference::new("col3").is_not_in(
                    vec![
                        Datum::timestamptz_micros(1514764799000000),
                        Datum::timestamptz_micros(1483228800000000),
                    ]
                    .into_iter(),
                ),
            )
            .and(
                Reference::new("col4").is_not_in(
                    vec![
                        Datum::timestamp_nanos(1514764799000000000),
                        Datum::timestamp_nanos(1483228800000000000),
                    ]
                    .into_iter(),
                ),
            )
            .and(
                Reference::new("col5").is_not_in(
                    vec![
                        Datum::timestamptz_nanos(1514764799000000000),
                        Datum::timestamptz_nanos(1483228800000000000),
                    ]
                    .into_iter(),
                ),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "((((pcol1 NOT IN (575, 564)) AND (pcol2 NOT IN (575, 564))) AND (pcol3 NOT IN (575, 564))) AND (pcol4 NOT IN (575, 564))) AND (pcol5 NOT IN (575, 564))".to_string());

        // test in
        let predicate = Reference::new("col1")
            .is_in(
                vec![
                    Datum::date_from_str("2017-12-31").unwrap(),
                    Datum::date_from_str("2017-01-01").unwrap(),
                ]
                .into_iter(),
            )
            .and(
                Reference::new("col2").is_in(
                    vec![
                        Datum::timestamp_micros(1514764799000000),
                        Datum::timestamp_micros(1483228800000000),
                    ]
                    .into_iter(),
                ),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "FALSE".to_string());
    }

    #[tokio::test]
    async fn test_strict_projection_negative_month_upper_bound() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        1,
                        "col1",
                        Type::Primitive(PrimitiveType::Date),
                    )),
                    Arc::new(NestedField::required(
                        2,
                        "col2",
                        Type::Primitive(PrimitiveType::Timestamp),
                    )),
                    Arc::new(NestedField::required(
                        3,
                        "col3",
                        Type::Primitive(PrimitiveType::Timestamptz),
                    )),
                    Arc::new(NestedField::required(
                        4,
                        "col4",
                        Type::Primitive(PrimitiveType::TimestampNs),
                    )),
                    Arc::new(NestedField::required(
                        5,
                        "col5",
                        Type::Primitive(PrimitiveType::TimestamptzNs),
                    )),
                ])
                .build()
                .unwrap(),
        );

        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(1)
                .add_partition_field("col1", "pcol1", Transform::Month)
                .unwrap()
                .add_partition_field("col2", "pcol2", Transform::Month)
                .unwrap()
                .add_partition_field("col3", "pcol3", Transform::Month)
                .unwrap()
                .add_partition_field("col4", "pcol4", Transform::Month)
                .unwrap()
                .add_partition_field("col5", "pcol5", Transform::Month)
                .unwrap()
                .build()
                .unwrap(),
        );

        let mut strict_projection = StrictProjection::new(partition_spec.clone());

        // test less: (col1 < 1969-12-31) AND (col2 < 1969-12-31) AND (col3 < 1969-12-31) AND (col4 < 1969-12-31) AND (col5 < 1969-12-31)
        let predicate = Reference::new("col1")
            .less_than(Datum::date_from_str("1969-12-31").unwrap())
            .and(Reference::new("col2").less_than(Datum::timestamp_micros(-86400000000)))
            .and(Reference::new("col3").less_than(Datum::timestamptz_micros(-86400000000)))
            .and(Reference::new("col4").less_than(Datum::timestamp_nanos(-86400000000000)))
            .and(Reference::new("col5").less_than(Datum::timestamptz_nanos(-86400000000000)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 < -1) AND (pcol2 < -1)) AND (pcol3 < -1)) AND (pcol4 < -1)) AND (pcol5 < -1)"
                .to_string()
        );

        // test less or eq: (col1 <= 1969-12-31) AND (col2 <= 1969-12-31) AND (col3 <= 1969-12-31) AND (col4 <= 1969-12-31) AND (col5 <= 1969-12-31)
        let predicate = Reference::new("col1")
            .less_than_or_equal_to(Datum::date_from_str("1969-12-31").unwrap())
            .and(
                Reference::new("col2").less_than_or_equal_to(Datum::timestamp_micros(-86400000000)),
            )
            .and(
                Reference::new("col3")
                    .less_than_or_equal_to(Datum::timestamptz_micros(-86400000000)),
            )
            .and(
                Reference::new("col4")
                    .less_than_or_equal_to(Datum::timestamp_nanos(-86400000000000)),
            )
            .and(
                Reference::new("col5")
                    .less_than_or_equal_to(Datum::timestamptz_nanos(-86400000000000)),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 < 0) AND (pcol2 < -1)) AND (pcol3 < -1)) AND (pcol4 < -1)) AND (pcol5 < -1)"
                .to_string()
        );

        // test greater: (col1 > 1969-12-31) AND (col2 > 1969-12-31) AND (col3 > 1969-12-31) AND (col4 > 1969-12-31) AND (col5 > 1969-12-31)
        let predicate = Reference::new("col1")
            .greater_than(Datum::date_from_str("1969-12-31").unwrap())
            .and(Reference::new("col2").greater_than(Datum::timestamp_micros(-86400000000)))
            .and(Reference::new("col3").greater_than(Datum::timestamptz_micros(-86400000000)))
            .and(Reference::new("col4").greater_than(Datum::timestamp_nanos(-86400000000000)))
            .and(Reference::new("col5").greater_than(Datum::timestamptz_nanos(-86400000000000)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 > -1) AND (pcol2 > -1)) AND (pcol3 > -1)) AND (pcol4 > -1)) AND (pcol5 > -1)"
                .to_string()
        );

        // test greater or eq: (col1 >= 1969-12-31) AND (col2 >= 1969-12-31) AND (col3 >= 1969-12-31) AND (col4 >= 1969-12-31) AND (col5 >= 1969-12-31)
        let predicate = Reference::new("col1")
            .greater_than_or_equal_to(Datum::date_from_str("1969-12-31").unwrap())
            .and(
                Reference::new("col2")
                    .greater_than_or_equal_to(Datum::timestamp_micros(-86400000000)),
            )
            .and(
                Reference::new("col3")
                    .greater_than_or_equal_to(Datum::timestamptz_micros(-86400000000)),
            )
            .and(
                Reference::new("col4")
                    .greater_than_or_equal_to(Datum::timestamp_nanos(-86400000000000)),
            )
            .and(
                Reference::new("col5")
                    .greater_than_or_equal_to(Datum::timestamptz_nanos(-86400000000000)),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 > -1) AND (pcol2 > -1)) AND (pcol3 > -1)) AND (pcol4 > -1)) AND (pcol5 > -1)"
                .to_string()
        );

        // test not eq: (col1 != 1969-12-31) AND (col2 != 1969-12-31) AND (col3 != 1969-12-31) AND (col4 != 1969-12-31) AND (col5 != 1969-12-31)
        let predicate = Reference::new("col1")
            .not_equal_to(Datum::date_from_str("1969-12-31").unwrap())
            .and(Reference::new("col2").not_equal_to(Datum::timestamp_micros(-86400000000)))
            .and(Reference::new("col3").not_equal_to(Datum::timestamptz_micros(-86400000000)))
            .and(Reference::new("col4").not_equal_to(Datum::timestamp_nanos(-86400000000000)))
            .and(Reference::new("col5").not_equal_to(Datum::timestamptz_nanos(-86400000000000)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "((((pcol1 != -1) AND (pcol2 != -1)) AND (pcol3 != -1)) AND (pcol4 != -1)) AND (pcol5 != -1)".to_string());
    }

    #[tokio::test]
    async fn test_strict_projection_day() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        1,
                        "col1",
                        Type::Primitive(PrimitiveType::Date),
                    )),
                    Arc::new(NestedField::required(
                        2,
                        "col2",
                        Type::Primitive(PrimitiveType::Timestamp),
                    )),
                    Arc::new(NestedField::required(
                        3,
                        "col3",
                        Type::Primitive(PrimitiveType::Timestamptz),
                    )),
                    Arc::new(NestedField::required(
                        4,
                        "col4",
                        Type::Primitive(PrimitiveType::TimestampNs),
                    )),
                    Arc::new(NestedField::required(
                        5,
                        "col5",
                        Type::Primitive(PrimitiveType::TimestamptzNs),
                    )),
                ])
                .build()
                .unwrap(),
        );

        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(1)
                .add_partition_field("col1", "pcol1", Transform::Day)
                .unwrap()
                .add_partition_field("col2", "pcol2", Transform::Day)
                .unwrap()
                .add_partition_field("col3", "pcol3", Transform::Day)
                .unwrap()
                .add_partition_field("col4", "pcol4", Transform::Day)
                .unwrap()
                .add_partition_field("col5", "pcol5", Transform::Day)
                .unwrap()
                .build()
                .unwrap(),
        );

        let mut strict_projection = StrictProjection::new(partition_spec.clone());

        // test less: (col1 < 2017-01-01) AND (col2 < 2017-01-01) AND (col3 < 2017-01-01) AND (col4 < 2017-01-01) AND (col5 < 2017-01-01)
        let predicate = Reference::new("col1")
            .less_than(Datum::date_from_str("2017-01-01").unwrap())
            .and(Reference::new("col2").less_than(Datum::timestamp_micros(1483228800000000)))
            .and(Reference::new("col3").less_than(Datum::timestamptz_micros(1483228800000000)))
            .and(Reference::new("col4").less_than(Datum::timestamp_nanos(1483228800000000000)))
            .and(Reference::new("col5").less_than(Datum::timestamptz_nanos(1483228800000000000)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 < 2017-01-01) AND (pcol2 < 2017-01-01)) AND (pcol3 < 2017-01-01)) AND (pcol4 < 2017-01-01)) AND (pcol5 < 2017-01-01)"
                .to_string()
        );

        // test less or eq: (col1 <= 2017-01-01) AND (col2 <= 2017-01-01) AND (col3 <= 2017-01-01) AND (col4 <= 2017-01-01) AND (col5 <= 2017-01-01)
        let predicate = Reference::new("col1")
            .less_than_or_equal_to(Datum::date_from_str("2017-01-01").unwrap())
            .and(
                Reference::new("col2")
                    .less_than_or_equal_to(Datum::timestamp_micros(1483228800000000)),
            )
            .and(
                Reference::new("col3")
                    .less_than_or_equal_to(Datum::timestamptz_micros(1483228800000000)),
            )
            .and(
                Reference::new("col4")
                    .less_than_or_equal_to(Datum::timestamp_nanos(1483228800000000000)),
            )
            .and(
                Reference::new("col5")
                    .less_than_or_equal_to(Datum::timestamptz_nanos(1483228800000000000)),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 < 2017-01-02) AND (pcol2 < 2017-01-01)) AND (pcol3 < 2017-01-01)) AND (pcol4 < 2017-01-01)) AND (pcol5 < 2017-01-01)"
                .to_string()
        );

        // test greater: (col1 > 2017-01-01) AND (col2 > 2017-01-01) AND (col3 > 2017-01-01) AND (col4 > 2017-01-01) AND (col5 > 2017-01-01)
        let predicate = Reference::new("col1")
            .greater_than(Datum::date_from_str("2017-01-01").unwrap())
            .and(Reference::new("col2").greater_than(Datum::timestamp_micros(1483228800000000)))
            .and(Reference::new("col3").greater_than(Datum::timestamptz_micros(1483228800000000)))
            .and(Reference::new("col4").greater_than(Datum::timestamp_nanos(1483228800000000000)))
            .and(Reference::new("col5").greater_than(Datum::timestamptz_nanos(1483228800000000000)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 > 2017-01-01) AND (pcol2 > 2017-01-01)) AND (pcol3 > 2017-01-01)) AND (pcol4 > 2017-01-01)) AND (pcol5 > 2017-01-01)"
                .to_string()
        );

        // test greater or eq: (col1 >= 2017-01-01) AND (col2 >= 2017-01-01) AND (col3 >= 2017-01-01) AND (col4 >= 2017-01-01) AND (col5 >= 2017-01-01)
        let predicate = Reference::new("col1")
            .greater_than_or_equal_to(Datum::date_from_str("2017-01-01").unwrap())
            .and(
                Reference::new("col2")
                    .greater_than_or_equal_to(Datum::timestamp_micros(1483228800000000)),
            )
            .and(
                Reference::new("col3")
                    .greater_than_or_equal_to(Datum::timestamptz_micros(1483228800000000)),
            )
            .and(
                Reference::new("col4")
                    .greater_than_or_equal_to(Datum::timestamp_nanos(1483228800000000000)),
            )
            .and(
                Reference::new("col5")
                    .greater_than_or_equal_to(Datum::timestamptz_nanos(1483228800000000000)),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 > 2016-12-31) AND (pcol2 > 2016-12-31)) AND (pcol3 > 2016-12-31)) AND (pcol4 > 2016-12-31)) AND (pcol5 > 2016-12-31)"
                .to_string()
        );

        // test not eq: (col1 != 2017-01-01) AND (col2 != 2017-01-01) AND (col3 != 2017-01-01) AND (col4 != 2017-01-01) AND (col5 != 2017-01-01)
        let predicate = Reference::new("col1")
            .not_equal_to(Datum::date_from_str("2017-01-01").unwrap())
            .and(Reference::new("col2").not_equal_to(Datum::timestamp_micros(1483228800000000)))
            .and(Reference::new("col3").not_equal_to(Datum::timestamptz_micros(1483228800000000)))
            .and(Reference::new("col4").not_equal_to(Datum::timestamp_nanos(1483228800000000000)))
            .and(Reference::new("col5").not_equal_to(Datum::timestamptz_nanos(1483228800000000000)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "((((pcol1 != 2017-01-01) AND (pcol2 != 2017-01-01)) AND (pcol3 != 2017-01-01)) AND (pcol4 != 2017-01-01)) AND (pcol5 != 2017-01-01)".to_string());

        // test not in: (col1 NOT IN (2017-01-01, 2017-12-31)) AND (col2 NOT IN (2017-01-01, 2017-12-31)) AND (col3 NOT IN (2017-01-01, 2017-12-31)) AND (col4 NOT IN (2017-01-01, 2017-12-31)) AND (col5 NOT IN (2017-01-01, 2017-12-31))
        let predicate = Reference::new("col1")
            .is_not_in(
                vec![
                    Datum::date_from_str("2017-01-01").unwrap(),
                    Datum::date_from_str("2017-12-31").unwrap(),
                ]
                .into_iter(),
            )
            .and(
                Reference::new("col2").is_not_in(
                    vec![
                        Datum::timestamp_micros(1483228800000000),
                        Datum::timestamp_micros(1514764799000000),
                    ]
                    .into_iter(),
                ),
            )
            .and(
                Reference::new("col3").is_not_in(
                    vec![
                        Datum::timestamptz_micros(1483228800000000),
                        Datum::timestamptz_micros(1514764799000000),
                    ]
                    .into_iter(),
                ),
            )
            .and(
                Reference::new("col4").is_not_in(
                    vec![
                        Datum::timestamp_nanos(1483228800000000000),
                        Datum::timestamp_nanos(1514764799000000000),
                    ]
                    .into_iter(),
                ),
            )
            .and(
                Reference::new("col5").is_not_in(
                    vec![
                        Datum::timestamptz_nanos(1483228800000000000),
                        Datum::timestamptz_nanos(1514764799000000000),
                    ]
                    .into_iter(),
                ),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "((((pcol1 NOT IN (2017-01-01, 2017-12-31)) AND (pcol2 NOT IN (2017-01-01, 2017-12-31))) AND (pcol3 NOT IN (2017-01-01, 2017-12-31))) AND (pcol4 NOT IN (2017-01-01, 2017-12-31))) AND (pcol5 NOT IN (2017-01-01, 2017-12-31))".to_string());

        // test in
        let predicate = Reference::new("col1")
            .is_in(
                vec![
                    Datum::date_from_str("2017-01-01").unwrap(),
                    Datum::date_from_str("2017-12-31").unwrap(),
                ]
                .into_iter(),
            )
            .and(
                Reference::new("col2").is_in(
                    vec![
                        Datum::timestamp_micros(1483228800000000),
                        Datum::timestamp_micros(1514764799000000),
                    ]
                    .into_iter(),
                ),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "FALSE".to_string());
    }

    #[tokio::test]
    async fn test_strict_projection_negative_day() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        1,
                        "col1",
                        Type::Primitive(PrimitiveType::Date),
                    )),
                    Arc::new(NestedField::required(
                        2,
                        "col2",
                        Type::Primitive(PrimitiveType::Timestamp),
                    )),
                    Arc::new(NestedField::required(
                        3,
                        "col3",
                        Type::Primitive(PrimitiveType::Timestamptz),
                    )),
                    Arc::new(NestedField::required(
                        4,
                        "col4",
                        Type::Primitive(PrimitiveType::TimestampNs),
                    )),
                    Arc::new(NestedField::required(
                        5,
                        "col5",
                        Type::Primitive(PrimitiveType::TimestamptzNs),
                    )),
                ])
                .build()
                .unwrap(),
        );

        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(1)
                .add_partition_field("col1", "pcol1", Transform::Day)
                .unwrap()
                .add_partition_field("col2", "pcol2", Transform::Day)
                .unwrap()
                .add_partition_field("col3", "pcol3", Transform::Day)
                .unwrap()
                .add_partition_field("col4", "pcol4", Transform::Day)
                .unwrap()
                .add_partition_field("col5", "pcol5", Transform::Day)
                .unwrap()
                .build()
                .unwrap(),
        );

        let mut strict_projection = StrictProjection::new(partition_spec.clone());

        // test less: (col1 < 1969-12-30) AND (col2 < 1969-12-30) AND (col3 < 1969-12-30) AND (col4 < 1969-12-30) AND (col5 < 1969-12-30)
        let predicate = Reference::new("col1")
            .less_than(Datum::date_from_str("1969-12-30").unwrap())
            .and(Reference::new("col2").less_than(Datum::timestamp_micros(-172800000000)))
            .and(Reference::new("col3").less_than(Datum::timestamptz_micros(-172800000000)))
            .and(Reference::new("col4").less_than(Datum::timestamp_nanos(-172800000000000)))
            .and(Reference::new("col5").less_than(Datum::timestamptz_nanos(-172800000000000)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 < 1969-12-30) AND (pcol2 < 1969-12-30)) AND (pcol3 < 1969-12-30)) AND (pcol4 < 1969-12-30)) AND (pcol5 < 1969-12-30)"
                .to_string()
        );

        // test less or eq: (col1 <= 1969-12-30) AND (col2 <= 1969-12-30) AND (col3 <= 1969-12-30) AND (col4 <= 1969-12-30) AND (col5 <= 1969-12-30)
        let predicate = Reference::new("col1")
            .less_than_or_equal_to(Datum::date_from_str("1969-12-30").unwrap())
            .and(
                Reference::new("col2")
                    .less_than_or_equal_to(Datum::timestamp_micros(-172800000000)),
            )
            .and(
                Reference::new("col3")
                    .less_than_or_equal_to(Datum::timestamptz_micros(-172800000000)),
            )
            .and(
                Reference::new("col4")
                    .less_than_or_equal_to(Datum::timestamp_nanos(-172800000000000)),
            )
            .and(
                Reference::new("col5")
                    .less_than_or_equal_to(Datum::timestamptz_nanos(-172800000000000)),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 < 1969-12-31) AND (pcol2 < 1969-12-30)) AND (pcol3 < 1969-12-30)) AND (pcol4 < 1969-12-30)) AND (pcol5 < 1969-12-30)"
                .to_string()
        );

        // test greater: (col1 > 1969-12-30) AND (col2 > 1969-12-30) AND (col3 > 1969-12-30) AND (col4 > 1969-12-30) AND (col5 > 1969-12-30)
        let predicate = Reference::new("col1")
            .greater_than(Datum::date_from_str("1969-12-30").unwrap())
            .and(Reference::new("col2").greater_than(Datum::timestamp_micros(-172800000000)))
            .and(Reference::new("col3").greater_than(Datum::timestamptz_micros(-172800000000)))
            .and(Reference::new("col4").greater_than(Datum::timestamp_nanos(-172800000000000)))
            .and(Reference::new("col5").greater_than(Datum::timestamptz_nanos(-172800000000000)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 > 1969-12-30) AND (pcol2 > 1969-12-30)) AND (pcol3 > 1969-12-30)) AND (pcol4 > 1969-12-30)) AND (pcol5 > 1969-12-30)"
                .to_string()
        );

        // test greater or eq: (col1 >= 1969-12-30) AND (col2 >= 1969-12-30) AND (col3 >= 1969-12-30) AND (col4 >= 1969-12-30) AND (col5 >= 1969-12-30)
        let predicate = Reference::new("col1")
            .greater_than_or_equal_to(Datum::date_from_str("1969-12-30").unwrap())
            .and(
                Reference::new("col2")
                    .greater_than_or_equal_to(Datum::timestamp_micros(-172800000000)),
            )
            .and(
                Reference::new("col3")
                    .greater_than_or_equal_to(Datum::timestamptz_micros(-172800000000)),
            )
            .and(
                Reference::new("col4")
                    .greater_than_or_equal_to(Datum::timestamp_nanos(-172800000000000)),
            )
            .and(
                Reference::new("col5")
                    .greater_than_or_equal_to(Datum::timestamptz_nanos(-172800000000000)),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 > 1969-12-29) AND (pcol2 > 1969-12-29)) AND (pcol3 > 1969-12-29)) AND (pcol4 > 1969-12-29)) AND (pcol5 > 1969-12-29)"
                .to_string()
        );

        // test not eq: (col1 != 1969-12-30) AND (col2 != 1969-12-30) AND (col3 != 1969-12-30) AND (col4 != 1969-12-30) AND (col5 != 1969-12-30)
        let predicate = Reference::new("col1")
            .not_equal_to(Datum::date_from_str("1969-12-30").unwrap())
            .and(Reference::new("col2").not_equal_to(Datum::timestamp_micros(-172800000000)))
            .and(Reference::new("col3").not_equal_to(Datum::timestamptz_micros(-172800000000)))
            .and(Reference::new("col4").not_equal_to(Datum::timestamp_nanos(-172800000000000)))
            .and(Reference::new("col5").not_equal_to(Datum::timestamptz_nanos(-172800000000000)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "((((pcol1 != 1969-12-30) AND (pcol2 != 1969-12-30)) AND (pcol3 != 1969-12-30)) AND (pcol4 != 1969-12-30)) AND (pcol5 != 1969-12-30)".to_string());

        // test not in: (col1 NOT IN (1969-12-30, 1969-12-31)) AND (col2 NOT IN (1969-12-30, 1969-12-31)) AND (col3 NOT IN (1969-12-30, 1969-12-31)) AND (col4 NOT IN (1969-12-30, 1969-12-31)) AND (col5 NOT IN (1969-12-30, 1969-12-31))
        let predicate = Reference::new("col1")
            .is_not_in(
                vec![
                    Datum::date_from_str("1969-12-30").unwrap(),
                    Datum::date_from_str("1969-12-31").unwrap(),
                ]
                .into_iter(),
            )
            .and(
                Reference::new("col2").is_not_in(
                    vec![
                        Datum::timestamp_micros(-172800000000),
                        Datum::timestamp_micros(-1),
                    ]
                    .into_iter(),
                ),
            )
            .and(
                Reference::new("col3").is_not_in(
                    vec![
                        Datum::timestamptz_micros(-172800000000),
                        Datum::timestamptz_micros(-1),
                    ]
                    .into_iter(),
                ),
            )
            .and(
                Reference::new("col4").is_not_in(
                    vec![
                        Datum::timestamp_nanos(-172800000000000),
                        Datum::timestamp_nanos(-1),
                    ]
                    .into_iter(),
                ),
            )
            .and(
                Reference::new("col5").is_not_in(
                    vec![
                        Datum::timestamptz_nanos(-172800000000000),
                        Datum::timestamptz_nanos(-1),
                    ]
                    .into_iter(),
                ),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "((((pcol1 NOT IN (1969-12-31, 1969-12-30)) AND (pcol2 NOT IN (1969-12-31, 1969-12-30))) AND (pcol3 NOT IN (1969-12-31, 1969-12-30))) AND (pcol4 NOT IN (1969-12-31, 1969-12-30))) AND (pcol5 NOT IN (1969-12-31, 1969-12-30))".to_string());

        // test in
        let predicate = Reference::new("col1")
            .is_in(
                vec![
                    Datum::date_from_str("1969-12-30").unwrap(),
                    Datum::date_from_str("1969-12-31").unwrap(),
                ]
                .into_iter(),
            )
            .and(
                Reference::new("col2").is_in(
                    vec![
                        Datum::timestamp_micros(-172800000000),
                        Datum::timestamp_micros(-1),
                    ]
                    .into_iter(),
                ),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "FALSE".to_string());
    }

    #[tokio::test]
    async fn test_strict_projection_year_lower_bound() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        1,
                        "col1",
                        Type::Primitive(PrimitiveType::Date),
                    )),
                    Arc::new(NestedField::required(
                        2,
                        "col2",
                        Type::Primitive(PrimitiveType::Timestamp),
                    )),
                    Arc::new(NestedField::required(
                        3,
                        "col3",
                        Type::Primitive(PrimitiveType::Timestamptz),
                    )),
                    Arc::new(NestedField::required(
                        4,
                        "col4",
                        Type::Primitive(PrimitiveType::TimestampNs),
                    )),
                    Arc::new(NestedField::required(
                        5,
                        "col5",
                        Type::Primitive(PrimitiveType::TimestamptzNs),
                    )),
                ])
                .build()
                .unwrap(),
        );

        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(1)
                .add_partition_field("col1", "pcol1", Transform::Year)
                .unwrap()
                .add_partition_field("col2", "pcol2", Transform::Year)
                .unwrap()
                .add_partition_field("col3", "pcol3", Transform::Year)
                .unwrap()
                .add_partition_field("col4", "pcol4", Transform::Year)
                .unwrap()
                .add_partition_field("col5", "pcol5", Transform::Year)
                .unwrap()
                .build()
                .unwrap(),
        );

        let mut strict_projection = StrictProjection::new(partition_spec.clone());

        // test less: (col1 < 2017-01-01) AND (col2 < 2017-01-01) AND (col3 < 2017-01-01) AND (col4 < 2017-01-01) AND (col5 < 2017-01-01)
        let predicate = Reference::new("col1")
            .less_than(Datum::date_from_str("2017-01-01").unwrap())
            .and(Reference::new("col2").less_than(Datum::timestamp_micros(1483228800000000)))
            .and(Reference::new("col3").less_than(Datum::timestamptz_micros(1483228800000000)))
            .and(Reference::new("col4").less_than(Datum::timestamp_nanos(1483228800000000000)))
            .and(Reference::new("col5").less_than(Datum::timestamptz_nanos(1483228800000000000)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 < 47) AND (pcol2 < 47)) AND (pcol3 < 47)) AND (pcol4 < 47)) AND (pcol5 < 47)"
                .to_string()
        );

        // test less or eq: (col1 <= 2017-01-01) AND (col2 <= 2017-01-01) AND (col3 <= 2017-01-01) AND (col4 <= 2017-01-01) AND (col5 <= 2017-01-01)
        let predicate = Reference::new("col1")
            .less_than_or_equal_to(Datum::date_from_str("2017-01-01").unwrap())
            .and(
                Reference::new("col2")
                    .less_than_or_equal_to(Datum::timestamp_micros(1483228800000000)),
            )
            .and(
                Reference::new("col3")
                    .less_than_or_equal_to(Datum::timestamptz_micros(1483228800000000)),
            )
            .and(
                Reference::new("col4")
                    .less_than_or_equal_to(Datum::timestamp_nanos(1483228800000000000)),
            )
            .and(
                Reference::new("col5")
                    .less_than_or_equal_to(Datum::timestamptz_nanos(1483228800000000000)),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 < 47) AND (pcol2 < 47)) AND (pcol3 < 47)) AND (pcol4 < 47)) AND (pcol5 < 47)"
                .to_string()
        );

        // test greater: (col1 > 2017-01-01) AND (col2 > 2017-01-01) AND (col3 > 2017-01-01) AND (col4 > 2017-01-01) AND (col5 > 2017-01-01)
        let predicate = Reference::new("col1")
            .greater_than(Datum::date_from_str("2017-01-01").unwrap())
            .and(Reference::new("col2").greater_than(Datum::timestamp_micros(1483228800000000)))
            .and(Reference::new("col3").greater_than(Datum::timestamptz_micros(1483228800000000)))
            .and(Reference::new("col4").greater_than(Datum::timestamp_nanos(1483228800000000000)))
            .and(Reference::new("col5").greater_than(Datum::timestamptz_nanos(1483228800000000000)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 > 47) AND (pcol2 > 47)) AND (pcol3 > 47)) AND (pcol4 > 47)) AND (pcol5 > 47)"
                .to_string()
        );

        // test greater or eq: (col1 >= 2017-01-01) AND (col2 >= 2017-01-01) AND (col3 >= 2017-01-01) AND (col4 >= 2017-01-01) AND (col5 >= 2017-01-01)
        let predicate = Reference::new("col1")
            .greater_than_or_equal_to(Datum::date_from_str("2017-01-01").unwrap())
            .and(
                Reference::new("col2")
                    .greater_than_or_equal_to(Datum::timestamp_micros(1483228800000000)),
            )
            .and(
                Reference::new("col3")
                    .greater_than_or_equal_to(Datum::timestamptz_micros(1483228800000000)),
            )
            .and(
                Reference::new("col4")
                    .greater_than_or_equal_to(Datum::timestamp_nanos(1483228800000000000)),
            )
            .and(
                Reference::new("col5")
                    .greater_than_or_equal_to(Datum::timestamptz_nanos(1483228800000000000)),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 > 46) AND (pcol2 > 46)) AND (pcol3 > 46)) AND (pcol4 > 46)) AND (pcol5 > 46)"
                .to_string()
        );

        // test not eq: (col1 != 2017-01-01) AND (col2 != 2017-01-01) AND (col3 != 2017-01-01) AND (col4 != 2017-01-01) AND (col5 != 2017-01-01)
        let predicate = Reference::new("col1")
            .not_equal_to(Datum::date_from_str("2017-01-01").unwrap())
            .and(Reference::new("col2").not_equal_to(Datum::timestamp_micros(1483228800000000)))
            .and(Reference::new("col3").not_equal_to(Datum::timestamptz_micros(1483228800000000)))
            .and(Reference::new("col4").not_equal_to(Datum::timestamp_nanos(1483228800000000000)))
            .and(Reference::new("col5").not_equal_to(Datum::timestamptz_nanos(1483228800000000000)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "((((pcol1 != 47) AND (pcol2 != 47)) AND (pcol3 != 47)) AND (pcol4 != 47)) AND (pcol5 != 47)".to_string());

        // test not in: (col1 NOT IN (2017-01-01, 2017-12-31)) AND (col2 NOT IN (2017-01-01, 2017-12-31)) AND (col3 NOT IN (2017-01-01, 2017-12-31)) AND (col4 NOT IN (2017-01-01, 2017-12-31)) AND (col5 NOT IN (2017-01-01, 2017-12-31))
        let predicate = Reference::new("col1")
            .is_not_in(
                vec![
                    Datum::date_from_str("2017-01-01").unwrap(),
                    Datum::date_from_str("2016-12-31").unwrap(),
                ]
                .into_iter(),
            )
            .and(
                Reference::new("col2").is_not_in(
                    vec![
                        Datum::timestamp_micros(1483228800000000),
                        Datum::timestamp_micros(1483142400000000),
                    ]
                    .into_iter(),
                ),
            )
            .and(
                Reference::new("col3").is_not_in(
                    vec![
                        Datum::timestamptz_micros(1483228800000000),
                        Datum::timestamptz_micros(1483142400000000),
                    ]
                    .into_iter(),
                ),
            )
            .and(
                Reference::new("col4").is_not_in(
                    vec![
                        Datum::timestamp_nanos(1483228800000000000),
                        Datum::timestamp_nanos(1483142400000000000),
                    ]
                    .into_iter(),
                ),
            )
            .and(
                Reference::new("col5").is_not_in(
                    vec![
                        Datum::timestamptz_nanos(1483228800000000000),
                        Datum::timestamptz_nanos(1483142400000000000),
                    ]
                    .into_iter(),
                ),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "((((pcol1 NOT IN (47, 46)) AND (pcol2 NOT IN (47, 46))) AND (pcol3 NOT IN (47, 46))) AND (pcol4 NOT IN (47, 46))) AND (pcol5 NOT IN (47, 46))".to_string());

        // test in
        let predicate = Reference::new("col1")
            .is_in(
                vec![
                    Datum::date_from_str("2017-01-01").unwrap(),
                    Datum::date_from_str("2017-12-31").unwrap(),
                ]
                .into_iter(),
            )
            .and(
                Reference::new("col2").is_in(
                    vec![
                        Datum::timestamp_micros(1483228800000000),
                        Datum::timestamp_micros(1514764799000000),
                    ]
                    .into_iter(),
                ),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "FALSE".to_string());
    }

    #[tokio::test]
    async fn test_strict_projection_year_negative_lower_bound() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        1,
                        "col1",
                        Type::Primitive(PrimitiveType::Date),
                    )),
                    Arc::new(NestedField::required(
                        2,
                        "col2",
                        Type::Primitive(PrimitiveType::Timestamp),
                    )),
                    Arc::new(NestedField::required(
                        3,
                        "col3",
                        Type::Primitive(PrimitiveType::Timestamptz),
                    )),
                    Arc::new(NestedField::required(
                        4,
                        "col4",
                        Type::Primitive(PrimitiveType::TimestampNs),
                    )),
                    Arc::new(NestedField::required(
                        5,
                        "col5",
                        Type::Primitive(PrimitiveType::TimestamptzNs),
                    )),
                ])
                .build()
                .unwrap(),
        );

        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(1)
                .add_partition_field("col1", "pcol1", Transform::Year)
                .unwrap()
                .add_partition_field("col2", "pcol2", Transform::Year)
                .unwrap()
                .add_partition_field("col3", "pcol3", Transform::Year)
                .unwrap()
                .add_partition_field("col4", "pcol4", Transform::Year)
                .unwrap()
                .add_partition_field("col5", "pcol5", Transform::Year)
                .unwrap()
                .build()
                .unwrap(),
        );

        let mut strict_projection = StrictProjection::new(partition_spec.clone());

        // test less: (col1 < 1970-01-01) AND (col2 < 1970-01-01) AND (col3 < 1970-01-01) AND (col4 < 1970-01-01) AND (col5 < 1970-01-01)
        let predicate = Reference::new("col1")
            .less_than(Datum::date_from_str("1970-01-01").unwrap())
            .and(Reference::new("col2").less_than(Datum::timestamp_micros(0)))
            .and(Reference::new("col3").less_than(Datum::timestamptz_micros(0)))
            .and(Reference::new("col4").less_than(Datum::timestamp_nanos(0)))
            .and(Reference::new("col5").less_than(Datum::timestamptz_nanos(0)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 < 0) AND (pcol2 < 0)) AND (pcol3 < 0)) AND (pcol4 < 0)) AND (pcol5 < 0)"
                .to_string()
        );

        // test less or eq: (col1 <= 1970-01-01) AND (col2 <= 1970-01-01) AND (col3 <= 1970-01-01) AND (col4 <= 1970-01-01) AND (col5 <= 1970-01-01)
        let predicate = Reference::new("col1")
            .less_than_or_equal_to(Datum::date_from_str("1970-01-01").unwrap())
            .and(Reference::new("col2").less_than_or_equal_to(Datum::timestamp_micros(0)))
            .and(Reference::new("col3").less_than_or_equal_to(Datum::timestamptz_micros(0)))
            .and(Reference::new("col4").less_than_or_equal_to(Datum::timestamp_nanos(0)))
            .and(Reference::new("col5").less_than_or_equal_to(Datum::timestamptz_nanos(0)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 < 0) AND (pcol2 < 0)) AND (pcol3 < 0)) AND (pcol4 < 0)) AND (pcol5 < 0)"
                .to_string()
        );

        // test greater: (col1 > 1970-01-01) AND (col2 > 1970-01-01) AND (col3 > 1970-01-01) AND (col4 > 1970-01-01) AND (col5 > 1970-01-01)
        let predicate = Reference::new("col1")
            .greater_than(Datum::date_from_str("1970-01-01").unwrap())
            .and(Reference::new("col2").greater_than(Datum::timestamp_micros(0)))
            .and(Reference::new("col3").greater_than(Datum::timestamptz_micros(0)))
            .and(Reference::new("col4").greater_than(Datum::timestamp_nanos(0)))
            .and(Reference::new("col5").greater_than(Datum::timestamptz_nanos(0)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 > 0) AND (pcol2 > 0)) AND (pcol3 > 0)) AND (pcol4 > 0)) AND (pcol5 > 0)"
                .to_string()
        );

        // test greater or eq: (col1 >= 1970-01-01) AND (col2 >= 1970-01-01) AND (col3 >= 1970-01-01) AND (col4 >= 1970-01-01) AND (col5 >= 1970-01-01)
        let predicate = Reference::new("col1")
            .greater_than_or_equal_to(Datum::date_from_str("1970-01-01").unwrap())
            .and(Reference::new("col2").greater_than_or_equal_to(Datum::timestamp_micros(0)))
            .and(Reference::new("col3").greater_than_or_equal_to(Datum::timestamptz_micros(0)))
            .and(Reference::new("col4").greater_than_or_equal_to(Datum::timestamp_nanos(0)))
            .and(Reference::new("col5").greater_than_or_equal_to(Datum::timestamptz_nanos(0)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 > -1) AND (pcol2 > -1)) AND (pcol3 > -1)) AND (pcol4 > -1)) AND (pcol5 > -1)"
                .to_string()
        );

        // test not eq: (col1 != 1970-01-01) AND (col2 != 1970-01-01) AND (col3 != 1970-01-01) AND (col4 != 1970-01-01) AND (col5 != 1970-01-01)
        let predicate = Reference::new("col1")
            .not_equal_to(Datum::date_from_str("1970-01-01").unwrap())
            .and(Reference::new("col2").not_equal_to(Datum::timestamp_micros(0)))
            .and(Reference::new("col3").not_equal_to(Datum::timestamptz_micros(0)))
            .and(Reference::new("col4").not_equal_to(Datum::timestamp_nanos(0)))
            .and(Reference::new("col5").not_equal_to(Datum::timestamptz_nanos(0)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "((((pcol1 != 0) AND (pcol2 != 0)) AND (pcol3 != 0)) AND (pcol4 != 0)) AND (pcol5 != 0)".to_string());

        // test not in: (col1 NOT IN (1970-01-01, 1970-12-31)) AND (col2 NOT IN (1970-01-01, 1970-12-31)) AND (col3 NOT IN (1970-01-01, 1970-12-31)) AND (col4 NOT IN (1970-01-01, 1970-12-31)) AND (col5 NOT IN (1970-01-01, 1970-12-31))
        let predicate = Reference::new("col1")
            .is_not_in(
                vec![
                    Datum::date_from_str("1970-01-01").unwrap(),
                    Datum::date_from_str("1970-12-31").unwrap(),
                ]
                .into_iter(),
            )
            .and(
                Reference::new("col2").is_not_in(
                    vec![
                        Datum::timestamp_micros(0),
                        Datum::timestamp_micros(3153599999000000),
                    ]
                    .into_iter(),
                ),
            )
            .and(
                Reference::new("col3").is_not_in(
                    vec![
                        Datum::timestamptz_micros(0),
                        Datum::timestamptz_micros(3153599999000000),
                    ]
                    .into_iter(),
                ),
            )
            .and(
                Reference::new("col4").is_not_in(
                    vec![
                        Datum::timestamp_nanos(0),
                        Datum::timestamp_nanos(3153599999000000000),
                    ]
                    .into_iter(),
                ),
            )
            .and(
                Reference::new("col5").is_not_in(
                    vec![
                        Datum::timestamptz_nanos(0),
                        Datum::timestamptz_nanos(3153599999000000000),
                    ]
                    .into_iter(),
                ),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "((((pcol1 NOT IN (0)) AND (pcol2 NOT IN (0, 99))) AND (pcol3 NOT IN (0, 99))) AND (pcol4 NOT IN (0, 99))) AND (pcol5 NOT IN (0, 99))".to_string());

        // test in
        let predicate = Reference::new("col1")
            .is_in(
                vec![
                    Datum::date_from_str("1970-01-01").unwrap(),
                    Datum::date_from_str("1970-12-31").unwrap(),
                ]
                .into_iter(),
            )
            .and(
                Reference::new("col2").is_in(
                    vec![
                        Datum::timestamp_micros(0),
                        Datum::timestamp_micros(3153599999000000),
                    ]
                    .into_iter(),
                ),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "FALSE".to_string());
    }

    #[tokio::test]
    async fn test_strict_projection_year_upper_bound() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        1,
                        "col1",
                        Type::Primitive(PrimitiveType::Date),
                    )),
                    Arc::new(NestedField::required(
                        2,
                        "col2",
                        Type::Primitive(PrimitiveType::Timestamp),
                    )),
                    Arc::new(NestedField::required(
                        3,
                        "col3",
                        Type::Primitive(PrimitiveType::Timestamptz),
                    )),
                    Arc::new(NestedField::required(
                        4,
                        "col4",
                        Type::Primitive(PrimitiveType::TimestampNs),
                    )),
                    Arc::new(NestedField::required(
                        5,
                        "col5",
                        Type::Primitive(PrimitiveType::TimestamptzNs),
                    )),
                ])
                .build()
                .unwrap(),
        );

        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(1)
                .add_partition_field("col1", "pcol1", Transform::Year)
                .unwrap()
                .add_partition_field("col2", "pcol2", Transform::Year)
                .unwrap()
                .add_partition_field("col3", "pcol3", Transform::Year)
                .unwrap()
                .add_partition_field("col4", "pcol4", Transform::Year)
                .unwrap()
                .add_partition_field("col5", "pcol5", Transform::Year)
                .unwrap()
                .build()
                .unwrap(),
        );

        let mut strict_projection = StrictProjection::new(partition_spec.clone());

        // test less: (col1 < 2017-12-31) AND (col2 < 2017-12-31) AND (col3 < 2017-12-31) AND (col4 < 2017-12-31) AND (col5 < 2017-12-31)
        let predicate = Reference::new("col1")
            .less_than(Datum::date_from_str("2017-12-31").unwrap())
            .and(Reference::new("col2").less_than(Datum::timestamp_micros(1514764799000000)))
            .and(Reference::new("col3").less_than(Datum::timestamptz_micros(1514764799000000)))
            .and(Reference::new("col4").less_than(Datum::timestamp_nanos(1514764799000000000)))
            .and(Reference::new("col5").less_than(Datum::timestamptz_nanos(1514764799000000000)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 < 47) AND (pcol2 < 47)) AND (pcol3 < 47)) AND (pcol4 < 47)) AND (pcol5 < 47)"
                .to_string()
        );

        // test less or eq: (col1 <= 2017-12-31) AND (col2 <= 2017-12-31) AND (col3 <= 2017-12-31) AND (col4 <= 2017-12-31) AND (col5 <= 2017-12-31)
        let predicate = Reference::new("col1")
            .less_than_or_equal_to(Datum::date_from_str("2017-12-31").unwrap())
            .and(
                Reference::new("col2")
                    .less_than_or_equal_to(Datum::timestamp_micros(1514764799000000)),
            )
            .and(
                Reference::new("col3")
                    .less_than_or_equal_to(Datum::timestamptz_micros(1514764799000000)),
            )
            .and(
                Reference::new("col4")
                    .less_than_or_equal_to(Datum::timestamp_nanos(1514764799000000000)),
            )
            .and(
                Reference::new("col5")
                    .less_than_or_equal_to(Datum::timestamptz_nanos(1514764799000000000)),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 < 48) AND (pcol2 < 47)) AND (pcol3 < 47)) AND (pcol4 < 47)) AND (pcol5 < 47)"
                .to_string()
        );

        // test greater: (col1 > 2017-12-31) AND (col2 > 2017-12-31) AND (col3 > 2017-12-31) AND (col4 > 2017-12-31) AND (col5 > 2017-12-31)
        let predicate = Reference::new("col1")
            .greater_than(Datum::date_from_str("2017-12-31").unwrap())
            .and(Reference::new("col2").greater_than(Datum::timestamp_micros(1514764799000000)))
            .and(Reference::new("col3").greater_than(Datum::timestamptz_micros(1514764799000000)))
            .and(Reference::new("col4").greater_than(Datum::timestamp_nanos(1514764799000000000)))
            .and(Reference::new("col5").greater_than(Datum::timestamptz_nanos(1514764799000000000)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 > 47) AND (pcol2 > 47)) AND (pcol3 > 47)) AND (pcol4 > 47)) AND (pcol5 > 47)"
                .to_string()
        );

        // test greater or eq: (col1 >= 2017-12-31) AND (col2 >= 2017-12-31) AND (col3 >= 2017-12-31) AND (col4 >= 2017-12-31) AND (col5 >= 2017-12-31)
        let predicate = Reference::new("col1")
            .greater_than_or_equal_to(Datum::date_from_str("2017-12-31").unwrap())
            .and(
                Reference::new("col2")
                    .greater_than_or_equal_to(Datum::timestamp_micros(1514764799000000)),
            )
            .and(
                Reference::new("col3")
                    .greater_than_or_equal_to(Datum::timestamptz_micros(1514764799000000)),
            )
            .and(
                Reference::new("col4")
                    .greater_than_or_equal_to(Datum::timestamp_nanos(1514764799000000000)),
            )
            .and(
                Reference::new("col5")
                    .greater_than_or_equal_to(Datum::timestamptz_nanos(1514764799000000000)),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((((pcol1 > 47) AND (pcol2 > 47)) AND (pcol3 > 47)) AND (pcol4 > 47)) AND (pcol5 > 47)"
                .to_string()
        );

        // test not eq: (col1 != 2017-12-31) AND (col2 != 2017-12-31) AND (col3 != 2017-12-31) AND (col4 != 2017-12-31) AND (col5 != 2017-12-31)
        let predicate = Reference::new("col1")
            .not_equal_to(Datum::date_from_str("2017-12-31").unwrap())
            .and(Reference::new("col2").not_equal_to(Datum::timestamp_micros(1514764799000000)))
            .and(Reference::new("col3").not_equal_to(Datum::timestamptz_micros(1514764799000000)))
            .and(Reference::new("col4").not_equal_to(Datum::timestamp_nanos(1514764799000000000)))
            .and(Reference::new("col5").not_equal_to(Datum::timestamptz_nanos(1514764799000000000)))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "((((pcol1 != 47) AND (pcol2 != 47)) AND (pcol3 != 47)) AND (pcol4 != 47)) AND (pcol5 != 47)".to_string());

        // test not in: (col1 NOT IN (2017-12-31, 2016-01-01)) AND (col2 NOT IN (2017-12-31, 2016-01-01)) AND (col3 NOT IN (2017-12-31, 2016-01-01)) AND (col4 NOT IN (2017-12-31, 2016-01-01)) AND (col5 NOT IN (2017-12-31, 2016-01-01))
        let predicate = Reference::new("col1")
            .is_not_in(
                vec![
                    Datum::date_from_str("2017-12-31").unwrap(),
                    Datum::date_from_str("2016-01-01").unwrap(),
                ]
                .into_iter(),
            )
            .and(
                Reference::new("col2").is_not_in(
                    vec![
                        Datum::timestamp_micros(1514764799000000),
                        Datum::timestamp_micros(1451606400000000),
                    ]
                    .into_iter(),
                ),
            )
            .and(
                Reference::new("col3").is_not_in(
                    vec![
                        Datum::timestamptz_micros(1514764799000000),
                        Datum::timestamptz_micros(1451606400000000),
                    ]
                    .into_iter(),
                ),
            )
            .and(
                Reference::new("col4").is_not_in(
                    vec![
                        Datum::timestamp_nanos(1514764799000000000),
                        Datum::timestamp_nanos(1451606400000000000),
                    ]
                    .into_iter(),
                ),
            )
            .and(
                Reference::new("col5").is_not_in(
                    vec![
                        Datum::timestamptz_nanos(1514764799000000000),
                        Datum::timestamptz_nanos(1451606400000000000),
                    ]
                    .into_iter(),
                ),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "((((pcol1 NOT IN (47, 46)) AND (pcol2 NOT IN (47, 46))) AND (pcol3 NOT IN (47, 46))) AND (pcol4 NOT IN (47, 46))) AND (pcol5 NOT IN (47, 46))".to_string());

        // test in
        let predicate = Reference::new("col1")
            .is_in(
                vec![
                    Datum::date_from_str("2017-12-31").unwrap(),
                    Datum::date_from_str("2017-01-01").unwrap(),
                ]
                .into_iter(),
            )
            .and(
                Reference::new("col2").is_in(
                    vec![
                        Datum::timestamp_micros(1514764799000000),
                        Datum::timestamp_micros(1483228800000000),
                    ]
                    .into_iter(),
                ),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "FALSE".to_string());
    }

    #[tokio::test]
    async fn test_strict_projection_bucket() {
        // int, long, decimal, string, bytes, uuid
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        1,
                        "col1",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::required(
                        2,
                        "col2",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::required(
                        3,
                        "col3",
                        Type::Primitive(PrimitiveType::Decimal {
                            precision: 9,
                            scale: 2,
                        }),
                    )),
                    Arc::new(NestedField::required(
                        4,
                        "col4",
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::required(
                        5,
                        "col5",
                        Type::Primitive(PrimitiveType::Binary),
                    )),
                    Arc::new(NestedField::required(
                        6,
                        "col6",
                        Type::Primitive(PrimitiveType::Uuid),
                    )),
                ])
                .build()
                .unwrap(),
        );

        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(1)
                .add_partition_field("col1", "pcol1", Transform::Bucket(10))
                .unwrap()
                .add_partition_field("col2", "pcol2", Transform::Bucket(10))
                .unwrap()
                .add_partition_field("col3", "pcol3", Transform::Bucket(10))
                .unwrap()
                .add_partition_field("col4", "pcol4", Transform::Bucket(10))
                .unwrap()
                .add_partition_field("col5", "pcol5", Transform::Bucket(10))
                .unwrap()
                .add_partition_field("col6", "pcol6", Transform::Bucket(10))
                .unwrap()
                .build()
                .unwrap(),
        );

        let mut strict_projection = StrictProjection::new(partition_spec.clone());

        // test not eq
        let predicate = Reference::new("col1")
            .not_equal_to(Datum::int(100))
            .and(Reference::new("col2").not_equal_to(Datum::long(100)))
            .and(Reference::new("col3").not_equal_to(Datum::new(
                PrimitiveType::Decimal {
                    precision: 9,
                    scale: 2,
                },
                PrimitiveLiteral::Int128(10000),
            )))
            .and(Reference::new("col4").not_equal_to(Datum::string("abcdefg")))
            .and(Reference::new("col5").not_equal_to(Datum::binary("abcdefg".as_bytes().to_vec())))
            .and(Reference::new("col6").not_equal_to(Datum::uuid(
                Uuid::parse_str("00000000-0000-007b-0000-0000000001c8").unwrap(),
            )))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "(((((pcol1 != 6) AND (pcol2 != 6)) AND (pcol3 != 2)) AND (pcol4 != 4)) AND (pcol5 != 4)) AND (pcol6 != 4)".to_string());

        // test eq, less, less or eq, greater, greater or eq, in
        let predicate = Reference::new("col1")
            .equal_to(Datum::int(100))
            .and(Reference::new("col2").less_than(Datum::long(100)))
            .and(Reference::new("col3").less_than_or_equal_to(Datum::new(
                PrimitiveType::Decimal {
                    precision: 9,
                    scale: 2,
                },
                PrimitiveLiteral::Int128(10000),
            )))
            .and(Reference::new("col4").greater_than(Datum::string("abcdefg")))
            .and(
                Reference::new("col5")
                    .greater_than_or_equal_to(Datum::binary("abcdefg".as_bytes().to_vec())),
            )
            .and(
                Reference::new("col6").is_in(
                    vec![
                        Datum::uuid(
                            Uuid::parse_str("00000000-0000-007b-0000-0000000001c8").unwrap(),
                        ),
                        Datum::uuid(
                            Uuid::parse_str("00000000-0000-007b-0000-0000000001c9").unwrap(),
                        ),
                    ]
                    .into_iter(),
                ),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "FALSE".to_string());

        // test not in
        let predicate =
            Reference::new("col1")
                .is_not_in(vec![Datum::int(99), Datum::int(100), Datum::int(101)].into_iter())
                .and(Reference::new("col2").is_not_in(
                    vec![Datum::long(99), Datum::long(100), Datum::long(101)].into_iter(),
                ))
                .and(
                    Reference::new("col3").is_not_in(
                        vec![
                            Datum::new(
                                PrimitiveType::Decimal {
                                    precision: 9,
                                    scale: 2,
                                },
                                PrimitiveLiteral::Int128(9900),
                            ),
                            Datum::new(
                                PrimitiveType::Decimal {
                                    precision: 9,
                                    scale: 2,
                                },
                                PrimitiveLiteral::Int128(10000),
                            ),
                            Datum::new(
                                PrimitiveType::Decimal {
                                    precision: 9,
                                    scale: 2,
                                },
                                PrimitiveLiteral::Int128(10100),
                            ),
                        ]
                        .into_iter(),
                    ),
                )
                .and(Reference::new("col4").is_not_in(
                    vec![Datum::string("abcdefg"), Datum::string("abcdefgabc")].into_iter(),
                ))
                .and(
                    Reference::new("col5").is_not_in(
                        vec![
                            Datum::binary("abcdefg".as_bytes().to_vec()),
                            Datum::binary("abcdehij".as_bytes().to_vec()),
                        ]
                        .into_iter(),
                    ),
                )
                .and(
                    Reference::new("col6").is_not_in(
                        vec![
                            Datum::uuid(
                                Uuid::parse_str("00000000-0000-007b-0000-0000000001c8").unwrap(),
                            ),
                            Datum::uuid(
                                Uuid::parse_str("00000000-0000-01c8-0000-00000000007b").unwrap(),
                            ),
                        ]
                        .into_iter(),
                    ),
                )
                .bind(schema.clone(), false)
                .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "(((((pcol1 NOT IN (8, 7, 6)) AND (pcol2 NOT IN (8, 7, 6))) AND (pcol3 NOT IN (6, 2))) AND (pcol4 NOT IN (9, 4))) AND (pcol5 NOT IN (4, 6))) AND (pcol6 NOT IN (4, 6))".to_string());
    }

    #[tokio::test]
    async fn test_strict_projection_identity() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::optional(
                    1,
                    "col1",
                    Type::Primitive(PrimitiveType::Long),
                ))])
                .build()
                .unwrap(),
        );

        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(1)
                .add_partition_field("col1", "pcol1", Transform::Identity)
                .unwrap()
                .build()
                .unwrap(),
        );

        let mut strict_projection = StrictProjection::new(partition_spec.clone());

        // test not null
        let predicate = Reference::new("col1")
            .is_not_null()
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "pcol1 IS NOT NULL".to_string());

        // test is null
        let predicate = Reference::new("col1")
            .is_null()
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "pcol1 IS NULL".to_string());

        // test less
        let predicate = Reference::new("col1")
            .less_than(Datum::long(100))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "pcol1 < 100".to_string());

        // test less or eq
        let predicate = Reference::new("col1")
            .less_than_or_equal_to(Datum::long(100))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "pcol1 <= 100".to_string());

        // test greater
        let predicate = Reference::new("col1")
            .greater_than(Datum::long(100))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "pcol1 > 100".to_string());

        // test greater or eq
        let predicate = Reference::new("col1")
            .greater_than_or_equal_to(Datum::long(100))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "pcol1 >= 100".to_string());

        // test eq
        let predicate = Reference::new("col1")
            .equal_to(Datum::long(100))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "pcol1 = 100".to_string());

        // test not eq
        let predicate = Reference::new("col1")
            .not_equal_to(Datum::long(100))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "pcol1 != 100".to_string());

        // test in
        let predicate = Reference::new("col1")
            .is_in(vec![Datum::long(100), Datum::long(101)].into_iter())
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "pcol1 IN (101, 100)".to_string());

        // test not in
        let predicate = Reference::new("col1")
            .is_not_in(vec![Datum::long(100), Datum::long(101)].into_iter())
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "pcol1 NOT IN (101, 100)".to_string());
    }

    #[tokio::test]
    async fn test_strict_projection_truncate_lower_bound() {
        // int, long, decimal
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        1,
                        "col1",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::required(
                        2,
                        "col2",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::required(
                        3,
                        "col3",
                        Type::Primitive(PrimitiveType::Decimal {
                            precision: 9,
                            scale: 2,
                        }),
                    )),
                ])
                .build()
                .unwrap(),
        );

        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(1)
                .add_partition_field("col1", "pcol1", Transform::Truncate(10))
                .unwrap()
                .add_partition_field("col2", "pcol2", Transform::Truncate(10))
                .unwrap()
                .add_partition_field("col3", "pcol3", Transform::Truncate(10))
                .unwrap()
                .build()
                .unwrap(),
        );

        let mut strict_projection = StrictProjection::new(partition_spec.clone());

        // test less
        let predicate = Reference::new("col1")
            .less_than(Datum::int(100))
            .and(Reference::new("col2").less_than(Datum::long(100)))
            .and(Reference::new("col3").less_than(Datum::new(
                PrimitiveType::Decimal {
                    precision: 9,
                    scale: 2,
                },
                PrimitiveLiteral::Int128(10000),
            )))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((pcol1 < 100) AND (pcol2 < 100)) AND (pcol3 < 10000)".to_string()
        );

        // test less or eq
        let predicate = Reference::new("col1")
            .less_than_or_equal_to(Datum::int(100))
            .and(Reference::new("col2").less_than_or_equal_to(Datum::long(100)))
            .and(Reference::new("col3").less_than_or_equal_to(Datum::new(
                PrimitiveType::Decimal {
                    precision: 9,
                    scale: 2,
                },
                PrimitiveLiteral::Int128(10000),
            )))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((pcol1 < 100) AND (pcol2 < 100)) AND (pcol3 < 10000)".to_string()
        );

        // test greater
        let predicate = Reference::new("col1")
            .greater_than(Datum::int(100))
            .and(Reference::new("col2").greater_than(Datum::long(100)))
            .and(Reference::new("col3").greater_than(Datum::new(
                PrimitiveType::Decimal {
                    precision: 9,
                    scale: 2,
                },
                PrimitiveLiteral::Int128(10000),
            )))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((pcol1 > 100) AND (pcol2 > 100)) AND (pcol3 > 10000)".to_string()
        );

        // test greater or eq
        let predicate = Reference::new("col1")
            .greater_than_or_equal_to(Datum::int(100))
            .and(Reference::new("col2").greater_than_or_equal_to(Datum::long(100)))
            .and(Reference::new("col3").greater_than_or_equal_to(Datum::new(
                PrimitiveType::Decimal {
                    precision: 9,
                    scale: 2,
                },
                PrimitiveLiteral::Int128(10000),
            )))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((pcol1 > 90) AND (pcol2 > 90)) AND (pcol3 > 9990)".to_string()
        );

        // test not eq
        let predicate = Reference::new("col1")
            .not_equal_to(Datum::int(100))
            .and(Reference::new("col2").not_equal_to(Datum::long(100)))
            .and(Reference::new("col3").not_equal_to(Datum::new(
                PrimitiveType::Decimal {
                    precision: 9,
                    scale: 2,
                },
                PrimitiveLiteral::Int128(10000),
            )))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((pcol1 != 100) AND (pcol2 != 100)) AND (pcol3 != 10000)".to_string()
        );

        // test not in
        let predicate =
            Reference::new("col1")
                .is_not_in(vec![Datum::int(99), Datum::int(100), Datum::int(101)].into_iter())
                .and(Reference::new("col2").is_not_in(
                    vec![Datum::long(99), Datum::long(100), Datum::long(101)].into_iter(),
                ))
                .and(
                    Reference::new("col3").is_not_in(
                        vec![
                            Datum::new(
                                PrimitiveType::Decimal {
                                    precision: 9,
                                    scale: 2,
                                },
                                PrimitiveLiteral::Int128(9900),
                            ),
                            Datum::new(
                                PrimitiveType::Decimal {
                                    precision: 9,
                                    scale: 2,
                                },
                                PrimitiveLiteral::Int128(10000),
                            ),
                            Datum::new(
                                PrimitiveType::Decimal {
                                    precision: 9,
                                    scale: 2,
                                },
                                PrimitiveLiteral::Int128(10100),
                            ),
                        ]
                        .into_iter(),
                    ),
                )
                .bind(schema.clone(), false)
                .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((pcol1 NOT IN (100, 90)) AND (pcol2 NOT IN (100, 90))) AND (pcol3 NOT IN (10000, 10100, 9900))"
                .to_string()
        );

        // test in
        let predicate = Reference::new("col1")
            .is_in(vec![Datum::int(99), Datum::int(100), Datum::int(101)].into_iter())
            .and(
                Reference::new("col2")
                    .is_in(vec![Datum::long(99), Datum::long(100), Datum::long(101)].into_iter()),
            )
            .and(
                Reference::new("col3").is_in(
                    vec![
                        Datum::new(
                            PrimitiveType::Decimal {
                                precision: 9,
                                scale: 2,
                            },
                            PrimitiveLiteral::Int128(9900),
                        ),
                        Datum::new(
                            PrimitiveType::Decimal {
                                precision: 9,
                                scale: 2,
                            },
                            PrimitiveLiteral::Int128(10000),
                        ),
                        Datum::new(
                            PrimitiveType::Decimal {
                                precision: 9,
                                scale: 2,
                            },
                            PrimitiveLiteral::Int128(10100),
                        ),
                    ]
                    .into_iter(),
                ),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "FALSE".to_string());
    }

    #[tokio::test]
    async fn test_strict_projection_truncate_upper_bound() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        1,
                        "col1",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::required(
                        2,
                        "col2",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::required(
                        3,
                        "col3",
                        Type::Primitive(PrimitiveType::Decimal {
                            precision: 9,
                            scale: 2,
                        }),
                    )),
                ])
                .build()
                .unwrap(),
        );

        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(1)
                .add_partition_field("col1", "pcol1", Transform::Truncate(10))
                .unwrap()
                .add_partition_field("col2", "pcol2", Transform::Truncate(10))
                .unwrap()
                .add_partition_field("col3", "pcol3", Transform::Truncate(10))
                .unwrap()
                .build()
                .unwrap(),
        );

        let mut strict_projection = StrictProjection::new(partition_spec.clone());

        // test less
        let predicate = Reference::new("col1")
            .less_than(Datum::int(99))
            .and(Reference::new("col2").less_than(Datum::long(99)))
            .and(Reference::new("col3").less_than(Datum::new(
                PrimitiveType::Decimal {
                    precision: 9,
                    scale: 2,
                },
                PrimitiveLiteral::Int128(9999),
            )))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((pcol1 < 90) AND (pcol2 < 90)) AND (pcol3 < 9990)".to_string()
        );

        // test less or eq
        let predicate = Reference::new("col1")
            .less_than_or_equal_to(Datum::int(99))
            .and(Reference::new("col2").less_than_or_equal_to(Datum::long(99)))
            .and(Reference::new("col3").less_than_or_equal_to(Datum::new(
                PrimitiveType::Decimal {
                    precision: 9,
                    scale: 2,
                },
                PrimitiveLiteral::Int128(9999),
            )))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((pcol1 < 100) AND (pcol2 < 100)) AND (pcol3 < 10000)".to_string()
        );

        // test greater
        let predicate = Reference::new("col1")
            .greater_than(Datum::int(99))
            .and(Reference::new("col2").greater_than(Datum::long(99)))
            .and(Reference::new("col3").greater_than(Datum::new(
                PrimitiveType::Decimal {
                    precision: 9,
                    scale: 2,
                },
                PrimitiveLiteral::Int128(9999),
            )))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((pcol1 > 90) AND (pcol2 > 90)) AND (pcol3 > 9990)".to_string()
        );

        // test greater or eq
        let predicate = Reference::new("col1")
            .greater_than_or_equal_to(Datum::int(99))
            .and(Reference::new("col2").greater_than_or_equal_to(Datum::long(99)))
            .and(Reference::new("col3").greater_than_or_equal_to(Datum::new(
                PrimitiveType::Decimal {
                    precision: 9,
                    scale: 2,
                },
                PrimitiveLiteral::Int128(9999),
            )))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((pcol1 > 90) AND (pcol2 > 90)) AND (pcol3 > 9990)".to_string()
        );

        // test not eq
        let predicate = Reference::new("col1")
            .not_equal_to(Datum::int(99))
            .and(Reference::new("col2").not_equal_to(Datum::long(99)))
            .and(Reference::new("col3").not_equal_to(Datum::new(
                PrimitiveType::Decimal {
                    precision: 9,
                    scale: 2,
                },
                PrimitiveLiteral::Int128(9999),
            )))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((pcol1 != 90) AND (pcol2 != 90)) AND (pcol3 != 9990)".to_string()
        );

        // test not in
        let predicate =
            Reference::new("col1")
                .is_not_in(vec![Datum::int(99), Datum::int(100), Datum::int(98)].into_iter())
                .and(Reference::new("col2").is_not_in(
                    vec![Datum::long(99), Datum::long(100), Datum::long(98)].into_iter(),
                ))
                .and(
                    Reference::new("col3").is_not_in(
                        vec![
                            Datum::new(
                                PrimitiveType::Decimal {
                                    precision: 9,
                                    scale: 2,
                                },
                                PrimitiveLiteral::Int128(9999),
                            ),
                            Datum::new(
                                PrimitiveType::Decimal {
                                    precision: 9,
                                    scale: 2,
                                },
                                PrimitiveLiteral::Int128(9899),
                            ),
                            Datum::new(
                                PrimitiveType::Decimal {
                                    precision: 9,
                                    scale: 2,
                                },
                                PrimitiveLiteral::Int128(10099),
                            ),
                        ]
                        .into_iter(),
                    ),
                )
                .bind(schema.clone(), false)
                .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(
            result.to_string(),
            "((pcol1 NOT IN (100, 90)) AND (pcol2 NOT IN (100, 90))) AND (pcol3 NOT IN (9890, 9990, 10090))"
                .to_string()
        );

        // test in
        let predicate = Reference::new("col1")
            .is_in(vec![Datum::int(99), Datum::int(100), Datum::int(98)].into_iter())
            .and(
                Reference::new("col2")
                    .is_in(vec![Datum::long(99), Datum::long(100), Datum::long(98)].into_iter()),
            )
            .and(
                Reference::new("col3").is_in(
                    vec![
                        Datum::new(
                            PrimitiveType::Decimal {
                                precision: 9,
                                scale: 2,
                            },
                            PrimitiveLiteral::Int128(9999),
                        ),
                        Datum::new(
                            PrimitiveType::Decimal {
                                precision: 9,
                                scale: 2,
                            },
                            PrimitiveLiteral::Int128(9899),
                        ),
                        Datum::new(
                            PrimitiveType::Decimal {
                                precision: 9,
                                scale: 2,
                            },
                            PrimitiveLiteral::Int128(10099),
                        ),
                    ]
                    .into_iter(),
                ),
            )
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "FALSE".to_string());
    }

    #[tokio::test]
    async fn test_strict_projection_truncate_string() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "col1",
                    Type::Primitive(PrimitiveType::String),
                ))])
                .build()
                .unwrap(),
        );

        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(1)
                .add_partition_field("col1", "pcol1", Transform::Truncate(5))
                .unwrap()
                .build()
                .unwrap(),
        );

        let mut strict_projection = StrictProjection::new(partition_spec.clone());

        // test less
        let predicate = Reference::new("col1")
            .less_than(Datum::string("abcdefg"))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "pcol1 < \"abcde\"".to_string());

        // test less or eq
        let predicate = Reference::new("col1")
            .less_than_or_equal_to(Datum::string("abcdefg"))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "pcol1 < \"abcde\"".to_string());

        // test greater
        let predicate = Reference::new("col1")
            .greater_than(Datum::string("abcdefg"))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "pcol1 > \"abcde\"".to_string());

        // test greater or eq
        let predicate = Reference::new("col1")
            .greater_than_or_equal_to(Datum::string("abcdefg"))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "pcol1 > \"abcde\"".to_string());

        // test not eq
        let predicate = Reference::new("col1")
            .not_equal_to(Datum::string("abcdefg"))
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "pcol1 != \"abcde\"".to_string());

        // test not in
        let predicate = Reference::new("col1")
            .is_not_in(vec![Datum::string("abcdefg"), Datum::string("abcdefgabc")].into_iter())
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "pcol1 NOT IN (\"abcde\")".to_string());

        // test in
        let predicate = Reference::new("col1")
            .is_in(vec![Datum::string("abcdefg"), Datum::string("abcdefgabc")].into_iter())
            .bind(schema.clone(), false)
            .unwrap();
        let result = strict_projection.strict_project(&predicate).unwrap();
        assert_eq!(result.to_string(), "FALSE".to_string());
    }

    // # TODO
    // test_strict_projection_truncate_binary
}
