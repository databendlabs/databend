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

pub(crate) struct InclusiveProjection {
    partition_spec: PartitionSpecRef,
    cached_parts: HashMap<i32, Vec<PartitionField>>,
}

impl InclusiveProjection {
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

    pub(crate) fn project(&mut self, predicate: &BoundPredicate) -> crate::Result<Predicate> {
        visit(self, predicate)
    }

    fn get_parts(
        &mut self,
        reference: &BoundReference,
        predicate: &BoundPredicate,
    ) -> Result<Predicate, Error> {
        let field_id = reference.field().id;

        // This could be made a bit neater if `try_reduce` ever becomes stable
        self.get_parts_for_field_id(field_id)
            .iter()
            .try_fold(Predicate::AlwaysTrue, |res, part| {
                Ok(
                    if let Some(pred_for_part) = part.transform.project(&part.name, predicate)? {
                        if res == Predicate::AlwaysTrue {
                            pred_for_part
                        } else {
                            res.and(pred_for_part)
                        }
                    } else {
                        res
                    },
                )
            })
    }
}

impl BoundPredicateVisitor for InclusiveProjection {
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
            "InclusiveProjection should not be performed against Predicates that contain a Not operator. Ensure that \"Rewrite Not\" gets applied to the originating Predicate before binding it.",
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

    use crate::expr::visitors::inclusive_projection::InclusiveProjection;
    use crate::expr::{Bind, Predicate, Reference};
    use crate::spec::{
        Datum, NestedField, PartitionSpec, PrimitiveType, Schema, Transform, Type,
        UnboundPartitionField,
    };

    fn build_test_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "a",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::required(
                    2,
                    "date",
                    Type::Primitive(PrimitiveType::Date),
                )),
                Arc::new(NestedField::required(
                    3,
                    "name",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .unwrap()
    }

    #[test]
    fn test_inclusive_projection_logic_ops() {
        let schema = build_test_schema();
        let arc_schema = Arc::new(schema);

        let partition_spec = PartitionSpec::builder(arc_schema.clone())
            .with_spec_id(1)
            .build()
            .unwrap();

        let arc_partition_spec = Arc::new(partition_spec);

        // this predicate contains only logic operators,
        // AlwaysTrue, and AlwaysFalse.
        let unbound_predicate = Predicate::AlwaysTrue
            .and(Predicate::AlwaysFalse)
            .or(Predicate::AlwaysTrue);

        let bound_predicate = unbound_predicate.bind(arc_schema.clone(), false).unwrap();

        // applying InclusiveProjection to bound_predicate
        // should result in the same Predicate as the original
        // `unbound_predicate`, since `InclusiveProjection`
        // simply unbinds logic ops, AlwaysTrue, and AlwaysFalse.
        let mut inclusive_projection = InclusiveProjection::new(arc_partition_spec.clone());
        let result = inclusive_projection.project(&bound_predicate).unwrap();

        assert_eq!(result.to_string(), "TRUE".to_string())
    }

    #[test]
    fn test_inclusive_projection_identity_transform() {
        let schema = build_test_schema();
        let arc_schema = Arc::new(schema);

        let partition_spec = PartitionSpec::builder(arc_schema.clone())
            .with_spec_id(1)
            .add_unbound_field(
                UnboundPartitionField::builder()
                    .source_id(1)
                    .name("a".to_string())
                    .field_id(1)
                    .transform(Transform::Identity)
                    .build(),
            )
            .unwrap()
            .build()
            .unwrap();

        let arc_partition_spec = Arc::new(partition_spec);

        let unbound_predicate = Reference::new("a").less_than(Datum::int(10));

        let bound_predicate = unbound_predicate.bind(arc_schema.clone(), false).unwrap();

        // applying InclusiveProjection to bound_predicate
        // should result in the same Predicate as the original
        // `unbound_predicate`, since we have just a single partition field,
        // and it has an Identity transform
        let mut inclusive_projection = InclusiveProjection::new(arc_partition_spec);
        let result = inclusive_projection.project(&bound_predicate).unwrap();

        let expected = "a < 10".to_string();

        assert_eq!(result.to_string(), expected)
    }

    #[test]
    fn test_inclusive_projection_date_year_transform() {
        let schema = build_test_schema();
        let arc_schema = Arc::new(schema);

        let partition_spec = PartitionSpec::builder(arc_schema.clone())
            .with_spec_id(1)
            .add_unbound_fields(vec![UnboundPartitionField {
                source_id: 2,
                name: "year".to_string(),
                field_id: Some(1000),
                transform: Transform::Year,
            }])
            .unwrap()
            .build()
            .unwrap();

        let arc_partition_spec = Arc::new(partition_spec);

        let unbound_predicate =
            Reference::new("date").less_than(Datum::date_from_str("2024-01-01").unwrap());

        let bound_predicate = unbound_predicate.bind(arc_schema.clone(), false).unwrap();

        // applying InclusiveProjection to bound_predicate
        // should result in a predicate that correctly handles
        // year, month and date
        let mut inclusive_projection = InclusiveProjection::new(arc_partition_spec);
        let result = inclusive_projection.project(&bound_predicate).unwrap();

        let expected = "year <= 53".to_string();

        assert_eq!(result.to_string(), expected);
    }

    #[test]
    fn test_inclusive_projection_date_month_transform() {
        let schema = build_test_schema();
        let arc_schema = Arc::new(schema);

        let partition_spec = PartitionSpec::builder(arc_schema.clone())
            .with_spec_id(1)
            .add_unbound_fields(vec![UnboundPartitionField {
                source_id: 2,
                name: "month".to_string(),
                field_id: Some(1000),
                transform: Transform::Month,
            }])
            .unwrap()
            .build()
            .unwrap();

        let arc_partition_spec = Arc::new(partition_spec);

        let unbound_predicate =
            Reference::new("date").less_than(Datum::date_from_str("2024-01-01").unwrap());

        let bound_predicate = unbound_predicate.bind(arc_schema.clone(), false).unwrap();

        // applying InclusiveProjection to bound_predicate
        // should result in a predicate that correctly handles
        // year, month and date
        let mut inclusive_projection = InclusiveProjection::new(arc_partition_spec);
        let result = inclusive_projection.project(&bound_predicate).unwrap();

        let expected = "month <= 647".to_string();

        assert_eq!(result.to_string(), expected);
    }

    #[test]
    fn test_inclusive_projection_date_day_transform() {
        let schema = build_test_schema();
        let arc_schema = Arc::new(schema);

        let partition_spec = PartitionSpec::builder(arc_schema.clone())
            .with_spec_id(1)
            .add_unbound_fields(vec![UnboundPartitionField {
                source_id: 2,
                name: "day".to_string(),
                field_id: Some(1000),
                transform: Transform::Day,
            }])
            .unwrap()
            .build()
            .unwrap();

        let arc_partition_spec = Arc::new(partition_spec);

        let unbound_predicate =
            Reference::new("date").less_than(Datum::date_from_str("2024-01-01").unwrap());

        let bound_predicate = unbound_predicate.bind(arc_schema.clone(), false).unwrap();

        // applying InclusiveProjection to bound_predicate
        // should result in a predicate that correctly handles
        // year, month and date
        let mut inclusive_projection = InclusiveProjection::new(arc_partition_spec);
        let result = inclusive_projection.project(&bound_predicate).unwrap();

        let expected = "day <= 2023-12-31".to_string();

        assert_eq!(result.to_string(), expected);
    }

    #[test]
    fn test_inclusive_projection_truncate_transform() {
        let schema = build_test_schema();
        let arc_schema = Arc::new(schema);

        let partition_spec = PartitionSpec::builder(arc_schema.clone())
            .with_spec_id(1)
            .add_unbound_field(
                UnboundPartitionField::builder()
                    .source_id(3)
                    .name("name_truncate".to_string())
                    .field_id(3)
                    .transform(Transform::Truncate(4))
                    .build(),
            )
            .unwrap()
            .build()
            .unwrap();

        let arc_partition_spec = Arc::new(partition_spec);

        let unbound_predicate = Reference::new("name").starts_with(Datum::string("Testy McTest"));

        let bound_predicate = unbound_predicate.bind(arc_schema.clone(), false).unwrap();

        // applying InclusiveProjection to bound_predicate
        // should result in the 'name STARTS WITH "Testy McTest"'
        // predicate being transformed to 'name_truncate STARTS WITH "Test"',
        // since a `Truncate(4)` partition will map values of
        // name that start with "Testy McTest" into a partition
        // for values of name that start with the first four letters
        // of that, ie "Test".
        let mut inclusive_projection = InclusiveProjection::new(arc_partition_spec);
        let result = inclusive_projection.project(&bound_predicate).unwrap();

        let expected = "name_truncate STARTS WITH \"Test\"".to_string();

        assert_eq!(result.to_string(), expected)
    }

    #[test]
    fn test_inclusive_projection_bucket_transform() {
        let schema = build_test_schema();
        let arc_schema = Arc::new(schema);

        let partition_spec = PartitionSpec::builder(arc_schema.clone())
            .with_spec_id(1)
            .add_unbound_field(
                UnboundPartitionField::builder()
                    .source_id(1)
                    .name("a_bucket[7]".to_string())
                    .field_id(1)
                    .transform(Transform::Bucket(7))
                    .build(),
            )
            .unwrap()
            .build()
            .unwrap();

        let arc_partition_spec = Arc::new(partition_spec);

        let unbound_predicate = Reference::new("a").equal_to(Datum::int(10));

        let bound_predicate = unbound_predicate.bind(arc_schema.clone(), false).unwrap();

        // applying InclusiveProjection to bound_predicate
        // should result in the "a = 10" predicate being
        // transformed into "a = 2", since 10 gets bucketed
        // to 2 with a Bucket(7) partition
        let mut inclusive_projection = InclusiveProjection::new(arc_partition_spec);
        let result = inclusive_projection.project(&bound_predicate).unwrap();

        let expected = "a_bucket[7] = 2".to_string();

        assert_eq!(result.to_string(), expected)
    }
}
