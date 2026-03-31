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
use std::sync::{Arc, RwLock};

use crate::expr::visitors::expression_evaluator::ExpressionEvaluator;
use crate::expr::visitors::inclusive_projection::InclusiveProjection;
use crate::expr::visitors::manifest_evaluator::ManifestEvaluator;
use crate::expr::{Bind, BoundPredicate};
use crate::spec::{Schema, TableMetadataRef};
use crate::{Error, ErrorKind, Result};

/// Manages the caching of [`BoundPredicate`] objects
/// for [`PartitionSpec`]s based on partition spec id.
#[derive(Debug)]
pub(crate) struct PartitionFilterCache(RwLock<HashMap<i32, Arc<BoundPredicate>>>);

impl PartitionFilterCache {
    /// Creates a new [`PartitionFilterCache`]
    /// with an empty internal HashMap.
    pub(crate) fn new() -> Self {
        Self(RwLock::new(HashMap::new()))
    }

    /// Retrieves a [`BoundPredicate`] from the cache
    /// or computes it if not present.
    pub(crate) fn get(
        &self,
        spec_id: i32,
        table_metadata: &TableMetadataRef,
        schema: &Schema,
        case_sensitive: bool,
        filter: BoundPredicate,
    ) -> Result<Arc<BoundPredicate>> {
        // we need a block here to ensure that the `read()` gets dropped before we hit the `write()`
        // below, otherwise we hit deadlock
        {
            let read = self.0.read().map_err(|_| {
                Error::new(
                    ErrorKind::Unexpected,
                    "PartitionFilterCache RwLock was poisoned",
                )
            })?;

            if read.contains_key(&spec_id) {
                return Ok(read.get(&spec_id).unwrap().clone());
            }
        }

        let partition_spec = table_metadata
            .partition_spec_by_id(spec_id)
            .ok_or(Error::new(
                ErrorKind::Unexpected,
                format!("Could not find partition spec for id {spec_id}"),
            ))?;

        let partition_type = partition_spec.partition_type(schema)?;
        let partition_fields = partition_type.fields().to_owned();
        let partition_schema = Arc::new(
            Schema::builder()
                .with_schema_id(partition_spec.spec_id())
                .with_fields(partition_fields)
                .build()?,
        );

        let mut inclusive_projection = InclusiveProjection::new(partition_spec.clone());

        let partition_filter = inclusive_projection
            .project(&filter)?
            .rewrite_not()
            .bind(partition_schema.clone(), case_sensitive)?;

        self.0
            .write()
            .map_err(|_| {
                Error::new(
                    ErrorKind::Unexpected,
                    "PartitionFilterCache RwLock was poisoned",
                )
            })?
            .insert(spec_id, Arc::new(partition_filter));

        let read = self.0.read().map_err(|_| {
            Error::new(
                ErrorKind::Unexpected,
                "PartitionFilterCache RwLock was poisoned",
            )
        })?;

        Ok(read.get(&spec_id).unwrap().clone())
    }
}

/// Manages the caching of [`ManifestEvaluator`] objects
/// for [`PartitionSpec`]s based on partition spec id.
#[derive(Debug)]
pub(crate) struct ManifestEvaluatorCache(RwLock<HashMap<i32, Arc<ManifestEvaluator>>>);

impl ManifestEvaluatorCache {
    /// Creates a new [`ManifestEvaluatorCache`]
    /// with an empty internal HashMap.
    pub(crate) fn new() -> Self {
        Self(RwLock::new(HashMap::new()))
    }

    /// Retrieves a [`ManifestEvaluator`] from the cache
    /// or computes it if not present.
    pub(crate) fn get(
        &self,
        spec_id: i32,
        partition_filter: Arc<BoundPredicate>,
    ) -> Arc<ManifestEvaluator> {
        // we need a block here to ensure that the `read()` gets dropped before we hit the `write()`
        // below, otherwise we hit deadlock
        {
            let read = self
                .0
                .read()
                .map_err(|_| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "ManifestEvaluatorCache RwLock was poisoned",
                    )
                })
                .unwrap();

            if read.contains_key(&spec_id) {
                return read.get(&spec_id).unwrap().clone();
            }
        }

        self.0
            .write()
            .map_err(|_| {
                Error::new(
                    ErrorKind::Unexpected,
                    "ManifestEvaluatorCache RwLock was poisoned",
                )
            })
            .unwrap()
            .insert(
                spec_id,
                Arc::new(ManifestEvaluator::builder(partition_filter.as_ref().clone()).build()),
            );

        let read = self
            .0
            .read()
            .map_err(|_| {
                Error::new(
                    ErrorKind::Unexpected,
                    "ManifestEvaluatorCache RwLock was poisoned",
                )
            })
            .unwrap();

        read.get(&spec_id).unwrap().clone()
    }
}

/// Manages the caching of [`ExpressionEvaluator`] objects
/// for [`PartitionSpec`]s based on partition spec id.
#[derive(Debug)]
pub(crate) struct ExpressionEvaluatorCache(RwLock<HashMap<i32, Arc<ExpressionEvaluator>>>);

impl ExpressionEvaluatorCache {
    /// Creates a new [`ExpressionEvaluatorCache`]
    /// with an empty internal HashMap.
    pub(crate) fn new() -> Self {
        Self(RwLock::new(HashMap::new()))
    }

    /// Retrieves a [`ExpressionEvaluator`] from the cache
    /// or computes it if not present.
    pub(crate) fn get(
        &self,
        spec_id: i32,
        partition_filter: &BoundPredicate,
    ) -> Result<Arc<ExpressionEvaluator>> {
        // we need a block here to ensure that the `read()` gets dropped before we hit the `write()`
        // below, otherwise we hit deadlock
        {
            let read = self.0.read().map_err(|_| {
                Error::new(
                    ErrorKind::Unexpected,
                    "PartitionFilterCache RwLock was poisoned",
                )
            })?;

            if read.contains_key(&spec_id) {
                return Ok(read.get(&spec_id).unwrap().clone());
            }
        }

        self.0
            .write()
            .map_err(|_| {
                Error::new(
                    ErrorKind::Unexpected,
                    "ManifestEvaluatorCache RwLock was poisoned",
                )
            })
            .unwrap()
            .insert(
                spec_id,
                Arc::new(ExpressionEvaluator::new(partition_filter.clone())),
            );

        let read = self
            .0
            .read()
            .map_err(|_| {
                Error::new(
                    ErrorKind::Unexpected,
                    "ManifestEvaluatorCache RwLock was poisoned",
                )
            })
            .unwrap();

        Ok(read.get(&spec_id).unwrap().clone())
    }
}
