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

use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check::common_super_type;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::binder::wrap_cast;
use crate::optimizer::ir::expr::SExpr;
use crate::optimizer::ir::property::Distribution;
use crate::optimizer::ir::property::PhysicalProperty;
use crate::optimizer::ir::property::RelExpr;
use crate::optimizer::ir::property::RequiredProperty;
use crate::plans::Exchange;
use crate::plans::RelOperator;

/// Enforcer is a trait that can enforce the physical property
pub trait Enforcer: std::fmt::Debug + Send + Sync {
    /// Check if necessary to enforce the physical property
    fn check_enforce(&self, input_prop: &PhysicalProperty) -> bool;

    /// Enforce the physical property
    fn enforce(&self) -> Result<RelOperator>;
}

/// Enforcer for distribution properties
#[derive(Debug)]
pub struct DistributionEnforcer(Distribution);

impl DistributionEnforcer {
    /// Create a new DistributionEnforcer
    pub fn new(distribution: Distribution) -> Self {
        Self(distribution)
    }

    /// Get the inner distribution
    pub fn into_inner(self) -> Distribution {
        self.0
    }
}

impl From<Distribution> for DistributionEnforcer {
    fn from(distribution: Distribution) -> Self {
        DistributionEnforcer(distribution)
    }
}

impl Enforcer for DistributionEnforcer {
    fn check_enforce(&self, input_prop: &PhysicalProperty) -> bool {
        !self.0.satisfied_by(&input_prop.distribution)
    }

    fn enforce(&self) -> Result<RelOperator> {
        match &self.0 {
            Distribution::Serial => Ok(Exchange::Merge.into()),
            Distribution::Broadcast => Ok(Exchange::Broadcast.into()),
            Distribution::NodeToNodeHash(hash_keys) => {
                Ok(Exchange::NodeToNodeHash(hash_keys.clone()).into())
            }
            Distribution::Random | Distribution::Any => Err(ErrorCode::Internal(
                "Cannot enforce random or any distribution",
            )),
        }
    }
}

/// Responsible for enforcing physical properties on expressions
pub struct PropertyEnforcer {
    ctx: Arc<dyn TableContext>,
}

impl PropertyEnforcer {
    /// Create a new PropertyEnforcer with the given context
    pub fn new(ctx: Arc<dyn TableContext>) -> Self {
        Self { ctx }
    }

    /// Require and enforce physical property from a physical `SExpr`
    #[stacksafe::stacksafe]
    pub fn require_property(&self, required: &RequiredProperty, s_expr: &SExpr) -> Result<SExpr> {
        // First, we will require the child SExpr with input `RequiredProperty`.
        let children = s_expr
            .children()
            .map(|child| Ok(Arc::new(self.require_property(required, child)?)))
            .collect::<Result<Vec<_>>>()?;

        let s_expr = SExpr::create(Arc::new(s_expr.plan().clone()), children, None, None, None);
        let rel_expr = RelExpr::with_s_expr(&s_expr);

        // Prepare containers for child properties
        let mut children = Vec::with_capacity(s_expr.arity());
        let mut required_properties = Vec::with_capacity(s_expr.arity());
        let mut physical_properties = Vec::with_capacity(s_expr.arity());

        // Compute required properties for each child
        for index in 0..s_expr.arity() {
            required_properties.push(rel_expr.compute_required_prop_child(
                self.ctx.clone(),
                index,
                required,
            )?);
            physical_properties.push(rel_expr.derive_physical_prop_child(index)?);
        }

        let plan = s_expr.plan.as_ref().clone();

        if let RelOperator::Join(_) = &plan {
            let (probe_required_property, build_required_property) =
                required_properties.split_at_mut(1);
            if let Distribution::NodeToNodeHash(probe_keys) =
                &mut probe_required_property[0].distribution
                && let Distribution::NodeToNodeHash(build_keys) =
                    &mut build_required_property[0].distribution
            {
                let cast_rules = &BUILTIN_FUNCTIONS.get_auto_cast_rules("eq");
                for (probe_key, build_key) in probe_keys.iter_mut().zip(build_keys.iter_mut()) {
                    let probe_key_data_type = probe_key.data_type()?;
                    let build_key_data_type = build_key.data_type()?;
                    let common_data_type = common_super_type(
                        probe_key_data_type.clone(),
                        build_key_data_type.clone(),
                        cast_rules,
                    )
                    .ok_or_else(|| {
                        ErrorCode::IllegalDataType(format!(
                            "Cannot find common type for probe key {:?} and build key {:?}",
                            &probe_key, &build_key
                        ))
                    })?;
                    for (key, data_type) in [
                        (probe_key, probe_key_data_type),
                        (build_key, build_key_data_type),
                    ] {
                        if data_type != common_data_type {
                            *key = wrap_cast(key, &common_data_type);
                        }
                    }
                }
            }
        }

        // Enforce properties on children if needed
        for index in 0..s_expr.arity() {
            if required_properties[index].satisfied_by(&physical_properties[index]) {
                children.push(Arc::new(s_expr.child(index)?.clone()));
                continue;
            }

            // Enforce required property on the child
            let enforced_child =
                self.enforce_property(s_expr.child(index)?, &required_properties[index])?;
            children.push(Arc::new(enforced_child));
        }

        Ok(SExpr::create(Arc::new(plan), children, None, None, None))
    }

    /// Try to enforce physical property from a physical `SExpr`
    pub fn enforce_property(&self, s_expr: &SExpr, required: &RequiredProperty) -> Result<SExpr> {
        // Enforce distribution if needed
        let s_expr = if !required.distribution.satisfied_by(&Distribution::Any) {
            self.enforce_distribution(&required.distribution, s_expr)?
        } else {
            s_expr.clone()
        };

        Ok(s_expr)
    }

    /// Enforce distribution on an expression
    pub fn enforce_distribution(
        &self,
        distribution: &Distribution,
        s_expr: &SExpr,
    ) -> Result<SExpr> {
        let physical_prop = RelExpr::with_s_expr(s_expr).derive_physical_prop()?;

        // Check if enforcement is needed
        if distribution.satisfied_by(&physical_prop.distribution) {
            return Ok(s_expr.clone());
        }

        // Create the appropriate enforcer
        let enforcer = DistributionEnforcer::from(distribution.clone());

        // Apply the enforcer
        let exchange_op = enforcer.enforce()?;
        let result = SExpr::create_unary(Arc::new(exchange_op), Arc::new(s_expr.clone()));

        Ok(result)
    }
}
