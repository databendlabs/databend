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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::optimizer::optimizers::rule::agg_rules::RuleHierarchicalGroupingSetsToUnion;
    use crate::optimizer::OptimizerContext;

    #[test]
    fn test_analyze_grouping_hierarchy() {
        // This is a basic test to ensure the hierarchical analysis works
        // In a real implementation, we would need proper test infrastructure
        
        let ctx = Arc::new(OptimizerContext::new(
            Default::default(),
            Default::default(),
        ));
        
        let rule = RuleHierarchicalGroupingSetsToUnion::new(ctx);
        
        // Test case: GROUPING SETS ((a,b,c), (a,b), (a), ())
        // Should create hierarchical relationships: (a,b,c) -> (a,b) -> (a) -> ()
        let grouping_sets = vec![
            vec![1, 2, 3], // (a,b,c)
            vec![1, 2],    // (a,b)
            vec![1],       // (a)
            vec![],        // ()
        ];
        
        let levels = rule.analyze_grouping_hierarchy(&grouping_sets);
        
        // Should have 4 levels
        assert_eq!(levels.len(), 4);
        
        // Find the most detailed level (a,b,c)
        let most_detailed = levels.iter().find(|l| l.level == 3).unwrap();
        assert_eq!(most_detailed.columns, vec![1, 2, 3]);
        
        // Check that hierarchical relationships are identified
        let has_hierarchy = levels.iter().any(|level| level.parent.is_some());
        assert!(has_hierarchy, "Should detect hierarchical relationships");
    }

    #[test]
    fn test_non_hierarchical_grouping_sets() {
        let ctx = Arc::new(OptimizerContext::new(
            Default::default(), 
            Default::default(),
        ));
        
        let rule = RuleHierarchicalGroupingSetsToUnion::new(ctx);
        
        // Test case: GROUPING SETS ((a,b), (c,d)) - no hierarchy
        let grouping_sets = vec![
            vec![1, 2], // (a,b)
            vec![3, 4], // (c,d)
        ];
        
        let levels = rule.analyze_grouping_hierarchy(&grouping_sets);
        
        // Should have 2 levels
        assert_eq!(levels.len(), 2);
        
        // No hierarchical relationships should be found
        let has_hierarchy = levels.iter().any(|level| level.parent.is_some());
        assert!(!has_hierarchy, "Should not detect hierarchical relationships");
    }
}