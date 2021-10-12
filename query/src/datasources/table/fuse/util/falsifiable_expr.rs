//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

//! Falsifiable expressions from "Big Metadata: When Metadata is Big Data" http://vldb.org/pvldb/vol14/p3083-edara.pdf
//! Here is not a proper place for it, @xx, please consider move to ....

use common_planners::Extras;

/// when FalsifiableExpr evaluates to true, it is guaranteed
/// that no row in the block satisfies the condition
#[allow(dead_code)]
enum FalsifiableExpr {}

impl FalsifiableExpr {
    /// columnar metadata are not exposed as system table in fuse table
    /// we use [`BlockStats`] directly to evaluate the falsifiable expression
    #[allow(dead_code)]
    fn eval(
        &self,
        _block_stats: &super::statistic_helper::BlockStats,
    ) -> common_exception::Result<bool> {
        todo!()
    }
}

/// convert push_downs to Falsifiable Expression
/// Rules: (section 5.2 of http://vldb.org/pvldb/vol14/p3083-edara.pdf)
/// - Any arbitrary expression 𝑃 (𝑋), always has a falsifiable expression 𝐹𝐴𝐿𝑆E (loose)
/// - 𝑃𝑋 (𝑋) AND 𝑃𝑌 (𝑌) ~> 𝐹𝑋 (𝐶𝑋 ) OR 𝐹𝑌 (𝐶𝑌)
/// - 𝑃𝑋 (𝑋) OR 𝑃𝑌 (𝑌) ~> 𝐹𝑋 (𝐶𝑋) AND 𝐹𝑌 (𝐶𝑌)
/// - comp between var and constant
/// - ...
#[allow(dead_code)]
fn map(_push_downs: &Option<Extras>) -> common_exception::Result<FalsifiableExpr> {
    todo!()
}
