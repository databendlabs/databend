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

use std::collections::BTreeMap;
use std::collections::HashMap;

use common_base::base::tokio::sync::mpsc;
use common_base::base::tokio::sync::mpsc::Receiver;
use common_base::base::tokio::sync::mpsc::Sender;
use common_catalog::plan::RuntimeFilterId;
use common_catalog::table_context::RuntimeFilter;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::Domain;
use common_expression::Evaluator;
use common_expression::FunctionContext;
use common_expression::RemoteExpr;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use parking_lot::Mutex;
use parking_lot::RwLock;

pub struct RuntimeFilterCollector {
    pub filters_tx: Mutex<Sender<HashMap<RuntimeFilterId, Domain>>>,
    pub filters_rx: Mutex<Receiver<HashMap<RuntimeFilterId, Domain>>>,
    pub domains: RwLock<HashMap<RuntimeFilterId, Domain>>,
}

impl RuntimeFilterCollector {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(100);
        Self {
            filters_tx: Mutex::new(tx),
            filters_rx: Mutex::new(rx),
            domains: Default::default(),
        }
    }
}

impl RuntimeFilter for RuntimeFilterCollector {
    fn collect(
        &self,
        exprs: &BTreeMap<RuntimeFilterId, RemoteExpr>,
        data: &DataBlock,
    ) -> Result<()> {
        for (id, remote_expr) in exprs.iter() {
            let expr = remote_expr.as_expr(&BUILTIN_FUNCTIONS);
            // expr represents equi condition in join build side
            // Such as: `select * from t1 inner join t2 on t1.a = t2.a + 1`
            // expr is `t2.a + 1`
            // First we need get expected values from data by expr
            let evaluator = Evaluator::new(data, FunctionContext::default(), &BUILTIN_FUNCTIONS);
            let column = evaluator
                .run(&expr)?
                .convert_to_full_column(expr.data_type(), data.num_rows());

            let mut domains = self.domains.write();
            if let Some(domain) = domains.get_mut(id) {
                *domain = Domain::merge(domain, &column.domain());
            } else {
                domains.insert(id.clone(), column.domain());
            }
        }
        Ok(())
    }

    fn send(&self) -> Result<()> {
        let mut domains = self.domains.write();
        let send_domains = domains.clone();
        domains.clear();
        let tx = self.filters_tx.lock();
        if tx.try_send(send_domains).is_err() {
            return Err(ErrorCode::Internal("send runtime filters fail"));
        }
        Ok(())
    }

    fn recv(&self) -> Result<HashMap<RuntimeFilterId, Domain>> {
        let mut rx = self.filters_rx.lock();
        loop {
            let res = rx.try_recv();
            if let Ok(res) = res {
                return Ok(res);
            }
        }
    }
}

impl Default for RuntimeFilterCollector {
    fn default() -> Self {
        Self::new()
    }
}
