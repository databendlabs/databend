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

use std::str::FromStr;
use std::time::Duration;

use common_base::Runtime;
use common_tracing::tracing;
use lazy_static::lazy_static;

pub const CONF_STORE_RT_TH_NUM: &str = "STORE_RT_TH_NUM";
pub const CONF_STORE_SYNC_CALL_TIMEOUT_SEC: &str = "STORE_SYNC_CALL_TIMEOUT_SEC";

lazy_static! {
    pub static ref STORE_RUNTIME: common_base::Runtime = build_rt();
    pub static ref STORE_SYNC_CALL_TIMEOUT: Option<Duration> = get_sync_call_timeout();
}

fn build_rt() -> Runtime {
    let conf_th_num = std::env::var(CONF_STORE_RT_TH_NUM).ok();
    let th_num = if let Some(num_str) = conf_th_num {
        let num = usize::from_str(&num_str);
        match num {
            Ok(v) => Some(v),
            Err(pe) => {
                tracing::info!(
                    "invalid configuration of store runtime thread number [{}], ignored. {}",
                    &num_str,
                    pe
                );
                None
            }
        }
    } else {
        None
    };

    if let Some(num) = th_num {
        tracing::info!("bring up store api runtime with {} thread", num);
        common_base::Runtime::with_worker_threads(num)
            .expect("FATAL, initialize store runtime failure")
    } else {
        tracing::info!("bring up store api runtime with default worker threads");
        common_base::Runtime::with_default_worker_threads()
            .expect("FATAL, initialize store runtime failure")
    }
}

fn get_sync_call_timeout() -> Option<Duration> {
    let conf_to = std::env::var(CONF_STORE_SYNC_CALL_TIMEOUT_SEC).ok();
    let th_num = if let Some(timeout_str) = conf_to {
        let num = u64::from_str(&timeout_str);
        match num {
            Ok(v) => Some(v),
            Err(pe) => {
                tracing::info!(
                    "invalid configuration of store rpc timeout (in sec) [{}], ignored. {}",
                    &timeout_str,
                    pe
                );
                None
            }
        }
    } else {
        None
    };

    if let Some(num) = th_num {
        tracing::info!("store sync rpc timeout set to {} sec.", num);
        Some(Duration::from_secs(num))
    } else {
        None
    }
}
