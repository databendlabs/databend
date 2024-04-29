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

use poem::http::StatusCode;
use poem::web::Json;
use poem::IntoResponse;
use serde::Deserialize;
use serde::Serialize;
use sysinfo::System;

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug)]
pub struct CpuInfo {
    vender: String,
    brand: String,
    name: String,
    cores: usize,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug)]
pub struct SystemInfo {
    cpu: CpuInfo,
    kernel_version: String,
    os_version: String,
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn system_handler() -> poem::Result<impl IntoResponse> {
    let mut sys = System::new();
    sys.refresh_cpu();
    let cpus = sys.cpus();
    let cpu = cpus.first().ok_or_else(|| {
        poem::Error::from_string("failed to get cpu info.", StatusCode::INTERNAL_SERVER_ERROR)
    })?;
    let vender = cpu.vendor_id().to_string();
    let brand = cpu.brand().to_string();
    let name = cpu.name().to_string();
    let sysinfo = SystemInfo {
        cpu: CpuInfo {
            vender,
            brand,
            name,
            cores: cpus.len(),
        },
        kernel_version: System::kernel_version().unwrap_or_default(),
        os_version: System::os_version().unwrap_or_default(),
    };
    Ok(Json(sysinfo))
}
