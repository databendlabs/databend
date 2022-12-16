// Copyright 2021 Datafuse Labs.
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

use std::collections::HashSet;
use std::fs::File;
use std::io::Read;

// Returns None if not get
#[allow(unused_assignments)]
pub fn get_physical_core_count() -> Option<usize> {
    let mut s = String::new();
    if let Err(_e) = File::open("/proc/cpuinfo").and_then(|mut f| f.read_to_string(&mut s)) {
        return None;
    }

    macro_rules! add_core {
        ($core_ids_and_physical_ids:ident, $core_id:ident, $physical_id:ident, $cpu:ident) => {{
            if !$core_id.is_empty() && !$physical_id.is_empty() {
                $core_ids_and_physical_ids.insert(format!("{} {}", $core_id, $physical_id));
            } else if !$cpu.is_empty() {
                // On systems with only physical cores like raspberry, there is no "core id" or
                // "physical id" fields. So if one of them is missing, we simply use the "CPU"
                // info and count it as a physical core.
                $core_ids_and_physical_ids.insert($cpu.to_owned());
            }
            $core_id = "";
            $physical_id = "";
            $cpu = "";
        }};
    }

    let mut core_ids_and_physical_ids: HashSet<String> = HashSet::new();
    let mut core_id = "";
    let mut physical_id = "";
    let mut cpu = "";

    for line in s.lines() {
        if line.is_empty() {
            add_core!(core_ids_and_physical_ids, core_id, physical_id, cpu);
        } else if line.starts_with("processor") {
            cpu = line
                .splitn(2, ':')
                .last()
                .map(|x| x.trim())
                .unwrap_or_default();
        } else if line.starts_with("core id") {
            core_id = line
                .splitn(2, ':')
                .last()
                .map(|x| x.trim())
                .unwrap_or_default();
        } else if line.starts_with("physical id") {
            physical_id = line
                .splitn(2, ':')
                .last()
                .map(|x| x.trim())
                .unwrap_or_default();
        }
    }
    add_core!(core_ids_and_physical_ids, core_id, physical_id, cpu);

    Some(core_ids_and_physical_ids.len())
}
