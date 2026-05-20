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

use std::fs::File;
use std::io;
use std::io::BufReader;
use std::io::BufWriter;

use databend_common_meta_process::filter_tenant::TenantFilterOptions;
use databend_common_meta_process::filter_tenant::filter_tenant_dump;

use crate::args::FilterTenantArgs;

pub fn filter_tenant(args: &FilterTenantArgs) -> anyhow::Result<()> {
    let options = TenantFilterOptions {
        tenant: args.tenant.clone(),
    };

    let report = match (args.input.as_str(), args.output.as_str()) {
        ("", "") => {
            let stdin = io::stdin();
            let stdout = io::stdout();
            filter_tenant_dump(stdin.lock(), stdout.lock(), options)?
        }
        ("", output) => {
            let stdin = io::stdin();
            let output = File::create(output)?;
            filter_tenant_dump(stdin.lock(), BufWriter::new(output), options)?
        }
        (input, "") => {
            let input = File::open(input)?;
            let stdout = io::stdout();
            filter_tenant_dump(BufReader::new(input), stdout.lock(), options)?
        }
        (input, output) => {
            let input = File::open(input)?;
            let output = File::create(output)?;
            filter_tenant_dump(BufReader::new(input), BufWriter::new(output), options)?
        }
    };

    eprintln!(
        "filter-tenant: total={}, header={}, raft_log_skipped={}, state_machine={}, kept={}, dropped={}",
        report.total_lines,
        report.header_lines,
        report.raft_log_lines,
        report.state_machine_lines,
        report.kept_state_machine_lines,
        report.dropped_state_machine_lines
    );

    Ok(())
}
