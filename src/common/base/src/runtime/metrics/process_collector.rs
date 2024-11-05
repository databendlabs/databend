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

use prometheus_client::collector::Collector;
use prometheus_client::encoding::EncodeMetric;
use prometheus_client::metrics::counter::ConstCounter;
use prometheus_client::metrics::gauge::ConstGauge;

#[derive(Debug)]
pub struct ProcessCollector {}

impl ProcessCollector {
    pub fn new() -> Box<Self> {
        Box::new(ProcessCollector {})
    }
}

impl Collector for ProcessCollector {
    fn encode(
        &self,
        mut encoder: prometheus_client::encoding::DescriptorEncoder,
    ) -> Result<(), std::fmt::Error> {
        let stat = match dump_process_stat() {
            Some(stat) => stat,
            None => return Ok(()),
        };

        let cpu_secs = ConstCounter::new(stat.cpu_secs);
        let cpu_secs_encoder = encoder.encode_descriptor(
            "process_cpu_seconds_total",
            "Total user and system CPU time spent in seconds.",
            None,
            cpu_secs.metric_type(),
        )?;
        cpu_secs.encode(cpu_secs_encoder)?;

        let open_fds = ConstGauge::new(stat.open_fds as f64);
        let open_fds_encoder = encoder.encode_descriptor(
            "process_open_fds",
            "Number of open file descriptors.",
            None,
            open_fds.metric_type(),
        )?;
        open_fds.encode(open_fds_encoder)?;

        let max_fds = ConstGauge::new(stat.max_fds as f64);
        let max_fds_encoder = encoder.encode_descriptor(
            "process_max_fds",
            "Maximum number of open file descriptors.",
            None,
            max_fds.metric_type(),
        )?;
        max_fds.encode(max_fds_encoder)?;

        let vsize = ConstGauge::new(stat.vsize as f64);
        let vsize_encoder = encoder.encode_descriptor(
            "process_virtual_memory_bytes",
            "Virtual memory size in bytes.",
            None,
            vsize.metric_type(),
        )?;
        vsize.encode(vsize_encoder)?;

        let rss = ConstGauge::new(stat.rss as f64);
        let rss_encoder = encoder.encode_descriptor(
            "process_resident_memory_bytes",
            "Resident memory size in bytes.",
            None,
            rss.metric_type(),
        )?;
        rss.encode(rss_encoder)?;

        let start_time = ConstGauge::new(stat.start_time as f64);
        let start_time_encoder = encoder.encode_descriptor(
            "process_start_time_seconds",
            "Start time of the process since unix epoch in seconds.",
            None,
            start_time.metric_type(),
        )?;
        start_time.encode(start_time_encoder)?;

        let threads_num = ConstGauge::new(stat.threads_num as f64);
        let threads_num_encoder = encoder.encode_descriptor(
            "process_threads",
            "Number of OS threads in the process.",
            None,
            threads_num.metric_type(),
        )?;
        threads_num.encode(threads_num_encoder)?;

        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct ProcessStat {
    pub cpu_secs: u64,
    pub open_fds: u64,
    pub max_fds: u64,
    pub vsize: u64,
    pub rss: u64,
    pub start_time: i64,
    pub threads_num: usize,
}

pub fn dump_process_stat() -> Option<ProcessStat> {
    #[cfg(target_os = "linux")]
    {
        dump_linux_process_stat()
    }

    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

#[cfg(target_os = "linux")]
fn dump_linux_process_stat() -> Option<ProcessStat> {
    let proc = match procfs::process::Process::myself() {
        Ok(p) => p,
        Err(_) => {
            return None;
        }
    };
    let stat = match proc.stat() {
        Ok(stat) => stat,
        Err(_) => {
            return None;
        }
    };

    // constants
    let clk_tck: i64 = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    let page_size: i64 = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };

    // fds
    let open_fds = proc.fd_count().unwrap_or(0) as u64;
    let max_fds = if let Ok(limits) = proc.limits() {
        match limits.max_open_files.soft_limit {
            procfs::process::LimitValue::Value(v) => v,
            procfs::process::LimitValue::Unlimited => 0,
        }
    } else {
        0
    };

    // memory
    let vsize = stat.vsize;
    let rss = stat.rss * (page_size as u64);

    // cpu time
    let cpu_secs = (stat.utime + stat.stime) / clk_tck as u64;

    // start time
    let start_time = stat.starttime as i64 * clk_tck;

    // threads
    let threads_num = stat.num_threads as usize;

    Some(ProcessStat {
        open_fds,
        max_fds,
        vsize,
        rss,
        cpu_secs,
        start_time,
        threads_num,
    })
}
