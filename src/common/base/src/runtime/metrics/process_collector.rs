use std::time;

use prometheus_client::collector::Collector;

struct ProcessCollector {}

impl Collector for ProcessCollector {
    fn encode(
        &self,
        encoder: prometheus_client::encoding::DescriptorEncoder,
    ) -> Result<(), std::fmt::Error> {
        todo!()
    }
}

#[derive(Clone, Default)]
struct ProcessStat {
    cpu_total: u64,
    open_fds: u64,
    max_fds: u64,
    vsize: u64,
    rss: u64,
    start_time: i64,
    threads_num: usize,
}

static CLK_TCK: LazyLock<i64> =
    LazyLock::new(|| unsafe { libc::sysconf(libc::_SC_CLK_TCK) }.into());

static PAGESIZE: LazyLock<i64> =
    LazyLock::new(|| unsafe { libc::sysconf(libc::_SC_PAGESIZE) }.into());

fn dump_process_stat() -> Option<ProcessStat> {
    let p = match procfs::process::Process::myself() {
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

    // fds
    let open_fds = proc.fd_count().unwrap_or(0) as u64;
    let max_fds = match p.limits().unwrap_or_default().max_open_files.soft_limit {
        procfs::process::LimitValue::Value(v) => v,
        procfs::process::LimitValue::Unlimited => 0,
    };

    // memory
    let vsize = stat.vsize;
    let rss = stat.rss * *PAGESIZE;

    // cpu time
    let cpu_total = (stat.utime + stat.stime) / *CLK_TCK as u64;

    // start time
    let start_time = stat.starttime as i64 * *CLK_TCK;

    // threads
    let threads_num = stat.num_threads;

    Self {
        open_fds,
        max_fds,
        vsize,
        rss,
        cpu_total,
        start_time,
        threads_num,
    }
}
