use std::fmt::Display;
use std::fmt::Formatter;

#[derive(Clone, serde::Serialize, serde::Deserialize, Debug)]
pub enum ProfileStatisticsName {
    /// The time spent to process in nanoseconds
    CpuTime,
    /// The time spent to wait in nanoseconds, usually used to
    /// measure the time spent on waiting for I/O
    WaitTime,
    ExchangeRows,
    ExchangeBytes,
    OutputRows,
    OutputBytes,
}

impl Display for ProfileStatisticsName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ProfileStatisticsName::CpuTime => write!(f, "CpuTime"),
            ProfileStatisticsName::WaitTime => write!(f, "WaitTime"),
            ProfileStatisticsName::ExchangeRows => write!(f, "ExchangeRows"),
            ProfileStatisticsName::ExchangeBytes => write!(f, "ExchangeBytes"),
            ProfileStatisticsName::OutputRows => write!(f, "OutputRows"),
            ProfileStatisticsName::OutputBytes => write!(f, "OutputBytes"),
        }
    }
}

impl From<usize> for ProfileStatisticsName {
    fn from(value: usize) -> Self {
        match value {
            _ if value == ProfileStatisticsName::CpuTime as usize => ProfileStatisticsName::CpuTime,
            _ if value == ProfileStatisticsName::WaitTime as usize => ProfileStatisticsName::WaitTime,
            _ if value == ProfileStatisticsName::ExchangeRows as usize => ProfileStatisticsName::ExchangeRows,
            _ if value == ProfileStatisticsName::ExchangeBytes as usize => ProfileStatisticsName::ExchangeBytes,
            _ if value == ProfileStatisticsName::OutputRows as usize => ProfileStatisticsName::OutputRows,
            _ if value == ProfileStatisticsName::OutputBytes as usize => ProfileStatisticsName::OutputBytes,
            _ => panic!("logical error"),
        }
    }
}

#[derive(Clone)]
pub struct ProfileDesc {
    name: ProfileStatisticsName,
    only_dev: bool,
    desc: &'static str,
    display_name: &'static str,
}

pub static PROFILES_DESC: [ProfileDesc; std::mem::variant_count::<ProfileStatisticsName>()] = [
    ProfileDesc {
        name: ProfileStatisticsName::CpuTime,
        only_dev: false,
        display_name: "cpu_time",
        desc: "",
    },
    ProfileDesc {
        name: ProfileStatisticsName::WaitTime,
        only_dev: false,
        display_name: "wait_time",
        desc: "",
    },
    ProfileDesc {
        name: ProfileStatisticsName::ExchangeRows,
        only_dev: false,
        display_name: "wait_time",
        desc: "",
    },
    ProfileDesc {
        name: ProfileStatisticsName::ExchangeBytes,
        only_dev: false,
        display_name: "wait_time",
        desc: "",
    },
    ProfileDesc {
        name: ProfileStatisticsName::OutputRows,
        only_dev: false,
        display_name: "wait_time",
        desc: "",
    },
    ProfileDesc {
        name: ProfileStatisticsName::OutputBytes,
        only_dev: false,
        display_name: "wait_time",
        desc: "",
    },
];
