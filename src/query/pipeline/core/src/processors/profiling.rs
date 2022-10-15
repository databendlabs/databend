use std::any::Any;

use serde::Serialize;

pub trait ExtraInfo {
    fn display_data(&self) -> String;
}

pub trait ProfileInfo {
    fn processor_name(&self) -> String;

    fn process_rows(&self) -> usize;

    fn process_bytes(&self) -> usize;

    fn extra_info(&self) -> Option<Box<dyn ExtraInfo>>;
}

pub type ProfileInfoPtr = Box<dyn ProfileInfo>;
