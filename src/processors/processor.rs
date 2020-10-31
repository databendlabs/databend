// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::processors::{InputPort, OutputPort};

pub trait IProcessor: fmt::Debug {
    fn id(&self) -> u32;
    fn input_port(&self) -> &InputPort;
    fn output_port(&self) -> &OutputPort;
    fn direct_edges(&self) -> Vec<u32>;
    fn back_edges(&self) -> Vec<u32>;
    fn work(&self, processors: Arc<Processors>) -> Result<()>;
}

#[derive(Default)]
pub struct Processors {
    map: HashMap<u32, usize>,
    list: Vec<Box<dyn IProcessor>>,
}

impl Processors {
    pub fn add(&mut self, node: Box<dyn IProcessor>) -> Result<()> {
        let id = node.id();
        if self.map.contains_key(&id) {
            return Err(Error::Internal(format!(
                "Processor[{}] already in the processors list",
                node.id()
            )));
        }

        for back_id in node.back_edges() {
            if !self.map.contains_key(&back_id) {
                return Err(Error::Internal(format!(
                    "Processor[{}] InputPort [{}] not found in the processors list",
                    id, back_id
                )));
            }
        }
        self.list.push(node);
        self.map.insert(id, self.list.len() - 1);
        Ok(())
    }

    pub fn processors(&self) -> &Vec<Box<dyn IProcessor>> {
        &self.list
    }

    pub fn get_processor(&self, id: u32) -> Result<&dyn IProcessor> {
        if self.list.is_empty() {
            return Err(Error::Internal("Processor list is empty".to_string()));
        }

        let idx = self.map.get(&id).unwrap();
        Ok(self.list[*idx].borrow())
    }

    pub fn last_processor(&self) -> Result<&dyn IProcessor> {
        if self.list.is_empty() {
            return Err(Error::Internal("Processor list is empty".to_string()));
        }

        let size = self.list.len() - 1;
        Ok(self.list[size].borrow())
    }
}

impl fmt::Debug for Processors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f)?;
        for proc in self.list.iter() {
            write!(f, "{:?}", proc)?;
        }
        write!(f, "")
    }
}
