// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::cell::RefCell;
use std::fmt;
use std::sync::{
    mpsc,
    mpsc::{Receiver, Sender},
};

use crate::datablocks::DataBlock;
use crate::error::Result;
use crate::processors::GraphNode;

#[derive(Clone)]
pub enum PortStatus {
    None,
    /// Check if port has data.
    HasData,
}

impl PortStatus {
    pub fn update(&mut self, state: PortStatus) {
        *self = state;
    }
}

impl fmt::Debug for PortStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PortStatus::None => write!(f, "None"),
            PortStatus::HasData => write!(f, "HasData"),
        }
    }
}

pub struct PortChannel {
    tx: Sender<Option<DataBlock>>,
    rx: Receiver<Option<DataBlock>>,
    state: RefCell<PortStatus>,
}

impl PortChannel {
    pub fn create() -> PortChannel {
        let (tx, rx) = mpsc::channel();
        PortChannel {
            tx,
            rx,
            state: RefCell::new(PortStatus::None),
        }
    }

    pub fn push(&self, data: Option<DataBlock>) -> Result<()> {
        self.tx.send(data)?;
        self.state.borrow_mut().update(PortStatus::HasData);
        Ok(())
    }

    pub fn pull(&self) -> Result<Option<DataBlock>> {
        let v = self.rx.recv()?;
        self.state.borrow_mut().update(PortStatus::None);
        Ok(v)
    }

    pub fn get_state(&self) -> PortStatus {
        self.state.borrow().clone()
    }
}

pub struct Port {
    chan: PortChannel,
    node: RefCell<GraphNode>,
}

pub type OutputPort = Port;
pub type InputPort = Port;

impl Port {
    pub fn new(node: GraphNode) -> Self {
        Port {
            chan: PortChannel::create(),
            node: RefCell::new(node),
        }
    }

    pub fn id(&self) -> u32 {
        self.node.borrow().id()
    }

    pub fn edges(&self) -> Vec<u32> {
        self.node.borrow().edges()
    }

    pub fn push(&self, v: Option<DataBlock>) -> Result<()> {
        self.chan.push(v)
    }

    pub fn pull(&self) -> Result<Option<DataBlock>> {
        self.chan.pull()
    }

    pub fn state(&self) -> PortStatus {
        self.chan.get_state()
    }
}

impl fmt::Debug for Port {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?}(state: {:?})->{:?}",
            self.node.borrow().id(),
            self.chan.get_state(),
            self.node.borrow().edges(),
        )
    }
}

/// connect Outputport to InputPort.
pub fn connect(output: &OutputPort, input: &InputPort) -> Result<()> {
    let input_id = input.node.borrow().id();
    let output_id = output.node.borrow().id();

    output.node.borrow_mut().add_edge(input_id)?;
    input.node.borrow_mut().add_edge(output_id)
}
