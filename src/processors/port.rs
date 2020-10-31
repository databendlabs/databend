// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::cell::RefCell;
use std::fmt;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};

use crate::datablocks::DataBlock;
use crate::error::Result;
use crate::processors::GraphNode;

pub struct Port {
    node: RefCell<GraphNode>,
    tx: Sender<Option<DataBlock>>,
    rx: Receiver<Option<DataBlock>>,
}

pub type OutputPort = Port;
pub type InputPort = Port;

impl Port {
    pub fn new(node: GraphNode) -> Self {
        let (tx, rx) = mpsc::channel();
        Port {
            node: RefCell::new(node),
            tx,
            rx,
        }
    }

    pub fn id(&self) -> u32 {
        self.node.borrow().id()
    }

    pub fn edges(&self) -> Vec<u32> {
        self.node.borrow().edges()
    }

    pub fn push(&self, v: Option<DataBlock>) -> Result<()> {
        self.tx.send(v)?;
        Ok(())
    }

    pub fn pull(&self) -> Result<Option<DataBlock>> {
        let v = self.rx.recv()?;
        Ok(v)
    }
}

impl fmt::Debug for Port {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?}->{:?}",
            self.node.borrow().id(),
            self.node.borrow().edges()
        )
    }
}

pub fn connect(output: &OutputPort, input: &InputPort) -> Result<()> {
    output
        .node
        .borrow_mut()
        .add_edge(input.node.borrow().id())?;
    input.node.borrow_mut().add_edge(output.node.borrow().id())
}
