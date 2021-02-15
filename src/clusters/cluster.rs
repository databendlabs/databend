// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

use crate::configs::Config;
use crate::error::FuseQueryResult;

#[derive(Clone)]
pub struct ClusterMeta;

pub type ClusterRef = Arc<Cluster>;

pub struct Cluster {
    metas: Mutex<Vec<ClusterMeta>>,
}

impl Cluster {
    pub fn create(_cfg: Config) -> ClusterRef {
        Arc::new(Cluster {
            metas: Mutex::new(vec![]),
        })
    }

    pub fn empty() -> ClusterRef {
        Arc::new(Cluster {
            metas: Mutex::new(vec![]),
        })
    }

    pub fn get_clusters(&self) -> FuseQueryResult<Vec<ClusterMeta>> {
        let mut metas = vec![];

        for meta in self.metas.lock()?.iter() {
            metas.push(meta.clone());
        }
        Ok(metas)
    }
}
