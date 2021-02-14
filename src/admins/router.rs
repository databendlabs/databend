// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::configs::Config;
use warp::Filter;

pub struct Router {
    cfg: Config,
}

impl Router {
    pub fn create(cfg: Config) -> Self {
        Router { cfg }
    }

    pub fn router(
        &self,
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        super::v1::config::config_handler(self.cfg.clone())
            .or(super::v1::hello::hello_handler(self.cfg.clone()))
    }
}
