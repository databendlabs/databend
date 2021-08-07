// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufReader;
use std::path::Path;

use crate::cmds::Config;
use crate::error::Result;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Status {
    path: String,
    pub version: String,
}

impl Status {
    pub fn read(conf: Config) -> Result<Self> {
        let status_path = format!("{}/.status.json", conf.datafuse_dir);

        if !Path::new(status_path.as_str()).exists() {
            // Create.
            let file = File::create(status_path.as_str())?;
            let status = Status {
                path: status_path.clone(),
                version: "".to_string(),
            };
            serde_json::to_writer(&file, &status)?;
        }

        let file = File::open(status_path)?;
        let reader = BufReader::new(file);
        let status: Status = serde_json::from_reader(reader)?;
        Ok(status)
    }

    pub fn write(&self) -> Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(self.path.clone())?;
        println!("{:?}", self);
        serde_json::to_writer(&file, self)?;
        Ok(())
    }
}
