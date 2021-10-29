// Copyright 2020 Datafuse Labs.
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

use async_trait::async_trait;
use dyn_clone::DynClone;
use clap::ArgMatches;

use crate::cmds::Writer;
use crate::error::Result;
use std::sync::Arc;

#[async_trait]
pub trait Command: DynClone + Send + Sync {
    fn name(&self) -> &str;

    fn clap(&self) -> clap::App<'static>;

    fn about(&self) -> &str;

    fn is(&self, s: &str) -> bool;

    fn subcommands(&self) -> Vec<Arc<dyn Command>>;

    async fn exec_matches(&self, writer: &mut Writer, args: Option<&ArgMatches>) -> Result<()>;

    async fn exec(&self, writer: &mut Writer, args: String) -> Result<()> {
        match self.clap().try_get_matches_from(args.split(' ')) {
            Ok(matches) => {
                return self.exec_matches(writer, Some(&matches)).await;
            }
            Err(err) => {
                println!("Cannot get subcommand matches: {}", err);
            }
        }

        Ok(())
    }
}

dyn_clone::clone_trait_object!(Command);
