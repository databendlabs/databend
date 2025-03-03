// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use anyhow::Result;
use clap::Parser;
use clap::Subcommand;
use databend_bendsave::backup;
use databend_bendsave::restore;
use logforth::append;
use logforth::filter::EnvFilter;
use logforth::Dispatch;
use logforth::Logger;

#[derive(Parser)]
#[command(name = "bendsave")]
#[command(about = "Databend backup and restore tool", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create backups of cluster data and metadata
    Backup {
        /// Configuration file path
        #[arg(long)]
        from: String,
        /// Backup destination
        #[arg(long)]
        to: String,
    },

    /// Restore a Databend cluster from a backup
    Restore {
        /// Backup manifest file path
        #[arg(long)]
        from: String,
        /// The target checkpoint to restore
        #[arg(long)]
        checkpoint: String,
        /// Target configuration file path of databend query
        #[arg(long)]
        to_query: String,
        /// Target configuration file path of databend meta
        #[arg(long)]
        to_meta: String,
        /// Confirm restoration and perform it immediately
        #[arg(long, default_value_t = false)]
        confirm: bool,
    },
    // /// List all backups in the specified location
    // List {
    //     /// Backup location
    //     #[arg(short, long)]
    //     location: String,
    // },
    // /// Manage backup retention policies
    // Vacuum,
}

#[tokio::main]
async fn main() -> Result<()> {
    Logger::new()
        .dispatch(
            Dispatch::new()
                .filter(EnvFilter::from_default_env())
                .append(append::Stderr::default()),
        )
        .apply()?;

    let cli = Cli::parse();

    match &cli.command {
        Commands::Backup { from, to } => {
            println!("Backing up from {} to {}", from, to);
            backup(from, to).await?;
        }
        Commands::Restore {
            from,
            checkpoint,
            to_query,
            to_meta,
            confirm,
        } => {
            if *confirm {
                println!(
                    "Restoring from {} at checkpoint {} to query {} and meta {} with confirmation",
                    from, checkpoint, to_query, to_meta
                );
                restore(from, checkpoint, to_query, to_meta).await?;
            } else {
                println!(
                    "Dry-run restore from {} at checkout {}  query {} and meta {}",
                    from, checkpoint, to_query, to_meta
                );
            }
        }
    }

    Ok(())
}
