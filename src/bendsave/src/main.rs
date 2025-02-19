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
use bendsave::backup;
use clap::Parser;
use clap::Subcommand;
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
        #[arg(short, long)]
        from: String,
        /// Backup destination
        #[arg(short, long)]
        to: String,
    },

    /// Restore a Databend cluster from a backup
    Restore {
        /// Backup manifest file path
        #[arg(short, long)]
        from: String,
        /// Target configuration file path
        #[arg(short, long)]
        to: String,
        /// Confirm restoration and perform it immediately
        #[arg(short, long, default_value_t = false)]
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
            // Implement backup functionality here
            println!("Backing up from {} to {}", from, to);
            backup(from, to).await?;
        }
        Commands::Restore { from, to, confirm } => {
            // Implement restore functionality here
            if *confirm {
                println!("Restoring from {} to {} with confirmation", from, to);
                // TODO: Add actual restore logic with confirmation
            } else {
                println!("Dry-run restore from {} to {}", from, to);
                // TODO: Add dry-run restore logic
            }
        }
    }

    Ok(())
}
