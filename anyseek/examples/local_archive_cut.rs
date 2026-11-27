//! Offline tool for capturing and restoring a retained local Record WAL window.
use anyseek::record::{LocalArchivePart, LocalArchiveWindow, RecordPartition};
use serde::de::DeserializeOwned;
use std::{error::Error, io::Read, path::Path};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.split_first() {
        Some((mode, [source, backup, archive, budget])) if mode == "capture" => {
            let witness = RecordPartition::capture_local_archive(
                Path::new(source),
                Path::new(backup),
                Path::new(archive),
                budget.parse()?,
            )
            .await?;
            println!("{}", serde_json::to_string(&witness)?);
        }
        Some((mode, [backup, archive, destination, budget, cut, witness])) if mode == "restore" => {
            let witness: LocalArchiveWindow = read_json(witness, 4096)?;
            RecordPartition::restore_local_archive(
                Path::new(backup),
                Path::new(archive),
                Path::new(destination),
                budget.parse()?,
                cut.parse()?,
                &witness,
            )
            .await?;
        }
        Some((mode, [backup, destination, budget, cut, manifest])) if mode == "restore-chain" => {
            let parts: Vec<LocalArchivePart> = read_json(manifest, 256 * 1024)?;
            RecordPartition::restore_local_archive_chain(
                Path::new(backup), &parts, Path::new(destination),
                budget.parse()?, cut.parse()?,
            ).await?;
        }
        _ => return Err("usage: local_archive_cut capture SOURCE BACKUP ARCHIVE BUDGET | restore BACKUP ARCHIVE DESTINATION BUDGET CUT WITNESS | restore-chain BACKUP DESTINATION BUDGET CUT MANIFEST".into()),
    }
    Ok(())
}

fn read_json<T: DeserializeOwned>(path: &str, limit: u64) -> Result<T, Box<dyn Error>> {
    let mut raw = Vec::new();
    std::fs::File::open(path)?
        .take(limit + 1)
        .read_to_end(&mut raw)?;
    if raw.len() as u64 > limit {
        return Err("archive manifest exceeds its byte limit".into());
    }
    Ok(serde_json::from_slice(&raw)?)
}
