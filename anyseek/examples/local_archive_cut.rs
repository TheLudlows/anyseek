//! Offline tool for capturing and restoring a retained local Record WAL window.
use anyseek::record::{LocalArchiveWindow, RecordPartition};
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
            let mut raw = Vec::new();
            std::fs::File::open(witness)?.take(4097).read_to_end(&mut raw)?;
            if raw.len() > 4096 {
                return Err("archive witness exceeds 4096 bytes".into());
            }
            let witness: LocalArchiveWindow = serde_json::from_slice(&raw)?;
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
        _ => return Err("usage: local_archive_cut capture SOURCE BACKUP ARCHIVE BUDGET | restore BACKUP ARCHIVE DESTINATION BUDGET CUT WITNESS".into()),
    }
    Ok(())
}
