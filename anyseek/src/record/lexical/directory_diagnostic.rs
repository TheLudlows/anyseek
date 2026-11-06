use super::*;
use std::time::{Duration, Instant};

// Run explicitly, with no other build/test workload. This is a directory I/O
// experiment, not an HTTP throughput or tail-latency acceptance test.
#[test]
#[ignore = "explicit filesystem contention diagnostic"]
fn directory_io_contention() {
    let workers: usize = std::env::var("ANYSEEK_IO_WORKERS").unwrap_or("4".into()).parse().unwrap();
    let rounds = 200;
    let root = tempfile::tempdir().unwrap();
    let directories: Vec<_> = (0..workers).map(|i| {
        let path = root.path().join(i.to_string());
        std::fs::create_dir(&path).unwrap();
        BudgetDirectory::with_scope(&path, 64 * 1024 * 1024, 1024, 0, None).unwrap()
    }).collect();
    let barrier = std::sync::Barrier::new(workers);
    let wall = Instant::now();
    let samples = std::thread::scope(|threads| {
        let handles: Vec<_> = directories.iter().map(|directory| {
            let barrier = &barrier;
            threads.spawn(move || {
                let mut samples = Vec::new();
                let payload = vec![42; 4096];
                barrier.wait();
                for i in 0..rounds {
                    let path = PathBuf::from(format!("{i}.data"));
                    // Same accounting, admission and gate as atomic_write; time
                    // the gate separately from atomic publication and dir fsync.
                    directory.budget.charge(&path, payload.len() as u64).unwrap();
                    let start = Instant::now();
                    let guard = directory.budget.io().unwrap();
                    let acquired = Instant::now();
                    directory.inner.atomic_write(&path, &payload).unwrap();
                    let synced = Instant::now();
                    directory.inner.sync_directory().unwrap();
                    let end = Instant::now();
                    drop(guard);
                    samples.push((acquired-start, end-acquired, synced-acquired, end-synced));
                }
                samples
            })
        }).collect();
        handles.into_iter().flat_map(|h| h.join().unwrap()).collect::<Vec<_>>()
    });
    let elapsed = wall.elapsed();
    for directory in &directories {
        for i in 0..rounds {
            assert_eq!(directory.atomic_read(Path::new(&format!("{i}.data"))).unwrap(), vec![42; 4096]);
        }
    }
    fn stats(mut values: Vec<Duration>) -> (f64,f64,f64) {
        values.sort();
        (values.iter().map(Duration::as_secs_f64).sum::<f64>()*1000.,
         values[values.len()/2].as_secs_f64()*1000.,
         values[(values.len()*99/100).min(values.len()-1)].as_secs_f64()*1000.)
    }
    println!("workers={workers} operations={} wall_ms={:.3}", samples.len(), elapsed.as_secs_f64()*1000.);
    for (label, values) in [
        ("wait", samples.iter().map(|s|s.0).collect()),
        ("hold", samples.iter().map(|s|s.1).collect()),
        ("atomic_write_including_file_sync", samples.iter().map(|s|s.2).collect()),
        ("directory_sync", samples.iter().map(|s|s.3).collect()),
    ] {
        let (sum,p50,p99)=stats(values);
        println!("{label} sum_ms={sum:.3} p50_ms={p50:.6} p99_ms={p99:.6}");
    }
}
