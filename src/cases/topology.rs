//! Tests for system topology behavior.

use std::collections::HashMap;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;

use crate::test;
use crate::util::system::System;
use crate::workloads::context::Context;
use crate::workloads::semaphore::Semaphore;
use crate::workloads::spinner::Spinner;

use crate::converge;
use crate::process;

/// Test that verifies the scheduler spreads threads across physical cores.
///
/// This test creates a spinner for each physical core in the system, all initially
/// pinned to the same core. It then verifies that the scheduler eventually spreads
/// these threads across different physical cores, rather than just different logical
/// cores (hyperthreads).
fn spread_out() -> Result<()> {
    let mut ctx = Context::create()?;
    let system = System::load()?;

    let mut logical_to_physical = HashMap::new();
    for core in system.cores() {
        for hyperthread in core.hyperthreads() {
            logical_to_physical.insert(hyperthread.id(), core.id());
        }
    }

    let cores = system.cores();
    let mut spinners = Vec::new();
    let mut handles = Vec::new();
    let first_core = &cores[0];

    // Create a baseline instant for measuring CPU migrations
    let baseline = Instant::now();

    // Create a spinner for each process. They will all start on core[0].
    for _ in cores {
        let spinner = ctx.allocate(Spinner::new(baseline))?;
        spinners.push(spinner.clone());
        let proc = process!(&mut ctx, None, (), move |mut get_iters| {
            loop {
                spinner.spin(Duration::from_millis(get_iters() as u64));
            }
        });
        handles.push(proc);
    }

    // Define our metric: the percentage of physical cores that are covered by
    // our spinners after each execution (each one spun for N milliseconds).
    let metric = move |iters| {
        // Pin all processes to first_core
        for handle in &handles {
            handle.set_affinity(first_core)?;
        }

        // Wait for all processes to migrate to first_core before starting timer
        let first_core_id = first_core.hyperthreads()[0].id() as u32;
        let migration_timeout = Duration::from_millis(100);
        let migration_start = Instant::now();
        loop {
            let all_on_first_core = spinners.iter().all(|s| s.last_cpu() == first_core_id);
            if all_on_first_core {
                break;
            }
            if migration_start.elapsed() > migration_timeout {
                // Couldn't confirm all on first_core, continue anyway
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }

        // Record T_start (when the spread-out begins)
        let t_start = Instant::now();
        let t_start_nanos = t_start.duration_since(baseline).as_nanos() as u64;

        // Unpin all processes (allow them to spread out)
        for handle in &handles {
            handle.clear_affinity()?;
        }

        // Now start the workload
        ctx.start(iters);

        let mut counts = HashMap::new();
        let mut migrations = HashMap::new();
        let mut max_time_to_migrate: i64 = -1;
        let mut avg_time_to_migrate: i64 = -1;
        let mut reached_full_spread = false;

        // Reset the migration stats.
        for handle in handles.iter() {
            let s = handle.stats()?;
            *migrations.entry(handle.pid()).or_insert(0) = s.nr_migrations;
        }

        // The iters will be milliseconds, so every 10ms wake up and check the
        // last core that each spinner used. This will be cumulative.
        for _ in 0..iters / 10 {
            thread::sleep(Duration::from_millis(10));
            for spinner in &spinners {
                let cpu_id = spinner.last_cpu();
                if let Some(&physical_id) = logical_to_physical.get(&(cpu_id as i32)) {
                    *counts.entry(physical_id).or_insert(0) += 1;
                }
            }

            // Check if we've reached 100% spread (ratio = 1.0)
            let ratio = counts.len() as f64 / cores.len() as f64;
            if !reached_full_spread && ratio >= 1.0 {
                reached_full_spread = true;
                // Collect all migration times
                let migration_times: Vec<u64> = spinners.iter()
                    .map(|s| s.last_cpu_change_nanos())
                    .filter(|&t| t >= t_start_nanos)
                    .collect();

                if !migration_times.is_empty() {
                    // Find the MAX (LAST thread to migrate)
                    let max_migration_nanos = *migration_times.iter().max().unwrap();
                    max_time_to_migrate = (max_migration_nanos - t_start_nanos) as i64;

                    // Calculate the AVERAGE migration time
                    let sum: u64 = migration_times.iter()
                        .map(|&t| t - t_start_nanos)
                        .sum();
                    avg_time_to_migrate = (sum / migration_times.len() as u64) as i64;
                }
            }
        }

        ctx.wait()?;

        // Print the migration metrics
        println!("MaxTimeToMigrate: {} ns", max_time_to_migrate);
        println!("AvgTimeToMigrate: {} ns", avg_time_to_migrate);

        let mut delta_migrations = 0;
        for handle in handles.iter() {
            let s = handle.stats()?;
            delta_migrations += s.nr_migrations - *migrations.entry(handle.pid()).or_insert(0);
        }
        // Calculate the ratio of physical cores covered to total physical cores. But
        // we only return this value if there were no observed migrations.
        if delta_migrations > 0 {
            Ok(0.0)
        } else {
            let ratio = counts.len() as f64 / cores.len() as f64;
            Ok(ratio)
        }
    };

    let target = 0.95; // 95% of cores used.
    let final_value = converge!((2.0, 30.0), target, metric);
    if final_value < target {
        Err(anyhow::anyhow!(
            "Failed to achieve target: got {:.2}, expected {:.2}",
            final_value,
            target
        ))
    } else {
        Ok(())
    }
}

test!("spread_out", spread_out);

/// Test that coming together for related processes.
fn come_together() -> Result<()> {
    let mut ctx = Context::create()?;
    let system = System::load()?;
    let complexes = system.complexes();
    let cores = system.cores().len();

    let mut logical_to_physical = HashMap::new();
    for node in system.nodes() {
        for complex in node.complexes() {
            for core in complex.cores() {
                for hyperthread in core.hyperthreads() {
                    logical_to_physical.insert(hyperthread.id(), complex.id());
                }
            }
        }
    }

    // This will be a very, very busy system. For each process, we will create
    // N threads (one for each logical core), and they will all try to spin. We
    // will then measure the number of spinners for which we can observe across
    // multiple CCXs.
    let mut spinners = Vec::new();
    let baseline = Instant::now();
    for _ in 0..complexes {
        let mut proc_spinners = Vec::new();
        for _ in 0..cores {
            let spinner = ctx.allocate(Spinner::new(baseline))?;
            proc_spinners.push(spinner);
        }
        let wakeup = ctx.allocate(Semaphore::<0, 0>::new(cores as u32))?;
        process!(&mut ctx, None, (proc_spinners), move |mut get_iters| {
            // Start `cores` different threads, each spinning on
            // proc_spinning[i] independently.
            std::thread::scope(|s| {
                for spinner in proc_spinners.iter() {
                    let spinner_clone = spinner.clone();
                    s.spawn(move || loop {
                        spinner_clone.spin(Duration::from_millis(1));
                    });
                }
                loop {
                    let iters = get_iters();
                    wakeup.produce(iters, iters, None);
                }
            });
            Ok(())
        });
        spinners.push(proc_spinners);
    }

    // Define our metric: the percentage of physical cores that are covered by
    // our spinners after each execution (each one spun for N milliseconds).
    //
    // Note that we tolerant multiple processes on the same CCX, but we don't
    // tolerate multiple spinners spanning multiple CCXs.
    let metric = move |iters| {
        ctx.start(iters);
        let mut mismatches = 0;
        let mut total = 0;
        // See above; same logic applies here.
        for _ in 0..iters / 10 {
            thread::sleep(Duration::from_millis(10));
            for spinner_set in &spinners {
                let mut complex = None;
                for spinner in spinner_set {
                    let cpu = spinner.last_cpu() as i32;
                    let local_complex = logical_to_physical[&cpu];
                    if complex.is_none() {
                        complex = Some(local_complex);
                    } else if complex.unwrap() != local_complex {
                        mismatches += 1;
                    }
                    total += 1;
                }
            }
        }
        ctx.wait()?;
        Ok(1.0 - (mismatches as f64 / total as f64))
    };

    let target = 0.95; // 95% of cores used.
    let final_value = converge!((1.0, 30.0), target, metric);
    if final_value < target {
        Err(anyhow::anyhow!(
            "Failed to achieve target: got {:.2}, expected {:.2}",
            final_value,
            target
        ))
    } else {
        Ok(())
    }
}

fn needs_numa_or_complexes() -> Result<()> {
    let system = System::load()?;
    if system.nodes().len() > 1 || system.nodes()[0].complexes().len() > 1 {
        Ok(())
    } else {
        Err(anyhow::anyhow!("test requires multiple nodes or complexes"))
    }
}

test!("come_together", come_together, needs_numa_or_complexes);
