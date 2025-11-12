//! Tests for latency scenarios.

use std::time::{Duration, Instant};

use anyhow::Result;

use crate::test;
use crate::util::system::{CPUSet, System};
use crate::workloads::context::Context;
use crate::workloads::spinner::Spinner;
use crate::workloads::benchmark::converge;
use crate::util::system::CPUMask;
use crate::util::stats::Distribution;

use crate::process;

/// Test that ensures basic fairness for affinitized vs. non-affinitized tasks.
///
/// We create one task per logical CPU, and affinitize each one. Then we create one
/// floating task, and ensure that everyone gets approximate fairness in this case.
fn fairness() -> Result<()> {
    let mut ctx = Context::create()?;
    let mut proc_handles = vec!();

    // Affinitized tasks.
    for core in System::load()?.cores().iter() {
        for hyperthread in core.hyperthreads().iter() {
            let mask = CPUMask::new(hyperthread);
            proc_handles.push(process!(
                &mut ctx,
                None,
                (mask),
                move |mut get_iters| {
                    mask.run(move || {
                        let spinner = Spinner::new(Instant::now());
                        loop {
                            spinner.spin(Duration::from_millis(get_iters() as u64));
                        }
                    })
                }
            ));
        }
    }

    // Floating tasks.
    for _ in 0..System::load()?.logical_cpus() {
        proc_handles.push(process!(
            &mut ctx,
            None,
            (),
            move |mut get_iters| {
                let spinner = Spinner::new(Instant::now());
                loop {
                    spinner.spin(Duration::from_millis(get_iters() as u64));
                }
            }
        ));
    }

    let metric = |iters| {
        ctx.start(iters);
        ctx.wait()?;
        let mut d = Distribution::<Duration>::new();
        let times: Vec<f64> = proc_handles.iter().map(|handle| {
            match handle.stats() {
                Ok(stats) => {
                    d.add(stats.total_time);
                    stats.total_time.as_secs_f64()
                },
                Err(_) => 0.0,
            }
        }).collect();
        let mean = times.iter().sum::<f64>() / times.len() as f64;
        let estimates = d.estimates();
        eprintln!("{}", estimates.visualize(None));
        // Return the p10 of the runtimes as a fraction of the average runtime.
        if let Some(p10) = estimates.percentile(0.1) {
            Ok(p10.as_secs_f64()/mean)
        } else {
            Ok(0.0) // Not enough samples.
        }
    };

    // If the p10 of the runtimes is 60% of the average runtime, then we have
    // achieved **some** level of fairness. If things were starved out completely,
    // then we would actually expect the p10 to be near zero.
    let target = 0.60;
    let final_value = converge(
        Some(Duration::from_secs_f64(5.0)),
        Some(Duration::from_secs_f64(10.0)),
        Some(target),
        metric,
    )?;
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

test!("fairness", fairness);
