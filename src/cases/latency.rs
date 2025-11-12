//! Tests for latency scenarios.

use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;

use crate::test;
use crate::util::stats::Distribution;
use crate::util::system::{CPUMask, CPUSet, System};
use crate::workloads::benchmark::converge;
use crate::workloads::context::Context;
use crate::workloads::semaphore::Semaphore;
use crate::workloads::spinner::Spinner;

use crate::process;

/// Test that verifies the scheduler favors threads with lower expected execution.
///
/// This test creates a two processes that are fighting for a single one: one has two
/// threads that each spin for 10ms, and then wake the other, while the other has two
/// threads that spin for only 10us.
///
/// Without any kind of adaptive priority, this has the tendency to favor the hogs.
fn adaptive_priority() -> Result<()> {
    let mut ctx = Context::create()?;
    let mask = CPUMask::new(
        System::load()?
            .cores()
            .first()
            .unwrap()
            .hyperthreads()
            .first()
            .unwrap(),
    );

    let slow_sem = ctx.allocate(Semaphore::<2, 1024>::new(1))?;
    let slow_sem_ret = ctx.allocate(Semaphore::<2, 1024>::new(1))?;
    process!(
        &mut ctx,
        None,
        (mask, slow_sem, slow_sem_ret),
        move |mut get_iters| {
            thread::scope(|s| {
                let mask_copy = mask.clone();
                let slow_sem_copy = slow_sem.clone();
                let slow_sem_ret_copy = slow_sem_ret.clone();
                s.spawn(move || {
                    let spinner = Spinner::new(Instant::now());
                    mask_copy.run(move || {
                        loop {
                            slow_sem_copy.consume(1, 1, None);
                            // Spin for a full 10ms, consuming CPU.
                            spinner.spin(Duration::from_millis(10));
                            slow_sem_ret_copy.produce(1, 1, None);
                        }
                    })
                });
                mask.run(move || loop {
                    let iters = get_iters();
                    for _ in 0..iters {
                        slow_sem.produce(1, 1, None);
                        slow_sem_ret.consume(1, 1, None);
                    }
                })
            })
        }
    );
    let fast_sem = ctx.allocate(Semaphore::<2, 1024>::new(1))?;
    let fast_sem_ret = ctx.allocate(Semaphore::<2, 1024>::new(1))?;
    process!(
        &mut ctx,
        None,
        (mask, fast_sem, fast_sem_ret),
        move |mut get_iters| {
            thread::scope(|s| {
                let mask_copy = mask.clone();
                let fast_sem_copy = fast_sem.clone();
                let fast_sem_ret_copy = fast_sem_ret.clone();
                s.spawn(move || {
                    let spinner = Spinner::new(Instant::now());
                    mask_copy.run(move || {
                        loop {
                            fast_sem_copy.consume(1, 1, None);
                            // Still take a millisecond, but yield after 100us.
                            spinner.spin(Duration::from_nanos(100_000));
                            thread::sleep(Duration::from_nanos(10_000_000 - 100_000));
                            fast_sem_ret_copy.produce(1, 1, None);
                        }
                    })
                });
                mask.run(move || loop {
                    let iters = get_iters();
                    for _ in 0..iters {
                        fast_sem.produce(1, 1, None);
                        fast_sem_ret.consume(1, 1, None);
                    }
                })
            })
        }
    );

    let metric = move |iters| {
        ctx.start(iters);
        ctx.wait()?;
        // See if the fast semaphore has a lower p90 than the slow semaphore,
        // which would indicate that in general it has a tighter deadline. Note
        // that we only collect the one way semaphore, not the return.
        let mut d_fast = Distribution::<Duration>::default();
        fast_sem.collect_wake_stats(&mut d_fast);
        let mut d_slow = Distribution::<Duration>::default();
        slow_sem.collect_wake_stats(&mut d_slow);
        let fast_est = d_fast.estimates();
        let slow_est = d_slow.estimates();
        eprintln!("Fast semaphore wake estimates:");
        eprintln!("{}", fast_est.visualize(None));
        eprintln!("Slow semaphore wake estimates:");
        eprintln!("{}", slow_est.visualize(None));
        let p90_fast = fast_est
            .percentile(0.9)
            .ok_or_else(|| anyhow::anyhow!("No p90 for fast semaphore"))?;
        let p90_slow = slow_est
            .percentile(0.9)
            .ok_or_else(|| anyhow::anyhow!("No p90 for slow semaphore"))?;
        let ratio = p90_fast.as_secs_f64() / p90_slow.as_secs_f64();
        Ok(ratio)
    };

    // If this converges above 50% after the mininum time, that means that
    // we don't really have a preference for waking the process on the fast
    // semaphore (at the p90), and therefore it fails the test.
    let target = 0.50; // Slow semaphore is slower than fast semaphore.
    let final_value = converge(
        Some(Duration::from_secs_f64(5.0)),
        Some(Duration::from_secs_f64(10.0)),
        Some(target),
        metric,
    )?;
    if final_value >= target {
        Err(anyhow::anyhow!(
            "Failed to achieve target: got {:.2}, expected {:.2}",
            final_value,
            target
        ))
    } else {
        Ok(())
    }
}

test!("adaptive_priority", adaptive_priority);
