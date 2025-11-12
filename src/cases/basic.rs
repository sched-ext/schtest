//! Basic tests.

use std::time::{Duration, Instant};

use crate::util::stats::Distribution;
use crate::util::system::{CPUMask, CPUSet, System};

use crate::workloads::benchmark::BenchArgs;
use crate::workloads::benchmark::BenchResult::{Count, Latency};
use crate::workloads::spinner::Spinner;
use crate::workloads::{context::Context, semaphore::Semaphore};

use crate::benchmark;
use crate::measure;
use crate::process;
use crate::test;

use anyhow::Result;

fn self_test() -> Result<()> {
    Ok(())
}

test!("self_test", self_test);

fn self_bench(args: &BenchArgs) -> Result<()> {
    let mut ctx = Context::create()?;
    measure!(&mut ctx, &args, "1ms", (), |iters| {
        let spinner = Spinner::new(Instant::now());
        spinner.spin(Duration::from_millis(iters as u64));
        Ok(Count(iters as u64))
    })
}

benchmark!("self_bench", self_bench, ());

#[derive(Debug, Clone, Copy)]
enum WakeType {
    SameHyperthread,
    SameCore,
    SameComplex,
    SameNode,
    Unpinned,
}

impl std::fmt::Display for WakeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WakeType::SameHyperthread => write!(f, "SameHyperthread"),
            WakeType::SameCore => write!(f, "SameCore"),
            WakeType::SameComplex => write!(f, "SameComplex"),
            WakeType::SameNode => write!(f, "SameNode"),
            WakeType::Unpinned => write!(f, "Unpinned"),
        }
    }
}

fn ping_pong(args: &BenchArgs, wake_type: WakeType) -> Result<()> {
    let mut ctx = Context::create()?;
    let sem1 = ctx.allocate(Semaphore::<256, 1024>::new(1))?;
    let sem2 = ctx.allocate(Semaphore::<256, 1024>::new(1))?;

    let system = System::load()?;
    let mask = match wake_type {
        WakeType::SameHyperthread => CPUMask::new(
            system
                .cores()
                .first()
                .unwrap()
                .hyperthreads()
                .first()
                .unwrap(),
        ),
        WakeType::SameCore => CPUMask::new(system.cores().first().unwrap()),
        WakeType::SameComplex => {
            CPUMask::new(system.nodes().first().unwrap().complexes().first().unwrap())
        }
        WakeType::SameNode => CPUMask::new(system.nodes().first().unwrap()),
        WakeType::Unpinned => CPUMask::new(&system),
    };

    process!(&mut ctx, None, (mask, sem1, sem2), move |mut get_iters| {
        mask.run(|| loop {
            let iters = get_iters();
            for _ in 0..iters {
                sem1.produce(1, 1, None);
                sem2.consume(1, 1, None);
            }
        })
    });
    process!(&mut ctx, None, (mask, sem1, sem2), move |mut get_iters| {
        mask.run(|| loop {
            let iters = get_iters();
            for _ in 0..iters {
                sem2.produce(1, 1, None);
                sem1.consume(1, 1, None);
            }
        })
    });
    measure!(&mut ctx, &args, "wake_latency", (sem1, sem2), |_| {
        let mut d = Distribution::<Duration>::default();
        sem1.collect_wake_stats(&mut d);
        sem2.collect_wake_stats(&mut d);
        Ok(Latency(d))
    })
}

fn check_wake_type(wake_type: WakeType) -> Result<()> {
    let system = System::load()?;
    match wake_type {
        WakeType::SameHyperthread => {
            if system.cores().first().unwrap().hyperthreads().len() > 1 {
                Ok(())
            } else {
                Err(anyhow::anyhow!("same as single core"))
            }
        }
        WakeType::SameCore => {
            if system.cores().len() > 1 {
                Ok(())
            } else {
                Err(anyhow::anyhow!("only one core available"))
            }
        }
        WakeType::SameComplex => {
            if system.complexes() > 1 {
                Ok(())
            } else {
                Err(anyhow::anyhow!("only one complex available"))
            }
        }
        WakeType::SameNode => {
            if system.nodes().len() > 1 {
                Ok(())
            } else {
                Err(anyhow::anyhow!("only one node available"))
            }
        }
        WakeType::Unpinned => Ok(()),
    }
}

benchmark!(
    "ping_pong",
    ping_pong,
    (
        WakeType::SameHyperthread,
        WakeType::SameCore,
        WakeType::SameComplex,
        WakeType::SameNode,
        WakeType::Unpinned
    ),
    check_wake_type
);
