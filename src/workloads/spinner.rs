//! Spinner workload implementation.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// A workload that spins for a specified duration.
pub struct Spinner {
    /// The ID of the CPU that the spinner last ran on.
    cpu_id: AtomicU32,
    /// The baseline instant from which we measure CPU change times.
    baseline: Instant,
    /// The time (in nanoseconds since baseline) when the CPU last changed.
    last_cpu_change_nanos: AtomicU64,
}

impl Spinner {
    /// Default duration is 99 years.
    pub const DEFAULT_DURATION: Duration = Duration::from_secs(60 * 60 * 24 * 365 * 99);

    /// Create a new Spinner with the given baseline instant.
    ///
    /// # Arguments
    ///
    /// * `baseline` - The baseline instant from which to measure CPU change times
    pub fn new(baseline: Instant) -> Self {
        Self {
            cpu_id: AtomicU32::new(0),
            baseline,
            last_cpu_change_nanos: AtomicU64::new(0),
        }
    }

    /// Get the time (in nanoseconds since baseline) when the CPU last changed.
    ///
    /// # Returns
    ///
    /// The time in nanoseconds since the baseline instant when the CPU last changed.
    pub fn last_cpu_change_nanos(&self) -> u64 {
        self.last_cpu_change_nanos.load(Ordering::Relaxed)
    }

    /// Spin for the specified duration.
    ///
    /// # Arguments
    ///
    /// * `duration` - The duration to spin for
    pub fn spin(&self, duration: Duration) {
        let start = std::time::Instant::now();

        let mut last_cpu = Self::get_current_cpu().unwrap_or(99999);
        while start.elapsed() < duration {
            std::sync::atomic::compiler_fence(Ordering::SeqCst);
            if let Some(cpu) = Self::get_current_cpu() {
                self.cpu_id.store(cpu, Ordering::Relaxed);
                if cpu != last_cpu {
                    let now = Instant::now();
                    let nanos_since_baseline = now.duration_since(self.baseline).as_nanos() as u64;
                    self.last_cpu_change_nanos.store(nanos_since_baseline, Ordering::Relaxed);
                }
                last_cpu = cpu;
            }
        }
    }

    /// Get the ID of the CPU that the spinner last ran on.
    ///
    /// # Returns
    ///
    /// The ID of the CPU that the spinner last ran on.
    pub fn last_cpu(&self) -> u32 {
        self.cpu_id.load(Ordering::Relaxed)
    }

    /// Get the ID of the CPU that the current thread is running on.
    ///
    /// # Returns
    ///
    /// The ID of the CPU that the current thread is running on, or None if the
    /// information is not available.
    fn get_current_cpu() -> Option<u32> {
        let cpu = unsafe { libc::sched_getcpu() };
        if cpu >= 0 {
            Some(cpu as u32)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use std::time::Duration;

    #[test]
    fn test_spinner() -> Result<()> {
        let baseline = Instant::now();
        let spinner = Spinner::new(baseline);

        // Spin for a short duration.
        spinner.spin(Duration::from_millis(10));

        // Check that the CPU ID was updated.
        println!("Last CPU: {}", spinner.last_cpu());

        Ok(())
    }
}
