//! Process management for workloads.

use anyhow::{anyhow, Result};
use libc;
use nix::sys::signal::Signal;
use nix::unistd::Pid;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::util::cgroups::Cgroup;
use crate::util::child::Child;
use crate::util::sched::{Sched, SchedStats};
use crate::util::shared::SharedBox;
use crate::util::system::{CPUSet, System};
use crate::workloads::context::Context;
use crate::workloads::semaphore::Semaphore;

/// A spec for a process to be started.
#[derive(Default)]
pub struct Spec {
    /// Priority for the process.
    priority: Option<i32>,

    /// Name for the process.
    name: Option<String>,
}

impl Spec {
    /// Set the priority of the process.
    ///
    /// # Arguments
    ///
    /// * `priority` - The priority to set
    ///
    /// # Returns
    ///
    /// A reference to self for method chaining.
    pub fn with_priority(&mut self, priority: i32) -> &mut Self {
        self.priority = Some(priority);
        self
    }

    /// Set the name of the process.
    ///
    /// # Arguments
    ///
    /// * `name` - The name to set
    ///
    /// # Returns
    ///
    /// A reference to self for method chaining.
    pub fn with_name(&mut self, name: &str) -> &mut Self {
        self.name = Some(name.to_string());
        self
    }
}

/// A handle for a process that is separate from the actual object.
pub struct ProcessHandle {
    /// The pid that can fetch stats, etc.
    pid: Pid,
}

impl ProcessHandle {
    /// Return the process pid.
    pub fn pid(&self) -> i32 {
        self.pid.as_raw()
    }

    /// Return stats for the process.
    pub fn stats(&self) -> Result<SchedStats> {
        Sched::get_process_thread_stats(Some(self.pid))
    }

    /// Set the affinity of this process to a specific CPU set.
    ///
    /// # Arguments
    ///
    /// * `set` - The CPU set to pin this process to
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure.
    pub fn set_affinity<T: CPUSet>(&self, set: &T) -> Result<()> {
        set.set_affinity_for_pid(self.pid.as_raw())
    }

    /// Clear the affinity of this process (set it to all CPUs).
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure.
    pub fn clear_affinity(&self) -> Result<()> {
        System::clear_affinity_for_pid(self.pid.as_raw())
    }
}

/// A process that can be started and joined.
///
/// This struct wraps a function to be executed in a child process, with control
/// over scheduling parameters and process names.
pub struct Process {
    /// The cgroup for the process.
    _cgroup: Cgroup,

    /// The child process itself.
    child: Child,

    /// Semaphore for synchronizing start. The number of iterations to run
    /// of the given function is provided as the first argument.
    start: SharedBox<Semaphore<0, 0>>,

    /// Semaphore for synchronizing at the end of an iteration.
    ready: SharedBox<Semaphore<0, 0>>,

    /// The number of iterations to complete.
    iters: SharedBox<AtomicU32>,
}

impl Process {
    /// Create a new process with the given function.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The context for the process
    /// * `func` - The function to execute in the child process. This function takes another function
    ///   as a parameter which, when called, returns the number of iterations to run.
    /// * `spec` - Optional specifications for the process
    ///
    /// # Safety
    ///
    /// This function is unsafe because forking a process in Rust can lead to undefined behavior if
    /// not handled correctly.
    ///
    /// # Returns
    ///
    /// A new `Process` instance.
    pub fn create<F>(ctx: &Context, func: F, spec: Option<Spec>) -> Result<Self>
    where
        F: FnOnce(Box<dyn FnMut() -> u32 + Send>) -> Result<()> + Send + 'static,
    {
        let cgroup = Cgroup::create()?;
        let cgroup_info = cgroup.info().clone();
        let start = ctx.allocate(Semaphore::<0, 0>::new(1))?;
        let ready = ctx.allocate(Semaphore::<0, 0>::new(1))?;
        let iters = ctx.allocate(AtomicU32::new(0))?;
        let child_start = start.clone();
        let child_ready = ready.clone();
        let child_iters = iters.clone();
        let child = Child::run(
            move || {
                let mut priority = 0;
                if let Some(spec) = spec {
                    if let Some(p) = spec.priority {
                        priority = p;
                    }
                    if let Some(name) = spec.name {
                        let rc = unsafe { libc::prctl(libc::PR_SET_NAME, name.as_ptr(), 0, 0, 0) };
                        if rc < 0 {
                            let err = std::io::Error::last_os_error();
                            return Err(anyhow!("failed to set process name: {}", err));
                        }
                    }
                }
                Sched::set_scheduler(7 /* SCHED_EXT */, priority)?;
                cgroup_info.enter()?;

                // Construct our callback to the iteration function.
                let get_iters = Box::new(move || {
                    child_ready.produce(1, 1, None);
                    if !child_start.consume(1, 1, None) {
                        // Since we have no timeout, this should never happen.
                        panic!("Failed to consume semaphore");
                    }
                    child_iters.swap(0, Ordering::Relaxed)
                });
                func(get_iters)
            },
            None,
        )?;

        let mut proc = Self {
            _cgroup: cgroup,
            child,
            start,
            ready,
            iters,
        };
        proc.wait()?;
        Ok(proc)
    }

    /// Return the handle.
    pub fn handle(&self) -> ProcessHandle {
        ProcessHandle {
            pid: self.child.pid(),
        }
    }

    /// Start the process for the given iterations.
    pub fn start(&self, iters: u32) {
        self.iters.store(iters, Ordering::Relaxed);
        self.start.produce(1, 1, None);
    }

    /// Wait for the process to become ready.
    pub fn wait(&mut self) -> Result<()> {
        loop {
            if self
                .ready
                .consume(1, 1, Some(std::time::Duration::from_secs(1)))
            {
                break;
            }
            if !self.child.alive() {
                return Err(anyhow!("Process exited unexpectedly"));
            }
        }
        Ok(())
    }

    /// Join the process.
    ///
    /// This method kills the process and waits for it to exit.
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure.
    pub fn join(&mut self) -> Result<()> {
        self.child.kill(Signal::SIGKILL)?;
        let _ = self.child.wait(true, false);
        Ok(())
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.join();
    }
}

#[macro_export]
macro_rules! process {
    ($ctx:expr, $spec:expr, ($($var:ident),*), $func:expr) => {{
        $(let $var = $var.clone();)*
        let p = $crate::workloads::process::Process::create($ctx, $func, $spec)?;
        $ctx.add(p)
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workloads::context::Context;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[test]
    fn test_process_iterations() -> Result<()> {
        let ctx = Context::create()?;
        let iter_count = ctx.allocate(AtomicU32::new(0))?;
        let iter_values = ctx.allocate_vec(2, |_| AtomicU32::new(0))?;
        let iter_count_clone = iter_count.clone();
        let iter_values_clone = iter_values.clone();
        let mut process = Process::create(
            &ctx,
            move |mut get_iters| loop {
                let iters = get_iters();
                let count = iter_count_clone.fetch_add(1, Ordering::SeqCst);
                iter_values_clone[count as usize].store(iters, Ordering::SeqCst);
                let iters = get_iters();
                let count = iter_count_clone.fetch_add(1, Ordering::SeqCst);
                iter_values_clone[count as usize].store(iters, Ordering::SeqCst);
            },
            None,
        )?;

        process.start(5);
        process.wait()?;
        assert_eq!(iter_count.load(Ordering::SeqCst), 1);
        assert_eq!(iter_values[0].load(Ordering::SeqCst), 5);
        assert_eq!(iter_values[1].load(Ordering::SeqCst), 0);

        process.start(10);
        process.wait()?;
        assert_eq!(iter_count.load(Ordering::SeqCst), 2);
        assert_eq!(iter_values[0].load(Ordering::SeqCst), 5);
        assert_eq!(iter_values[1].load(Ordering::SeqCst), 10);

        process.join()
    }
}
