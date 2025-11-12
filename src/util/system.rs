//! System topology and affinity utilties.

use anyhow::{anyhow, Result};
use std::fmt;
use std::fs;
use std::io::Read;
use std::path::Path;

unsafe fn cpu_set(cpu: usize, set: &mut libc::cpu_set_t) {
    // Calculate which element in the array contains this CPU's bit.
    let cpu_elem = cpu / (8 * std::mem::size_of::<libc::c_ulong>());
    // Calculate the bit position within that element.
    let cpu_bit = cpu % (8 * std::mem::size_of::<libc::c_ulong>());
    // Get a pointer to the array of c_ulong elements.
    let set_ptr = set as *mut libc::cpu_set_t as *mut libc::c_ulong;
    // Set the bit.
    *set_ptr.add(cpu_elem) |= 1 << cpu_bit;
}

unsafe fn cpu_or(dest: &mut libc::cpu_set_t, src: &libc::cpu_set_t) {
    let dest_ptr = dest as *mut libc::cpu_set_t as *mut libc::c_ulong;
    let src_ptr = src as *const libc::cpu_set_t as *const libc::c_ulong;
    let size = std::mem::size_of::<libc::cpu_set_t>() / std::mem::size_of::<libc::c_ulong>();
    for i in 0..size {
        *dest_ptr.add(i) |= *src_ptr.add(i);
    }
}

#[derive(Debug, Clone)]
pub struct CPUMask {
    /// The CPU mask as a `libc::cpu_set_t`.
    mask: libc::cpu_set_t,
}

/// A trait for types that can provide a CPU mask.
pub trait CPUSet {
    /// Get the CPU mask for this set
    fn mask(&self) -> libc::cpu_set_t;

    /// Run a function with the current thread pinned.
    fn run<F>(&self, f: F) -> Result<()>
    where
        F: FnOnce(),
    {
        let mut orig = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
        let target = self.mask();

        // Retrieve the current affinity.
        let rc = unsafe {
            libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut orig)
        };
        if rc < 0 {
            return Err(anyhow!(
                "unable to get current CPU mask: {}",
                std::io::Error::last_os_error()
            ));
        }

        // Set new affinity; when this returns, we will be executing on these cores.
        let rc =
            unsafe { libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &target) };
        if rc < 0 {
            return Err(anyhow!(
                "unable to set CPU mask: {}",
                std::io::Error::last_os_error()
            ));
        }

        // Run the required function.
        f();

        // Restore original affinity.
        let rc =
            unsafe { libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &orig) };
        if rc < 0 {
            return Err(anyhow!(
                "unable to restore CPU mask: {}",
                std::io::Error::last_os_error()
            ));
        }

        Ok(())
    }

    /// Migrate the current thread to this set.
    fn migrate(&self) -> Result<()> {
        self.run(|| {})
    }

    /// Set the affinity of a specific process/thread to this CPU set.
    ///
    /// # Arguments
    ///
    /// * `pid` - The process ID (or 0 for current process)
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure.
    fn set_affinity_for_pid(&self, pid: i32) -> Result<()> {
        let target = self.mask();
        let rc = unsafe {
            libc::sched_setaffinity(
                pid,
                std::mem::size_of::<libc::cpu_set_t>(),
                &target,
            )
        };
        if rc < 0 {
            return Err(anyhow!(
                "unable to set CPU affinity for pid {}: {}",
                pid,
                std::io::Error::last_os_error()
            ));
        }
        Ok(())
    }
}

impl CPUMask {
    /// Create a new `CPUMask` from a CPUSet.
    pub fn new<T>(set: &T) -> Self
    where
        T: CPUSet,
    {
        Self { mask: set.mask() }
    }
}

impl CPUSet for CPUMask {
    /// Get the underlying `libc::cpu_set_t`.
    fn mask(&self) -> libc::cpu_set_t {
        self.mask
    }
}

/// A hyperthread (logical CPU).
#[derive(Debug, Clone)]
pub struct Hyperthread {
    /// The ID of this hyperthread.
    id: i32,
}

impl Hyperthread {
    /// Create a new hyperthread with the given ID.
    pub fn new(id: i32) -> Self {
        Self { id }
    }

    /// Get the ID of this hyperthread.
    pub fn id(&self) -> i32 {
        self.id
    }
}

impl CPUSet for Hyperthread {
    fn mask(&self) -> libc::cpu_set_t {
        let mut mask = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
        unsafe { cpu_set(self.id as usize, &mut mask) };
        mask
    }
}

impl fmt::Display for Hyperthread {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Hyperthread{{id={}}}", self.id)
    }
}

/// A physical CPU core.
#[derive(Debug, Clone)]
pub struct Core {
    /// The ID of this core.
    id: i32,
    /// The hyperthreads belonging to this core.
    hyperthreads: Vec<Hyperthread>,
}

impl Core {
    /// Create a new core with the given ID and hyperthreads.
    pub fn new(id: i32, hyperthreads: Vec<Hyperthread>) -> Self {
        Self { id, hyperthreads }
    }

    /// Get the ID of this core.
    pub fn id(&self) -> i32 {
        self.id
    }

    /// Get the hyperthreads belonging to this core.
    pub fn hyperthreads(&self) -> &Vec<Hyperthread> {
        &self.hyperthreads
    }
}

impl CPUSet for Core {
    fn mask(&self) -> libc::cpu_set_t {
        let mut mask = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
        for ht in &self.hyperthreads {
            let ht_mask = ht.mask();
            unsafe { cpu_or(&mut mask, &ht_mask) };
        }
        mask
    }
}

impl fmt::Display for Core {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Core{{id={}, hyperthreads=[", self.id)?;
        for (i, ht) in self.hyperthreads.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{ht}")?;
        }
        write!(f, "]}}")
    }
}

/// A core complex (e.g., a group of cores sharing an L3 cache).
#[derive(Debug, Clone)]
pub struct CoreComplex {
    /// The ID of this core complex.
    id: i32,
    /// The cores belonging to this complex.
    cores: Vec<Core>,
}

impl CoreComplex {
    /// Create a new core complex with the given ID and cores.
    pub fn new(id: i32, cores: Vec<Core>) -> Self {
        Self { id, cores }
    }

    /// Get the ID of this core complex.
    pub fn id(&self) -> i32 {
        self.id
    }

    /// Get the cores belonging to this complex.
    pub fn cores(&self) -> &[Core] {
        &self.cores
    }
}

impl CPUSet for CoreComplex {
    fn mask(&self) -> libc::cpu_set_t {
        let mut mask = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
        for core in &self.cores {
            let core_mask = core.mask();
            unsafe { cpu_or(&mut mask, &core_mask) };
        }
        mask
    }
}

impl fmt::Display for CoreComplex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CoreComplex{{id={}, cores=[", self.id)?;
        for (i, core) in self.cores.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{core}")?;
        }
        write!(f, "]}}")
    }
}

/// A NUMA node.
#[derive(Debug, Clone)]
pub struct Node {
    /// The ID of this node.
    id: i32,
    /// The core complexes belonging to this node.
    complexes: Vec<CoreComplex>,
}

impl Node {
    /// Create a new node with the given ID and core complexes.
    pub fn new(id: i32, complexes: Vec<CoreComplex>) -> Self {
        Self { id, complexes }
    }

    /// Get the ID of this node.
    pub fn id(&self) -> i32 {
        self.id
    }

    /// Get the core complexes belonging to this node.
    pub fn complexes(&self) -> &Vec<CoreComplex> {
        &self.complexes
    }

    /// Get all cores in this node.
    pub fn cores(&self) -> Vec<Core> {
        let mut all_cores = Vec::new();
        for complex in &self.complexes {
            all_cores.extend_from_slice(complex.cores());
        }
        all_cores
    }
}

impl CPUSet for Node {
    fn mask(&self) -> libc::cpu_set_t {
        let mut mask = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
        for complex in &self.complexes {
            let complex_mask = complex.mask();
            unsafe { cpu_or(&mut mask, &complex_mask) };
        }
        mask
    }
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Node{{id={}, complexes=[", self.id)?;
        for (i, complex) in self.complexes.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{complex}")?;
        }
        write!(f, "]}}")
    }
}

/// System information.
#[derive(Debug, Clone)]
pub struct System {
    /// The nodes in the system.
    nodes: Vec<Node>,
    /// All cores in the system, sorted by ID.
    all_cores: Vec<Core>,
    /// Map of CPU ID to topology indices (node_idx, complex_idx, core_idx) for quick lookup
    cpu_to_topology: std::collections::HashMap<i32, (usize, usize, usize)>,
}

impl System {
    /// Clear the affinity for a specific process (set it to all CPUs).
    ///
    /// # Arguments
    ///
    /// * `pid` - The process ID (or 0 for current process)
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure.
    pub fn clear_affinity_for_pid(pid: i32) -> Result<()> {
        // Load the system to get all CPUs
        let system = System::load()?;
        system.set_affinity_for_pid(pid)
    }

    /// Get the current CPU ID.
    pub fn current_cpu() -> Result<i32> {
        let cpu = unsafe { libc::sched_getcpu() };
        if cpu < 0 {
            return Err(anyhow!(
                "failed to get current CPU: {}",
                std::io::Error::last_os_error()
            ));
        }
        Ok(cpu)
    }

    /// Get the current Hyperthread
    pub fn current_hyperthread(&self) -> Result<Hyperthread> {
        let cpu_id = Self::current_cpu()?;

        if let Some(&(node_idx, complex_idx, core_idx)) = self.cpu_to_topology.get(&cpu_id) {
            // Find the hyperthread with the matching CPU ID
            for ht in &self.nodes[node_idx].complexes[complex_idx].cores[core_idx].hyperthreads {
                if ht.id() == cpu_id {
                    return Ok(ht.clone());
                }
            }
        }

        Err(anyhow!("failed to find hyperthread for CPU {}", cpu_id))
    }

    /// Get the current Core
    pub fn current_core(&self) -> Result<Core> {
        let cpu_id = Self::current_cpu()?;

        if let Some(&(node_idx, complex_idx, core_idx)) = self.cpu_to_topology.get(&cpu_id) {
            return Ok(self.nodes[node_idx].complexes[complex_idx].cores[core_idx].clone());
        }

        Err(anyhow!("failed to find core for CPU {}", cpu_id))
    }

    /// Get the current CoreComplex
    pub fn current_complex(&self) -> Result<CoreComplex> {
        let cpu_id = Self::current_cpu()?;

        if let Some(&(node_idx, complex_idx, _)) = self.cpu_to_topology.get(&cpu_id) {
            return Ok(self.nodes[node_idx].complexes[complex_idx].clone());
        }

        Err(anyhow!("failed to find complex for CPU {}", cpu_id))
    }

    /// Get the current Node
    pub fn current_node(&self) -> Result<Node> {
        let cpu_id = Self::current_cpu()?;

        if let Some(&(node_idx, _, _)) = self.cpu_to_topology.get(&cpu_id) {
            return Ok(self.nodes[node_idx].clone());
        }

        Err(anyhow!("failed to find node for CPU {}", cpu_id))
    }

    /// Create a new system with the given nodes.
    pub fn new(nodes: Vec<Node>) -> Self {
        // Build the flat list of all cores for quick lookup.
        let mut all_cores = Vec::new();
        for node in &nodes {
            all_cores.extend(node.cores());
        }
        all_cores.sort_by_key(|core| core.id());

        // Build the CPU ID to topology indices map for quick lookups
        let mut cpu_to_topology = std::collections::HashMap::new();
        for (node_idx, node) in nodes.iter().enumerate() {
            for (complex_idx, complex) in node.complexes().iter().enumerate() {
                for (core_idx, core) in complex.cores().iter().enumerate() {
                    for ht in core.hyperthreads() {
                        cpu_to_topology.insert(ht.id(), (node_idx, complex_idx, core_idx));
                    }
                }
            }
        }

        Self {
            nodes,
            all_cores,
            cpu_to_topology,
        }
    }

    /// Get the nodes in the system.
    pub fn nodes(&self) -> &Vec<Node> {
        &self.nodes
    }

    /// Get all cores in the system.
    pub fn cores(&self) -> &Vec<Core> {
        &self.all_cores
    }

    /// Get the number of logical CPUs in the system.
    pub fn logical_cpus(&self) -> usize {
        let mut count = 0;
        for core in &self.all_cores {
            count += core.hyperthreads().len();
        }
        count
    }

    // Get the number of core complexes in the system.
    pub fn complexes(&self) -> usize {
        self.nodes.iter().map(|n| n.complexes.len()).sum()
    }

    /// Load system information.
    ///
    /// # Returns
    ///
    /// A `System` instance with information about the system, or an error if
    /// the information could not be loaded.
    pub fn load() -> Result<Self> {
        // Maps to store the topology information
        let mut topology: std::collections::BTreeMap<
            i32,
            std::collections::BTreeMap<i32, std::collections::BTreeMap<i32, Vec<i32>>>,
        > = std::collections::BTreeMap::new();

        // Get the number of CPUs in the system
        let num_cpus = unsafe { libc::sysconf(libc::_SC_NPROCESSORS_CONF) };
        if num_cpus <= 0 {
            return Err(anyhow!(
                "failed to get number of CPUs: {}",
                std::io::Error::last_os_error()
            ));
        }

        // Process each CPU.
        let base_path = Path::new("/sys/devices/system/cpu");
        for cpu_id in 0..num_cpus {
            let cpu_path = base_path.join(format!("cpu{cpu_id}"));

            // Skip if the CPU directory doesn't exist
            if !cpu_path.exists() {
                continue;
            }

            // Try to read from topology/physical_package_id.
            let mut node_id = 0; // Default to zero.
            let package_path = cpu_path.join("topology").join("physical_package_id");
            if package_path.exists() {
                let mut package_file = fs::File::open(&package_path)?;
                let mut package_id = String::new();
                package_file.read_to_string(&mut package_id)?;
                node_id = package_id.trim().parse::<i32>().unwrap_or(0);
            }

            // Try to read from topology/core_id.
            let mut core_id = cpu_id as i32;
            let core_path = cpu_path.join("topology").join("core_id");
            if core_path.exists() {
                let mut core_file = fs::File::open(&core_path)?;
                let mut core_id_str = String::new();
                core_file.read_to_string(&mut core_id_str)?;
                core_id = core_id_str.trim().parse::<i32>().unwrap_or(cpu_id as i32);
            }

            // Try to read from topology/die_id or use L3 cache as a proxy.
            let mut complex_id = 0; // Default to 0 if not found.
            let die_path = cpu_path.join("topology").join("die_id");
            if die_path.exists() {
                let mut die_file = fs::File::open(&die_path)?;
                let mut die_id = String::new();
                die_file.read_to_string(&mut die_id)?;
                complex_id = die_id.trim().parse::<i32>().unwrap_or(0);
            } else {
                // Try to use L3 cache as a proxy for complex.
                let cache_path = base_path.join(format!("cpu{cpu_id}")).join("cache");
                if cache_path.exists() {
                    // Look for L3 cache.
                    for entry in fs::read_dir(&cache_path)? {
                        let entry = entry?;
                        let index_path = entry.path().join("level");
                        if index_path.exists() {
                            let mut index_file = fs::File::open(&index_path)?;
                            let mut level = String::new();
                            index_file.read_to_string(&mut level)?;
                            if let Ok(level_num) = level.trim().parse::<i32>() {
                                if level_num == 3 {
                                    // Found L3 cache, use its ID as complex ID.
                                    let id_path = entry.path().join("id");
                                    if id_path.exists() {
                                        let mut id_file = fs::File::open(&id_path)?;
                                        let mut id = String::new();
                                        id_file.read_to_string(&mut id)?;
                                        if let Ok(id_num) = id.trim().parse::<i32>() {
                                            complex_id = id_num;
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Add this CPU to the topology map.
            topology
                .entry(node_id)
                .or_default()
                .entry(complex_id)
                .or_default()
                .entry(core_id)
                .or_default()
                .push(cpu_id as i32);
        }

        // Build the system topology from the collected information.
        let mut nodes = Vec::new();
        for (node_id, complexes_map) in topology {
            let mut complexes = Vec::new();
            for (complex_id, cores_map) in complexes_map {
                let mut cores = Vec::new();
                for (core_id, cpu_ids) in cores_map {
                    let mut hyperthreads = Vec::new();
                    for cpu_id in cpu_ids {
                        hyperthreads.push(Hyperthread::new(cpu_id));
                    }
                    cores.push(Core::new(core_id, hyperthreads));
                }
                complexes.push(CoreComplex::new(complex_id, cores));
            }
            nodes.push(Node::new(node_id, complexes));
        }

        Ok(System::new(nodes))
    }
}

impl CPUSet for System {
    fn mask(&self) -> libc::cpu_set_t {
        let mut mask = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
        for node in &self.nodes {
            let node_mask = node.mask();
            unsafe { cpu_or(&mut mask, &node_mask) };
        }
        mask
    }
}

impl fmt::Display for System {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "System{{nodes=[",)?;
        for (i, node) in self.nodes.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{node}")?;
        }
        write!(f, "], logical_cpus={}}}", self.logical_cpus())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_load() {
        let system = System::load().unwrap();
        println!("System: {}", system);

        assert!(system.logical_cpus() > 0);
        assert!(!system.nodes().is_empty());
        assert!(!system.cores().is_empty());
    }

    #[test]
    fn test_cpu_mask() {
        let system = System::load().unwrap();

        // Test that we can get a mask for the system.
        let _mask = system.mask();

        // Test that we can get a mask for a node.
        if !system.nodes().is_empty() {
            let _node_mask = system.nodes()[0].mask();
        }

        // Test that we can get a mask for a core.
        if !system.cores().is_empty() {
            let _core_mask = system.cores()[0].mask();
        }

        // Test that we can get a mask for a hyperthread.
        if !system.cores().is_empty() && !system.cores()[0].hyperthreads().is_empty() {
            let _ht_mask = system.cores()[0].hyperthreads()[0].mask();
        }
    }

    #[test]
    fn test_run_on_cpu() {
        let system = System::load().unwrap();

        // Test that we can run a function on a specific CPU.
        if !system.cores().is_empty() && !system.cores()[0].hyperthreads().is_empty() {
            let ht = &system.cores()[0].hyperthreads()[0];
            ht.run(|| {
                // This function should be running on the specified CPU.
                println!("Running on CPU {}", ht.id());
                assert_eq!(ht.id(), System::current_cpu().unwrap());
            })
            .unwrap();
        }
    }

    #[test]
    fn test_current_cpu_methods() {
        let system = System::load().unwrap();

        // Test that we can get the current CPU ID.
        let cpu_id = System::current_cpu().unwrap();
        println!("Current CPU ID: {}", cpu_id);

        // Test that we can get the current hyperthread.
        let ht = system.current_hyperthread().unwrap();
        println!("Current hyperthread: {}", ht);
        assert_eq!(ht.id(), cpu_id);

        // Test that we can get the current core.
        let core = system.current_core().unwrap();
        println!("Current core: {}", core);

        // Test that we can get the current complex.
        let complex = system.current_complex().unwrap();
        println!("Current complex: {}", complex);

        // Test that we can get the current node.
        let node = system.current_node().unwrap();
        println!("Current node: {}", node);
    }
}
