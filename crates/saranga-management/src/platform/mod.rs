//! The JDK's platform MBeans the console reads (`java.lang:type=Runtime`,
//! `Memory`, `OperatingSystem`, `Threading` and `ClassLoading`), answered
//! from the Saranga process: its start, uptime, arguments and system
//! properties, its resident memory as the heap, the machine's CPU, memory
//! and load, and on Linux its threads by name. What only a JVM has (loaded
//! classes, garbage collection, the JVM's specification) reads as zero or
//! as unsupported, which a read of all attributes leaves out.

pub mod catalog;

use std::sync::{Mutex, OnceLock};

use saranga_server::management::json::Json;
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

use crate::mbean::Control;

/// One of the platform MBeans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    Runtime,
    Memory,
    OperatingSystem,
    Threading,
    ClassLoading,
}

impl Platform {
    pub const ALL: [Platform; 5] = [
        Self::ClassLoading,
        Self::Memory,
        Self::OperatingSystem,
        Self::Runtime,
        Self::Threading,
    ];

    /// The `type` of its name, `java.lang:type=<type>`.
    pub fn type_name(self) -> &'static str {
        match self {
            Self::Runtime => "Runtime",
            Self::Memory => "Memory",
            Self::OperatingSystem => "OperatingSystem",
            Self::Threading => "Threading",
            Self::ClassLoading => "ClassLoading",
        }
    }

    pub fn control(self) -> &'static Control {
        match self {
            Self::Runtime => &catalog::RUNTIME,
            Self::Memory => &catalog::MEMORY,
            Self::OperatingSystem => &catalog::OPERATING_SYSTEM,
            Self::Threading => &catalog::THREADING,
            Self::ClassLoading => &catalog::CLASS_LOADING,
        }
    }

    pub fn class_name(self) -> &'static str {
        match self {
            Self::Runtime => "sun.management.RuntimeImpl",
            Self::Memory => "sun.management.MemoryImpl",
            Self::OperatingSystem => "com.sun.management.internal.OperatingSystemImpl",
            Self::Threading => "com.sun.management.internal.HotSpotThreadImpl",
            Self::ClassLoading => "sun.management.ClassLoadingImpl",
        }
    }
}

/// What the platform MBeans report about the process.
pub struct Process {
    /// The system properties: those `artemis run` defines and those given
    /// with `-D`.
    pub properties: Vec<(String, String)>,
    system: Mutex<System>,
    /// The settings a client may write; nothing acts on them.
    flags: Mutex<Flags>,
}

#[derive(Default)]
struct Flags {
    contention_monitoring: bool,
    cpu_time: bool,
    allocated_memory: bool,
    memory_verbose: bool,
    class_verbose: bool,
}

/// The answer to an attribute or operation of a platform MBean, or why
/// there is none.
pub enum Failure {
    Unsupported(String),
    NotFound,
}

fn pid() -> Pid {
    Pid::from_u32(std::process::id())
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// `MemoryUsage` as `CompositeData`.
fn usage(init: i64, used: i64, committed: i64, max: i64) -> Json {
    Json::object()
        .with("init", init)
        .with("committed", committed)
        .with("max", max)
        .with("used", used)
}

fn object_name(t: &str) -> Json {
    Json::object().with("objectName", format!("java.lang:type={t}"))
}

/// The `os.name` a JVM reports.
fn os_name() -> String {
    match std::env::consts::OS {
        "linux" => "Linux".to_owned(),
        "macos" => "Mac OS X".to_owned(),
        "windows" => {
            let v = System::os_version().unwrap_or_default();
            match v.split_whitespace().next() {
                Some(major) => format!("Windows {major}"),
                None => "Windows".to_owned(),
            }
        }
        other => other.to_owned(),
    }
}

/// The `os.arch` a JVM reports.
fn os_arch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "x86" => "x86",
        other => other,
    }
}

fn processors() -> i64 {
    std::thread::available_parallelism().map_or(1, |n| n.get() as i64)
}

/// A thread of this process: its id, name and state, from
/// `/proc/self/task`.
#[cfg(target_os = "linux")]
fn threads() -> Option<Vec<(i64, String, &'static str)>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir("/proc/self/task").ok()?.flatten() {
        let Some(id) = entry
            .file_name()
            .to_str()
            .and_then(|s| s.parse::<i64>().ok())
        else {
            continue;
        };
        let name = std::fs::read_to_string(entry.path().join("comm"))
            .map(|s| s.trim_end().to_owned())
            .unwrap_or_default();
        // The state follows the parenthesized name in `stat`.
        let stat = std::fs::read_to_string(entry.path().join("stat")).unwrap_or_default();
        let state = stat
            .rsplit_once(')')
            .and_then(|(_, rest)| rest.split_whitespace().next())
            .map_or("RUNNABLE", |s| match s {
                "R" => "RUNNABLE",
                "S" | "I" => "WAITING",
                "D" => "BLOCKED",
                "T" | "t" => "WAITING",
                "Z" | "X" => "TERMINATED",
                _ => "RUNNABLE",
            });
        out.push((id, name, state));
    }
    out.sort_by_key(|t| t.0);
    Some(out)
}

#[cfg(not(target_os = "linux"))]
fn threads() -> Option<Vec<(i64, String, &'static str)>> {
    None
}

/// `ThreadInfo` as `CompositeData`, without a stack.
fn thread_info(id: i64, name: &str, state: &str) -> Json {
    Json::object()
        .with("threadId", id)
        .with("threadName", name)
        .with("threadState", state)
        .with("blockedCount", -1i64)
        .with("blockedTime", -1i64)
        .with("waitedCount", -1i64)
        .with("waitedTime", -1i64)
        .with("lockInfo", Json::Null)
        .with("lockName", Json::Null)
        .with("lockOwnerId", -1i64)
        .with("lockOwnerName", Json::Null)
        .with("inNative", true)
        .with("suspended", false)
        .with("daemon", false)
        .with("priority", 5i64)
        .with("stackTrace", Json::Array(Vec::new()))
        .with("lockedMonitors", Json::Array(Vec::new()))
        .with("lockedSynchronizers", Json::Array(Vec::new()))
}

impl Process {
    pub fn new(properties: Vec<(String, String)>) -> Self {
        Self {
            properties,
            system: Mutex::new(System::new()),
            flags: Mutex::new(Flags::default()),
        }
    }

    /// The machine and this process, refreshed now. CPU loads compare
    /// with the last refresh, so the first reads 0.
    fn sample<T>(&self, f: impl FnOnce(&System, Option<&sysinfo::Process>) -> T) -> T {
        let mut s = self.system.lock().unwrap();
        s.refresh_memory();
        s.refresh_cpu_usage();
        s.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[pid()]),
            true,
            ProcessRefreshKind::nothing().with_memory().with_cpu(),
        );
        let p = s.process(pid());
        f(&s, p)
    }

    /// When the process started, in milliseconds since the epoch.
    fn start_time(&self) -> i64 {
        static START: OnceLock<i64> = OnceLock::new();
        *START.get_or_init(|| {
            self.sample(|_, p| p.map(|p| p.start_time() as i64 * 1000))
                .unwrap_or_else(now_millis)
        })
    }

    fn system_properties(&self) -> Json {
        let mut props: Vec<(String, String)> = vec![
            ("os.name".to_owned(), os_name()),
            ("os.arch".to_owned(), os_arch().to_owned()),
            (
                "os.version".to_owned(),
                System::kernel_version().unwrap_or_default(),
            ),
            (
                "file.separator".to_owned(),
                std::path::MAIN_SEPARATOR.to_string(),
            ),
            (
                "path.separator".to_owned(),
                if cfg!(windows) { ";" } else { ":" }.to_owned(),
            ),
            (
                "line.separator".to_owned(),
                if cfg!(windows) { "\r\n" } else { "\n" }.to_owned(),
            ),
            (
                "user.dir".to_owned(),
                std::env::current_dir()
                    .map(|d| d.display().to_string())
                    .unwrap_or_default(),
            ),
            (
                "saranga.version".to_owned(),
                env!("CARGO_PKG_VERSION").to_owned(),
            ),
        ];
        for (k, v) in &self.properties {
            match props.iter_mut().find(|(n, _)| n == k) {
                Some(p) => p.1 = v.clone(),
                None => props.push((k.clone(), v.clone())),
            }
        }
        let mut out = Json::object();
        for (k, v) in props {
            out.put(&k, v);
        }
        out
    }

    /// An attribute of a platform MBean.
    pub fn attribute(&self, bean: Platform, name: &str) -> Result<Json, Failure> {
        let unsupported = || {
            Err(Failure::Unsupported(format!(
                "{name} is not supported by Saranga"
            )))
        };
        let flags = || self.flags.lock().unwrap();
        Ok(match (bean, name) {
            (_, "ObjectName") => object_name(bean.type_name()),

            (Platform::Runtime, "Name") => Json::from(format!(
                "{}@{}",
                std::process::id(),
                System::host_name().unwrap_or_else(|| "localhost".to_owned())
            )),
            (Platform::Runtime, "Pid") => Json::from(std::process::id() as i64),
            (Platform::Runtime, "VmName" | "VmVendor") => Json::from("Saranga"),
            (Platform::Runtime, "VmVersion") => Json::from(env!("CARGO_PKG_VERSION")),
            (Platform::Runtime, "StartTime") => Json::from(self.start_time()),
            (Platform::Runtime, "Uptime") => Json::from(now_millis() - self.start_time()),
            (Platform::Runtime, "InputArguments") => {
                Json::Array(std::env::args().skip(1).map(Json::from).collect())
            }
            (Platform::Runtime, "SystemProperties") => self.system_properties(),
            (Platform::Runtime, "BootClassPathSupported") => Json::from(false),
            (Platform::Runtime, "ClassPath" | "LibraryPath") => Json::from(""),

            (Platform::Memory, "HeapMemoryUsage") => {
                let rss = self.sample(|_, p| p.map_or(0, |p| p.memory() as i64));
                usage(0, rss, rss, -1)
            }
            (Platform::Memory, "NonHeapMemoryUsage") => usage(0, 0, 0, -1),
            (Platform::Memory, "ObjectPendingFinalizationCount") => Json::from(0i64),
            (Platform::Memory, "Verbose") => Json::from(flags().memory_verbose),

            (Platform::OperatingSystem, "Name") => Json::from(os_name()),
            (Platform::OperatingSystem, "Arch") => Json::from(os_arch()),
            (Platform::OperatingSystem, "Version") => {
                Json::from(System::kernel_version().unwrap_or_default())
            }
            (Platform::OperatingSystem, "AvailableProcessors") => Json::from(processors()),
            (Platform::OperatingSystem, "SystemLoadAverage") => {
                if cfg!(windows) {
                    Json::from(-1.0)
                } else {
                    Json::from(System::load_average().one)
                }
            }
            (Platform::OperatingSystem, "CpuLoad" | "SystemCpuLoad") => {
                self.sample(|s, _| Json::from(f64::from(s.global_cpu_usage()) / 100.0))
            }
            (Platform::OperatingSystem, "ProcessCpuLoad") => self.sample(|_, p| {
                let cores = processors() as f64 * 100.0;
                Json::from(p.map_or(0.0, |p| f64::from(p.cpu_usage()) / cores))
            }),
            (Platform::OperatingSystem, "ProcessCpuTime") => self.sample(|_, p| {
                Json::from(p.map_or(0, |p| p.accumulated_cpu_time() as i64 * 1_000_000))
            }),
            (Platform::OperatingSystem, "CommittedVirtualMemorySize") => {
                self.sample(|_, p| Json::from(p.map_or(0, |p| p.virtual_memory() as i64)))
            }
            (Platform::OperatingSystem, "TotalPhysicalMemorySize" | "TotalMemorySize") => {
                self.sample(|s, _| Json::from(s.total_memory() as i64))
            }
            (Platform::OperatingSystem, "FreePhysicalMemorySize" | "FreeMemorySize") => {
                self.sample(|s, _| Json::from(s.available_memory() as i64))
            }
            (Platform::OperatingSystem, "TotalSwapSpaceSize") => {
                self.sample(|s, _| Json::from(s.total_swap() as i64))
            }
            (Platform::OperatingSystem, "FreeSwapSpaceSize") => {
                self.sample(|s, _| Json::from(s.free_swap() as i64))
            }
            #[cfg(unix)]
            (Platform::OperatingSystem, "OpenFileDescriptorCount") => Json::from(
                std::fs::read_dir("/proc/self/fd")
                    .or_else(|_| std::fs::read_dir("/dev/fd"))
                    .map_or(0, |d| d.count() as i64),
            ),
            #[cfg(unix)]
            (Platform::OperatingSystem, "MaxFileDescriptorCount") => self.sample(|_, p| {
                Json::from(
                    p.and_then(|p| p.open_files_limit())
                        .map_or(-1, |n| n as i64),
                )
            }),

            (Platform::Threading, "ThreadCount" | "PeakThreadCount") => match threads() {
                Some(t) => Json::from(t.len() as i64),
                None => return unsupported(),
            },
            (Platform::Threading, "DaemonThreadCount") => Json::from(0i64),
            (Platform::Threading, "AllThreadIds") => match threads() {
                Some(t) => Json::Array(t.into_iter().map(|(id, ..)| Json::from(id)).collect()),
                None => return unsupported(),
            },
            (Platform::Threading, "ThreadContentionMonitoringEnabled") => {
                Json::from(flags().contention_monitoring)
            }
            (Platform::Threading, "ThreadCpuTimeEnabled") => Json::from(flags().cpu_time),
            (Platform::Threading, "ThreadAllocatedMemoryEnabled") => {
                Json::from(flags().allocated_memory)
            }
            (
                Platform::Threading,
                "ThreadContentionMonitoringSupported"
                | "ThreadCpuTimeSupported"
                | "CurrentThreadCpuTimeSupported"
                | "ThreadAllocatedMemorySupported"
                | "ObjectMonitorUsageSupported"
                | "SynchronizerUsageSupported",
            ) => Json::from(false),

            (Platform::ClassLoading, "LoadedClassCount") => Json::from(0i64),
            (Platform::ClassLoading, "TotalLoadedClassCount" | "UnloadedClassCount") => {
                Json::from(0i64)
            }
            (Platform::ClassLoading, "Verbose") => Json::from(flags().class_verbose),

            _ if bean.control().attr(name).is_some() => return unsupported(),
            _ => return Err(Failure::NotFound),
        })
    }

    /// Writes one of the flags a client may set.
    pub fn write(&self, bean: Platform, name: &str, value: &Json) -> Result<(), Failure> {
        let on = value.as_bool().unwrap_or(false);
        let mut f = self.flags.lock().unwrap();
        match (bean, name) {
            (Platform::Threading, "ThreadContentionMonitoringEnabled") => {
                f.contention_monitoring = on
            }
            (Platform::Threading, "ThreadCpuTimeEnabled") => f.cpu_time = on,
            (Platform::Threading, "ThreadAllocatedMemoryEnabled") => f.allocated_memory = on,
            (Platform::Memory, "Verbose") => f.memory_verbose = on,
            (Platform::ClassLoading, "Verbose") => f.class_verbose = on,
            _ => return Err(Failure::NotFound),
        }
        Ok(())
    }

    /// An operation of a platform MBean, by name and arguments.
    pub fn invoke(&self, bean: Platform, op: &str, args: &[Json]) -> Result<Json, Failure> {
        let unsupported = || {
            Err(Failure::Unsupported(format!(
                "{op} is not supported by Saranga"
            )))
        };
        match (bean, op) {
            (Platform::Memory, "gc") => Ok(Json::Null),
            (Platform::Threading, "dumpAllThreads") => match threads() {
                Some(t) => Ok(Json::Array(
                    t.iter()
                        .map(|(id, name, state)| thread_info(*id, name, state))
                        .collect(),
                )),
                None => unsupported(),
            },
            (Platform::Threading, "getThreadInfo") => {
                let Some(t) = threads() else {
                    return unsupported();
                };
                let find = |id: i64| {
                    t.iter()
                        .find(|(i, ..)| *i == id)
                        .map_or(Json::Null, |(id, name, state)| {
                            thread_info(*id, name, state)
                        })
                };
                Ok(match args.first() {
                    Some(Json::Array(ids)) => {
                        Json::Array(ids.iter().filter_map(Json::as_i64).map(find).collect())
                    }
                    Some(id) => find(id.as_i64().unwrap_or(-1)),
                    None => Json::Null,
                })
            }
            (Platform::Threading, "findDeadlockedThreads" | "findMonitorDeadlockedThreads") => {
                Ok(Json::Null)
            }
            (Platform::Threading, "resetPeakThreadCount") => Ok(Json::Null),
            _ if !bean.control().ops_named(op).is_empty() => unsupported(),
            _ => Err(Failure::NotFound),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attributes() {
        let p = Process::new(vec![("artemis.home".to_owned(), "/opt/a".to_owned())]);
        let props = p
            .attribute(Platform::Runtime, "SystemProperties")
            .ok()
            .unwrap();
        assert_eq!(
            props.get("artemis.home").and_then(Json::as_str),
            Some("/opt/a")
        );
        assert!(props.get("os.name").is_some());
        let start = p.attribute(Platform::Runtime, "StartTime").ok().unwrap();
        assert!(start.as_i64().unwrap() > 0);
        let heap = p
            .attribute(Platform::Memory, "HeapMemoryUsage")
            .ok()
            .unwrap();
        assert!(heap.get("used").and_then(Json::as_i64).unwrap() > 0);
        assert!(matches!(
            p.attribute(Platform::Runtime, "SpecName"),
            Err(Failure::Unsupported(_))
        ));
        assert!(matches!(
            p.attribute(Platform::Runtime, "Nope"),
            Err(Failure::NotFound)
        ));
        p.write(
            Platform::Threading,
            "ThreadContentionMonitoringEnabled",
            &Json::from(true),
        )
        .ok()
        .unwrap();
        assert_eq!(
            p.attribute(Platform::Threading, "ThreadContentionMonitoringEnabled")
                .ok()
                .and_then(|j| j.as_bool()),
            Some(true)
        );
        // On Linux, the threads are listed by name.
        #[cfg(target_os = "linux")]
        {
            let count = p
                .attribute(Platform::Threading, "ThreadCount")
                .ok()
                .unwrap();
            assert!(count.as_i64().unwrap() >= 1);
            let dump = p
                .invoke(
                    Platform::Threading,
                    "dumpAllThreads",
                    &[Json::from(false), Json::from(false)],
                )
                .ok()
                .unwrap();
            let Json::Array(threads) = dump else {
                panic!("{dump}")
            };
            assert!(
                threads
                    .iter()
                    .any(|t| t.get("threadName").and_then(Json::as_str).is_some())
            );
        }
        // Every attribute the catalog lists is answered or unsupported.
        for bean in Platform::ALL {
            for a in bean.control().attrs {
                assert!(
                    !matches!(p.attribute(bean, a.name), Err(Failure::NotFound)),
                    "{} {}",
                    bean.type_name(),
                    a.name
                );
            }
        }
    }
}
