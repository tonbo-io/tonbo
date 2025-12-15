use serde::Serialize;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub enum WorkloadKind {
    SequentialWrite,
    ReadOnly,
    Mixed,
}

#[derive(Debug, Clone, Serialize)]
pub struct Workload {
    pub kind: WorkloadKind,
    pub num_records: u64,
    pub value_size_bytes: usize,
    pub concurrency: usize,
    pub warmup_records: u64,
    pub read_ratio: f64,
    pub write_ratio: f64,
}

impl Workload {
    pub fn from_config(config: &crate::harness::BenchConfig) -> Option<Self> {
        if config.workload.value_size_bytes == 0 || config.runtime.concurrency == 0 {
            return None;
        }

        let kind = match config.workload.name.as_str() {
            "sequential_write" | "write_only" => WorkloadKind::SequentialWrite,
            "read_only" => WorkloadKind::ReadOnly,
            "mixed" => WorkloadKind::Mixed,
            _ => return None,
        };

        let warmup_records = config.workload.warmup_records.unwrap_or(0);
        let concurrency = config.runtime.concurrency;

        let (read_ratio, write_ratio) = match kind {
            WorkloadKind::SequentialWrite => (0.0, 1.0),
            WorkloadKind::ReadOnly => (1.0, 0.0),
            WorkloadKind::Mixed => {
                let read_ratio = config.workload.read_ratio?;
                let write_ratio = config.workload.write_ratio?;
                if !(read_ratio >= 0.0 && write_ratio >= 0.0) {
                    return None;
                }
                let sum = read_ratio + write_ratio;
                if sum <= f64::EPSILON {
                    return None;
                }
                (read_ratio, write_ratio)
            }
        };

        Some(Self {
            kind,
            num_records: config.workload.num_records,
            value_size_bytes: config.workload.value_size_bytes,
            concurrency,
            warmup_records,
            read_ratio,
            write_ratio,
        })
    }
}
