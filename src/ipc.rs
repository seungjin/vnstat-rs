use crate::models::{
    HistoryEntry, InterfaceStats, NintyFifthData, SummaryData,
};

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub enum IpcRequest {
    GetStats {
        interface: Option<String>,
        host: Option<String>,
        filter_type: Option<u8>,
    },
    GetHistory {
        table: String,
        interface: Option<String>,
        host: Option<String>,
        filter_type: Option<u8>,
        limit: usize,
        begin: Option<i64>,
        end: Option<i64>,
    },
    GetSummary {
        interface: Option<String>,
        host: Option<String>,
        filter_type: Option<u8>,
    },
    GetInfo,
    GetConfig {
        name: String,
    },
    SetConfig {
        name: String,
        value: String,
    },
    ListHosts {
        host: Option<String>,
    },
    Get95th {
        interface: Option<String>,
        host: Option<String>,
        filter_type: Option<u8>,
    },
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub enum IpcResponse {
    Stats {
        stats: Vec<InterfaceStats>,
        load_average: Option<(f64, f64, f64)>,
        num_cores: Option<usize>,
    },
    History {
        history: Vec<HistoryEntry>,
        load_average: Option<(f64, f64, f64)>,
        num_cores: Option<usize>,
    },
    Summary(Vec<SummaryData>),
    NintyFifth(NintyFifthData),
    Info {
        hostname: String,
        machine_id: String,
        mac_address: Option<String>,
        version: String,
        local_schema: i64,
        remote_schema: Option<i64>,
    },
    Hosts(Vec<(String, String, Option<String>, Option<i64>, Option<i64>)>),
    Config(Option<String>),
    Ok,
    Error(String),
}
