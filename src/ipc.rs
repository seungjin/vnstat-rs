use crate::models::{InterfaceStats, HistoryEntry, SummaryData};

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub enum IpcRequest {
    GetStats { interface: Option<String>, host: Option<String> },
    GetHistory { 
        table: String, 
        interface: Option<String>, 
        host: Option<String>,
        limit: usize,
        begin: Option<i64>,
        end: Option<i64>,
    },
    GetSummary { interface: Option<String>, host: Option<String> },
    GetInfo,
    GetConfig { name: String },
    SetConfig { name: String, value: String },
    ListHosts,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub enum IpcResponse {
    Stats(Vec<InterfaceStats>),
    History(Vec<HistoryEntry>),
    Summary(Vec<SummaryData>),
    Info { hostname: String, machine_id: String, mac_address: Option<String> },
    Hosts(Vec<(String, String)>),
    Config(Option<String>),
    Ok,
    Error(String),
}
