use crate::models::{InterfaceStats, HistoryEntry, SummaryData};

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub enum IpcRequest {
    GetStats { interface: Option<String> },
    GetHistory { 
        table: String, 
        interface: Option<String>, 
        limit: usize,
        begin: Option<i64>,
        end: Option<i64>,
    },
    GetSummary { interface: Option<String> },
    GetInfo,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub enum IpcResponse {
    Stats(Vec<InterfaceStats>),
    History(Vec<HistoryEntry>),
    Summary(Vec<SummaryData>),
    Info { hostname: String, machine_id: String },
    Error(String),
}
