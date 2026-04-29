use chrono::{Datelike, Timelike};

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct InterfaceStats {
    pub name: String,
    pub alias: Option<String>,
    pub mac_address: Option<String>,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    pub rx_packets: u64,
    pub tx_packets: u64,
    pub hostname: String,
    pub created: i64,
    pub updated: i64,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct VnStatJson {
    pub vnstatversion: String,
    pub jsonversion: String,
    pub interfaces: Vec<JsonInterface>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct JsonInterface {
    pub name: String,
    pub alias: String,
    pub mac_address: Option<String>,
    pub created: JsonTimestamp,
    pub updated: JsonTimestamp,
    pub traffic: JsonTraffic,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct JsonTimestamp {
    pub date: JsonDate,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<JsonTime>,
    pub timestamp: i64,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Copy)]
pub struct JsonDate {
    pub year: i32,
    pub month: u32,
    pub day: u32,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Copy)]
pub struct JsonTime {
    pub hour: u32,
    pub minute: u32,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Default)]
pub struct JsonTraffic {
    pub total: JsonTotal,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub fiveminute: Vec<JsonHistoryEntry>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub hour: Vec<JsonHistoryEntry>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub day: Vec<JsonHistoryEntry>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub month: Vec<JsonHistoryEntry>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub year: Vec<JsonHistoryEntry>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub top: Vec<JsonHistoryEntry>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Default)]
pub struct JsonTotal {
    pub rx: u64,
    pub tx: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct JsonHistoryEntry {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<i64>,
    pub date: JsonDate,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<JsonTime>,
    pub timestamp: i64,
    pub rx: u64,
    pub tx: u64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct SummaryData {
    pub name: String,
    pub hostname: String,
    pub today: (u64, u64),
    pub yesterday: (u64, u64),
    pub this_month: (u64, u64),
    pub last_month: (u64, u64),
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct NintyFifthData {
    pub interface: String,
    pub hostname: String,
    pub begin: i64,
    pub end: i64,
    pub count: usize,
    pub coverage: f64,
    pub rx: Vec<u64>,
    pub tx: Vec<u64>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct HistoryEntry {
    pub hostname: String,
    pub interface: String,
    pub date: i64,
    pub rx: u64,
    pub tx: u64,
}

impl JsonTimestamp {
    pub fn from_timestamp(ts: i64, include_time: bool) -> Self {
        let dt = chrono::DateTime::from_timestamp(ts, 0).unwrap_or_default();
        Self {
            date: JsonDate {
                year: dt.year(),
                month: dt.month(),
                day: dt.day(),
            },
            time: if include_time {
                Some(JsonTime {
                    hour: dt.hour(),
                    minute: dt.minute(),
                })
            } else {
                None
            },
            timestamp: ts,
        }
    }
}

impl HistoryEntry {
    pub fn to_json(&self, include_time: bool) -> JsonHistoryEntry {
        let ts = JsonTimestamp::from_timestamp(self.date, include_time);
        JsonHistoryEntry {
            id: None,
            date: ts.date,
            time: ts.time,
            timestamp: self.date,
            rx: self.rx,
            tx: self.tx,
        }
    }
}

impl InterfaceStats {
    pub fn to_json(&self) -> JsonInterface {
        JsonInterface {
            name: self.name.clone(),
            alias: self.alias.clone().unwrap_or_default(),
            mac_address: self.mac_address.clone(),
            created: JsonTimestamp::from_timestamp(self.created, false),
            updated: JsonTimestamp::from_timestamp(self.updated, true),
            traffic: JsonTraffic {
                total: JsonTotal {
                    rx: self.rx_bytes,
                    tx: self.tx_bytes,
                },
                ..Default::default()
            },
        }
    }
}

impl JsonDate {
    pub fn to_xml(&self) -> String {
        format!("<date><year>{}</year><month>{:02}</month><day>{:02}</day></date>", self.year, self.month, self.day)
    }
}

impl JsonTime {
    pub fn to_xml(&self) -> String {
        format!("<time><hour>{:02}</hour><minute>{:02}</minute></time>", self.hour, self.minute)
    }
}

impl JsonTimestamp {
    pub fn to_xml(&self, tag: &str) -> String {
        let mut out = format!("<{}>{}", tag, self.date.to_xml());
        if let Some(ref t) = self.time {
            out.push_str(&t.to_xml());
        }
        out.push_str(&format!("<timestamp>{}</timestamp></{}>", self.timestamp, tag));
        out
    }
}

impl JsonHistoryEntry {
    pub fn to_xml(&self, tag: &str) -> String {
        let mut out = format!("<{} id=\"{}\">{}", tag, self.id.unwrap_or(0), self.date.to_xml());
        if let Some(ref t) = self.time {
            out.push_str(&t.to_xml());
        }
        out.push_str(&format!("<timestamp>{}</timestamp><rx>{}</rx><tx>{}</tx></{}>", self.timestamp, self.rx, self.tx, tag));
        out
    }
}

impl JsonTraffic {
    pub fn to_xml(&self) -> String {
        let mut out = String::from("<traffic>");
        out.push_str(&format!("<total><rx>{}</rx><tx>{}</tx></total>", self.total.rx, self.total.tx));
        
        let write_entries = |entries: &[JsonHistoryEntry], plural: &str, singular: &str| -> String {
            if entries.is_empty() { return String::new(); }
            let mut s = format!("<{}>", plural);
            for entry in entries {
                s.push_str(&entry.to_xml(singular));
            }
            s.push_str(&format!("</{}>", plural));
            s
        };

        out.push_str(&write_entries(&self.fiveminute, "fiveminutes", "fiveminute"));
        out.push_str(&write_entries(&self.hour, "hours", "hour"));
        out.push_str(&write_entries(&self.day, "days", "day"));
        out.push_str(&write_entries(&self.month, "months", "month"));
        out.push_str(&write_entries(&self.year, "years", "year"));
        out.push_str(&write_entries(&self.top, "tops", "top"));

        out.push_str("</traffic>");
        out
    }
}

impl JsonInterface {
    pub fn to_xml(&self) -> String {
        let mut out = format!(" <interface name=\"{}\">", self.name);
        out.push_str(&format!("<name>{}</name>", self.name));
        out.push_str(&format!("<alias>{}</alias>", self.alias));
        if let Some(ref mac) = self.mac_address {
            out.push_str(&format!("<mac_address>{}</mac_address>", mac));
        }
        out.push_str(&self.created.to_xml("created"));
        out.push_str(&self.updated.to_xml("updated"));
        out.push_str(&self.traffic.to_xml());
        out.push_str(" </interface>");
        out
    }
}

impl VnStatJson {
    pub fn to_xml(&self) -> String {
        let mut out = format!("<vnstat version=\"{}\" xmlversion=\"2\">\n", self.vnstatversion);
        for iface in &self.interfaces {
            out.push_str(&iface.to_xml());
            out.push('\n');
        }
        out.push_str("</vnstat>");
        out
    }
    
    pub fn new(stats: Vec<InterfaceStats>) -> Self {
        Self {
            vnstatversion: env!("CARGO_PKG_VERSION").to_string(),
            jsonversion: "2".to_string(),
            interfaces: stats.into_iter().map(|s| s.to_json()).collect(),
        }
    }

    pub fn from_history(history: Vec<HistoryEntry>, table: &str) -> Self {
        let mut json = Self::new(vec![]);
        json.insert_history(history, table);
        json
    }

    pub fn insert_history(&mut self, history: Vec<HistoryEntry>, table: &str) {
        for entry in history {
            if let Some(iface) = self.interfaces.iter_mut().find(|i| i.name == entry.interface) {
                let json_entry = entry.to_json(table == "fiveminute" || table == "hour");
                match table {
                    "fiveminute" => iface.traffic.fiveminute.push(json_entry),
                    "hour" => iface.traffic.hour.push(json_entry),
                    "day" => iface.traffic.day.push(json_entry),
                    "month" => iface.traffic.month.push(json_entry),
                    "year" => iface.traffic.year.push(json_entry),
                    "top" => iface.traffic.top.push(json_entry),
                    _ => {}
                }
            } else {
                // If interface not found in stats (unlikely but possible), create a dummy one
                let mut traffic = JsonTraffic::default();
                let json_entry = entry.to_json(table == "fiveminute" || table == "hour");
                match table {
                    "fiveminute" => traffic.fiveminute.push(json_entry),
                    "hour" => traffic.hour.push(json_entry),
                    "day" => traffic.day.push(json_entry),
                    "month" => traffic.month.push(json_entry),
                    "year" => traffic.year.push(json_entry),
                    "top" => traffic.top.push(json_entry),
                    _ => {}
                }
                self.interfaces.push(JsonInterface {
                    name: entry.interface.clone(),
                    alias: String::new(),
                    mac_address: None,
                    created: JsonTimestamp::from_timestamp(0, false),
                    updated: JsonTimestamp::from_timestamp(0, false),
                    traffic,
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vnstat_xml_format() {
        let stats = vec![InterfaceStats {
            name: "eth0".to_string(),
            alias: Some("lan".to_string()),
            mac_address: Some("00:11:22:33:44:55".to_string()),
            rx_bytes: 1000,
            tx_bytes: 2000,
            rx_packets: 10,
            tx_packets: 20,
            hostname: "test-host".to_string(),
            created: 1700000000,
            updated: 1700003600,
        }];

        let json = VnStatJson::new(stats);
        let xml = json.to_xml();
        
        assert!(xml.contains("<vnstat version="));
        assert!(xml.contains("xmlversion=\"2\""));
        assert!(xml.contains("<interface name=\"eth0\">"));
        assert!(xml.contains("<name>eth0</name>"));
        assert!(xml.contains("<alias>lan</alias>"));
        assert!(xml.contains("<mac_address>00:11:22:33:44:55</mac_address>"));
        assert!(xml.contains("<traffic>"));
        assert!(xml.contains("<total><rx>1000</rx><tx>2000</tx></total>"));
    }
}
