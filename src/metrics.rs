use prometheus::{self, histogram_opts, register_gauge, register_histogram, Gauge, Histogram, IntGauge, IntCounter, register_int_gauge, register_int_counter};

use lazy_static::lazy_static;

pub struct Metrics {
    pub boottime: Gauge,

    pub first_id: IntGauge,
    pub last_id: IntGauge,
    pub database_size: IntGauge,
    pub entries_now_in_database: IntGauge,

    pub entries_prerolled_to_clients: IntCounter, 
    pub entries_streamed_to_clients: IntCounter,
    pub client_connects: IntCounter,
    pub clients_connected: IntGauge,
    pub client_preroll_commands: IntCounter,
    pub client_monitor_comands: IntCounter,

    pub client_settings_reads: IntCounter, 
    pub client_settings_writes: IntCounter,
    pub client_status_queries: IntCounter,
    pub client_invalid_password: IntCounter, 

    pub upstream_is_connected: IntGauge,
    pub upstream_connect_starts: IntCounter,
    pub upstream_connect_ws: IntCounter,
    pub upstream_connect_auth: IntCounter,
    pub entries_received_from_upstream: IntCounter,
    pub slowdown_mode_enters: IntCounter,
    pub slowdown_mode_exits: IntCounter,
    pub upstream_websocket_events: IntCounter,


    pub client_session_durations: Histogram,
    pub upstream_session_durations: Histogram,
    pub preroll_durations: Histogram,
    pub upstream_lull_durations: Histogram,
    pub upstream_processing_durations: Histogram,
    pub client_backpressure_preroll: Histogram,
    pub client_backpressure_stream: Histogram,
}

impl Metrics {
    pub fn new() -> Metrics {
        Metrics {
            boottime: register_gauge!(
                "boottime",
                "Seconds it took to boot up and open the database"
            )
            .unwrap(),
            client_session_durations: register_histogram!(histogram_opts!(
                "client_session_durations",
                "Client connection duration",
                prometheus::exponential_buckets(5.0, 1.5, 20).unwrap(),
            ))
            .unwrap(),
            upstream_session_durations: register_histogram!(histogram_opts!(
                "upstream_session_durations",
                "Upstream connection duration",
                prometheus::exponential_buckets(5.0, 2.0, 20).unwrap(),
            ))
            .unwrap(),
            preroll_durations: register_histogram!(histogram_opts!(
                "preroll_durations",
                "Durations of preroll events from clients",
                prometheus::exponential_buckets(0.05, 1.8, 20).unwrap(),
            ))
            .unwrap(),
            upstream_lull_durations: register_histogram!(histogram_opts!(
                "upstream_lull_durations",
                "Durations between updates from upstream",
                prometheus::exponential_buckets(1.0, 2.0, 20).unwrap(),
            ))
            .unwrap(),
            upstream_processing_durations: register_histogram!(histogram_opts!(
                "upstream_processing_durations",
                "Durations between receiving entry from upstream and being ready to receive next entry from upstream",
                prometheus::exponential_buckets(0.01, 1.5, 20).unwrap(),
            ))
            .unwrap(),
            client_backpressure_preroll: register_histogram!(histogram_opts!(
                "client_backpressure_preroll",
                "Durations between initiating sending an entry to client while prerolling and being able to send another one",
                prometheus::exponential_buckets(0.01, 1.5, 20).unwrap(),
            ))
            .unwrap(),
            client_backpressure_stream: register_histogram!(histogram_opts!(
                "client_backpressure_stream",
                "Durations between initiating sending an entry to client while streaming and being able to send another one",
                prometheus::exponential_buckets(0.01, 1.5, 20).unwrap(),
            ))
            .unwrap(),
            first_id: register_int_gauge!(
                "first_id",
                "First id of the bar currently in the database"
            )
            .unwrap(),
            last_id: register_int_gauge!(
                "last_id",
                "Last id of the bar currently in the database"
            ).unwrap(),
            database_size: register_int_gauge!(
                "database_size",
                "Current size of the database in bytes"
            )
            .unwrap(),
            entries_now_in_database: register_int_gauge!(
                "entries_now_in_database",
                "Number of bars currently in the database"
            )
            .unwrap(),
            entries_prerolled_to_clients: register_int_counter!(
                "entries_prerolled_to_clients",
                "Number of entries prerolled to clients"
            ).unwrap(),
            entries_received_from_upstream: register_int_counter!(
                "entries_received_from_upstream",
                "Number of entries received from upstream"
            ).unwrap(),
            entries_streamed_to_clients: register_int_counter!(
                "entries_streamed_to_clients",
                "Number of entries streamed to clients"
            ).unwrap(),
            client_connects: register_int_counter!(
                "client_connects",
                "Number of times new client connection were registered"
            ).unwrap(),
            client_preroll_commands: register_int_counter!(
                "client_preroll_commands",
                "Number of times client requested a preroll"
            ).unwrap(),
            client_monitor_comands: register_int_counter!(
                "client_monitor_comands",
                "Number of times client requsted a monitor mode"
            ).unwrap(),
            upstream_connect_starts: register_int_counter!(
                "upstream_connect_starts",
                "Number of times upstream connection were initiated"
            ).unwrap(),
            upstream_connect_ws: register_int_counter!(
                "upstream_connect_ws",
                "Number of times upstream connection resulted in a WebSocket"
            ).unwrap(),
            upstream_connect_auth: register_int_counter!(
                "upstream_connect_auth",
                "Number of times upstream connection authenticated successfully"
            ).unwrap(),
            upstream_is_connected: register_int_gauge!(
                "upstream_is_connected",
                "Is upstream active now"
            ).unwrap(),
            client_settings_reads:register_int_counter!(
                "settings_reads",
                "Number of times settings were loaded from websocket"
            ).unwrap(),
            client_settings_writes: register_int_counter!(
                "settings_writes",
                "Number of times settings were saved from websocket"
            ).unwrap(),
            client_status_queries: register_int_counter!(
                "status_queries",
                "Number of times server stats were requested"
            ).unwrap(),
            client_invalid_password: register_int_counter!(
                "invalid_password",
                "Number of times nonempty password was invalid"
            ).unwrap(),
            clients_connected:  register_int_gauge!(
                "clients_connected",
                "Number of currently connected clients"
            ).unwrap(),
            slowdown_mode_enters: register_int_counter!(
                "slowdown_mode_enters",
                "Number of times upstream entered slow mode"
            ).unwrap(),
            slowdown_mode_exits: register_int_counter!(
                "slowdown_mode_exits",
                "Number of times upstream exited slow mode"
            ).unwrap(),
            upstream_websocket_events: register_int_counter!(
                "upstream_websocket_events",
                "WebSocket message received from upstream"
            ).unwrap(),
        }
    }
}

lazy_static! {
    pub static ref METRICS: Metrics = Metrics::new();
}

pub async fn periodic_poller(db : crate::database::Db) {
    let mut intvl = tokio::time::interval(std::time::Duration::new(10, 0));
    loop {
        intvl.tick().await;

        if let Ok(sz) = db.get_database_disk_size() {
            METRICS.database_size.set(sz as i64);
        }
        if let Ok((Some(f), Some(l))) = db.get_first_last_id() {
            METRICS.first_id.set(f as i64);
            METRICS.last_id.set(l as i64);
            METRICS.entries_now_in_database.set((l as i64) - (f as i64) + 1);
        }
    }
}

pub mod guarded;
