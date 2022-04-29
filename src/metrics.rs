use prometheus::{self, histogram_opts, register_gauge, register_histogram, Gauge, Histogram, IntGauge, IntCounter, register_int_gauge, register_int_counter};

use lazy_static::lazy_static;

pub struct Metrics {
    pub boottime: Gauge,

    pub first_id: IntCounter, // TODO
    pub last_id: IntCounter, // TODO
    pub database_size: IntGauge, // TODO
    pub entries_now_in_database: IntGauge, // TODO
    pub entries_prerolled_to_clients: IntCounter, // TODO
    pub entries_streamed_to_clients: IntCounter, // TODO
    pub client_connects: IntCounter, // TODO
    pub upstream_connects: IntCounter, // TODO
    pub settings_reads: IntCounter, // TODO
    pub settings_writes: IntCounter, // TODO
    pub status_queries: IntCounter, // TODO
    pub invalid_password: IntCounter, // TODO

    pub client_session_durations: Histogram, // TODO
    pub upstream_session_durations: Histogram, // TODO
    pub preroll_durations: Histogram, // TODO
    pub upstream_lull_durations: Histogram, // TODO
    pub upstream_processing_durations: Histogram, // TODO
    pub client_backpressure_preroll: Histogram, // TODO
    pub client_backpressure_stream: Histogram, // TODO
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
                "Durations between updates from upstream (excluding zeroes)",
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
            first_id: register_int_counter!(
                "first_id",
                "First id of the bar currently in the database"
            )
            .unwrap(),
            last_id: register_int_counter!(
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
            entries_streamed_to_clients: register_int_counter!(
                "entries_streamed_to_clients",
                "Number of entries streamed to clients"
            ).unwrap(),
            client_connects: register_int_counter!(
                "client_connects",
                "Number of times new client connection were registered"
            ).unwrap(),
            upstream_connects: register_int_counter!(
                "upstream_connects",
                "Number of times upstream connection were initiated"
            ).unwrap(),
            settings_reads:register_int_counter!(
                "settings_reads",
                "Number of times settings were loaded from websocket"
            ).unwrap(),
            settings_writes: register_int_counter!(
                "settings_writes",
                "Number of times settings were saved from websocket"
            ).unwrap(),
            status_queries: register_int_counter!(
                "status_queries",
                "Number of times server stats were requested"
            ).unwrap(),
            invalid_password: register_int_counter!(
                "invalid_password",
                "Number of times nonempty password was invalid"
            ).unwrap(),
        }
    }
}

lazy_static! {
    pub static ref METRICS: Metrics = Metrics::new();
}
