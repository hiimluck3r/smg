//! The driver against the memory store and a probe in memory, under paused
//! time: the arbitration end to end, the per-rank writes, the failures, the
//! scrape and its interval, the rows it never dials.

use std::{
    collections::{HashMap, HashSet},
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use openai_protocol::worker::{RuntimeType, SchedulerLoadSnapshot, WorkerLoadResponse};
use parking_lot::Mutex;
use tokio::{sync::watch, task::JoinSet};

use super::{now, LoadDriver, LoadDriverConfig, LoadProbe, ReadError, Target};
use crate::{
    store::{
        GatewayId, LoadSink, LoadSource, MemoryStore, Reachability, Signals, WorkerFacts,
        WorkerStatus, WorkersStore,
    },
    view::{LoadRow, Role, SignalSource, WireModes, WorkerId},
};

/// A metrics body of the first gauge family: three running, five waiting,
/// three quarters of the KV cache in use.
const METRICS_TEXT: &str = "vllm:num_requests_running{model_name=\"m\"} 3\n\
                           vllm:num_requests_waiting{model_name=\"m\"} 5\n\
                           vllm:kv_cache_usage_perc{model_name=\"m\"} 0.75\n";

/// One rank's figures the fake engine answers with.
#[derive(Clone, Copy)]
struct Figures {
    running: i32,
    waiting: i32,
    usage: f64,
}

#[derive(Default)]
struct FakeProbe {
    /// The endpoints polled, in order.
    polls: Mutex<Vec<String>>,
    /// The URLs scraped, in order.
    scrapes: Mutex<Vec<String>>,
    /// The ranks an endpoint answers with, by rank index.
    answers: Mutex<HashMap<String, Vec<Figures>>>,
    /// Endpoints whose poll fails on the transport.
    failing: Mutex<HashSet<String>>,
    /// The metrics text a URL serves.
    texts: Mutex<HashMap<String, String>>,
    forgotten: Mutex<Vec<String>>,
}

impl FakeProbe {
    fn answers(&self, endpoint: &str, ranks: &[Figures]) {
        self.answers
            .lock()
            .insert(endpoint.to_owned(), ranks.to_vec());
    }

    fn polls(&self) -> Vec<String> {
        self.polls.lock().clone()
    }
}

impl LoadProbe for FakeProbe {
    async fn poll(
        &self,
        target: &Target,
        _deadline: Duration,
    ) -> Result<WorkerLoadResponse, ReadError> {
        self.polls.lock().push(target.endpoint.clone());
        if self.failing.lock().contains(&target.endpoint) {
            return Err(ReadError::Transport("connection refused".to_owned()));
        }
        let ranks = self
            .answers
            .lock()
            .get(&target.endpoint)
            .cloned()
            .unwrap_or_default();
        let loads = ranks
            .iter()
            .enumerate()
            .map(|(rank, figures)| SchedulerLoadSnapshot {
                dp_rank: i32::try_from(rank).unwrap_or(i32::MAX),
                num_running_reqs: figures.running,
                num_waiting_reqs: figures.waiting,
                token_usage: figures.usage,
                max_total_num_tokens: 8_192,
                num_used_tokens: 1_024,
                ..Default::default()
            })
            .collect::<Vec<_>>();
        Ok(WorkerLoadResponse {
            dp_rank_count: i32::try_from(loads.len()).unwrap_or(i32::MAX),
            loads,
            ..Default::default()
        })
    }

    async fn scrape(&self, url: &str, _timeout: Duration) -> Result<String, ReadError> {
        self.scrapes.lock().push(url.to_owned());
        self.texts
            .lock()
            .get(url)
            .cloned()
            .ok_or_else(|| ReadError::Transport("connection refused".to_owned()))
    }

    fn forget(&self, endpoint: &str) {
        self.forgotten.lock().push(endpoint.to_owned());
    }
}

/// A ready row: one rank of `engine` at `key`.
fn ready(
    store: &MemoryStore,
    key: &str,
    engine: &str,
    rank: u16,
    size: u16,
    wires: WireModes,
    scrape_ms: u32,
) -> WorkerId {
    let id = store
        .upsert(WorkerFacts {
            key: key.to_owned(),
            engine: engine.to_owned(),
            incarnation: 1,
            role: Role::Single,
            wire_modes: wires,
            engine_kind: RuntimeType::Vllm.as_str().to_owned(),
            models: vec!["m".to_owned()],
            dp_rank: rank,
            dp_size: size,
            signals: Signals {
                events: false,
                loads: false,
                scrape_interval_ms: scrape_ms,
            },
            ..WorkerFacts::default()
        })
        .id();
    let revision = store.get(id).expect("the row").revision;
    store.set_status(id, WorkerStatus::Ready, revision);
    id
}

fn driver(
    store: &Arc<MemoryStore>,
    probe: &Arc<FakeProbe>,
    poll_interval: Duration,
) -> LoadDriver<MemoryStore, FakeProbe> {
    LoadDriver::new(
        Arc::clone(store),
        Arc::clone(probe),
        LoadDriverConfig {
            poll_interval,
            tick: Duration::from_millis(100),
            ..LoadDriverConfig::default()
        },
    )
}

fn row(store: &MemoryStore, id: WorkerId, rank: u16) -> Option<(LoadRow, LoadSource)> {
    store
        .load(id, rank)
        .map(|sample| (sample.row, sample.source))
}

const TEN: Duration = Duration::from_secs(10);

#[tokio::test(start_paused = true)]
async fn the_first_tick_polls_every_ready_engine_once_and_writes_a_row_per_rank() {
    let store = Arc::new(MemoryStore::new());
    let probe = Arc::new(FakeProbe::default());
    let single = ready(&store, "grpc://a:1", "grpc://a:1", 0, 1, WireModes::GRPC, 0);
    // A data-parallel engine's rows carry the registry's rank keys; the poll
    // dials the engine, never a rank key.
    let rank0 = ready(
        &store,
        "grpc://b:1@0",
        "grpc://b:1",
        0,
        2,
        WireModes::GRPC,
        0,
    );
    let rank1 = ready(
        &store,
        "grpc://b:1@1",
        "grpc://b:1",
        1,
        2,
        WireModes::GRPC,
        0,
    );
    let busy = Figures {
        running: 4,
        waiting: 2,
        usage: 0.5,
    };
    let idle = Figures {
        running: 0,
        waiting: 0,
        usage: 0.1,
    };
    probe.answers("grpc://a:1", &[busy]);
    probe.answers("grpc://b:1", &[idle, busy]);
    let driver = driver(&store, &probe, TEN);
    let mut due = Default::default();

    driver.tick(&mut due).await;

    assert_eq!(
        probe.polls(),
        vec!["grpc://a:1", "grpc://b:1"],
        "one call per engine, at the engine's address"
    );
    let (one, source) = row(&store, single, 0).expect("the single engine's row");
    assert_eq!(
        (one.running, one.queued, one.kv_usage_permille),
        (4, 2, 500)
    );
    assert_eq!((one.source, source), (SignalSource::Poll, LoadSource::Poll));
    let (zero, _) = row(&store, rank0, 0).expect("rank 0's row");
    assert_eq!((zero.running, zero.kv_usage_permille), (0, 100));
    let (one, _) = row(&store, rank1, 1).expect("rank 1's row");
    assert_eq!((one.running, one.kv_usage_permille), (4, 500));
    assert!(
        row(&store, rank0, 1).is_none(),
        "a rank writes its own row alone"
    );
    let stats = driver.stats();
    assert_eq!(stats.polls.load(Ordering::Relaxed), 2);
    assert_eq!(stats.rows.load(Ordering::Relaxed), 3);
}

#[tokio::test(start_paused = true)]
async fn a_rank_served_by_its_stream_is_not_polled_and_a_quiet_one_falls_back() {
    let store = Arc::new(MemoryStore::new());
    let probe = Arc::new(FakeProbe::default());
    let id = ready(&store, "grpc://a:1", "grpc://a:1", 0, 1, WireModes::GRPC, 0);
    probe.answers(
        "grpc://a:1",
        &[Figures {
            running: 9,
            waiting: 0,
            usage: 0.9,
        }],
    );
    let streamed = LoadRow {
        running: 1,
        source: SignalSource::Events,
        ..LoadRow::default()
    };
    store.record(id, 0, streamed, now(), LoadSource::Stream);
    let driver = driver(&store, &probe, TEN);
    let mut due = Default::default();

    driver.tick(&mut due).await;
    assert!(probe.polls().is_empty(), "a fresh record serves the rank");
    assert_eq!(row(&store, id, 0).map(|(r, _)| r.running), Some(1));

    tokio::time::advance(TEN + Duration::from_secs(1)).await;
    driver.tick(&mut due).await;
    assert_eq!(
        probe.polls(),
        vec!["grpc://a:1"],
        "a quiet record falls back to the poll"
    );
    let (polled, source) = row(&store, id, 0).expect("the poll's row");
    assert_eq!((polled.running, source), (9, LoadSource::Poll));
    let stats = driver.stats();
    assert_eq!(stats.suppressed.load(Ordering::Relaxed), 1);
    assert_eq!(stats.fallbacks.load(Ordering::Relaxed), 1);
}

#[tokio::test(start_paused = true)]
async fn a_stream_that_stops_is_polled_within_one_tick_of_the_freshness_bound() {
    let store = Arc::new(MemoryStore::new());
    let probe = Arc::new(FakeProbe::default());
    let id = ready(&store, "grpc://a:1", "grpc://a:1", 0, 1, WireModes::GRPC, 0);
    probe.answers(
        "grpc://a:1",
        &[Figures {
            running: 3,
            waiting: 0,
            usage: 0.3,
        }],
    );
    let record = LoadRow {
        running: 1,
        ..LoadRow::default()
    };
    store.record(id, 0, record, now(), LoadSource::Stream);
    let driver = driver(&store, &probe, TEN);
    let mut due = Default::default();

    // t = 0: the record is fresh, the engine is skipped, not postponed.
    driver.tick(&mut due).await;
    assert!(probe.polls().is_empty());
    // t = 1: the stream's last record.
    tokio::time::advance(Duration::from_secs(1)).await;
    store.record(id, 0, record, now(), LoadSource::Stream);
    driver.tick(&mut due).await;
    // t = 10: nine seconds old, still within the bound.
    tokio::time::advance(Duration::from_secs(9)).await;
    driver.tick(&mut due).await;
    assert!(
        probe.polls().is_empty(),
        "a record nine seconds old still serves its rank"
    );
    // t = 11: the bound passed one second ago; the fallback goes now, not at
    // the next interval.
    tokio::time::advance(Duration::from_secs(1)).await;
    driver.tick(&mut due).await;
    assert_eq!(probe.polls(), vec!["grpc://a:1"]);
    let stats = driver.stats();
    assert_eq!(
        stats.suppressed.load(Ordering::Relaxed),
        3,
        "three ticks asked and skipped"
    );
    assert_eq!(stats.fallbacks.load(Ordering::Relaxed), 1);
    // The call moved the due instant: the next tick asks nothing.
    tokio::time::advance(Duration::from_secs(1)).await;
    driver.tick(&mut due).await;
    assert_eq!(probe.polls().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_dp_engine_polls_once_for_its_quiet_ranks_and_leaves_the_fresh_rank_alone() {
    let store = Arc::new(MemoryStore::new());
    let probe = Arc::new(FakeProbe::default());
    let rank0 = ready(
        &store,
        "grpc://b:1@0",
        "grpc://b:1",
        0,
        2,
        WireModes::GRPC,
        0,
    );
    let rank1 = ready(
        &store,
        "grpc://b:1@1",
        "grpc://b:1",
        1,
        2,
        WireModes::GRPC,
        0,
    );
    let figures = Figures {
        running: 7,
        waiting: 1,
        usage: 0.3,
    };
    probe.answers("grpc://b:1", &[figures, figures]);
    store.record(
        rank0,
        0,
        LoadRow {
            running: 2,
            ..LoadRow::default()
        },
        now(),
        LoadSource::Stream,
    );
    let driver = driver(&store, &probe, TEN);
    let mut due = Default::default();

    driver.tick(&mut due).await;

    assert_eq!(probe.polls(), vec!["grpc://b:1"]);
    assert_eq!(
        row(&store, rank0, 0),
        Some((
            LoadRow {
                running: 2,
                ..LoadRow::default()
            },
            LoadSource::Stream
        )),
        "the fresh rank keeps its stream's row"
    );
    let (one, source) = row(&store, rank1, 1).expect("the quiet rank's row");
    assert_eq!((one.running, source), (7, LoadSource::Poll));
}

#[tokio::test(start_paused = true)]
async fn a_rank_ready_alone_takes_its_own_figures_from_the_engines_answer() {
    let store = Arc::new(MemoryStore::new());
    let probe = Arc::new(FakeProbe::default());
    // Rank 0 is registered and pending; rank 1 alone is ready.
    let pending = store
        .upsert(WorkerFacts {
            key: "grpc://b:1@0".to_owned(),
            engine: "grpc://b:1".to_owned(),
            incarnation: 1,
            wire_modes: WireModes::GRPC,
            dp_rank: 0,
            dp_size: 2,
            ..WorkerFacts::default()
        })
        .id();
    let rank1 = ready(
        &store,
        "grpc://b:1@1",
        "grpc://b:1",
        1,
        2,
        WireModes::GRPC,
        0,
    );
    probe.answers(
        "grpc://b:1",
        &[
            Figures {
                running: 9,
                waiting: 9,
                usage: 0.9,
            },
            Figures {
                running: 2,
                waiting: 1,
                usage: 0.2,
            },
        ],
    );
    let driver = driver(&store, &probe, TEN);
    let mut due = Default::default();

    driver.tick(&mut due).await;

    assert_eq!(probe.polls(), vec!["grpc://b:1"]);
    let (one, _) = row(&store, rank1, 1).expect("rank 1's row");
    assert_eq!(
        (one.running, one.queued, one.kv_usage_permille),
        (2, 1, 200),
        "rank 1 takes its own figures, never the engine's sums"
    );
    assert!(
        row(&store, pending, 0).is_none(),
        "a pending rank is not written"
    );
}

#[tokio::test(start_paused = true)]
async fn a_failed_poll_leaves_the_row_on_file_and_counts() {
    let store = Arc::new(MemoryStore::new());
    let probe = Arc::new(FakeProbe::default());
    let id = ready(&store, "grpc://a:1", "grpc://a:1", 0, 1, WireModes::GRPC, 0);
    probe.answers(
        "grpc://a:1",
        &[Figures {
            running: 5,
            waiting: 0,
            usage: 0.5,
        }],
    );
    let driver = driver(&store, &probe, TEN);
    let mut due = Default::default();
    driver.tick(&mut due).await;
    assert_eq!(row(&store, id, 0).map(|(r, _)| r.running), Some(5));

    probe.failing.lock().insert("grpc://a:1".to_owned());
    tokio::time::advance(TEN + Duration::from_secs(1)).await;
    driver.tick(&mut due).await;

    assert_eq!(probe.polls().len(), 2);
    assert_eq!(
        row(&store, id, 0).map(|(r, _)| r.running),
        Some(5),
        "the old row stands"
    );
    let stats = driver.stats();
    assert_eq!(stats.poll_failures.transport.load(Ordering::Relaxed), 1);
    assert_eq!(stats.rows.load(Ordering::Relaxed), 1);
}

#[tokio::test(start_paused = true)]
async fn rows_the_controller_never_dials_are_not_polled() {
    let store = Arc::new(MemoryStore::new());
    let probe = Arc::new(FakeProbe::default());
    // A host-local wire, relayed by its gateway.
    let relayed = store
        .upsert(WorkerFacts {
            key: "zmq://local".to_owned(),
            engine: "zmq://local".to_owned(),
            incarnation: 1,
            wire_modes: WireModes::ZMQ.union(WireModes::GRPC),
            dp_size: 1,
            reachable_from: Reachability::Gateway(GatewayId(7)),
            ..WorkerFacts::default()
        })
        .id();
    let revision = store.get(relayed).expect("the row").revision;
    store.set_status(relayed, WorkerStatus::Ready, revision);
    // An HTTP engine with no scrape interval: nothing to ask it.
    ready(&store, "http://h:1", "http://h:1", 0, 1, WireModes::HTTP, 0);
    // A row not yet ready.
    store.upsert(WorkerFacts {
        key: "grpc://pending:1".to_owned(),
        engine: "grpc://pending:1".to_owned(),
        incarnation: 1,
        wire_modes: WireModes::GRPC,
        dp_size: 1,
        ..WorkerFacts::default()
    });
    let driver = driver(&store, &probe, TEN);
    let mut due = Default::default();

    driver.tick(&mut due).await;

    assert!(probe.polls().is_empty());
    assert!(probe.scrapes.lock().is_empty());
    assert_eq!(driver.stats().rows.load(Ordering::Relaxed), 0);
}

#[tokio::test(start_paused = true)]
async fn the_poll_waits_the_interval_between_calls() {
    let store = Arc::new(MemoryStore::new());
    let probe = Arc::new(FakeProbe::default());
    ready(&store, "grpc://a:1", "grpc://a:1", 0, 1, WireModes::GRPC, 0);
    let driver = driver(&store, &probe, TEN);
    let mut due = Default::default();

    driver.tick(&mut due).await;
    tokio::time::advance(Duration::from_secs(5)).await;
    driver.tick(&mut due).await;
    assert_eq!(probe.polls().len(), 1, "half an interval later: no call");
    tokio::time::advance(Duration::from_secs(5)).await;
    driver.tick(&mut due).await;
    assert_eq!(probe.polls().len(), 2, "the interval passed: one call");
}

#[tokio::test(start_paused = true)]
async fn an_engine_without_a_servicer_is_scraped_at_its_own_interval() {
    let store = Arc::new(MemoryStore::new());
    let probe = Arc::new(FakeProbe::default());
    let id = ready(
        &store,
        "http://h:1",
        "http://h:1",
        0,
        1,
        WireModes::HTTP,
        500,
    );
    probe
        .texts
        .lock()
        .insert("http://h:1".to_owned(), METRICS_TEXT.to_owned());
    let driver = driver(&store, &probe, TEN);
    let mut due = Default::default();

    driver.tick(&mut due).await;

    assert_eq!(*probe.scrapes.lock(), vec!["http://h:1"]);
    assert!(probe.polls().is_empty(), "an HTTP engine has no GetLoads");
    let (scraped, source) = row(&store, id, 0).expect("the scrape's row");
    assert_eq!(
        (scraped.running, scraped.queued, scraped.kv_usage_permille),
        (3, 5, 750)
    );
    assert_eq!(
        (scraped.source, source),
        (SignalSource::Scrape, LoadSource::Scrape)
    );

    tokio::time::advance(Duration::from_millis(400)).await;
    driver.tick(&mut due).await;
    assert_eq!(
        probe.scrapes.lock().len(),
        1,
        "before the interval: no call"
    );
    tokio::time::advance(Duration::from_millis(200)).await;
    driver.tick(&mut due).await;
    assert_eq!(probe.scrapes.lock().len(), 2);
    assert_eq!(driver.stats().scrapes.load(Ordering::Relaxed), 2);
}

#[tokio::test(start_paused = true)]
async fn a_scrape_without_the_load_gauge_writes_no_row() {
    let store = Arc::new(MemoryStore::new());
    let probe = Arc::new(FakeProbe::default());
    let id = ready(
        &store,
        "http://h:1",
        "http://h:1",
        0,
        1,
        WireModes::HTTP,
        500,
    );
    probe.texts.lock().insert(
        "http://h:1".to_owned(),
        "vllm:num_requests_running{model_name=\"m\"} 3\n".to_owned(),
    );
    let driver = driver(&store, &probe, TEN);
    let mut due = Default::default();

    driver.tick(&mut due).await;

    assert!(row(&store, id, 0).is_none());
    let stats = driver.stats();
    assert_eq!(stats.scrape_failures.no_load.load(Ordering::Relaxed), 1);
    assert_eq!(stats.scrapes.load(Ordering::Relaxed), 0);
}

#[tokio::test(start_paused = true)]
async fn a_data_parallel_engine_without_a_servicer_is_scraped_once_and_every_rank_reads_its_sample()
{
    let store = Arc::new(MemoryStore::new());
    let probe = Arc::new(FakeProbe::default());
    let rank0 = ready(
        &store,
        "http://h:1@0",
        "http://h:1",
        0,
        2,
        WireModes::HTTP,
        500,
    );
    let rank1 = ready(
        &store,
        "http://h:1@1",
        "http://h:1",
        1,
        2,
        WireModes::HTTP,
        500,
    );
    probe
        .texts
        .lock()
        .insert("http://h:1".to_owned(), METRICS_TEXT.to_owned());
    let driver = driver(&store, &probe, TEN);
    let mut due = Default::default();

    driver.tick(&mut due).await;

    assert_eq!(
        *probe.scrapes.lock(),
        vec!["http://h:1"],
        "one scrape per engine"
    );
    for (id, rank) in [(rank0, 0), (rank1, 1)] {
        let (sample, source) = row(&store, id, rank).expect("every rank reads the engine's sample");
        assert_eq!(
            (sample.running, sample.kv_usage_permille),
            (3, 750),
            "rank {rank}"
        );
        assert_eq!(source, LoadSource::Scrape);
    }
    assert_eq!(driver.stats().rows.load(Ordering::Relaxed), 2);

    // A rank whose own records arrive keeps them; the other keeps reading
    // the engine's sample.
    let own = LoadRow {
        running: 1,
        source: SignalSource::Events,
        ..LoadRow::default()
    };
    store.record(rank0, 0, own, now(), LoadSource::Stream);
    tokio::time::advance(Duration::from_millis(500)).await;
    driver.tick(&mut due).await;
    assert_eq!(probe.scrapes.lock().len(), 2);
    assert_eq!(row(&store, rank0, 0), Some((own, LoadSource::Stream)));
    assert_eq!(row(&store, rank1, 1).map(|(r, _)| r.running), Some(3));
}

#[tokio::test(start_paused = true)]
async fn an_engine_with_a_servicer_is_polled_not_scraped() {
    let store = Arc::new(MemoryStore::new());
    let probe = Arc::new(FakeProbe::default());
    ready(
        &store,
        "grpc://a:1",
        "grpc://a:1",
        0,
        1,
        WireModes::GRPC.union(WireModes::HTTP),
        500,
    );
    probe.answers(
        "grpc://a:1",
        &[Figures {
            running: 4,
            waiting: 0,
            usage: 0.4,
        }],
    );
    let driver = driver(&store, &probe, TEN);
    let mut due = Default::default();

    driver.tick(&mut due).await;

    assert_eq!(probe.polls(), vec!["grpc://a:1"]);
    assert!(
        probe.scrapes.lock().is_empty(),
        "the scrape stands in for a servicer alone"
    );
}

#[tokio::test(start_paused = true)]
async fn an_engine_that_left_is_forgotten_by_the_probe() {
    let store = Arc::new(MemoryStore::new());
    let probe = Arc::new(FakeProbe::default());
    let id = ready(&store, "grpc://a:1", "grpc://a:1", 0, 1, WireModes::GRPC, 0);
    let driver = driver(&store, &probe, TEN);
    let mut due = Default::default();
    driver.tick(&mut due).await;
    assert!(probe.forgotten.lock().is_empty());

    let revision = store.get(id).expect("the row").revision;
    store.remove(id, revision);
    tokio::time::advance(Duration::from_secs(1)).await;
    driver.tick(&mut due).await;

    assert_eq!(*probe.forgotten.lock(), vec!["grpc://a:1"]);
    assert!(due.is_empty());
}

#[tokio::test(start_paused = true)]
async fn run_ticks_until_told_to_stop() {
    let store = Arc::new(MemoryStore::new());
    let probe = Arc::new(FakeProbe::default());
    ready(&store, "grpc://a:1", "grpc://a:1", 0, 1, WireModes::GRPC, 0);
    let driver = Arc::new(driver(&store, &probe, TEN));
    let (stop, stopped) = watch::channel(false);
    let mut tasks = JoinSet::new();
    let running = Arc::clone(&driver);
    tasks.spawn(async move { running.run(stopped).await });

    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(
        probe.polls().len(),
        1,
        "the first tick polled, the next ones waited"
    );
    stop.send_replace(true);
    tasks
        .join_next()
        .await
        .expect("the run task")
        .expect("it ended without a panic");
    assert!(tasks.is_empty());
}
