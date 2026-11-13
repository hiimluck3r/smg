//! The load path driver (design 5.1, the loads row; 6, the ingestion): the
//! `GetLoads` poll of every engine whose stream is not serving it, under
//! the store's poll arbitration, and the metrics scrape of an engine
//! without a servicer, both written to the store's [`LoadSink`].
//!
//! One scheduler tick per second reads the membership (regrouped only when
//! the store's epoch moved) and runs what is due: an engine's poll every
//! `poll_interval`, its scrape every `scrape_interval_ms` of its row. A poll
//! asks the engine once, at the engine's own address, and writes one row per
//! rank whose own records are not serving it, each rank's figures scoped
//! from the answer. A scrape asks the engine's metrics once and writes the
//! engine's sample (the metrics aggregate across its ranks) to every rank
//! row without a fresher sample of its own: no synthetic rank, no rank left
//! without a row. A failed read writes nothing: the row on file ages, and
//! health is the prober's (a later PR). A row reachable only through a
//! gateway is never dialed: its records arrive relayed. The engines are
//! asked through a [`LoadProbe`], so the tests drive the arbitration without
//! a socket and the binary dials the engines.

use std::{
    collections::{BTreeMap, HashSet},
    future::Future,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use openai_protocol::worker::{RuntimeType, WorkerLoadResponse};
use parking_lot::Mutex;
use tokio::{sync::watch, task::JoinSet, time::MissedTickBehavior};
use tracing::{debug, warn};

use super::{rows::load_row, scrape};
use crate::{
    store::{LoadSink, LoadSource, PollMode, Reachability, WorkerRow, WorkerStatus, WorkersStore},
    view::{Role, SignalSource, WireModes, WorkerId},
};

/// The driver's timings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LoadDriverConfig {
    /// How often an engine is polled while its stream is not serving it;
    /// also the freshness bound of the arbitration (a rank whose newest
    /// record is older than this is polled). The gateway's
    /// `--load-monitor-interval`, 10 s.
    pub poll_interval: Duration,
    /// The bound on one poll, dial included, so one half-open connection
    /// cannot stall a tick. A poll past it is a slow engine, not a dead one.
    pub poll_deadline: Duration,
    /// The bound on one scrape.
    pub scrape_timeout: Duration,
    /// The scheduler's tick: the resolution of every interval above.
    pub tick: Duration,
}

impl Default for LoadDriverConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(10),
            poll_deadline: Duration::from_secs(3),
            scrape_timeout: Duration::from_secs(5),
            tick: Duration::from_secs(1),
        }
    }
}

/// Where a poll goes: the engine's dialable address and its family, which
/// picks the gRPC client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Target {
    /// The engine's address as the store holds it (`WorkerFacts::engine`,
    /// one per engine and shared by the rows of its ranks), which answers
    /// once for every rank. A rank row's own key is not dialed.
    pub endpoint: String,
    pub kind: RuntimeType,
}

/// Why a read produced no report. The variant is the failure's label.
#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    /// The dial failed or the connection was lost.
    #[error("transport: {0}")]
    Transport(String),
    /// The engine answered the call with a status.
    #[error("the engine answered: {0}")]
    Answer(String),
    /// The deadline passed.
    #[error("no answer within {0:?}")]
    Deadline(Duration),
    /// The metrics body carried no load for this family.
    #[error("no load in the metrics of a {0} engine")]
    NoLoad(String),
    /// The family has no `GetLoads`.
    #[error("no GetLoads on a {0} engine")]
    Unsupported(String),
}

impl ReadError {
    /// The failure's label, for the counters.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Transport(_) => "transport",
            Self::Answer(_) => "answer",
            Self::Deadline(_) => "deadline",
            Self::NoLoad(_) => "no_load",
            Self::Unsupported(_) => "unsupported",
        }
    }
}

/// What the driver asks of an engine.
pub trait LoadProbe: Send + Sync + 'static {
    /// The engine's `GetLoads` answer, every rank as the engine reports it,
    /// within `deadline`.
    fn poll(
        &self,
        target: &Target,
        deadline: Duration,
    ) -> impl Future<Output = Result<WorkerLoadResponse, ReadError>> + Send;

    /// The text of the engine's metrics endpoint, within `timeout`.
    fn scrape(
        &self,
        url: &str,
        timeout: Duration,
    ) -> impl Future<Output = Result<String, ReadError>> + Send;

    /// The engine left the membership: whatever the probe holds for it goes.
    fn forget(&self, endpoint: &str);
}

/// What the driver did, as counters the metrics render.
#[derive(Debug, Default)]
pub struct Stats {
    /// Polls decided `poll` (no record on file).
    pub polls: AtomicU64,
    /// Polls decided `fallback` (a rank's records went quiet).
    pub fallbacks: AtomicU64,
    /// Ticks an engine was asked and skipped because every rank's stream
    /// was fresh.
    pub suppressed: AtomicU64,
    /// Polls that produced no report, by label.
    pub poll_failures: Failures,
    /// Scrapes that produced a row.
    pub scrapes: AtomicU64,
    /// Scrapes that produced no row, by label.
    pub scrape_failures: Failures,
    /// Rank rows handed to the store by polls and scrapes. A rank whose own
    /// record arrived while the read was out keeps that record, so the rows
    /// on file can be fewer.
    pub rows: AtomicU64,
}

/// Failed reads by [`ReadError::label`].
#[derive(Debug, Default)]
pub struct Failures {
    pub transport: AtomicU64,
    pub answer: AtomicU64,
    pub deadline: AtomicU64,
    pub no_load: AtomicU64,
    pub unsupported: AtomicU64,
}

impl Failures {
    /// The labels, in the order of [`Self::each`]: the one table the error,
    /// the counters and the metrics share.
    pub const LABELS: [&'static str; 5] =
        ["transport", "answer", "deadline", "no_load", "unsupported"];

    /// The counters, in the order of [`Self::LABELS`].
    fn counters(&self) -> [&AtomicU64; 5] {
        [
            &self.transport,
            &self.answer,
            &self.deadline,
            &self.no_load,
            &self.unsupported,
        ]
    }

    fn count(&self, error: &ReadError) {
        let label = error.label();
        for (counter, name) in self.counters().into_iter().zip(Self::LABELS) {
            if name == label {
                counter.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// The counts, with their labels.
    pub fn each(&self) -> [(&'static str, u64); 5] {
        let mut out = [("", 0); 5];
        for (slot, (counter, name)) in out
            .iter_mut()
            .zip(self.counters().into_iter().zip(Self::LABELS))
        {
            *slot = (name, counter.load(Ordering::Relaxed));
        }
        out
    }
}

/// The driver over a store and a probe.
pub struct LoadDriver<S, P> {
    store: Arc<S>,
    probe: Arc<P>,
    config: LoadDriverConfig,
    stats: Arc<Stats>,
    /// The membership grouped by engine, kept until the store's epoch moves.
    grouped: Mutex<Option<Grouped>>,
}

/// The engines of one membership epoch.
struct Grouped {
    epoch: u64,
    engines: Arc<Vec<Engine>>,
}

/// One engine's rows as the driver reads them from the membership.
#[derive(Debug)]
struct Engine {
    key: String,
    role: Role,
    /// The family, when the handshake named one the host knows.
    kind: Option<RuntimeType>,
    /// The engine's data-parallel width from its rows' facts: what decides
    /// whether an answer is scoped per rank, whatever rows are ready.
    dp_size: u16,
    /// Where a poll goes: the engine's address, when a ready row every
    /// gateway dials speaks gRPC.
    poll: Option<Target>,
    /// Where a scrape goes, and how often: an HTTP engine without a poll (no
    /// servicer) with a scrape interval on its row.
    scrape: Option<(String, Duration)>,
    /// The ready rows, by rank.
    ranks: Vec<(u16, WorkerId)>,
}

/// When an engine's next poll and scrape are due.
struct Due {
    poll: tokio::time::Instant,
    scrape: tokio::time::Instant,
}

/// The outcome of one tick's call, applied under the loop.
enum Done {
    Poll {
        engine: usize,
        /// The ranks the arbitration sent the poll for.
        ranks: Vec<(u16, WorkerId)>,
        /// When the call began, for the store's pushed-since rule.
        polled_at: Instant,
        answer: Result<WorkerLoadResponse, ReadError>,
    },
    Scrape {
        engine: usize,
        polled_at: Instant,
        text: Result<String, ReadError>,
    },
}

/// The host's clock through tokio's, so a test under paused time moves
/// every instant the store compares.
fn now() -> Instant {
    tokio::time::Instant::now().into_std()
}

impl<S, P> LoadDriver<S, P>
where
    S: WorkersStore + LoadSink + 'static,
    P: LoadProbe,
{
    pub fn new(store: Arc<S>, probe: Arc<P>, config: LoadDriverConfig) -> Self {
        Self {
            store,
            probe,
            config,
            stats: Arc::new(Stats::default()),
            grouped: Mutex::new(None),
        }
    }

    /// The counters, shared with whoever renders them.
    pub fn stats(&self) -> Arc<Stats> {
        Arc::clone(&self.stats)
    }

    /// Ticks until `stop` reads `true`. A tick's calls complete within the
    /// tick (each under its deadline); a tick that runs long delays the
    /// next, it never doubles.
    pub async fn run(&self, mut stop: watch::Receiver<bool>) {
        let mut ticker = tokio::time::interval(self.config.tick);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut due: BTreeMap<String, Due> = BTreeMap::new();
        loop {
            // The stop first: after a long tick both arms are ready, and
            // one more tick would run otherwise.
            tokio::select! {
                biased;
                _ = stop.wait_for(|stop| *stop) => return,
                _ = ticker.tick() => {}
            }
            self.tick(&mut due).await;
        }
    }

    /// The membership grouped by engine: the snapshot is read and regrouped
    /// only when the store's epoch moved since the last tick (every write,
    /// a status change included, moves it).
    fn engines(&self) -> Arc<Vec<Engine>> {
        let epoch = self.store.epoch();
        let mut grouped = self.grouped.lock();
        if let Some(kept) = grouped.as_ref() {
            if kept.epoch == epoch {
                return Arc::clone(&kept.engines);
            }
        }
        let (epoch, rows) = self.store.snapshot();
        let engines = Arc::new(engines(rows));
        *grouped = Some(Grouped {
            epoch,
            engines: Arc::clone(&engines),
        });
        engines
    }

    /// One tick: the membership read, the due calls run together, their
    /// answers written.
    async fn tick(&self, due: &mut BTreeMap<String, Due>) {
        let engines = self.engines();
        let tick_at = tokio::time::Instant::now();
        let live: HashSet<&str> = engines.iter().map(|engine| engine.key.as_str()).collect();
        due.retain(|key, _| {
            let kept = live.contains(key.as_str());
            if !kept {
                self.probe.forget(key);
            }
            kept
        });
        let mut calls: JoinSet<Done> = JoinSet::new();
        for (index, engine) in engines.iter().enumerate() {
            let slot = due.entry(engine.key.clone()).or_insert(Due {
                poll: tick_at,
                scrape: tick_at,
            });
            if let Some(target) = &engine.poll {
                // The arbitration is asked at every tick the poll is due and
                // the due instant moves only when a call goes: an engine whose
                // ranks are all served by their stream is asked again at the
                // next tick, so a rank whose records stop is polled within
                // one tick of the freshness bound, not one interval later.
                if tick_at >= slot.poll {
                    if let Some(ranks) = self.ranks_to_poll(engine) {
                        slot.poll = tick_at + self.config.poll_interval;
                        let probe = Arc::clone(&self.probe);
                        let target = target.clone();
                        let deadline = self.config.poll_deadline;
                        let polled_at = now();
                        calls.spawn(async move {
                            let answer = probe.poll(&target, deadline).await;
                            Done::Poll {
                                engine: index,
                                ranks,
                                polled_at,
                                answer,
                            }
                        });
                    }
                }
            }
            if let Some((url, every)) = &engine.scrape {
                if tick_at >= slot.scrape {
                    slot.scrape = tick_at + *every;
                    let probe = Arc::clone(&self.probe);
                    let url = url.clone();
                    let timeout = self.config.scrape_timeout;
                    let polled_at = now();
                    calls.spawn(async move {
                        let text = probe.scrape(&url, timeout).await;
                        Done::Scrape {
                            engine: index,
                            polled_at,
                            text,
                        }
                    });
                }
            }
        }
        while let Some(joined) = calls.join_next().await {
            match joined {
                Ok(done) => self.apply(&engines, done),
                Err(join) => warn!(%join, "a load read ended abnormally"),
            }
        }
    }

    /// The ranks a poll is for: every ready rank whose own records are not
    /// serving it. `None` when the stream serves them all, and no call goes.
    fn ranks_to_poll(&self, engine: &Engine) -> Option<Vec<(u16, WorkerId)>> {
        let now = now();
        let mut wanted = Vec::with_capacity(engine.ranks.len());
        let mut fallback = false;
        for &(rank, id) in &engine.ranks {
            match self.store.poll_mode(id, now, self.config.poll_interval) {
                PollMode::Suppressed => {}
                PollMode::Fallback => {
                    fallback = true;
                    wanted.push((rank, id));
                }
                PollMode::Poll => wanted.push((rank, id)),
            }
        }
        if wanted.is_empty() {
            self.stats.suppressed.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let counter = if fallback {
            &self.stats.fallbacks
        } else {
            &self.stats.polls
        };
        counter.fetch_add(1, Ordering::Relaxed);
        Some(wanted)
    }

    /// Writes one call's answer: a poll's rows for the ranks it was for, a
    /// scrape's sample for the ranks without a fresher one of their own; a
    /// failure counts and writes nothing.
    fn apply(&self, engines: &[Engine], done: Done) {
        match done {
            Done::Poll {
                engine,
                ranks,
                polled_at,
                answer,
            } => {
                let Some(engine) = engines.get(engine) else {
                    return;
                };
                let report = match answer {
                    Ok(report) => report,
                    Err(error) => {
                        debug!(engine = %engine.key, %error, "the load poll produced no report");
                        self.stats.poll_failures.count(&error);
                        return;
                    }
                };
                let received = now();
                // A data-parallel engine's answer is scoped to each rank,
                // whatever rows are ready; a rank the answer omits is
                // unavailable: nothing is written for it, never another
                // rank's figures.
                let parallel = engine.dp_size > 1;
                for (rank, id) in ranks {
                    let Some(scoped) = report
                        .clone()
                        .for_dp_rank(parallel.then_some(usize::from(rank)))
                    else {
                        continue;
                    };
                    let row =
                        load_row(&scoped, engine.role, received, received, SignalSource::Poll);
                    self.store
                        .report(id, &[(rank, row)], polled_at, LoadSource::Poll);
                    self.stats.rows.fetch_add(1, Ordering::Relaxed);
                }
            }
            Done::Scrape {
                engine,
                polled_at,
                text,
            } => {
                let Some(engine) = engines.get(engine) else {
                    return;
                };
                let kind = engine.kind.unwrap_or_default();
                let snapshot = text.and_then(|text| {
                    scrape::snapshot(kind, &text)
                        .ok_or_else(|| ReadError::NoLoad(kind.as_str().to_owned()))
                });
                let snapshot = match snapshot {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        debug!(engine = %engine.key, %error, "the scrape produced no row");
                        self.stats.scrape_failures.count(&error);
                        return;
                    }
                };
                self.stats.scrapes.fetch_add(1, Ordering::Relaxed);
                let received = now();
                let response = scrape::single_rank(snapshot);
                // The gauge tables carry no rank series, so the metrics
                // aggregate across the ranks is the engine's sample: every
                // rank row whose own records are not serving it reads it. A
                // rank served by its stream keeps its record.
                for &(rank, id) in &engine.ranks {
                    if self
                        .store
                        .poll_mode(id, received, self.config.poll_interval)
                        == PollMode::Suppressed
                    {
                        continue;
                    }
                    let row = load_row(
                        &response,
                        engine.role,
                        received,
                        received,
                        SignalSource::Scrape,
                    );
                    self.store
                        .report(id, &[(rank, row)], polled_at, LoadSource::Scrape);
                    self.stats.rows.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}

/// The engines of the membership: the ready rows grouped by their engine
/// key, each with where its poll and its scrape go.
fn engines(rows: Vec<WorkerRow>) -> Vec<Engine> {
    let mut by_key: BTreeMap<String, Engine> = BTreeMap::new();
    // The scrape candidates (engine key, interval), settled once every row is
    // read: the scrape stands in for a servicer, so an engine with a poll
    // has none.
    let mut scrapes: BTreeMap<String, u32> = BTreeMap::new();
    for row in rows {
        if row.status != WorkerStatus::Ready {
            continue;
        }
        let facts = row.facts;
        let engine = by_key
            .entry(facts.engine.clone())
            .or_insert_with(|| Engine {
                key: facts.engine.clone(),
                role: facts.role,
                kind: facts.engine_kind.parse().ok(),
                dp_size: 1,
                poll: None,
                scrape: None,
                ranks: Vec::new(),
            });
        engine.dp_size = engine.dp_size.max(facts.dp_size);
        let dialable = facts.reachable_from == Reachability::Any;
        if engine.poll.is_none() && dialable && facts.wire_modes.contains(WireModes::GRPC) {
            engine.poll = Some(Target {
                endpoint: facts.engine.clone(),
                kind: engine.kind.unwrap_or_default(),
            });
        }
        if dialable
            && facts.wire_modes.contains(WireModes::HTTP)
            && facts.signals.scrape_interval_ms > 0
        {
            scrapes
                .entry(facts.engine.clone())
                .or_insert(facts.signals.scrape_interval_ms);
        }
        engine.ranks.push((facts.dp_rank, row.id));
    }
    let mut engines: Vec<Engine> = by_key.into_values().collect();
    for engine in &mut engines {
        engine.ranks.sort_unstable();
        if engine.poll.is_none() {
            if let Some(every) = scrapes.remove(&engine.key) {
                engine.scrape = Some((engine.key.clone(), Duration::from_millis(u64::from(every))));
            }
        }
    }
    engines
}

#[cfg(test)]
mod tests;
