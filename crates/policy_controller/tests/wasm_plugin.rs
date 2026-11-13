//! The WebAssembly plugin host against the test guests: what loads and what
//! is refused, the honest pick through both worlds, the four host imports,
//! and the failure rules (a decline keeps the instance, a trap, an exhausted
//! budget or an invalid answer drops it and counts on the breaker, the
//! breaker bypasses, the fallbacks and the closed policy), one resource per
//! decision and per side of a pair, the lifecycle once per profile, and the
//! instances kept per thread.
#![cfg(feature = "wasm")]
#![expect(
    clippy::expect_used,
    reason = "the guests' assembly and the loads run inside the tests; a failure there is the test's failure"
)]

mod support {
    pub mod wasm_guest;
}

use std::sync::Arc;

use smg_policy_controller::{
    builtins::{Best, Healthy, LeastRequests},
    wasm::{
        Abi, BreakerSpec, Failure, LoadError, Plugin, PluginSpec, Reveal, Runtime, WasmFilter,
        WasmPicker, WasmScorer, World,
    },
    Binding, Candidate, Constraints, Decision, Draw, Engine, Health, Lease, Lifecycle, Needs,
    PairPools, Pick, Plan, PoolFacts, Profile, RequestFacts, Scratch, ShedReason, Side, Target,
    Weighted, WorkerId,
};
use support::wasm_guest::{self as guest, canonical, counter, mode};
use wit_component::{ComponentEncoder, StringEncoding};
use wit_parser::Resolve;

const FRAME_MICROS: u32 = 10_000;
const COUNTERS: [&str; 6] = [
    "observed",
    "now",
    "random",
    "observe-now",
    "observe-session",
    "pick-random",
];

fn runtime() -> Arc<Runtime> {
    Arc::new(Runtime::new().expect("a runtime"))
}

fn spec(name: &str, component: Vec<u8>, mode: u8) -> PluginSpec {
    let mut spec = PluginSpec::new(name, component);
    spec.config = vec![mode];
    spec.budget_micros = FRAME_MICROS;
    spec.metrics = COUNTERS.iter().map(|s| s.to_string()).collect();
    spec
}

fn load(runtime: &Arc<Runtime>, name: &str, component: Vec<u8>, mode: u8) -> Arc<Plugin> {
    Plugin::load(runtime, spec(name, component, mode)).expect("the guest loads")
}

/// A profile whose three stages are the plugin's, each with the given
/// fallback.
fn plugin_profile(
    plugin: &Arc<Plugin>,
    filter_fallback: Option<Box<dyn smg_policy_controller::Filter>>,
    scorer_fallback: Option<Box<dyn smg_policy_controller::Scorer>>,
    picker_fallback: Option<Box<dyn smg_policy_controller::Picker>>,
) -> Profile {
    let bound = plugin.bind();
    Profile {
        name: "plugin".into(),
        filters: vec![Box::new(WasmFilter::new(
            Arc::clone(&bound),
            filter_fallback,
        ))],
        scorers: vec![Weighted {
            scorer: Box::new(WasmScorer::new(Arc::clone(&bound), scorer_fallback)),
            weight: 1.0,
        }],
        picker: Box::new(WasmPicker::new(bound, picker_fallback)),
    }
}

fn single_engine(profile: Profile) -> Engine {
    Engine::new(
        vec![profile],
        vec![Binding {
            model: "*".into(),
            phases: vec![],
            plan: Plan::Single,
            profile: 0,
            decode_profile: None,
            sessions: None,
        }],
        1,
        Needs::default(),
    )
}

fn plugin_engine(plugin: &Arc<Plugin>) -> Engine {
    single_engine(plugin_profile(plugin, None, None, None))
}

/// `n` healthy candidates whose in-flight counts descend, so the least
/// loaded is the last one.
fn pool(n: u32) -> Vec<Candidate> {
    (0..n)
        .map(|i| Candidate {
            position: i,
            id: WorkerId(u64::from(i) + 100),
            health: Health {
                healthy: true,
                ..Health::default()
            },
            in_flight: n - i,
            ..Candidate::default()
        })
        .collect()
}

fn facts(seed: u64) -> RequestFacts<'static> {
    RequestFacts {
        model: "m",
        budget_micros: FRAME_MICROS,
        seed,
        now_micros: 777_000,
        ..RequestFacts::default()
    }
}

fn decide(engine: &Engine, candidates: &[Candidate], seed: u64) -> Decision {
    let pool_facts = PoolFacts {
        pool_size: candidates.len() as u32,
        generation: 1,
        ..PoolFacts::default()
    };
    engine.decide(
        &facts(seed),
        &pool_facts,
        candidates,
        &Constraints::default(),
        &mut Scratch::new(),
    )
}

/// The targets' ids; a shed or a queue has none, so an assert reads `[]`.
fn pick_ids(decision: &Decision) -> Vec<u64> {
    decision
        .pick()
        .map(|pick| pick.targets.iter().map(|t| t.id.0).collect())
        .unwrap_or_default()
}

fn counter_value(plugin: &Plugin, id: u32) -> u64 {
    plugin
        .counters()
        .read()
        .nth(id as usize)
        .map(|(_, v)| v)
        .expect("a declared counter")
}

#[test]
fn the_record_guests_offsets_match_the_canonical_layout() {
    guest::check_canonical_layout();
    assert_eq!(canonical::CANDIDATE_SIZE % 8, 0);
}

#[test]
fn both_worlds_load_and_the_load_instantiates_on_the_applying_thread() {
    let runtime = runtime();
    let record = load(
        &runtime,
        "record",
        guest::record_guest(),
        mode::LEAST_IN_FLIGHT,
    );
    let packed = load(
        &runtime,
        "packed",
        guest::packed_guest(),
        mode::LEAST_IN_FLIGHT,
    );
    assert_eq!(record.world(), World::Record);
    assert_eq!(packed.world(), World::Packed);
    assert_eq!(record.abi(), Abi::V1_0);
    assert!(record.is_warm() && packed.is_warm());
    assert_eq!(record.stats().instantiations, 1);
    assert_eq!(record.name(), "record");
}

#[test]
fn a_wasi_import_is_refused_at_load() {
    let runtime = runtime();
    let err = Plugin::load(
        &runtime,
        spec("wasi", guest::wasi_importing_guest(), mode::LEAST_IN_FLIGHT),
    )
    .expect_err("refused");
    assert!(
        matches!(&err, LoadError::Wasi { import, .. } if import.starts_with("wasi:clocks/monotonic-clock")),
        "{err}"
    );
}

#[test]
fn a_component_of_another_world_has_no_policy_world() {
    let mut resolve = Resolve::new();
    let package = resolve
        .push_str("empty.wit", "package test:empty;\nworld nothing {}")
        .expect("the world parses");
    let world = resolve
        .select_world(&[package], Some("nothing"))
        .expect("the world exists");
    let mut module = wat::parse_str("(module)").expect("assembles");
    wit_component::embed_component_metadata(&mut module, &resolve, world, StringEncoding::UTF8)
        .expect("embeds");
    let component = ComponentEncoder::default()
        .module(&module)
        .expect("matches")
        .validate(true)
        .encode()
        .expect("encodes");
    let err = Plugin::load(&runtime(), spec("empty", component, 0)).expect_err("refused");
    assert_eq!(
        err,
        LoadError::NoWorld {
            name: "empty".into()
        }
    );
}

#[test]
fn the_abi_pin_the_memory_floor_the_init_failure_and_bad_bytes_are_refused() {
    let runtime = runtime();
    let mut pinned = spec("pinned", guest::record_guest(), mode::LEAST_IN_FLIGHT);
    pinned.abi = Some(Abi { major: 1, minor: 1 });
    let err = Plugin::load(&runtime, pinned).expect_err("refused");
    assert!(
        matches!(
            err,
            LoadError::AbiPin {
                found: Abi::V1_0,
                ..
            }
        ),
        "{err}"
    );

    let mut no_memory = spec("tiny", guest::record_guest(), mode::LEAST_IN_FLIGHT);
    no_memory.memory_mib = 0;
    assert!(matches!(
        Plugin::load(&runtime, no_memory).expect_err("refused"),
        LoadError::Memory { .. }
    ));

    let mut unfuelled = spec("unfuelled", guest::record_guest(), mode::LEAST_IN_FLIGHT);
    unfuelled.fuel_per_micro = 0;
    assert!(matches!(
        Plugin::load(&runtime, unfuelled).expect_err("refused"),
        LoadError::Fuel { .. }
    ));

    let err = Plugin::load(
        &runtime,
        spec("refusing", guest::record_guest(), mode::REFUSE_INIT),
    )
    .expect_err("init failed");
    assert_eq!(
        err,
        LoadError::Init {
            name: "refusing".into(),
            reason: "refused".into()
        }
    );

    let err = Plugin::load(&runtime, spec("bytes", b"not wasm".to_vec(), 0)).expect_err("refused");
    assert!(matches!(err, LoadError::Compile { .. }), "{err}");

    let specs = [
        spec("a", Vec::new(), 0),
        spec("b", Vec::new(), 0),
        spec("c", Vec::new(), 0),
    ];
    assert_eq!(
        Plugin::check_memory_budget(&specs, 47),
        Err(LoadError::MemoryBudget {
            asked: 48,
            budget: 47
        })
    );
    assert_eq!(Plugin::check_memory_budget(&specs, 48), Ok(()));
}

#[test]
fn the_honest_guest_picks_the_least_in_flight_through_either_world() {
    let runtime = runtime();
    for (name, component) in [
        ("record", guest::record_guest()),
        ("packed", guest::packed_guest()),
    ] {
        let plugin = load(&runtime, name, component, mode::LEAST_IN_FLIGHT);
        let engine = plugin_engine(&plugin);
        let candidates = pool(8);
        let decision = decide(&engine, &candidates, 5);
        let ids = pick_ids(&decision);
        assert_eq!(
            ids[0], 107,
            "{name}: the last row has one request in flight"
        );
        assert_eq!(
            ids,
            vec![107, 106, 105, 104],
            "{name}: the alternates follow by score"
        );
        let Some(pick) = decision.pick() else {
            panic!("a pick, not {decision:?}")
        };
        assert_eq!(pick.trace.survivors, 8);
        let stats = plugin.stats();
        assert_eq!(
            stats.answered, 4,
            "{name}: the constructor, filter, score and pick"
        );
        assert_eq!(
            (
                stats.declined,
                stats.traps,
                stats.fuel_exhausted,
                stats.invalid
            ),
            (0, 0, 0, 0)
        );
        assert!(stats.fuel_used > 0);
        // The host imports: the decision's clock and its seed's draw.
        assert_eq!(counter_value(&plugin, counter::NOW), 777_000, "{name}");
        assert_eq!(
            counter_value(&plugin, counter::RANDOM),
            Draw::new(5).next_u64(),
            "{name}"
        );
        assert_eq!(plugin.counters().undeclared(), 0);
    }
}

#[test]
fn a_decision_on_a_kept_instance_opens_one_resource_and_leaves_no_instance_behind() {
    let runtime = runtime();
    let plugin = load(&runtime, "p", guest::record_guest(), mode::LEAST_IN_FLIGHT);
    let engine = plugin_engine(&plugin);
    let candidates = pool(4);
    for seed in 0..50 {
        decide(&engine, &candidates, seed);
    }
    let stats = plugin.stats();
    assert_eq!(
        stats.instantiations, 1,
        "one kept instance for fifty decisions"
    );
    assert_eq!(stats.answered, 200);
    assert!(plugin.is_warm());
}

#[test]
fn the_filters_answer_narrows_the_pool_once_per_decision_and_row_by_row() {
    let runtime = runtime();
    let plugin = load(&runtime, "even", guest::record_guest(), mode::KEEP_EVEN);
    let engine = plugin_engine(&plugin);
    let candidates = pool(8);
    let decision = decide(&engine, &candidates, 1);
    assert_eq!(pick_ids(&decision), vec![106, 104, 102, 100]);
    let Some(pick) = decision.pick() else {
        panic!("a pick, not {decision:?}")
    };
    assert_eq!(pick.trace.survivors, 4);
    assert_eq!(plugin.stats().answered, 4, "filter asked once");
}

#[test]
fn a_declined_filter_keeps_the_instance_and_takes_its_fallback() {
    let runtime = runtime();
    let plugin = load(
        &runtime,
        "declining",
        guest::record_guest(),
        mode::DECLINE_FILTER,
    );
    let engine = single_engine(plugin_profile(&plugin, Some(Box::new(Healthy)), None, None));
    let mut candidates = pool(4);
    candidates[3].health.healthy = false;
    let decision = decide(&engine, &candidates, 1);
    assert_eq!(
        pick_ids(&decision),
        vec![102, 101, 100],
        "the fallback dropped the unhealthy best row"
    );
    let stats = plugin.stats();
    assert_eq!(stats.declined, 1);
    assert_eq!(stats.instantiations, 1, "a decline keeps the instance");
    assert!(plugin.is_warm());

    // Without a fallback: open keeps every row, closed empties the pool.
    let open = load(
        &runtime,
        "open",
        guest::record_guest(),
        mode::DECLINE_FILTER,
    );
    assert_eq!(
        pick_ids(&decide(&plugin_engine(&open), &candidates, 1)),
        vec![103, 102, 101, 100]
    );
    let mut closed = spec("closed", guest::record_guest(), mode::DECLINE_FILTER);
    closed.failure = Failure::Closed;
    let closed = Plugin::load(&runtime, closed).expect("loads");
    match decide(&plugin_engine(&closed), &candidates, 1) {
        Decision::Shed(shed) => {
            assert_eq!(shed.reason, ShedReason::NoEndpoints);
            assert_eq!(shed.emptied_by, "closed");
        }
        other => panic!("a shed, not {other:?}"),
    }
}

#[test]
fn a_trap_drops_the_instance_and_the_close_rebuilds_it() {
    let runtime = runtime();
    let plugin = load(
        &runtime,
        "trapping",
        guest::record_guest(),
        mode::TRAP_IN_FILTER,
    );
    let engine = plugin_engine(&plugin);
    let candidates = pool(4);
    let decision = decide(&engine, &candidates, 1);
    let mut ids = pick_ids(&decision);
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec![100, 101, 102, 103],
        "the filter fell back to keep-all; the flat scores tie and the seed orders them"
    );
    let stats = plugin.stats();
    assert_eq!(stats.traps, 1);
    assert!(
        plugin.is_warm(),
        "the faulted instance went and the close rebuilt the slot"
    );
    assert_eq!((stats.instantiations, stats.lazy_instantiations), (2, 1));
    decide(&engine, &candidates, 2);
    let stats = plugin.stats();
    assert_eq!(
        stats.instantiations, 3,
        "rebuilt after each fault, never reused"
    );
    assert_eq!(stats.lazy_instantiations, 2);
    assert_eq!(stats.traps, 2);
}

#[test]
fn an_exhausted_budget_is_a_fuel_fault_and_the_scorer_falls_back() {
    let runtime = runtime();
    let mut spinning = spec("spinning", guest::record_guest(), mode::SPIN_IN_SCORE);
    spinning.budget_micros = 200;
    let plugin = Plugin::load(&runtime, spinning).expect("loads");
    let engine = single_engine(plugin_profile(
        &plugin,
        None,
        Some(Box::new(LeastRequests)),
        None,
    ));
    let candidates = pool(4);
    let decision = decide(&engine, &candidates, 1);
    assert_eq!(
        pick_ids(&decision).first().copied(),
        Some(103),
        "the fallback scorer decided"
    );
    let stats = plugin.stats();
    assert_eq!(stats.fuel_exhausted, 1);
    assert_eq!(stats.traps, 0);
    assert!(stats.fuel_used >= 200 * PluginSpec::DEFAULT_FUEL_PER_MICRO);
    assert!(
        plugin.is_warm(),
        "the faulted instance went and the close rebuilt the slot"
    );
}

#[test]
fn an_invalid_answer_is_a_fault_of_the_plugin() {
    let runtime = runtime();
    let cases = [
        (mode::FILTER_OUT_OF_RANGE, "out of range"),
        (mode::FILTER_DUPLICATE, "duplicate"),
        (mode::NAN_SCORE, "nan"),
        (mode::PICK_FILTERED_OUT, "filtered out"),
    ];
    for (mode, name) in cases {
        let plugin = load(&runtime, name, guest::record_guest(), mode);
        let engine = plugin_engine(&plugin);
        let decision = decide(&engine, &pool(4), 1);
        assert!(
            matches!(decision, Decision::Pick(_)),
            "{name}: the fallbacks decide"
        );
        let stats = plugin.stats();
        assert_eq!(stats.invalid, 1, "{name}");
        assert!(
            plugin.is_warm(),
            "{name}: the instance is dropped and the close rebuilds the slot"
        );
        assert_eq!(plugin.stats().lazy_instantiations, 1, "{name}");
    }
}

#[test]
fn a_declined_pick_takes_the_fallback_picker_or_sheds_when_closed() {
    let runtime = runtime();
    let plugin = load(
        &runtime,
        "no-pick",
        guest::record_guest(),
        mode::DECLINE_PICK,
    );
    let engine = single_engine(plugin_profile(&plugin, None, None, Some(Box::new(Best))));
    assert_eq!(
        pick_ids(&decide(&engine, &pool(4), 1)).first().copied(),
        Some(103)
    );
    assert_eq!(plugin.stats().declined, 1);

    let mut closed = spec("closed-pick", guest::record_guest(), mode::DECLINE_PICK);
    closed.failure = Failure::Closed;
    let plugin = Plugin::load(&runtime, closed).expect("loads");
    match decide(&plugin_engine(&plugin), &pool(4), 1) {
        Decision::Shed(shed) => {
            assert_eq!(shed.reason, ShedReason::PolicyDeclined);
            assert_eq!(shed.emptied_by, "closed-pick");
        }
        other => panic!("a shed, not {other:?}"),
    }
}

#[test]
fn the_breaker_opens_on_the_failure_threshold_and_bypasses_the_guest() {
    let runtime = runtime();
    let mut spec = spec("breaking", guest::record_guest(), mode::TRAP_IN_FILTER);
    spec.breaker = BreakerSpec {
        failures: 2,
        window_micros: 60_000_000,
        cooldown_micros: 60_000_000,
    };
    let plugin = Plugin::load(&runtime, spec).expect("loads");
    let engine = plugin_engine(&plugin);
    let candidates = pool(4);
    decide(&engine, &candidates, 1);
    decide(&engine, &candidates, 2);
    let stats = plugin.stats();
    assert_eq!(
        (stats.traps, stats.breaker_opens, stats.bypassed),
        (2, 1, 0)
    );
    assert!(matches!(decide(&engine, &candidates, 3), Decision::Pick(_)));
    let stats = plugin.stats();
    assert_eq!(
        stats.bypassed, 1,
        "the third decision never reached the guest"
    );
    assert_eq!(
        stats.instantiations, 2,
        "no instance made for a bypassed decision"
    );
    assert_eq!(stats.traps, 2);
}

#[test]
fn a_gone_budget_is_never_forwarded_and_counts_on_the_breaker() {
    let runtime = runtime();
    let plugin = load(
        &runtime,
        "budgetless",
        guest::record_guest(),
        mode::LEAST_IN_FLIGHT,
    );
    let engine = plugin_engine(&plugin);
    let candidates = pool(4);
    let pool_facts = PoolFacts {
        pool_size: 4,
        generation: 1,
        ..PoolFacts::default()
    };
    let decide_gone = |seed: u64| {
        let gone = RequestFacts {
            budget_micros: 0,
            ..facts(seed)
        };
        engine.decide(
            &gone,
            &pool_facts,
            &candidates,
            &Constraints::default(),
            &mut Scratch::new(),
        )
    };
    assert!(
        matches!(decide_gone(1), Decision::Pick(_)),
        "the fallbacks decide"
    );
    let stats = plugin.stats();
    assert_eq!((stats.answered, stats.declined), (0, 0));
    assert_eq!(
        stats.budget_gone, 1,
        "one budget hit per decision: the rest is bypassed"
    );
    assert!(plugin.is_warm(), "no churn for a request past its budget");
    plugin.forget_instance();
    decide_gone(2);
    let stats = plugin.stats();
    assert!(
        !plugin.is_warm() && stats.instantiations == 1,
        "a gone budget instantiates nothing: a slot the decision never touched stays cold for the next open: {stats:?}"
    );
    // Three more reach the default threshold of five: the breaker opens.
    for seed in 3..6 {
        decide_gone(seed);
    }
    let stats = plugin.stats();
    assert_eq!((stats.budget_gone, stats.breaker_opens), (5, 1));
    assert!(matches!(decide(&engine, &candidates, 7), Decision::Pick(_)));
    assert_eq!(
        plugin.stats().bypassed,
        1,
        "a budgeted decision inside the cool-down is bypassed"
    );
}

#[test]
fn the_packed_form_skips_a_request_it_cannot_carry() {
    let runtime = runtime();
    let plugin = load(
        &runtime,
        "packed",
        guest::packed_guest(),
        mode::LEAST_IN_FLIGHT,
    );
    let engine = plugin_engine(&plugin);
    let candidates = pool(4);
    let long = "m".repeat(70_000);
    let pool_facts = PoolFacts {
        pool_size: 4,
        generation: 1,
        ..PoolFacts::default()
    };
    let long_model = RequestFacts {
        model: &long,
        ..facts(1)
    };
    let decision = engine.decide(
        &long_model,
        &pool_facts,
        &candidates,
        &Constraints::default(),
        &mut Scratch::new(),
    );
    assert!(
        matches!(decision, Decision::Pick(_)),
        "the fallbacks decide"
    );
    let stats = plugin.stats();
    assert_eq!(stats.too_long, 1);
    assert_eq!(stats.answered, 0);
    assert!(plugin.is_warm());
}

#[test]
fn the_memory_cap_is_the_specs_and_a_growth_past_it_traps() {
    let runtime = runtime();
    let capped = load(&runtime, "capped", guest::record_guest(), mode::GROW_32_MIB);
    decide(&plugin_engine(&capped), &pool(4), 1);
    assert_eq!(
        capped.stats().traps,
        1,
        "16 MiB by default: 32 MiB more traps"
    );

    let mut roomy = spec("roomy", guest::record_guest(), mode::GROW_32_MIB);
    roomy.memory_mib = 64;
    let roomy = Plugin::load(&runtime, roomy).expect("loads");
    decide(&plugin_engine(&roomy), &pool(4), 1);
    let stats = roomy.stats();
    assert_eq!(
        (stats.traps, stats.answered),
        (0, 4),
        "under 64 MiB the growth fits"
    );
}

#[test]
fn the_lifecycle_reaches_the_plugin_once_per_profile_on_the_hosts_clock() {
    let runtime = Arc::new(Runtime::with_clock(|| 424_242).expect("a runtime"));
    let plugin = load(
        &runtime,
        "observer",
        guest::record_guest(),
        mode::LEAST_IN_FLIGHT,
    );
    let engine = plugin_engine(&plugin);
    let lease = Lease::default();
    engine.on_dispatched(&lease, &Target::default(), Side::Single, 3);
    assert_eq!(
        counter_value(&plugin, counter::OBSERVED),
        1,
        "three adapters of one profile, one observe"
    );
    assert_eq!(counter_value(&plugin, counter::OBSERVE_NOW), 424_242);
    engine.on_released(&lease, smg_policy_controller::ReleaseReason::ClientGone);
    engine.on_membership(&smg_policy_controller::Membership::Snapshot(&[
        WorkerId(1),
        WorkerId(2),
    ]));
    engine.on_signal(&smg_policy_controller::Signal::GatewayVeto {
        worker: WorkerId(1),
        until_micros: 500_000,
    });
    assert_eq!(counter_value(&plugin, counter::OBSERVED), 4);
    assert_eq!(plugin.stats().observed, 4);
}

#[test]
fn a_pair_decision_opens_one_resource_per_side() {
    let runtime = runtime();
    let plugin = load(
        &runtime,
        "pair",
        guest::record_guest(),
        mode::LEAST_IN_FLIGHT,
    );
    let prefill = plugin_profile(&plugin, None, None, None);
    let decode = plugin_profile(&plugin, None, None, None);
    let engine = pair_engine(vec![prefill, decode], 1);
    let decision = pair_decide(&engine);
    let Some(pick) = decision.pick() else {
        panic!("a pair pick, not {decision:?}")
    };
    assert_pair_pick(pick);
    let stats = plugin.stats();
    assert_eq!(
        stats.answered, 8,
        "per side: the constructor, one filter (the partner pass reads it), one score, one pick"
    );
    assert_eq!(stats.instantiations, 1);
}

/// One profile bound to both sides: the decode side opens its own resource
/// (the open decision is keyed by phase), so neither side reads the other's.
#[test]
fn one_profile_on_both_pair_sides_opens_a_resource_per_side() {
    let runtime = runtime();
    let plugin = load(
        &runtime,
        "both-sides",
        guest::record_guest(),
        mode::LEAST_IN_FLIGHT,
    );
    let shared = plugin_profile(&plugin, None, None, None);
    let engine = pair_engine(vec![shared], 0);
    let decision = pair_decide(&engine);
    let Some(pick) = decision.pick() else {
        panic!("a pair pick, not {decision:?}")
    };
    assert_pair_pick(pick);
    let stats = plugin.stats();
    assert_eq!(
        stats.answered, 8,
        "the decode side's four calls are its own resource's, not the prefill's"
    );
    assert_eq!(
        (stats.declined, stats.invalid, stats.instantiations),
        (0, 0, 1)
    );
}

fn pair_engine(profiles: Vec<Profile>, decode_profile: u16) -> Engine {
    Engine::new(
        profiles,
        vec![Binding {
            model: "*".into(),
            phases: vec![],
            plan: Plan::PdPair,
            profile: 0,
            decode_profile: Some(decode_profile),
            sessions: None,
        }],
        1,
        Needs::default(),
    )
}

/// Three prefills (100..102) and four decodes (200..203) of one pair class,
/// each pool's last row the least loaded.
fn pair_decide(engine: &Engine) -> Decision {
    let mut prefills = pool(3);
    let mut decodes = pool(4);
    for c in prefills.iter_mut().chain(decodes.iter_mut()) {
        c.pair_class = 7;
    }
    for (i, c) in decodes.iter_mut().enumerate() {
        c.id = WorkerId(200 + i as u64);
    }
    let pool_facts = PoolFacts {
        pool_size: 7,
        generation: 1,
        ..PoolFacts::default()
    };
    engine.decide_pair(
        &facts(9),
        &pool_facts,
        PairPools {
            prefills: &prefills,
            decodes: &decodes,
        },
        &Constraints::default(),
        &Constraints::default(),
        &mut Scratch::new(),
    )
}

fn assert_pair_pick(pick: &Pick) {
    let lead = pick.targets.first().expect("a target");
    assert_eq!(lead.id, WorkerId(102), "the least-loaded prefill");
    assert_eq!(
        lead.pair.map(|p| p.decode_id),
        Some(WorkerId(203)),
        "the least-loaded decode of its class"
    );
}

/// The alternates follow the lead by position, so a second data-parallel
/// rank of the lead's worker (same id, another row) stays in the answer.
#[test]
fn the_alternates_keep_the_lead_workers_other_ranks() {
    let runtime = runtime();
    let plugin = load(
        &runtime,
        "ranks",
        guest::record_guest(),
        mode::LEAST_IN_FLIGHT,
    );
    let engine = plugin_engine(&plugin);
    let mut candidates = pool(4);
    candidates[0].id = WorkerId(103);
    candidates[0].dp_rank = 1;
    let decision = decide(&engine, &candidates, 3);
    assert_eq!(pick_ids(&decision), vec![103, 102, 101, 103]);
    let Some(pick) = decision.pick() else {
        panic!("a pick, not {decision:?}")
    };
    let positions: Vec<u32> = pick.targets.iter().map(|t| t.position).collect();
    assert_eq!(
        positions,
        vec![3, 2, 1, 0],
        "the lead, then the rest by score"
    );
}

#[test]
fn warm_creates_the_threads_instance_and_a_cold_thread_counts_its_first_decision() {
    let runtime = runtime();
    let plugin = load(
        &runtime,
        "warm",
        guest::record_guest(),
        mode::LEAST_IN_FLIGHT,
    );
    // Both threads live at once: the slot crate recycles the id of a thread
    // that exited, and a later thread would adopt its instance.
    let gate = Arc::new(std::sync::Barrier::new(3));
    let warmer = {
        let (plugin, gate) = (Arc::clone(&plugin), Arc::clone(&gate));
        std::thread::spawn(move || {
            let before = plugin.is_warm();
            plugin.warm().expect("warms");
            let after = plugin.is_warm();
            gate.wait();
            (before, after)
        })
    };
    let cold = {
        let (plugin, gate) = (Arc::clone(&plugin), Arc::clone(&gate));
        std::thread::spawn(move || {
            let cold = plugin.is_warm();
            gate.wait();
            cold
        })
    };
    gate.wait();
    assert_eq!(warmer.join().expect("joins"), (false, true));
    assert!(
        !cold.join().expect("joins"),
        "a thread that neither warmed nor decided holds nothing"
    );
    let engine = plugin_engine(&plugin);
    plugin.forget_instance();
    decide(&engine, &pool(4), 1);
    let stats = plugin.stats();
    assert_eq!(stats.instantiations, 3, "the load, the warm, the lazy one");
    assert_eq!(stats.lazy_instantiations, 1);
}

#[test]
fn a_profile_of_built_ins_holds_no_scope_and_a_plugin_profile_does() {
    let native = Profile {
        name: "native".into(),
        filters: vec![Box::new(Healthy)],
        scorers: vec![Weighted {
            scorer: Box::new(LeastRequests),
            weight: 1.0,
        }],
        picker: Box::new(Best),
    };
    assert!(!native.scoped());
    let runtime = runtime();
    let plugin = load(
        &runtime,
        "scoped",
        guest::record_guest(),
        mode::LEAST_IN_FLIGHT,
    );
    assert!(plugin_profile(&plugin, None, None, None).scoped());
}

/// The exposure gates the lifecycle as it gates the request: a withheld
/// session key reads zero in every lease the plugin observes.
#[test]
fn a_withheld_session_key_is_withheld_from_the_lifecycle_too() {
    let runtime = runtime();
    for (name, component) in [
        ("record", guest::record_guest()),
        ("packed", guest::packed_guest()),
    ] {
        let mut withheld = spec(name, component.clone(), mode::LEAST_IN_FLIGHT);
        withheld.view.session_key = Reveal::None;
        let withheld = Plugin::load(&runtime, withheld).expect("the guest loads");
        let hashed = load(&runtime, name, component, mode::LEAST_IN_FLIGHT);
        let lease = Lease {
            session_key: 0xBEEF,
            ..Lease::default()
        };
        for plugin in [&withheld, &hashed] {
            plugin_engine(plugin).on_dispatched(&lease, &Target::default(), Side::Single, 3);
        }
        assert_eq!(
            counter_value(&withheld, counter::OBSERVE_SESSION),
            0,
            "{name}: withheld"
        );
        assert_eq!(
            counter_value(&hashed, counter::OBSERVE_SESSION),
            0xBEEF,
            "{name}: the fingerprint, as in the request"
        );
    }
}

/// The sandbox is one linear memory under the cap and tables of a bounded
/// size: a second memory is refused at instantiation, a table grown past the
/// cap traps.
#[test]
fn a_second_memory_and_a_table_past_the_cap_are_refused() {
    let runtime = runtime();
    let err = Plugin::load(
        &runtime,
        spec(
            "two-memories",
            guest::two_memory_guest(),
            mode::LEAST_IN_FLIGHT,
        ),
    )
    .expect_err("a second memory is refused");
    assert!(matches!(err, LoadError::Trap { .. }), "{err}");

    let plugin = load(
        &runtime,
        "table-grower",
        guest::record_guest(),
        mode::GROW_TABLE,
    );
    let engine = plugin_engine(&plugin);
    assert!(matches!(decide(&engine, &pool(4), 1), Decision::Pick(_)));
    let stats = plugin.stats();
    assert_eq!(
        (stats.traps, stats.answered),
        (1, 1),
        "the constructor answered, the filter trapped"
    );
    assert!(
        plugin.is_warm(),
        "the instance went and the close rebuilt the slot"
    );

    let err = Plugin::load(
        &runtime,
        spec(
            "four-tables",
            guest::four_table_guest(),
            mode::LEAST_IN_FLIGHT,
        ),
    )
    .expect_err("a fifth table is refused");
    assert!(matches!(err, LoadError::Trap { .. }), "{err}");
}

/// The request's budget bounds the decision, not each call: a call finds the
/// remainder, and a call that runs past it ran out of its frame.
#[test]
fn the_decisions_budget_bounds_the_sum_of_its_calls() {
    let runtime = runtime();
    let mut burner = spec("burner", guest::record_guest(), mode::BURN_IN_STAGES);
    // The frame is the request's: each call may take all of what is left.
    burner.budget_micros = 0;
    let fuel_per_micro = burner.fuel_per_micro;
    let plugin = Plugin::load(&runtime, burner).expect("the guest loads");
    let engine = plugin_engine(&plugin);
    let candidates = pool(4);
    let pool_facts = PoolFacts {
        pool_size: 4,
        generation: 1,
        ..PoolFacts::default()
    };
    let decide_under = |budget_micros: u32, seed: u64| {
        let facts = RequestFacts {
            budget_micros,
            ..facts(seed)
        };
        engine.decide(
            &facts,
            &pool_facts,
            &candidates,
            &Constraints::default(),
            &mut Scratch::new(),
        )
    };
    let before = plugin.stats().fuel_used;
    assert!(matches!(decide_under(10_000, 1), Decision::Pick(_)));
    let one = plugin.stats();
    let whole = one.fuel_used - before;
    assert_eq!(
        (one.answered, one.budget_gone, one.fuel_exhausted),
        (4, 0, 0)
    );
    // Two and a half calls' worth: the third stage call finds the remainder
    // short of its work and stops there.
    let per_call = whole / 3;
    let budget_micros = u32::try_from((per_call * 5 / 2).div_ceil(fuel_per_micro)).expect("fits");
    assert!(
        matches!(decide_under(budget_micros, 2), Decision::Pick(_)),
        "the fallbacks decide"
    );
    let two = plugin.stats();
    assert_eq!(
        two.answered - one.answered,
        3,
        "the constructor, the filter and the scorer answered"
    );
    assert_eq!(
        (two.budget_gone, two.fuel_exhausted, two.traps),
        (0, 1, 0),
        "the picker's grant was the remainder and ran out: the calls together exceeded the budget"
    );
    assert!(
        two.fuel_used - one.fuel_used <= u64::from(budget_micros) * fuel_per_micro,
        "no decision burns past its budget"
    );
    assert!(
        plugin.is_warm(),
        "the interrupted instance went and the close rebuilt the slot"
    );
}

/// A declined filter declines once per open decision: the pair plan's
/// partner scan asks row by row and reads the cached decline.
#[test]
fn a_declined_filter_declines_once_per_side_of_a_pair() {
    let runtime = runtime();
    let plugin = load(
        &runtime,
        "decliner",
        guest::record_guest(),
        mode::DECLINE_FILTER,
    );
    let shared = plugin_profile(&plugin, None, None, None);
    let engine = pair_engine(vec![shared], 0);
    let decision = pair_decide(&engine);
    let Some(pick) = decision.pick() else {
        panic!("a pair pick, not {decision:?}")
    };
    assert_pair_pick(pick);
    let stats = plugin.stats();
    assert_eq!(
        stats.declined, 2,
        "one decline per side, however many rows the scan asks"
    );
    assert_eq!(
        stats.answered, 6,
        "per side: the constructor, the scorer and the picker"
    );
}

/// Each open decision of a pair draws its own sequence from its own seed:
/// the prefill side's stages never read the decode side's draws.
#[test]
fn each_open_decision_of_a_pair_draws_its_own_sequence() {
    let runtime = runtime();
    let plugin = load(
        &runtime,
        "drawer",
        guest::record_guest(),
        mode::DRAW_IN_PICK,
    );
    let shared = plugin_profile(&plugin, None, None, None);
    let engine = pair_engine(vec![shared], 0);
    let decision = pair_decide(&engine);
    let Some(pick) = decision.pick() else {
        panic!("a pair pick, not {decision:?}")
    };
    assert_pair_pick(pick);
    // The prefill side draws from the request's seed (9), the decode side
    // from the first draw of it (the engine's derivation); the constructor
    // takes the first value of each sequence, the pick the second.
    let mut prefill = Draw::new(9);
    let (p1, p2) = (prefill.next_u64(), prefill.next_u64());
    let mut decode = Draw::new(p1);
    let (d1, d2) = (decode.next_u64(), decode.next_u64());
    assert_eq!(counter_value(&plugin, counter::RANDOM), p1.wrapping_add(d1));
    assert_eq!(
        counter_value(&plugin, counter::PICK_RANDOM),
        p2.wrapping_add(d2),
        "each pick drew the second value of its own side's sequence"
    );
}

/// A plugin bound to two profiles observes each lifecycle event once: the
/// engine broadcasts to every profile, the plugin's first adapter forwards.
#[test]
fn a_plugin_bound_to_two_profiles_observes_each_event_once() {
    let runtime = runtime();
    let plugin = load(
        &runtime,
        "twice-bound",
        guest::record_guest(),
        mode::LEAST_IN_FLIGHT,
    );
    let prefill = plugin_profile(&plugin, None, None, None);
    let decode = plugin_profile(&plugin, None, None, None);
    let engine = pair_engine(vec![prefill, decode], 1);
    engine.on_dispatched(&Lease::default(), &Target::default(), Side::Single, 3);
    assert_eq!(plugin.stats().observed, 1);
    assert_eq!(counter_value(&plugin, counter::OBSERVED), 1);
}

/// Fuel prices operators, not bytes: a loop of whole-memory fills burns a
/// few units per millisecond. The frame's epoch deadline stops it on the
/// clock, counted as the frame running out.
#[test]
fn a_bulk_memory_loop_meets_the_frames_wall_bound() {
    let runtime = runtime();
    // A calibration no host has: at it the frame's fuel lasts seconds of
    // fills (a fill is charged by its length), so only the clock can end the
    // loop inside the bound below; with the deadline late or the ticker
    // asleep the case reads seconds and fails.
    let mut filler = spec("filler", guest::record_guest(), mode::FILL_IN_SCORE);
    filler.fuel_per_micro = 20_000_000;
    let plugin = Plugin::load(&runtime, filler).expect("the guest loads");
    let engine = plugin_engine(&plugin);
    let started = std::time::Instant::now();
    assert!(
        matches!(decide(&engine, &pool(4), 1), Decision::Pick(_)),
        "the fallback scorer decided"
    );
    let elapsed = started.elapsed();
    let stats = plugin.stats();
    assert_eq!(
        (stats.fuel_exhausted, stats.traps),
        (1, 0),
        "the frame ran out on the clock, not on fuel: {stats:?}"
    );
    // The fills cost a few units each under a frame of ten thousand million
    // units, so the loop would run for seconds on fuel alone; the deadline
    // ends it within a few ticks of the frame (a trapped call's fuel reads
    // as spent: the store's counter is not synced on an interrupt).
    let bound = std::time::Duration::from_micros(u64::from(FRAME_MICROS) * 20);
    assert!(
        elapsed < bound,
        "the wall bound held: {elapsed:?} for a {FRAME_MICROS} us frame"
    );
    assert!(
        plugin.is_warm(),
        "the interrupted instance went and the close rebuilt the slot"
    );
}

/// One plugin on both sides of a pair draws on one decision budget: the
/// sides' calls together never exceed the request's.
#[test]
fn a_pair_shares_one_decision_budget() {
    let runtime = runtime();
    let mut burner = spec("pair-burner", guest::record_guest(), mode::BURN_IN_STAGES);
    burner.budget_micros = 0;
    let fuel_per_micro = burner.fuel_per_micro;
    let plugin = Plugin::load(&runtime, burner).expect("the guest loads");
    // One burning decision on a single profile calibrates a call's cost.
    let single = plugin_engine(&plugin);
    let before = plugin.stats().fuel_used;
    let facts_of = |budget_micros: u32| RequestFacts {
        budget_micros,
        ..facts(1)
    };
    let pool_facts = PoolFacts {
        pool_size: 4,
        generation: 1,
        ..PoolFacts::default()
    };
    single.decide(
        &facts_of(10_000),
        &pool_facts,
        &pool(4),
        &Constraints::default(),
        &mut Scratch::new(),
    );
    let per_call = (plugin.stats().fuel_used - before) / 3;
    // Four calls' worth for a pair that would take six: the budget is one.
    let budget_micros = u32::try_from((per_call * 4).div_ceil(fuel_per_micro)).expect("fits");
    let shared = plugin_profile(&plugin, None, None, None);
    let pair = pair_engine(vec![shared], 0);
    let before = plugin.stats().fuel_used;
    let mut prefills = pool(3);
    let mut decodes = pool(4);
    for c in prefills.iter_mut().chain(decodes.iter_mut()) {
        c.pair_class = 7;
    }
    for (i, c) in decodes.iter_mut().enumerate() {
        c.id = WorkerId(200 + i as u64);
    }
    let decision = pair.decide_pair(
        &facts_of(budget_micros),
        &PoolFacts {
            pool_size: 7,
            generation: 1,
            ..PoolFacts::default()
        },
        PairPools {
            prefills: &prefills,
            decodes: &decodes,
        },
        &Constraints::default(),
        &Constraints::default(),
        &mut Scratch::new(),
    );
    assert!(
        matches!(decision, Decision::Pick(_)),
        "the fallbacks decide"
    );
    let stats = plugin.stats();
    assert!(
        stats.fuel_used - before <= u64::from(budget_micros) * fuel_per_micro,
        "both sides drew on one budget: {stats:?}"
    );
    assert!(
        stats.fuel_exhausted + stats.budget_spent >= 1,
        "the later side met the remainder the earlier side spent: {stats:?}"
    );
    assert_eq!(
        stats.budget_gone, 0,
        "nothing was gone on arrival: {stats:?}"
    );
}

/// One plugin bound by a prefill profile and a decode profile of their own:
/// a fault on the decode side drops the one instance both sides' resources
/// live on. The prefill side's close finds its resource's store gone and
/// drops nothing through the rebuilt instance: one trap, one rebuild.
#[test]
fn a_fault_on_one_side_of_a_two_profile_pair_rebuilds_once() {
    let runtime = runtime();
    let plugin = load(
        &runtime,
        "four-trap",
        guest::record_guest(),
        mode::TRAP_ON_FOUR_CANDIDATES,
    );
    let prefill = plugin_profile(&plugin, None, None, None);
    let decode = plugin_profile(&plugin, None, None, None);
    let pair = pair_engine(vec![prefill, decode], 1);
    let mut prefills = pool(3);
    let mut decodes = pool(4);
    for c in prefills.iter_mut().chain(decodes.iter_mut()) {
        c.pair_class = 7;
    }
    for (i, c) in decodes.iter_mut().enumerate() {
        c.id = WorkerId(200 + i as u64);
    }
    let decision = pair.decide_pair(
        &facts(1),
        &PoolFacts {
            pool_size: 7,
            generation: 1,
            ..PoolFacts::default()
        },
        PairPools {
            prefills: &prefills,
            decodes: &decodes,
        },
        &Constraints::default(),
        &Constraints::default(),
        &mut Scratch::new(),
    );
    assert!(
        matches!(decision, Decision::Pick(_)),
        "the fallbacks decide"
    );
    let stats = plugin.stats();
    assert_eq!(
        stats.traps, 1,
        "the decode filter's trap and no second one from a stale handle: {stats:?}"
    );
    assert_eq!(
        (stats.instantiations, stats.lazy_instantiations),
        (2, 1),
        "rebuilt once: {stats:?}"
    );
    assert!(plugin.is_warm());
}

/// A budget the decision's own calls spend to the last microsecond drops no
/// healthy instance: the later stage falls back without a call, the
/// destructor runs under its own small frame, nothing is rebuilt.
#[test]
fn a_spent_budget_drops_no_healthy_instance() {
    let runtime = runtime();
    let plugin = load(
        &runtime,
        "thrifty",
        guest::record_guest(),
        mode::LEAST_IN_FLIGHT,
    );
    let engine = plugin_engine(&plugin);
    // Three microseconds: every call is charged at least one, so the
    // constructor, the filter and the scorer spend it and the pick finds
    // nothing left.
    let facts = RequestFacts {
        budget_micros: 3,
        ..facts(1)
    };
    let pool_facts = PoolFacts {
        pool_size: 4,
        generation: 1,
        ..PoolFacts::default()
    };
    let decision = engine.decide(
        &facts,
        &pool_facts,
        &pool(4),
        &Constraints::default(),
        &mut Scratch::new(),
    );
    assert!(
        matches!(decision, Decision::Pick(_)),
        "the fallbacks decide"
    );
    let stats = plugin.stats();
    assert!(
        stats.budget_spent >= 1,
        "a later call met the spent remainder: {stats:?}"
    );
    assert_eq!(
        (stats.fuel_exhausted, stats.traps, stats.instantiations),
        (0, 0, 1),
        "no fault, no rebuild: {stats:?}"
    );
    assert!(
        plugin.is_warm(),
        "the instance stands through the destructor's own frame"
    );
}

/// The budget is wall time: a long call that answers, then an endless one,
/// end at the budget together, because the second call's deadline is what
/// the first one left, not its whole frame. Fuel cannot end either call
/// inside the bound at this calibration; the clock does.
#[test]
fn the_decisions_wall_is_bounded_by_its_budget_whatever_the_fuel() {
    let runtime = runtime();
    let mut spec = spec(
        "fill-then-fill",
        guest::record_guest(),
        mode::FILL_IN_FILTER_THEN_FILL,
    );
    spec.fuel_per_micro = 20_000_000;
    spec.budget_micros = 100_000;
    let plugin = Plugin::load(&runtime, spec).expect("the guest loads");
    let engine = plugin_engine(&plugin);
    let started = std::time::Instant::now();
    let decision = engine.decide(
        &RequestFacts {
            budget_micros: 100_000,
            ..facts(1)
        },
        &PoolFacts {
            pool_size: 4,
            generation: 1,
            ..PoolFacts::default()
        },
        &pool(4),
        &Constraints::default(),
        &mut Scratch::new(),
    );
    let elapsed = started.elapsed();
    assert!(
        matches!(decision, Decision::Pick(_)),
        "the fallbacks decide"
    );
    let stats = plugin.stats();
    assert_eq!(
        (stats.fuel_exhausted, stats.traps),
        (1, 0),
        "the endless fill ran out on the clock: {stats:?}"
    );
    // Sixty fills take tens of milliseconds and answer; the endless fill then
    // gets what the budget has left, plus a tick, the fill in flight and the
    // runner's slack. A whole-frame deadline would run it the full hundred
    // milliseconds on top (127 ms on the cut).
    assert!(
        elapsed < std::time::Duration::from_millis(115),
        "the decision ended at its budget: {elapsed:?}"
    );
    assert!(
        plugin.is_warm(),
        "the interrupted instance went and the close rebuilt the slot"
    );
}

/// Two plugins in one profile: the scorer spends most of the budget, the
/// picker runs out under what is left. The decision's budget is spent, the
/// picker's instance goes, and the picker's breaker is untouched.
#[test]
fn an_exhaustion_under_another_plugins_clamp_is_the_decisions_budget_spent() {
    let runtime = runtime();
    let scorer = load(
        &runtime,
        "heavy-scorer",
        guest::record_guest(),
        mode::BURN_IN_STAGES,
    );
    let picker_spec = spec("picker", guest::record_guest(), mode::BURN_IN_STAGES);
    let fuel_per_micro = picker_spec.fuel_per_micro;
    let picker = Plugin::load(&runtime, picker_spec).expect("the guest loads");
    // One burn's fuel, from the scorer alone at a large budget.
    let before = scorer.stats().fuel_used;
    plugin_engine(&scorer).decide(
        &RequestFacts {
            budget_micros: 1_000_000,
            ..facts(1)
        },
        &PoolFacts {
            pool_size: 4,
            generation: 1,
            ..PoolFacts::default()
        },
        &pool(4),
        &Constraints::default(),
        &mut Scratch::new(),
    );
    let burn = (scorer.stats().fuel_used - before) / 3;
    let profile = Profile {
        name: "two".into(),
        filters: vec![],
        scorers: vec![Weighted {
            scorer: Box::new(WasmScorer::new(scorer.bind(), None)),
            weight: 1.0,
        }],
        picker: Box::new(WasmPicker::new(picker.bind(), None)),
    };
    let engine = single_engine(profile);
    let gauged = scorer.stats().answered;
    // A burn and a half, plus a little for the constructors: the scorer's
    // burn fits, the picker's does not.
    let budget_micros = u32::try_from((burn * 3 / 2).div_ceil(fuel_per_micro) + 20).expect("fits");
    let decision = engine.decide(
        &RequestFacts {
            budget_micros,
            ..facts(1)
        },
        &PoolFacts {
            pool_size: 4,
            generation: 1,
            ..PoolFacts::default()
        },
        &pool(4),
        &Constraints::default(),
        &mut Scratch::new(),
    );
    assert!(
        matches!(decision, Decision::Pick(_)),
        "the fallback picker decides"
    );
    let (s, p) = (scorer.stats(), picker.stats());
    assert_eq!(
        (s.fuel_exhausted, s.traps, s.answered - gauged),
        (0, 0, 2),
        "the scorer's constructor and score answered: {s:?}"
    );
    assert_eq!(
        (p.fuel_exhausted, p.traps, p.budget_spent),
        (0, 0, 1),
        "the picker ran out under the scorer's clamp: {p:?}"
    );
    assert_eq!(
        (p.instantiations, p.lazy_instantiations),
        (2, 1),
        "the picker's interrupted instance went and the close rebuilt it: {p:?}"
    );
    assert_eq!(p.breaker_opens, 0);
}

/// The decision's first open seeds its budget and every later open draws on
/// it: one plugin on both sides of a pair, a constructor that burns, a budget
/// of a burn and a half. The second constructor gets what the first left and
/// runs out; a budget re-seeded per open would let it through.
#[test]
fn the_first_open_seeds_the_budget_and_later_opens_draw_on_it() {
    let runtime = runtime();
    let mut spec = spec(
        "ctor-burner",
        guest::record_guest(),
        mode::BURN_IN_CONSTRUCTOR,
    );
    spec.budget_micros = 0;
    let fuel_per_micro = spec.fuel_per_micro;
    let plugin = Plugin::load(&runtime, spec).expect("the guest loads");
    // One decision's fuel on a single profile: the burning constructor and
    // three cheap stages.
    let before = plugin.stats().fuel_used;
    plugin_engine(&plugin).decide(
        &RequestFacts {
            budget_micros: 1_000_000,
            ..facts(1)
        },
        &PoolFacts {
            pool_size: 4,
            generation: 1,
            ..PoolFacts::default()
        },
        &pool(4),
        &Constraints::default(),
        &mut Scratch::new(),
    );
    let one = plugin.stats().fuel_used - before;
    let budget_micros = u32::try_from((one * 3 / 2).div_ceil(fuel_per_micro)).expect("fits");
    let shared = plugin_profile(&plugin, None, None, None);
    let pair = pair_engine(vec![shared], 0);
    let mut prefills = pool(3);
    let mut decodes = pool(4);
    for c in prefills.iter_mut().chain(decodes.iter_mut()) {
        c.pair_class = 7;
    }
    for (i, c) in decodes.iter_mut().enumerate() {
        c.id = WorkerId(200 + i as u64);
    }
    let before = plugin.stats();
    let decision = pair.decide_pair(
        &RequestFacts {
            budget_micros,
            ..facts(1)
        },
        &PoolFacts {
            pool_size: 7,
            generation: 1,
            ..PoolFacts::default()
        },
        PairPools {
            prefills: &prefills,
            decodes: &decodes,
        },
        &Constraints::default(),
        &Constraints::default(),
        &mut Scratch::new(),
    );
    assert!(
        matches!(decision, Decision::Pick(_)),
        "the fallbacks decide"
    );
    let stats = plugin.stats();
    assert_eq!(
        (
            stats.fuel_exhausted - before.fuel_exhausted,
            stats.answered - before.answered
        ),
        (1, 1),
        "the first constructor answered and the second ran out on what it left: {stats:?}"
    );
    assert!(plugin.is_warm(), "the close rebuilt the slot");
}

/// A destructor long on the clock but cheap in fuel is read against the
/// decision's frame, not against its own small fuel frame: sixty-five
/// decisions of it open no breaker by the latency rule and drop no instance.
#[test]
fn a_slow_destructor_is_judged_against_the_decisions_frame() {
    let runtime = runtime();
    let plugin = load(
        &runtime,
        "slow-dtor",
        guest::record_guest(),
        mode::SLOW_DESTRUCTOR,
    );
    let engine = plugin_engine(&plugin);
    let candidates = pool(4);
    for seed in 0..65 {
        assert!(
            matches!(decide(&engine, &candidates, seed), Decision::Pick(_)),
            "decision {seed}"
        );
    }
    let stats = plugin.stats();
    assert_eq!(
        (
            stats.breaker_opens,
            stats.fuel_exhausted,
            stats.traps,
            stats.instantiations
        ),
        (0, 0, 0, 1),
        "the destructors were neither slowness nor faults: {stats:?}"
    );
    assert!(plugin.is_warm());
}

/// Reads `VmRSS` of this process, in KiB.
fn rss_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmRSS:"))
                .and_then(|l| l.split_whitespace().nth(1).and_then(|v| v.parse().ok()))
        })
        .unwrap_or(0)
}

/// This thread's user and system CPU time, in clock ticks.
fn cpu_ticks() -> u64 {
    std::fs::read_to_string("/proc/thread-self/stat")
        .ok()
        .and_then(|s| {
            let after = s.rsplit(") ").next()?;
            let fields: Vec<&str> = after.split_whitespace().collect();
            let utime: u64 = fields.get(11)?.parse().ok()?;
            let stime: u64 = fields.get(12)?.parse().ok()?;
            Some(utime + stime)
        })
        .unwrap_or(0)
}

/// A measurement for the Numbers table, not a check: run in release with
/// `--ignored measure --nocapture`. RSS per kept instance over 64 warmed
/// threads (their stacks included), and the CPU of one deciding thread over
/// 200,000 decisions at 64 candidates, read from the thread's own clock
/// ticks.
#[test]
#[ignore = "a measurement for the Numbers table; run in release with --nocapture"]
#[expect(
    clippy::print_stdout,
    reason = "the measurement reports its rows on stdout under --nocapture"
)]
fn measure_rss_per_kept_instance_and_cpu_per_decision() {
    let runtime = runtime();
    for (name, component) in [
        ("record", guest::record_guest()),
        ("packed", guest::packed_guest()),
    ] {
        let plugin = load(&runtime, name, component, mode::LEAST_IN_FLIGHT);
        let threads = 64u64;
        let barrier = Arc::new(std::sync::Barrier::new(threads as usize + 1));
        let before = rss_kib();
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let plugin = Arc::clone(&plugin);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    plugin.warm().expect("warms");
                    barrier.wait();
                    barrier.wait();
                })
            })
            .collect();
        barrier.wait();
        let after = rss_kib();
        println!(
            "{name}: RSS per kept instance {} KiB ({threads} instances: {before} -> {after} KiB)",
            after.saturating_sub(before) / threads
        );
        barrier.wait();
        for h in handles {
            h.join().expect("joins");
        }
        let engine = plugin_engine(&plugin);
        let candidates = pool(64);
        let pool_facts = PoolFacts {
            pool_size: 64,
            generation: 1,
            ..PoolFacts::default()
        };
        let cx = Constraints::default();
        let mut scratch = Scratch::new();
        let decisions = 200_000u64;
        let ticks0 = cpu_ticks();
        let started = std::time::Instant::now();
        for seed in 0..decisions {
            engine.decide(&facts(seed), &pool_facts, &candidates, &cx, &mut scratch);
        }
        let wall = started.elapsed();
        let cpu_ms = (cpu_ticks() - ticks0) * 10;
        let per_decision_us = wall.as_secs_f64() * 1e6 / decisions as f64;
        // CPU per decision times 10,000 per second, as a share of one core.
        let core_share = cpu_ms as f64 / decisions as f64 * 10_000.0 / 1_000.0 * 100.0;
        println!(
            "{name}: {decisions} decisions at 64 candidates: wall {per_decision_us:.2} us each, CPU {cpu_ms} ms in all = {core_share:.2}% of a core at 10,000 decisions/s"
        );
        let stats = plugin.stats();
        println!(
            "{name}: fuel per decision {} (four calls), fuel per microsecond of wall {:.0}",
            stats.fuel_used / decisions,
            stats.fuel_used as f64 / (wall.as_secs_f64() * 1e6)
        );
    }
    // The epoch ticker with no call in flight: its CPU over two idle seconds.
    let idle = std::time::Duration::from_secs(2);
    let ticks0 = ticker_cpu_ticks();
    std::thread::sleep(idle);
    let ticker_ms = (ticker_cpu_ticks() - ticks0) * 10;
    println!(
        "epoch ticker idle: {ticker_ms} ms CPU over {} ms = {:.3}% of a core",
        idle.as_millis(),
        ticker_ms as f64 / idle.as_millis() as f64 * 100.0
    );
}

/// The CPU ticks (user and system, at 100 per second) of the runtime's epoch
/// ticker thread, from `/proc`.
fn ticker_cpu_ticks() -> u64 {
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
        return 0;
    };
    tasks
        .flatten()
        .filter_map(|task| {
            let comm = std::fs::read_to_string(task.path().join("comm")).ok()?;
            (comm.trim() == "wasm-epoch").then_some(task.path().join("stat"))
        })
        .filter_map(|stat| {
            let s = std::fs::read_to_string(stat).ok()?;
            let after = s.rsplit(") ").next()?;
            let fields: Vec<&str> = after.split_whitespace().collect();
            let utime: u64 = fields.get(11)?.parse().ok()?;
            let stime: u64 = fields.get(12)?.parse().ok()?;
            Some(utime + stime)
        })
        .sum()
}
