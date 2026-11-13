//! The committed `smg:policy` WIT is the rendering of its template with the
//! `types` interface generated from the engine's types: this test renders it
//! again and compares byte for byte, so neither the template nor the Rust
//! types can drift from the file a plugin is built against. A change to
//! either is committed by `SMG_POLICY_WIT_WRITE=1 cargo test -p
//! smg-policy-controller --test wit`, which rewrites the file (the pattern of
//! the committed spec schema, `tests/schema.rs`).

use std::path::PathBuf;

fn contracts() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("contracts")
}

#[test]
fn the_committed_wit_is_the_rendering_of_its_template() {
    let template =
        std::fs::read_to_string(contracts().join("smg-policy.wit.in")).expect("the template");
    let rendered = smg_policy_controller::wit::render(&template).expect("the template renders");
    let committed = contracts().join("smg-policy.wit");
    let current = std::fs::read_to_string(&committed).expect("the committed WIT");
    if rendered == current {
        return;
    }
    if std::env::var_os("SMG_POLICY_WIT_WRITE").is_some() {
        std::fs::write(&committed, &rendered).expect("the committed WIT rewritten");
        return;
    }
    let first = rendered
        .lines()
        .zip(current.lines())
        .position(|(r, c)| r != c)
        .map_or(0, |i| i + 1);
    panic!(
        "the committed WIT differs from its rendering at line {first}: run \
         `SMG_POLICY_WIT_WRITE=1 cargo test -p smg-policy-controller --test wit` and commit \
         contracts/smg-policy.wit"
    );
}

/// Every record's fields are the engine's struct's, in its order and with
/// its types in WIT spelling, the host-only fields left out.
#[test]
fn the_records_are_the_engines_types_field_for_field() {
    let drift = smg_policy_controller::wit::drift();
    assert!(drift.is_empty(), "{}", drift.join("\n"));
}

/// The rendering stays a WIT package the component tooling resolves, with
/// both worlds.
#[test]
fn the_rendering_resolves_as_the_package_with_both_worlds() {
    let template =
        std::fs::read_to_string(contracts().join("smg-policy.wit.in")).expect("the template");
    let rendered = smg_policy_controller::wit::render(&template).expect("the template renders");
    let dir = std::env::temp_dir().join(format!("smg-policy-wit-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("a scratch directory");
    let path = dir.join("smg-policy.wit");
    std::fs::write(&path, rendered).expect("the rendering written");
    let mut resolve = wit_parser::Resolve::new();
    let (package, _) = resolve.push_path(&path).expect("the rendering resolves");
    let worlds: Vec<&str> = resolve.packages[package]
        .worlds
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(worlds, ["policy", "policy-packed"]);
    let _ = std::fs::remove_dir_all(&dir);
}

/// The cost of the check, for the Numbers: one rendering of the package.
#[test]
#[ignore = "a measurement for the Numbers table; run in release with --nocapture"]
#[expect(
    clippy::print_stdout,
    reason = "the measurement reports its row on stdout under --nocapture"
)]
fn measure_the_rendering() {
    let template =
        std::fs::read_to_string(contracts().join("smg-policy.wit.in")).expect("the template");
    let rounds = 1_000u32;
    let started = std::time::Instant::now();
    for _ in 0..rounds {
        let rendered = smg_policy_controller::wit::render(&template).expect("the template renders");
        assert!(rendered.len() > 1_000);
    }
    let each = started.elapsed() / rounds;
    println!("render: {each:?} per rendering of the package");
}
