//! The committed `smg:policy` WIT package, generated from the engine's types.
//!
//! `contracts/smg-policy.wit.in` is the hand-written template: the package
//! header, the `host`, `plugin` and `plugin-packed` interfaces and the two
//! worlds, prose for human readers. Its `{{types}}` marker is where this
//! module renders the `types` interface from its descriptor table, every record,
//! flags, enum and variant as data in the committed order with its
//! documentation, each record tied to the engine's Rust struct by a probe in
//! `probes` (an exhaustive destructuring, so a struct that gains, loses or
//! renames a field fails to compile, and a field whose type moved fails the
//! probe). `contracts/smg-policy.wit` is the rendering, committed: the test
//! in `tests/wit.rs` renders the template again and compares it byte for
//! byte, so neither the template nor the types can drift from the file a
//! plugin is built against (`SMG_POLICY_WIT_WRITE=1` rewrites it).

mod probes;
mod types;

pub use probes::drift;

/// The marker the template carries where the `types` interface goes.
pub const TYPES_MARKER: &str = "{{types}}";

/// The first line of every generated section, so a reader of the committed
/// file tells the template's prose from the rendering.
const GENERATED: &str =
    "  // generated from smg_policy_controller::view by the crate's wit module; do not edit by hand";

/// One field of a WIT record: its name and type, both in WIT spelling.
pub(crate) struct Field {
    pub(crate) name: &'static str,
    pub(crate) ty: &'static str,
}

/// One item of the `types` interface, in WIT spelling. `doc` is the lines of
/// its documentation comment without the `///`; `blank_before` is the blank
/// line that separates it from the item above; `inline` is a record or
/// variant written on one line.
pub(crate) enum Item {
    Flags {
        name: &'static str,
        doc: &'static [&'static str],
        cases: &'static [&'static str],
        blank_before: bool,
    },
    Enum {
        name: &'static str,
        doc: &'static [&'static str],
        cases: &'static [&'static str],
        blank_before: bool,
    },
    Variant {
        name: &'static str,
        doc: &'static [&'static str],
        cases: &'static [&'static str],
        inline: bool,
        blank_before: bool,
    },
    Record {
        name: &'static str,
        doc: &'static [&'static str],
        fields: &'static [Field],
        inline: bool,
        blank_before: bool,
    },
}

impl Item {
    fn doc(&self) -> &'static [&'static str] {
        match self {
            Item::Flags { doc, .. }
            | Item::Enum { doc, .. }
            | Item::Variant { doc, .. }
            | Item::Record { doc, .. } => doc,
        }
    }

    fn blank_before(&self) -> bool {
        match self {
            Item::Flags { blank_before, .. }
            | Item::Enum { blank_before, .. }
            | Item::Variant { blank_before, .. }
            | Item::Record { blank_before, .. } => *blank_before,
        }
    }

    /// The item's record fields, for the probes.
    pub(crate) fn fields(&self) -> &'static [Field] {
        match self {
            Item::Record { fields, .. } => fields,
            _ => &[],
        }
    }

    pub(crate) fn name(&self) -> &'static str {
        match self {
            Item::Flags { name, .. }
            | Item::Enum { name, .. }
            | Item::Variant { name, .. }
            | Item::Record { name, .. } => name,
        }
    }

    fn render(&self, out: &mut String) {
        if self.blank_before() {
            out.push('\n');
        }
        for line in self.doc() {
            out.push_str("  ///");
            out.push_str(line);
            out.push('\n');
        }
        match self {
            Item::Flags { name, cases, .. } => one_line(out, "flags", name, cases),
            Item::Enum { name, cases, .. } => one_line(out, "enum", name, cases),
            Item::Variant {
                name,
                cases,
                inline: true,
                ..
            } => one_line(out, "variant", name, cases),
            Item::Variant {
                name,
                cases,
                inline: false,
                ..
            } => many_lines(out, "variant", name, cases.iter().map(|c| (*c).to_string())),
            Item::Record {
                name,
                fields,
                inline,
                ..
            } => {
                let entries = fields.iter().map(|f| format!("{}: {}", f.name, f.ty));
                if *inline {
                    let entries: Vec<String> = entries.collect();
                    let entries: Vec<&str> = entries.iter().map(String::as_str).collect();
                    one_line(out, "record", name, &entries);
                } else {
                    many_lines(out, "record", name, entries);
                }
            }
        }
    }
}

fn one_line(out: &mut String, kind: &str, name: &str, entries: &[&str]) {
    out.push_str("  ");
    out.push_str(kind);
    out.push(' ');
    out.push_str(name);
    out.push_str(" { ");
    out.push_str(&entries.join(", "));
    out.push_str(" }\n");
}

fn many_lines(out: &mut String, kind: &str, name: &str, entries: impl Iterator<Item = String>) {
    out.push_str("  ");
    out.push_str(kind);
    out.push(' ');
    out.push_str(name);
    out.push_str(" {\n");
    for entry in entries {
        out.push_str("    ");
        out.push_str(&entry);
        out.push_str(",\n");
    }
    out.push_str("  }\n");
}

/// The `types` interface, rendered.
pub fn render_types() -> String {
    let mut out = String::from("interface types {\n");
    out.push_str(GENERATED);
    out.push('\n');
    for item in types::TYPES {
        item.render(&mut out);
    }
    out.push_str("}\n");
    out
}

/// The whole package: the template with its markers rendered. A template
/// without the marker is an error of the template, so the test reads it as
/// a drift.
pub fn render(template: &str) -> Result<String, String> {
    if template.matches(TYPES_MARKER).count() != 1 {
        return Err(format!("the template carries {TYPES_MARKER} once"));
    }
    let types = render_types();
    Ok(template.replacen(&format!("{TYPES_MARKER}\n"), &types, 1))
}

/// The records of the interface, by name, for the probes and their test.
pub(crate) fn record(name: &str) -> Option<&'static Item> {
    types::TYPES
        .iter()
        .find(|item| matches!(item, Item::Record { .. }) && item.name() == name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_rendering_starts_with_the_interface_and_its_marker_line() {
        let text = render_types();
        let mut lines = text.lines();
        assert_eq!(lines.next(), Some("interface types {"));
        assert_eq!(lines.next(), Some(GENERATED));
        assert!(text.ends_with("}\n"));
    }

    #[test]
    fn a_template_without_the_marker_is_a_drift() {
        assert!(render("package smg:policy@1.0.0;\n").is_err());
        assert!(render("a\n{{types}}\nb\n")
            .unwrap()
            .starts_with("a\ninterface types {\n"));
    }
}
