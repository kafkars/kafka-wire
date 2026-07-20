//! the module-scoped naming rule's claim, run against the whole pinned corpus rather than argued.
//!
//! Scenario: load every vendored message, then check the two properties the
//! decision rests on. Every struct reference resolves to a declaration in its
//! own message, and no two emitted struct identities collide inside the module
//! one message renders into.
//!
//! The decision was made from a measurement, and a measurement expires. These
//! tests are the measurement made executable, so an upstream schema that breaks
//! the invariant fails here with both owners named instead of surfacing as
//! rustc `E0428` on generated code eight files later.
//!
//! Read the naming census with:
//! `cargo test -p kafka-wire-schema --test struct_corpus -- --nocapture`

#![allow(clippy::expect_used)]

mod support;

use std::collections::{BTreeMap, BTreeSet};

use kafka_wire_schema::{Field, Message, Qualification, load_message_with, validate_struct_names};

use support::{exceptions, schema_files};

/// Declarations the pinned corpus makes, across all 201 message files.
///
/// the earlier flat naming rule counts 302 over the 188 request and response files; the six further
/// declarations are in the 11 data schemas, which the front end also reads.
const DECLARATIONS: usize = 308;

/// Longest generated struct name this corpus produces.
///
/// `DescribeShareGroupOffsetsResponsePartition`, which is upstream's own
/// spelling: forty declarations hand-qualify themselves and this is the longest.
/// the earlier flat naming rule bounded the same corpus at 74, and the 32 characters between the two
/// numbers are the scope this decision bought.
const NAME_LIMIT: usize = 42;

/// Modules that would not compile if the module were the API key, not the
/// message.
///
/// the module-scoped naming rule rejects a per-API-key module, and this is the measurement behind the
/// rejection rather than the argument for it. the earlier flat naming rule's table lists **8** API
/// keys, counting those whose two directions declare a same-named struct with a
/// *different shape*. This constant is 11 because rustc does not care about
/// shape: two `struct Cursor` items in one module are `E0428` whether or not
/// their fields agree.
///
/// The three further keys are exactly the ones the earlier flat naming rule's "trap in structural
/// deduplication" section names — `ConsumerGroupHeartbeat` (68) on
/// `TopicPartitions`, `DescribeTopicPartitions` (75) on `Cursor`, and
/// `StreamsGroupHeartbeat` (88) on `Endpoint` and `TaskIds`. They collide with
/// identical shallow signatures, which is why a rule that deduplicated by
/// comparing shapes would merge them and be wrong.
const API_KEY_COLLISION_MODULES: usize = 11;

/// Distinct name clashes across those modules, counted per name rather than key.
const API_KEY_COLLISION_NAMES: usize = 22;

#[test]
fn every_struct_reference_in_the_corpus_resolves_within_its_message() {
    let mut unresolved = Vec::new();

    for message in corpus() {
        let mut references = Vec::new();
        for fields in message
            .common_structs
            .iter()
            .map(|common| common.fields.as_slice())
            .chain(std::iter::once(message.fields.as_slice()))
        {
            collect_references(fields, &mut references);
        }

        for declared in references {
            if message.structs.resolve(&declared).is_none() {
                unresolved.push(format!("{}: `{declared}`", message.name.protocol()));
            }
        }
    }

    assert!(
        unresolved.is_empty(),
        "these struct references bind to no declaration:\n{}",
        unresolved.join("\n"),
    );
}

#[test]
fn no_two_generated_struct_identities_collide_in_one_module() {
    // `kafka-wire-codegen` renders one module per *message* holding that message's
    // type and every struct it declares, so this is the collision that actually
    // stops the build. The grouping key here has to be the same one the emitter
    // uses, or this test watches a namespace nothing is emitted into.
    let mut modules: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    let mut collisions = Vec::new();

    for message in corpus() {
        let module = modules
            .entry(message.name.rust_module().to_owned())
            .or_default();

        for (rust_type, owner) in claims(&message) {
            if let Some(previous) = module.insert(rust_type.clone(), owner.clone()) {
                collisions.push(format!(
                    "module `{}`: `{rust_type}` claimed by {owner} and {previous}",
                    message.name.rust_module(),
                ));
            }
        }
    }

    assert!(
        collisions.is_empty(),
        "one module cannot declare a name twice:\n{}",
        collisions.join("\n"),
    );

    // An emptiness assertion proves nothing on its own: a walk that reached no
    // struct at all would also report no collision. Replaying the same walk
    // under the module the module-scoped naming rule rejected has to reproduce the compile failures
    // the per-message module exists to prevent, or this test is not watching
    // anything — and it is the same corpus, so the only difference is the scope.
    let (modules, names) = collisions_when_grouped_by_api_key();

    assert_eq!(
        (modules, names),
        (API_KEY_COLLISION_MODULES, API_KEY_COLLISION_NAMES),
        "the corpus no longer exhibits the collisions a per-message module fixes",
    );
}

/// Counts the modules and names a per-API-key module would clash on.
///
/// A request and its response share an API key, so this is the scope the module-scoped naming rule
/// considered and rejected: it keeps exactly the collisions the decision removes.
fn collisions_when_grouped_by_api_key() -> (usize, usize) {
    let mut modules: BTreeMap<i16, BTreeSet<String>> = BTreeMap::new();
    let mut clashing_modules = BTreeSet::new();
    let mut clashing_names = 0;

    for message in corpus() {
        let Some(api_key) = message.api_key else {
            continue;
        };
        let module = modules.entry(api_key).or_default();

        for declaration in message.structs.declarations() {
            if !module.insert(declaration.name.declared().to_owned()) {
                clashing_modules.insert(api_key);
                clashing_names += 1;
            }
        }
    }

    (clashing_modules.len(), clashing_names)
}

#[test]
fn both_scopes_can_carry_the_names_the_corpus_hands_them() {
    // The guard itself, over the whole corpus: message types unique across the
    // flat facade, and every module able to hold its own declarations.
    let messages = corpus();

    assert_eq!(validate_struct_names(&messages), Ok(()));
}

#[test]
fn the_naming_census_matches_the_decision_it_was_measured_from() {
    let messages = corpus();
    let mut declarations = 0;
    let mut module_scoped = 0;
    let mut qualified = BTreeMap::new();
    let mut longest = String::new();
    let mut renamed = Vec::new();

    for message in &messages {
        for declaration in message.structs.declarations() {
            declarations += 1;
            if declaration.name.qualification() == Qualification::ModuleScoped {
                module_scoped += 1;
            }
            if declaration.name.rust_type() != declaration.name.protocol() {
                renamed.push(format!(
                    "{} -> {}",
                    declaration.name.protocol(),
                    declaration.name.rust_type()
                ));
            }
            if declaration.name.rust_type().len() > longest.len() {
                longest = declaration.name.rust_type().to_owned();
            }
            *qualified
                .entry(declaration.name.rust_type().to_owned())
                .or_insert(0_usize) += 1;
        }
    }

    println!("messages:              {}", messages.len());
    println!("struct declarations:   {declarations}");
    println!("distinct spellings:    {}", qualified.len());
    println!("module-scoped arm:     {module_scoped}");
    println!("longest name:          {} {longest}", longest.len());

    assert_eq!(declarations, DECLARATIONS);
    assert_eq!(
        module_scoped, DECLARATIONS,
        "every declaration takes the one arm the rule has left",
    );
    // Deliberately not distinct. the earlier flat naming rule asserted equality here, because a flat
    // namespace had to carry every name at once; asserting it still would mean
    // the module scope was bought and never spent. The inequality is the
    // decision: `PartitionData` is one spelling naming 17 shapes, and each one
    // lives in its own module.
    assert!(
        qualified.len() < DECLARATIONS,
        "spellings are scoped by module, so the corpus must reuse some; \
         got {} distinct across {DECLARATIONS} declarations",
        qualified.len(),
    );
    assert!(
        longest.len() <= NAME_LIMIT,
        "the module-scoped naming rule bounds generated names at {NAME_LIMIT} characters, \
         but `{longest}` is {}",
        longest.len(),
    );

    // Upstream spells struct names in `UpperCamelCase` already, so normalizing
    // to a Rust identifier must be a no-op. A spelling that changed here would
    // silently move a generated type name away from the protocol name it is
    // supposed to be greppable back to.
    assert!(
        renamed.is_empty(),
        "normalization rewrote these upstream spellings:\n{}",
        renamed.join("\n"),
    );
}

/// Every generated type one message contributes, with what claims it.
fn claims(message: &Message) -> Vec<(String, String)> {
    let mut claims = vec![(
        message.name.rust_type().to_owned(),
        format!("message `{}`", message.name.protocol()),
    )];
    claims.extend(message.structs.declarations().iter().map(|declaration| {
        (
            declaration.name.rust_type().to_owned(),
            format!(
                "`{}` declared by `{}`",
                declaration.name.declared(),
                declaration.name.owner()
            ),
        )
    }));
    claims
}

fn collect_references(fields: &[Field], references: &mut Vec<String>) {
    for field in fields {
        if let Some(reference) = field.ty.struct_reference() {
            references.push(reference.declared().to_owned());
        }
        collect_references(&field.fields, references);
    }
}

/// The whole pinned corpus, loaded with the reviewed upstream exceptions.
fn corpus() -> Vec<Message> {
    let exceptions = exceptions();

    schema_files()
        .into_iter()
        .map(|path| {
            load_message_with(&path, &exceptions)
                .unwrap_or_else(|error| panic!("{} must load: {error}", path.display()))
        })
        .collect()
}
