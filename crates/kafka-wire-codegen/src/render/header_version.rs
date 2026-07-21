//! Header-version policy rendering.
//!
//! Which header version frames a message follows one rule with reviewed
//! exceptions, and both are emitted here as a function over the API key and
//! version. The exceptions arrive as data from `spec/overrides/`; nothing in
//! this file names a message.

use crate::{overrides::HeaderOverrides, provenance::generated_banner};

use super::text::RustText;

pub(crate) fn render_header_version(overrides: &HeaderOverrides, commit: &str) -> String {
    let mut rust = RustText::default();
    rust.line(generated_banner());
    rust.line("//!");
    rust.line("//! Header-version policy for Apache Kafka commit");
    rust.line(format!("//! `{commit}`."));
    rust.blank();
    rust.line("use kafka_wire_core::{ApiKey, ApiVersion};");
    rust.blank();

    rust.line("/// The request header version that frames a request.");
    rust.line("///");
    rust.line("/// Flexible requests take v2, which carries a tagged-field section; every");
    rust.line("/// other request takes v1. There is no v0 here: v0 omits the client id, and");
    rust.line("/// no supported request predates it.");
    rust.open("pub const fn request_header_version(flexible: bool) -> i16");
    rust.line("if flexible { 2 } else { 1 }");
    rust.close("");
    rust.blank();

    rust.line("/// The response header version that frames a response.");
    rust.line("///");
    rust.line("/// The rule is that a flexible response takes v1 and every other response");
    rust.line("/// takes v0. The exceptions below are reviewed protocol quirks carried as");
    rust.line("/// data rather than decided by name.");
    rust.open(
        "pub fn response_header_version(api_key: ApiKey, version: ApiVersion, flexible: bool) -> i16",
    );
    for exception in &overrides.response_header_exceptions {
        rust.line(format!("// {}", exception.reason));
        rust.open(format!(
            "if api_key.value() == {} && version.value() >= {}",
            exception.api_key,
            exception.first_version()
        ));
        rust.line(format!("return {};", exception.header_version));
        rust.close("");
    }
    rust.line("i16::from(flexible)");
    rust.close("");
    rust.finish()
}
