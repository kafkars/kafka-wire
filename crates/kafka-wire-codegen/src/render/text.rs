//! Tiny indentation-aware text sink for complete generated files.

/// Deterministic line-oriented Rust source builder.
#[derive(Debug, Default)]
pub(crate) struct RustText {
    output: String,
    indent: usize,
}

impl RustText {
    pub(crate) fn line(&mut self, line: impl AsRef<str>) {
        for _ in 0..self.indent {
            self.output.push_str("    ");
        }
        self.output.push_str(line.as_ref());
        self.output.push('\n');
    }

    pub(crate) fn blank(&mut self) {
        self.output.push('\n');
    }

    pub(crate) fn open(&mut self, line: impl AsRef<str>) {
        self.line(format!("{} {{", line.as_ref()));
        self.indent += 1;
    }

    pub(crate) fn close(&mut self, suffix: &str) {
        self.indent = self.indent.saturating_sub(1);
        self.line(format!("}}{suffix}"));
    }

    pub(crate) fn reopen(&mut self, line: &str) {
        self.indent = self.indent.saturating_sub(1);
        self.line(line);
        self.indent += 1;
    }

    pub(crate) fn finish(mut self) -> String {
        while self.output.ends_with("\n\n") {
            self.output.pop();
        }
        self.output
    }
}
