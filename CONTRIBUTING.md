# Contributing

The generated wire corpus is an executable provenance contract. Before a
change, identify whether the owner is schema ingestion, validation, code
generation, runtime wire primitives, RecordBatch mechanics, or conformance.
Do not hand-edit generated modules.

For each patch:

1. Keep the change within one named ownership boundary.
2. Add the narrowest positive and negative evidence beside that owner.
3. Regenerate only through the checked-in `xtask` command surface.
4. Run `just check` and inspect `git diff --check` plus the complete diff.
5. Preserve the pinned Apache Kafka revision and generated-tree identity unless
   the patch is an explicit, reviewed corpus advance.

Do not weaken an architecture check merely to make a change pass. A real exception needs
a narrow rule, a written reason, an owner, and a removal condition.

Participation is governed by [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md).
Report security issues through [`SECURITY.md`](SECURITY.md), never a public
issue.
