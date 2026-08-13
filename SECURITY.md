# Security policy

## Supported versions

There is no published stable release yet. Security fixes are made on `main` and
the latest 0.1 release candidate while 0.1.0 is being qualified. After 0.1.0 is
published, only the latest 0.1 release will receive security fixes until a
newer supported line is announced here.

| Version | Supported |
| --- | --- |
| `main` | Yes |
| Latest published 0.1 release candidate | Yes |
| Older prereleases | No |

## Reporting a vulnerability

Do not open a public issue. Use the repository's private GitHub vulnerability
reporting form. If that form is unavailable, email `shawn@zsumz.com` with
`[kafka-wire security]` in the subject.

Include the affected revision or version, the smallest practical reproducer,
the security impact, and any known mitigations. Do not include live credentials
or third-party private data. You should receive an acknowledgement within three
business days and an initial triage assessment within seven; those targets are
coordination goals, not a service-level agreement.

Please allow a reasonable remediation and coordinated-disclosure window. The
maintainer will credit reporters who want attribution and will keep reporters
informed when a fix, advisory, or release is ready.

## Scope

This policy covers wire framing, bounded encode/decode, generated message
types, schema ingestion, RecordBatch mechanics, and compression code owned by
this repository. Apache Kafka broker, `kafka-driver`, and high-level `kafkars`
issues belong with their respective maintainers.
