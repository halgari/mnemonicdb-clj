# Deviations from Datomic

Intentional differences between MnemonicDB and the Datomic Peer API, with rationale.

| Area | Datomic | MnemonicDB | Why |
|---|---|---|---|
| Query parsing | Unknown keys in map-form queries are ignored | Rejected with `:db.error/invalid-query` | Catches typos early |
| Query parsing | Data patterns tolerate a 6th term | Capped at 5 terms (`[?e ?a ?v ?tx ?added]`) | Extra terms have no meaning |
| Query parsing | `:with` var also in `:find`, or duplicate plain find vars, crash internally | Rejected with `:db.error/invalid-query` | Clear error instead of internal crash |
| Query parsing | Nested `:in` bindings `[[?a ...]]` accepted without real semantics | Rejected | Avoid silently wrong results |
