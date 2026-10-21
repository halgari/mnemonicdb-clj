# Deviations from Datomic

Intentional differences between MnemonicDB and the Datomic Peer API, with rationale.

| Area | Datomic | MnemonicDB | Why |
|---|---|---|---|
| Query parsing | Unknown keys in map-form queries are ignored | Rejected with `:db.error/invalid-query` | Catches typos early |
| Query parsing | Data patterns tolerate a 6th term | Capped at 5 terms (`[?e ?a ?v ?tx ?added]`) | Extra terms have no meaning |
| Query parsing | `:with` var also in `:find`, or duplicate plain find vars, crash internally | Rejected with `:db.error/invalid-query` | Clear error instead of internal crash |
| Query parsing | Nested `:in` bindings `[[?a ...]]` accepted without real semantics | Rejected | Avoid silently wrong results |
| Value order | `0.0` and `-0.0` compare equal in index order (tie broken by entity id) | `-0.0` sorts strictly before `0.0` (IEEE total order, value is part of the key) | Keys are bytewise; the sign of zero cannot be kept in the value yet compare equal |
| Transact | A raw negative long used as an entity id is a tempid (placed in an internal partition) | Rejected with `:db.error/invalid-entity-id`; use strings or `d/tempid` | Undocumented behavior with an arbitrary partition |
| Transact | Reverse attribute of a non-ref attribute (`{:p/_age e}`) is accepted for long attributes (asserts the tempid's raw number) | Rejected with `:db.error/wrong-type-for-attribute` | Datomic's behavior is a type-check hole |
| Transact | Entity in `:db.part/db` carrying a unique attribute plus a non-schema attribute fails with an internal `ArithmeticException` | `:db.error/schema-without-install` | Datomic's error is an internal bug |
| Transact | Changing `:db/valueType` of an existing attribute follows Datomic's schema-alteration rules | `:db.error/unsupported` until schema alteration is implemented | Schema alteration is milestone M6 |
| Transact | `d/transact` returns a `datomic.promise` future | Returns a `CompletableFuture`; deref throws `ExecutionException` wrapping the `ex-info` exactly as Datomic | Same observable behavior |
| API | `d/datoms` with an unknown index throws `IllegalArgumentException` | `ex-info` with `:db.error/invalid-index` | Consistent error shape |
