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
| Transact | `:db/fn` values are `datomic.function.Function` objects | `m/function` returns a callable record; the stored datom value is the EDN text of its map (`:lang :imports :requires :params :code`), so reads return a string | The value codec has no function type |
| Transact | Excision attributes (`:db/excise`, `:db.excise/*`) are accepted | Rejected with `:db.error/unsupported` | Excision is out of scope |
| Transact | Every alteration of an attribute adds a fresh `[0 :db.alter/attribute a]` datom | Added only if the attribute has none yet | Avoids two current datoms with the same e/a/v |
| Index | After `:db/index` or `:db/unique` is removed, `d/datoms :avet` still lists the attribute until reindexing | Stops listing it immediately | AVET visibility follows the current schema |
| Transact | Entity preds (`:db/ensure`) get the real db-after | db-after is an overlay over db-before: schema changes made in the same tx are not visible to the pred, and `history` is unsupported on it | No With layer in the transactor |
| Transact | `(d/invoke db :db.fn/retractEntity ...)` returns ops with numeric operation ids | Returns `[:db/retract e a v]` vectors | Same effect when transacted |
| Transact | Stored tx functions are cached per definition | Cache is keyed by the function text (not `[eid tx]`) so it is safe across databases | Same eval-once behavior |
