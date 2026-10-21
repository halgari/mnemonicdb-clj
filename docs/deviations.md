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
| Values | `-0.0` and `0.0` are equal | Distinct keys; `-0.0` sorts before `0.0` | Byte-order encoding of IEEE bits |
| Transactions | Negative longs may be used as raw entity ids | Rejected | Avoid ambiguous ids |
| Transactions | Reverse refs (`:ns/_attr`) on non-ref attrs | Rejected with an error | Clear error |
| `d/with` | Allowed on as-of and filter dbs | Throws `:db.error/unsupported` | Not yet implemented (overlay over a time/filter view) |
| `:db/noHistory` | Mem db keeps history until indexing | Never written to history | No indexing job; matches durable semantics |
| Bootstrap history | Two retractions at t0 | Not present | Bootstrap is loaded as final state only |
| `d/next-t` on a with-db | Returns the speculative next t | Returns the base db's next t | Minor; not yet implemented |
| as-of schema | (n/a) | as-of dbs use the latest schema | Simplicity; schema needed to decode |
| `d/as-of`/`d/since` with a `java.util.Date` | Converts via log | Converted via `:db/txInstant` scan; `as-of-t` may differ | Not diff-tested |
| Pull | `:xform` fn must be listed in `datomic/extensions.edn` | Any fn, var, keyword, or symbol of an already-loaded namespace (unqualified symbols resolve in `clojure.core`); namespaces are never loaded on demand | No extensions.edn; no code loading from patterns |
| Pull/Entity | A component cycle (A comp-ref B, B comp-ref A) overflows the stack under `[*]`, default component expansion and `d/touch` | The component already being expanded on the path is rendered as `{:db/id n}`; touch leaves it untouched | Robustness |
| Entity | `EntityMap` is not callable | `Entity` also implements `IFn` (`(e :attr)`) | Superset, requested by the design |
| Entity | `str`/print of a touched entity includes cached reverse lookups | Prints forward attributes only | Cosmetic |
| Pull | String attribute names (`["*"]` gives string keys; `[":p/name"]`) | Rejected with `:db.error/invalid-attr-spec` | Rarely used |
| Pull | `[* {:p/boss 1}]` (wildcard plus depth recursion) throws an NPE | Returns the expected result | Datomic bug |
| Entity/Pull | History dbs return retractions in datoms | Entity and pull read asserted datoms only | Retractions have no meaning for a map view |
