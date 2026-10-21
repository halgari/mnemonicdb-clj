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
| Query | `:in $` with no arguments throws `:db.error/too-few-inputs` (a query without `:in` accepts zero args) | Both accepted; `$` fails with `:db.error/invalid-data-source` on use | The parser AST does not distinguish the implicit `[$]` from an explicit `:in $` |
| Query fns | Allowlist plus anything the Datomic compiler resolves | Allowlist, any `clojure.core` var, auto-required qualified vars, `Class/static`, `.instance`, and `?f` variables holding a fn | Superset; matches probed Datomic for clojure.core, interop and qualified names |
| Query fns | `< > <= >=` binary only | Same (compare-based, works on strings/Dates); `+ - * /` etc. variadic | Probed equal |
| Query | `:limit`/`:offset` in a `d/query` map | Ignored (as Datomic 1.0.7745 does) | Probed equal |
| Query | `fulltext` | Throws `:db.error/unsupported` | Not implemented |
| Query errors | Plain exceptions for unresolvable fns, bad binding values, nil inputs | `ex-info` with `:db.error/invalid-query` (unresolvable ident/lookup ref: `:db.error/not-an-entity`) | Datomic has no keyword for these |
| Query timeout | `java.util.concurrent.TimeoutException` wrapped by a plain exception (no `:db/error` keyword; probed) | `ex-info` `:db.error/query-timeout` with a `TimeoutException` cause, checked at clause boundaries, every 4096 tuples/datoms, in rule rounds and in grouping | Datomic has no keyword to match |
| Query | Deferral of predicates/fn exprs until args are bound | Same (probed: a predicate before the pattern that binds its vars works); constant-only predicates are evaluated but never filter | Probed equal |
| Query rules | A rule call with more arguments than the rule head silently ignores the extras | `:db.error/invalid-query` | Catches typos; fewer arguments is an error in both |
| Query rules | Rule bodies cannot use `$` explicitly (`Cannot resolve key: $`); head variable left unbound by a body fails with `IndexOutOfBoundsException` | `$` in a rule body (explicit or default) is the source of the rule call; an unbound head variable is `:db.error/insufficient-binding` | Datomic quirks |
| Query rules | A pattern like `[?x :a ?x]` in a rule body binds ?x to the attribute values (quirk) | Matches only datoms whose entity equals the value | Datomic quirk, not reproduced |
| Query | Top-level `(and ...)` and `(not (and ...))` fail to compile | Accepted as a conjunction | Parser keeps `:and` clauses |
| Query pull | `(pull ?e pattern)` takes a bare symbol bound by a top-level scalar `:in`; a `?`-prefixed pattern variable is rejected (`:db.error/invalid-pull`) | Bare symbols work as in Datomic (unbound: `:db.error/pattern-not-bound`); a `?`-prefixed variable is also accepted as the pattern input | Superset |
| Query pull | Several pull elements over the same variable crash with an array-index error | Work | Datomic bug |
| Query aggregates | `variance`/`stddev` over doubles sum in Datomic's set order | Sum in tuple order (equal for exactly representable data, may differ in the last digits) | Order of a de-duplicated set is unspecified |
| Query qseq | Accepts only a query map; a scalar `.` find throws a seq error | Also accepts `(qseq q & inputs)`; scalar finds throw the same seq error; empty results are `nil`; pulls are realized one element at a time | Superset |
| Query rules | Explicit source vars in rule bodies fail even when declared (`Cannot resolve key: $2`) | Declared ones work; an undeclared one fails with `:db.error/invalid-query` when the rule is called | Superset |
| Query rules | A rule call whose evaluation would need a full scan (e.g. all-free calls of a rule whose body starts with an unbound pattern) throws `:db.error/insufficient-binding` ("would cause full scan") in some shapes | Calls with all-free arguments are evaluated bottom-up to their fixpoint and succeed whenever the body's own patterns are scannable | Bottom-up evaluation does not depend on call-site binding order |
| Query rules | Top-down evaluation | Demand-driven semi-naive evaluation; deep linear recursion re-seeds each round with all demanded tuples, so a chain of n nodes costs about O(n^2) (1500-chain over a collection ~4 s; trees and db-backed patterns are fast) | Simplicity; no per-tuple dependency tracking |
| Query timeout | See above | The keyword is `:db.error/query-timeout` (not `:db.error/timeout`): Datomic has no keyword to match, so the Task 8 name was kept | Own error keyword |
| Values | Two byte arrays with equal content asserted on the same card-one attribute in one tx fail with `:db.error/datoms-conflict` (compared by identity) | Compared by content: they are the same value | Identity comparison of arrays is an accident |
| Transact | Asserting the same unique-identity value twice for one tempid in a tx that also unifies tempids (e.g. `[:db/add "t0" :a v1] {:a [v2 v1 v2]}`) fails with a spurious `:db.error/datoms-conflict` | Duplicate identical assertions are deduplicated; the tx succeeds | Datomic's behavior depends on the repeated assertion, which has no meaning |
| Transact | A tx with several simultaneous uniqueness conflicts involving tempid unification and more than one unique-identity attribute reports `:db.error/unique-conflict` or `:db.error/datoms-conflict` depending on Datomic's internal processing order | Reports whichever our upsert order meets first; the tx fails either way | Only the error kind of an already-failing tx differs |
| Transact | Tempid unification in one tx when a tempid carries several identity assertions (several values of a cardinality-many identity attribute, or values of several `:db.unique/identity` attributes) follows Datomic's undocumented internal order | Processed op by op: assertions join or bind in order, an assertion and a retraction of one new value for different tempids conflict, a tempid joined to others cannot also bind to an existing entity; outcomes (or the conflict kind) can differ from Datomic in such transactions | The generative suite keeps one cardinality-one identity attribute per schema
