# MnemonicDB — Design Spec

Date: 2026-10-20
Status: Draft for review

## 1. Purpose

MnemonicDB is an **embedded, in-process, single-writer** database for Clojure that
is API-compatible with the **Datomic Peer API** (`datomic.api`). It stores data in
RocksDB and is written in Clojure (Java only where profiling proves it necessary).

It is *not* a distributed Datomic clone: there is no transactor process, no peer
cache protocol, no storage service. One JVM process owns the database directory.

### Success criteria

- Code written against `datomic.api` runs against `mnemonic.api` with a namespace
  swap, for every feature in scope (§10).
- Differential tests against Datomic Pro's in-memory peer agree on tempid
  resolution, ids/`t`, datom order in every index, time-travel views, entity,
  pull, query results, and error kinds — except for an explicit, documented
  allowlist of deviations.
- Runs on Linux, macOS, and Windows with no native code beyond what
  `rocksdbjni` ships.
- Performance is a goal, but correctness first; a dedicated performance milestone
  (M7) follows functional completeness.

### Constraints

- Latest stable Clojure (1.12.x) and latest OpenJDK (27, Temurin).
- RocksDB via `org.rocksdb/rocksdbjni` (11.8.1) used directly through interop.
  No third-party Clojure RocksDB wrapper. No FFM/Panama bindings (would require
  shipping our own native `librocksdb` per platform).
- Single writer, in-process. Many concurrent readers.

## 2. Architecture overview

```
mnemonic.api            Peer API facade (datomic.api-compatible)
 ├─ mnemonic.query.*    Datalog: parser, planner, executor, rules, aggregates
 ├─ mnemonic.pull       Pull patterns
 ├─ mnemonic.entity     Lazy entity maps
 ├─ mnemonic.tx         Transactor pipeline (single-writer executor)
 ├─ mnemonic.db         Db layers: RocksDb, AsOf, Since, History, Filter, With
 ├─ mnemonic.index      IIndex / ICursor protocols, merge & filter cursors
 ├─ mnemonic.rocks      rocksdbjni wrapper, CF handles, snapshots, cursors
 └─ mnemonic.codec      Order-preserving value/key/datom encoding
```

Every consumer above `mnemonic.db` (transactor lookups, entity, pull, query,
`d/datoms`) talks **only** to the `IIndex` interface, so every feature works
on every kind of db (current, as-of, since, history, filtered, with).

Implementation approach: Clojure throughout, using `deftype`/`definterface`
with primitive type hints, zero reflection, and rocksdbjni's direct-`ByteBuffer`
APIs for zero-copy iteration. Hot classes may be moved to Java in M7 behind the
same interfaces.

## 3. Identifiers and time

Matches Datomic's model exactly.

- Entity ids are 64-bit longs: `(partition << 42) | n`.
- Built-in partitions: `:db.part/db` (0), `:db.part/tx` (3), `:db.part/user` (4).
  User-defined partitions are supported.
- `t` and entity-id `n` come from **one shared, monotonically increasing
  counter**, as in Datomic. A transaction allocates its tx entity's `t` first,
  then new entities consume subsequent values, producing the same gaps in `t`
  that Datomic produces.
- The **tx entity id** is `t->tx(t) = (3 << 42) | t`. Datoms store the **tx
  entity id** (so `(:tx datom)` needs no conversion). `d/t->tx`, `d/tx->t`,
  `d/part`, `d/entid-at` behave as in Datomic.
- `:db/txInstant` is assigned per transaction from the wall clock, forced to be
  monotonically non-decreasing.

## 4. Storage layout

### 4.1 Column families

| CF | Key | Value |
|---|---|---|
| `default` | metadata keys (format version, counters) | metadata |
| `eavt` `aevt` `avet` `vaet` | encoded datom (with asserting tx) | empty |
| `eavt-h` `aevt-h` `avet-h` `vaet-h` | encoded datom (with asserting tx) | retracting tx (8 bytes) |
| `log` | tx id (8 bytes BE) | encoded tx datoms `[e a v tx added]` |

- **Current** CFs hold every currently-asserted datom. Value is empty.
- **History** CFs hold every assertion that has since been retracted; the
  RocksDB value is the tx id that retracted it. "Has a value" ⇔ "retracted".
- AVET contains every attribute (not only `:db/index`/`:db/unique` ones), unless
  differential testing shows Datomic's `d/datoms :avet` must reject non-indexed
  attributes, in which case we match Datomic's behavior at the API level.
- VAET contains only `:db.type/ref` attributes.

### 4.2 Key layouts

E, A, TX are 8-byte big-endian longs (all non-negative, so unsigned order ==
signed order). V is variable-length (§4.3).

| Index | Layout |
|---|---|
| EAVT | `E A V TX` |
| AEVT | `A E V TX` |
| AVET | `A V E TX` |
| VAET | `V A E TX` (V is an 8-byte ref) |

All keys compare correctly with RocksDB's default bytewise comparator. No
custom (JNI-callback) comparator.

### 4.3 Value encoding

`[type-tag:1 byte][payload]`, with payloads self-delimiting and order-preserving:

| Type | Payload |
|---|---|
| long, instant (epoch ms), ref | 8 bytes, sign bit flipped |
| double, float | IEEE-754 bits: if negative flip all bits, else flip sign bit |
| boolean | 1 byte |
| uuid | 16 bytes, sign bit of each 64-bit half flipped (matches `UUID.compareTo`) |
| string, uri, bytes | escaped + terminated: `0x00`→`0x00 0xFF`, end = `0x00 0x00` |
| keyword, symbol | namespace (escaped+terminated) then name (escaped+terminated) |
| bigint | sign, length, magnitude (order-preserving) |
| bigdec | sign, exponent, digits (order-preserving, scale-normalized) |
| tuple | element encodings concatenated, then a terminator byte below every tag |

**Goal: match Datomic's value sort order exactly.** The encoding above is the
starting hypothesis; differential tests on `d/datoms` / `d/index-range` are the
authority. Known risk: Java's `String.compareTo` orders by UTF-16 code unit, while
UTF-8 bytes order by code point; they differ only for supplementary characters vs
`U+E000–U+FFFF`. If Datomic follows UTF-16 order, the string encoding is adjusted.

### 4.4 Log encoding

Log value: count, then for each datom `E A V added?` (TX is the key). V uses the
same value codec. `d/tx-range` / `d/log` iterate the `log` CF.

## 5. Writes

### 5.1 Single writer

A single-threaded executor is the transaction queue. `d/transact` and
`d/transact-async` enqueue and return a future; `d/transact` callers deref it.
Serialization is by construction; no locks held across the async API.

### 5.2 Write batch

Each transaction commits as **one atomic `WriteBatch`**:

1. Put each asserted datom into the 4 current CFs (VAET only for refs).
2. For each retraction: delete the key from the current CFs and put it into the
   matching history CFs with value = retracting tx. For `:db/noHistory`
   attributes, delete only (no history entry).
3. Put the log entry.
4. Update counters in `default`.

After the write, the connection publishes a new db value (new snapshot).

### 5.3 Recovery

On open, basis-t is the last key in `log`; counters are read from `default` and
validated against the log. No separate WAL beyond RocksDB's own.

## 6. Reads and the layer model

### 6.1 Interfaces

```clojure
(defprotocol IIndex
  (-seek   [db index components]) ; -> ICursor positioned at first datom >= components
  (-range  [db attr start end])   ; -> ICursor for index-range over AVET
  (-basis-t [db])
  (-schema  [db]))

(definterface ICursor            ; stateful, single-thread, flyweight
  (^boolean valid [])
  (^void next [])
  (datom [])                     ; reused flyweight Datom; valid until next
  (copy []))                     ; materialized immutable Datom
```

`Datom` behaves like Datomic's: `:e :a :v :tx :added`, `nth` 0–4, destructurable,
equality/hash by value. The flyweight decodes E/A/TX directly from the key buffer
and decodes V lazily.

### 6.2 Layers

Each layer wraps an `IIndex` and is an `IIndex`:

| Layer | Semantics |
|---|---|
| `RocksDb` | Base. Holds a RocksDB `Snapshot`, basis-t, schema cache. Scans current CFs. |
| `AsOf t` | Merge of current (`tx ≤ t`) and history (`tx_assert ≤ t < tx_retract`). |
| `Since t` | Datoms with `tx > t` (current), per Datomic `since` semantics. |
| `History` | Merge of current and history; each history entry yields an assert datom at `tx_assert` then a retract datom at `tx_retract`. Within one `e a v` these are already in tx order. |
| `Filter pred` | Drops datoms failing `(pred db datom)` (`d/filter`). |
| `With` | In-memory sorted overlay of speculative datoms (asserts and retractions) merged over any layer (`d/with`). Also used by the transactor to stage a tx. |

Layers compose freely, e.g. `(d/filter (d/as-of (d/history db) t) p)`. Merges are
k-way merge cursors comparing raw key bytes; no datom allocation on the hot path.

### 6.3 Db value lifetime

`d/db` returns a `RocksDb` holding a RocksDB snapshot. Snapshots are released
when the db value becomes unreachable (`java.lang.ref.Cleaner`) or explicitly via
`mnemonic.api/release-db`. Known cost: long-lived db values keep tombstones from
being compacted.

Native safety (no public path may touch a closed or freed RocksDB object):

- Every store-level native call (new iterator, get, write batch build + write,
  snapshot create/free) runs inside the store guard: a striped in-flight counter
  (one atomic add on a per-thread stripe) plus a `closed` flag that is re-checked
  after the increment. `close-store` sets `closed` first, then waits until every
  stripe is zero (sequential consistency: a caller either sees `closed` and backs
  out, or the closer sees its increment and waits), then force-closes every
  registered iterator, frees every registered snapshot, and only then closes column
  family handles, the db and options. Later calls throw
  `:db.error/connection-released`. (This replaced a `ReentrantReadWriteLock`, whose
  shared read-lock word serialized concurrent readers.)
- Iterators live in slots. A slot serializes its native calls on its monitor and
  checks its state first (one monitor enter per datom: `nextKey` advances and reads
  the key in one call); force-close and destroy take the same monitor and set the
  state before closing the native iterator, exactly once. A caller holds a lease
  (`RawCursor`) on a slot; a closed lease never touches the slot again.
- Snapshots are reference counted (one reference per live slot, leased or idle).
  Closing a cursor returns its slot to a bounded per-snapshot pool (stacks per
  column family, mode and thread stripe, all sharing the snapshot's `ReadOptions`)
  instead of closing the iterator. `release-db` marks a snapshot released (new
  cursors throw `:db.error/db-released`), destroys the idle slots (a returning slot
  re-checks the flag after pushing, so a release racing a return cannot strand a
  slot) and the native snapshot is freed when the last slot is destroyed. Lock
  order: slot monitor -> store guard (non-blocking counter); snapshot state is a CAS
  word, never held across a lock.
- Prefix bloom filters: the index column families use a fixed-length prefix
  extractor (16 bytes for EAVT/AEVT/VAET, `avet-prefix-len` for AVET) and a
  prefix-only filter. Prefix cursors whose prefix spans at least one extractor
  prefix run in RocksDB's default (prefix) mode; every other cursor (seek-datoms,
  index-range, short prefixes, history seeks) uses total_order_seek. SST files record
  the extractor they were built with, so the extractor is not a format change.
- An iterator that becomes invalid has its `status()` checked, so I/O errors and
  corruption surface as `:db.error/storage-error` faults.
- Connections: file paths are canonicalized for the registry; connect / create /
  delete of a database wait for a pending close of the same store. A released
  current db is rebuilt (on the writer thread, inline when already on it).
- The on-disk format version (meta `:format`, currently 2) and the counters
  (`next-t` > last log t) are checked on connect.

## 7. Transactor pipeline

Runs on the writer thread against a `With` overlay over the latest db.

1. **Expand** tx-data into flat `[:db/add|:db/retract e a v]` ops:
   - Maps, nested maps, reverse refs (`:_attr`), component sub-entities, `:db/id`.
   - `:db/add`, `:db/retract` (with and without value), `:db/retractEntity`
     (cascades to components), `:db/cas`.
   - Transaction functions: classpath fns (qualified symbol) and stored `:db/fn`
     (Clojure code, evaluated and cached). Called with the in-progress db;
     results expanded recursively.
   - Entity references: ids, idents, lookup refs, tempids (strings,
     `d/tempid` records, `"datomic.tx"` for the current tx / tx metadata).
2. **Resolve tempids**: allocate from the shared counter in the right partition;
   `:db.unique/identity` upsert iterated to fixpoint; tempid unification.
3. **Validate & compute datoms**:
   - Value type coercion/checking per `:db/valueType`; refs resolved.
   - Cardinality-one implicit retraction; redundant assertions elided;
     retracting a nonexistent datom is a no-op.
   - `:db.error/datoms-conflict`, `:db.error/unique-conflict`.
   - `:db.attr/preds`, `:db/ensure` entity specs (`:db.entity/attrs`,
     `:db.entity/preds`).
   - Composite `:db/tupleAttrs` maintained automatically.
   - Schema alteration rules as Datomic; new attributes usable from the next tx.
   - Tx entity gets `:db/txInstant`.
4. **Write** (§5.2), publish db, deliver tx report
   `{:db-before :db-after :tx-data :tempids}` to the future and to
   `d/tx-report-queue` subscribers.

Errors are `ex-info` with Datomic's data keys (`:db/error :db.error/...`,
`:cognitect.anomalies/category`). A failed transaction writes nothing.

Bootstrap: a fresh database is seeded at the first tx with Datomic's system
schema (`:db/ident`, `:db/valueType`, `:db/cardinality`, `:db/unique`, ...,
value-type and cardinality idents, partitions).

## 8. Entity, pull, query

### 8.1 Entity

Lazy, cached map-like value over EAVT. Card-many → sets; refs → entities;
components nest; reverse lookup via VAET (`(:_attr e)`); `:db/id`; `d/touch`;
`d/entity-db`; equality by `[db eid]`. Behaves like Datomic's `EntityMap`.

### 8.2 Pull

Full grammar: wildcard (with component auto-expansion), nested maps, reverse refs,
attribute options `:as :limit :default :xform` (map and legacy list syntax),
recursion (`...` and depth), cycle detection. `d/pull`, `d/pull-many`.

### 8.3 Query engine (own implementation)

- **Parse** to a typed AST, cached by query form. All find specs (relation,
  scalar `.`, collection `...`, tuple), pull in `:find`, `:keys/:syms/:strs`,
  `:with`, all `:in` binding forms, multiple `$` sources (dbs or collections),
  `%` rules.
- **Clauses**: data patterns, predicates, function expressions with all binding
  forms, rule invocation, `not`, `not-join`, `or`, `or-join`, `and`,
  `get-else`, `get-some`, `missing?`, `ground`, `tuple`, `untuple`; classpath
  fns by qualified symbol and Datomic's allowlist of `clojure.core` fns.
- **Aggregates**: `count`, `count-distinct`, `sum`, `avg`, `min`/`max` (with
  and without n), `median`, `variance`, `stddev`, `distinct`, `rand`, `sample`,
  plus custom aggregate fns.
- **Execution**: relations as tuple sets; clauses evaluated **in written order**
  (Datomic semantics, predictable). Per data pattern, choose the best index given
  bound variables; small bound relations → per-binding cursor seeks; large →
  range scan + hash join.
- **Rules**: semi-naive bottom-up evaluation (handles recursion).
- `d/query` with `:timeout`; `d/qseq` lazy results with lazy pull.
- Future, opt-in: cost-based clause reordering.

## 9. Remaining Peer API

`d/datoms`, `d/seek-datoms`, `d/index-range`, `d/log`, `d/tx-range`,
`d/entid`, `d/ident`, `d/attribute`, `d/basis-t`, `d/next-t`, `d/as-of-t`,
`d/since-t`, `d/is-history`, `d/as-of`, `d/since`, `d/history`, `d/filter`,
`d/with`, `d/squuid`, `d/squuid-time-millis`, `d/tempid`, `d/resolve-tempid`,
`d/function`, `d/invoke`, `d/create-database`, `d/delete-database`,
`d/connect`, `d/release`, `d/sync` (immediate), `d/request-index` (no-op),
`d/tx-report-queue`, `d/remove-tx-report-queue`.

URIs:
- `mnemonic:file:///path/to/dir` — on-disk RocksDB.
- `mnemonic:mem://name` — in-memory, via rocksdbjni's built-in memory `Env`
  (no extra native code). Primarily for tests.

## 10. Scope

**In scope**: everything in §§3–9.

**Deferred** (accepted in schema, rejected with a clear `:db.error/unsupported`
error at use): `:db/fulltext` and the `fulltext` query function (needs a search
index, likely Lucene, later); excision.

**Out of scope**: distributed transactor/peers, client API, Datomic storage
protocols, backup/restore compatibility with Datomic.

## 11. Project setup

- `deps.edn` + `tools.build`; `mise.toml` pinning JDK 27 and Clojure CLI.
- Dependencies: `org.clojure/clojure 1.12.x`, `org.rocksdb/rocksdbjni 11.8.1`.
- Aliases: `:test` (cognitect test-runner, `test.check`,
  `com.datomic/peer 1.0.7745` as test oracle only), `:bench` (criterium).
- `*warn-on-reflection*` on everywhere; reflection in hot namespaces fails the
  build.
- GitHub Actions matrix: Linux, macOS, Windows.

## 12. Testing

1. **Codec properties** (test.check): round-trip for every type; byte order ==
   value order, including NaN, -0.0, `\u0000`, supplementary characters,
   extreme longs/bigdecs.
2. **Storage invariants** after random tx sequences: the 4 current CFs contain the
   same datom set; likewise the 4 history CFs; replaying the log reconstructs both;
   `as-of t` equals a db rebuilt from the log up to `t`.
3. **Differential vs Datomic** (`datomic:mem://`): generated schemas + tx-data
   (tempids, upserts, retractions, card-one replacement) applied to both; compare
   `:tempids`, `t`/ids, `d/datoms` order for every index, as-of/since/history,
   entity, pull, a generated query corpus, and error kinds. Intentional deviations
   live in an explicit allowlist with rationale.
4. **Example tests** from Datomic's documented behavior for edge cases.

## 13. Milestones

Each milestone ends with its differential tests green.

| | Milestone | Contents |
|---|---|---|
| M1 | Storage core | codec, rocks wrapper, cursors, bootstrap schema, basic transactor (maps, add/retract, tempids, cardinality, uniqueness/upsert), snapshot dbs, `datoms`/`seek-datoms`/`index-range`, connect/create/delete |
| M2 | Layers | `as-of`, `since`, `history`, `filter`, `with`, log API, `tx-report-queue` |
| M3 | Entity & pull | §8.1, §8.2 |
| M4 | Query core | patterns, joins, predicates/fns, all `:in`/`:find` forms, multiple sources |
| M5 | Query complete | rules, `not`/`or`, aggregates, pull-in-find, `qseq`, `:timeout` |
| M6 | Advanced tx | tx fns (classpath + `:db/fn`), `cas`, `retractEntity`, tuples, `:db/ensure`, attr preds, schema alteration |
| M7 | Performance | benchmarks, profiling, Java for proven hot spots |
