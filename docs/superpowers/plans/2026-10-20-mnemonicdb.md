# MnemonicDB Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An embedded, single-writer, Datomic-Peer-API-compatible database for Clojure on RocksDB.

**Architecture:** Order-preserving byte codec → thin rocksdbjni wrapper (10 CFs) → `IDb` protocol with composable layers (Base, With, TimeView, Filter) → transactor pipeline (pure: db-before + tx-data → ops) → entity / pull / Datalog engine → `mnemonic.api` facade mirroring `datomic.api`.

**Tech Stack:** Clojure 1.12.6, Temurin JDK 27, `org.rocksdb/rocksdbjni 11.8.1`, test.check, cognitect test-runner, `com.datomic/peer 1.0.7745` (test oracle only).

**Spec:** `docs/superpowers/specs/2026-10-20-mnemonicdb-design.md`

## Global Constraints

- Clojure `1.12.6`, JDK `temurin-27.0.0+35` (pinned in `mise.toml`), rocksdbjni `11.8.1`. No other runtime deps.
- Run tooling via `mise exec -- clojure ...` (or with mise activated, plain `clojure`).
- Every source ns starts with `(set! *warn-on-reflection* true)`; `test/mnemonic/reflection_test.clj` fails on any reflection warning in `src/`.
- No native code besides rocksdbjni. No custom RocksDB comparator (bytewise default only).
- Datomic semantics are the authority. When behavior is unclear, **probe Datomic** (`datomic:mem://` via `com.datomic/peer` on the `:test` alias) and match it; record intentional deviations in `docs/deviations.md`.
- Errors: `ex-info` with `{:db/error :db.error/<kind>, :cognitect.anomalies/category :cognitect.anomalies/<cat>}` matching Datomic's keyword where one exists. Unsupported features: `:db.error/unsupported`.
- Subagents do **not** `git commit`; the controller commits after each task's review. Only edit files listed in your task.
- Test commands: all tests `clojure -M:test`; one ns `clojure -M:test -n <ns>`.

## Pinned facts (probed from Datomic 1.0.7745)

- Bootstrap: 272 datoms, 64 idents, eids 0–72, basis-t `72`, `next-t` `1000`. Exact datoms in `resources/mnemonic/bootstrap.edn` as `[e a v tx]`, `v` of `(inst <ms>)` = `java.util.Date`.
- System attr ids: `:db/ident 10`, `:db.install/partition 11`, `:db.install/valueType 12`, `:db.install/attribute 13`, `:db/valueType 40`, `:db/cardinality 41`, `:db/unique 42`, `:db/isComponent 43`, `:db/index 44`, `:db/noHistory 45`, `:db/txInstant 50`, `:db/fulltext 51`, `:db/fn 52`, `:db/doc 62`, `:db/tupleType 65`, `:db/tupleTypes 66`, `:db/tupleAttrs 67`, `:db/ensure 68`, `:db.attr/preds 71`. Partitions: `:db.part/db 0`, `:db.part/tx 3`, `:db.part/user 4`.
- Ids: `eid = (part << 42) | n`; `tx = (3 << 42) | t`.
- Counters: `:db.part/db` has its own counter (next n = `73` after bootstrap). Tx `t` and all other partitions share one counter (starts at `1000`). A tx takes `t` first, then its new non-db entities take subsequent n. Example: schema tx (2 attrs) → t=1000, attrs eids 73, 74; next tx with 2 new user entities → t=1001, eids `(4<<42)|1002`, `(4<<42)|1003`.
- New attribute entity (has `:db/valueType`) gets partition `:db.part/db`; Datomic adds `[0 :db.install/attribute <aid>]` automatically. Every tx asserts `[tx :db/txInstant <Date>]`.
- `d/datoms :avet` and `d/index-range` on an attr without `:db/index`/`:db/unique` throw `:db.error/attribute-not-indexed`. (We still store all attrs in AVET for query use.)
- `d/datoms` returns an `Iterable` (not `Counted`); `seq` it.

## Review Focus

1. Strings containing `\u0000` and supplementary chars, doubles `NaN`/`-0.0`/`±Inf` must round-trip and sort like Datomic in `d/datoms` — tested in Task 1 and the differential suite (Task 5).
2. Reassert-after-retract of the same `[e a v]` (card-many and card-one) across several txs: as-of at every t and history must be exact — tested in Task 6.
3. Close and reopen an on-disk db: basis-t, both counters, schema, and log restored; next tx ids continue correctly — tested in Task 5.
4. Reader iterating an old db value while the writer commits many txs sees a stable view; thousands of `d/db` calls do not leak snapshots/iterators (Cleaner) — tested in Task 4/5.
5. Two tempids that upsert to the same `:db.unique/identity` entity unify; a tempid used both as a lookup-ref target and with an identity value — tested in Task 5.

---

## Waves (parallelism)

| Wave | Tasks (parallel within a wave) |
|---|---|
| 0 | Task 0 (controller) |
| 1 | Task 1 codec, Task 2 rocks, Task 3 query parser |
| 2 | Task 4 index/db base |
| 3 | Task 5 transactor + api + oracle |
| 4 | Task 6 layers, Task 7 entity+pull, Task 8 query engine, Task 9 advanced tx |
| 5 | Task 10 query complete |
| 6 | Task 11 CI + generative differential suite |
| later | Task 12 performance (only on request) |

---

### Task 0: Scaffold (controller does this)

**Files:** Create `deps.edn`, `mise.toml`, `.gitignore`, `resources/mnemonic/bootstrap.edn` (from the Datomic dump), `test/mnemonic/reflection_test.clj`, `docs/deviations.md`.

- [ ] `deps.edn`: `:paths ["src" "resources"]`, deps clojure 1.12.6 + rocksdbjni 11.8.1; `:test` alias adds `"test"` path, `io.github.cognitect-labs/test-runner`, `org.clojure/test.check`, `com.datomic/peer 1.0.7745`, `org.slf4j/slf4j-nop`, main-opts `["-m" "cognitect.test-runner"]`.
- [ ] Reflection test: require every ns under `src/` with `*warn-on-reflection*` bound, capture `*err*`, assert no `"Reflection warning"`.
- [ ] Verify `clojure -M:test` runs (0 failures), commit.

---

### Task 1: Codec and Datom (wave 1)

**Files:** Create `src/mnemonic/codec.clj`, `src/mnemonic/datom.clj`; Test `test/mnemonic/codec_test.clj`, `test/mnemonic/datom_test.clj`.

**Interfaces — Produces:**
- `mnemonic.datom/Datom` — `deftype [^long e ^long a v ^long tx ^boolean added]`; implements `ILookup` (`:e :a :v :tx :added`, also `:t`→tx for compat), `Indexed`/`nth` 0–4, `Seqable` (`[e a v tx added]`), `Counted` (5), `equals`/`hashCode` by all 5 fields, `print-method` `#datom[e a v tx added]`.
- `(mnemonic.datom/datom e a v tx added) -> Datom`.
- `mnemonic.codec` constants: `tag-tuple-end 0x00, tag-nil 0x01, tag-boolean 0x10, tag-long 0x11, tag-bigint 0x12, tag-float 0x13, tag-double 0x14, tag-bigdec 0x15, tag-instant 0x16, tag-uuid 0x17, tag-string 0x18, tag-keyword 0x19, tag-symbol 0x1A, tag-uri 0x1B, tag-bytes 0x1C, tag-tuple 0x1D`.
- `(encode-value ^java.io.ByteArrayOutputStream out v) -> nil` — dispatch on runtime class: Long/Integer→long, Boolean, BigInteger/clojure.lang.BigInt→bigint, Float, Double, BigDecimal, java.util.Date→instant, UUID, String, Keyword, Symbol, java.net.URI, byte[], IPersistentVector→tuple (elements, `nil` allowed in tuples → tag-nil).
- `(value->bytes v) -> bytes`; `(decode-value ^java.nio.ByteBuffer buf) -> Object` (reads from `buf`'s position, advances it).
- Key encoders, one per index, all components long except v:
  `(encode-key index e a v tx) -> bytes` for `index ∈ #{:eavt :aevt :avet :vaet}`. Layouts: `:eavt` E A V TX, `:aevt` A E V TX, `:avet` A V E TX, `:vaet` V A E TX where V is a raw 8-byte ref (no tag).
- `(encode-prefix index components) -> bytes` — components in index order (`:eavt [e a v tx]`, `:aevt [a e v tx]`, `:avet [a v e tx]`, `:vaet [v a e tx]`), 0–4 present, already resolved to longs/values. Encodings are prefix-free, so "datom matches components" ⇔ "key starts with prefix".
- `(decode-key index ^bytes k) -> Datom` (added = true).
- `(long->bytes ^long x) -> bytes` / `(bytes->long ^bytes b) -> long` — 8-byte big-endian (used for log keys and history values).
- `(encode-log-entry datoms) -> bytes` / `(decode-log-entry tx ^bytes b) -> vector of Datom` — count (int) then per datom E A V(value codec) added(1 byte).
- `(compare-bytes ^bytes a ^bytes b) -> int` — `java.util.Arrays/compareUnsigned`.
- Ids: `(part-of eid)`, `(eid part n)`, `(t->tx t)`, `(tx->t tx)` — longs, per Pinned facts.

**Encoding details** (spec §4.3): longs/instant(ms) 8 bytes sign-flipped; double/float IEEE trick (negative → flip all bits, else flip sign bit; canonicalize NaN); uuid 2×8 bytes sign-flipped; string/uri/bytes UTF-8 (bytes raw) with `0x00`→`0x00 0xFF`, terminator `0x00 0x00`; keyword/symbol: namespace (nil ns ⇒ empty) escaped+terminated then name escaped+terminated; bigint: sign byte, then 4-byte length (complemented for negatives) and magnitude bytes (complemented for negatives); bigdec: `stripTrailingZeros`, sign, exponent (adjusted, complemented for negatives) then unscaled digits as decimal string escaped+terminated (complemented for negatives); tuple: elements then `tag-tuple-end`.

- [ ] **Step 1: failing tests** — `codec_test.clj`:
  - `round-trip` (test.check `defspec`, 1000 runs): for generators of every type above, `(= v (decode-value (ByteBuffer/wrap (value->bytes v))))` (use `Arrays/equals` for bytes; NaN by `Double/isNaN`).
  - `order-preserving` (defspec 2000 runs, per type): `(= (Integer/signum (compare a b)) (Integer/signum (compare-bytes (value->bytes a) (value->bytes b))))` with `compare` for keywords/symbols/strings/numbers/Date/UUID, `Double/compare` for doubles.
  - `edge-values`: strings `"" "\u0000" "a\u0000b" "😀" "￿"`, doubles `NaN -0.0 0.0 ##Inf ##-Inf Double/MIN_VALUE`, `Long/MIN_VALUE Long/MAX_VALUE`, keywords `:a :a/b :a.b/c`, bigdec `1.0M 1M -0.001M 1E+10M` round-trip (bigdec equality by `compareTo`).
  - `key-prefix`: for random datoms and every index, `(encode-key ...)` starts with `(encode-prefix index (take n components))` for n 0–4, and keys of datoms with a different component at position < n do not.
  - `key-round-trip`: `(decode-key idx (encode-key idx e a v tx))` = `(datom e a v tx true)`; `:vaet` with ref v.
  - `log-entry-round-trip`.
  - `datom_test.clj`: lookup/nth/destructure/equality/hash/print.
- [ ] **Step 2:** run `clojure -M:test -n mnemonic.codec-test -n mnemonic.datom-test` → FAIL (ns not found).
- [ ] **Step 3:** implement both namespaces.
- [ ] **Step 4:** run → PASS; run reflection test → PASS.
- [ ] **Step 5:** report to controller (no commit).

---

### Task 2: RocksDB wrapper (wave 1)

**Files:** Create `src/mnemonic/rocks.clj`; Test `test/mnemonic/rocks_test.clj`.

**Interfaces — Produces:**
- `cf-names` = `[:default :eavt :aevt :avet :vaet :eavt-h :aevt-h :avet-h :vaet-h :log]` (RocksDB CF names are the keyword names; `:default` → `"default"`).
- `(open-store {:keys [path mem?]}) -> Store` — `Store` record `{:db RocksDB, :cfs {kw ColumnFamilyHandle}, :env Env-or-nil, :closeables [...]}`. Creates missing CFs. `mem? true` uses `RocksMemEnv` with `path` as a virtual name. Options: `setCreateIfMissing`, `setCreateMissingColumnFamilies`; per-CF: block-based table with bloom filter (10 bits) and prefix-friendly defaults.
- `(close-store store)`; `(destroy-store path)` (on-disk only; `RocksDB/destroyDB`).
- `(snapshot store) -> Snapshot`; `(release-snapshot store snap)`.
- `(get-bytes store cf ^Snapshot snap ^bytes k) -> bytes|nil` (snap may be nil).
- `(write! store ops {:keys [sync?]})` — atomic `WriteBatch`; `ops` seq of `[:put cf k v]` / `[:delete cf k]`; `v` may be empty byte array.
- `RawCursor` (`deftype`, implements `java.lang.AutoCloseable`) created by `(raw-cursor store cf ^Snapshot snap) -> RawCursor`, methods via `definterface IRawCursor`: `(seek [^bytes k])`, `(seekToFirst [])`, `(seekToLast [])`, `(seekForPrev [^bytes k])`, `(^boolean isValid [])`, `(next [])`, `(prev [])`, `(^bytes key [])`, `(^bytes value [])`, `(close [])`. Owns its `ReadOptions` and `RocksIterator`; `close` idempotent.
- `(register-cleanup! obj f)` — shared `java.lang.ref.Cleaner`; `f` must not close over `obj`.

- [ ] **Step 1: failing tests** — `rocks_test.clj`: open mem store, all 10 CFs exist; `write!` puts+deletes atomically across CFs; `raw-cursor` seek/next order is bytewise unsigned (keys `[0x01] [0x7F] [0x80] [0xFF]`); snapshot isolation (write after snapshot not visible through cursor on snapshot); on-disk store in a temp dir: write, close, reopen, data present; `destroy-store` removes it; closing a cursor twice is safe.
- [ ] **Step 2:** run `clojure -M:test -n mnemonic.rocks-test` → FAIL.
- [ ] **Step 3:** implement.
- [ ] **Step 4:** run → PASS; reflection test PASS.
- [ ] **Step 5:** report (no commit).

---

### Task 3: Query parser (wave 1)

**Files:** Create `src/mnemonic/query/parser.clj`, `docs/query-ast.md`; Test `test/mnemonic/query/parser_test.clj`.

**Interfaces — Produces:**
- `(parse-query q) -> ast` — accepts map form, list/vector form (`[:find ... :in ... :where ...]`), and quoted forms. Throws `ex-info` `:db.error/invalid-query` (probe Datomic for its exact error keywords on malformed queries and match them).
- `ast` is plain data documented in `docs/query-ast.md` (this file is the contract for Tasks 8 and 10). Must represent: find specs (`:find-rel`, `:find-coll`, `:find-scalar`, `:find-tuple`), find elements (variable, aggregate `{:agg sym :args [...]}`, pull `{:pull src var pattern}`), `:keys/:syms/:strs`, `:with`, `:in` bindings (src-var `$`/`$name`, rules `%`, scalar, tuple, coll, rel, `_`), and where clauses: data pattern (with optional src, `_`, constants), predicate expr, fn expr with binding, rule-expr, `not`, `not-join`, `or`, `or-join`, `and`. Rules: `(parse-rules rules) -> {rule-name [{:head [vars...] :required [vars] :body [clauses]}]}`.
- `(parse-pull-pattern p) -> pattern-ast` is **not** here (Task 7 owns pull grammar); parser keeps the raw pattern.
- Validation: unbound find vars, `not`/`or` var rules (Datomic's "insufficient binding" / "all clauses in 'or' must use same set of vars") as Datomic.

- [ ] **Step 1: failing tests** — one test per construct above asserting the exact AST; error tests for malformed forms; parse is memoized (same object for `identical?` query input is fine).
- [ ] **Step 2–4:** fail, implement, pass.
- [ ] **Step 5:** report (no commit).

---

### Task 4: Index protocols, Base db, schema, datoms (wave 2)

**Files:** Create `src/mnemonic/index.clj`, `src/mnemonic/schema.clj`, `src/mnemonic/db.clj`, `src/mnemonic/bootstrap.clj`; Test `test/mnemonic/index_test.clj`, `test/mnemonic/schema_test.clj`, `test/mnemonic/db_test.clj`.

**Interfaces — Consumes:** Task 1 codec/datom, Task 2 rocks.

**Produces:**
- `mnemonic.index/ICursor` (definterface): `(^boolean valid [])`, `(advance [])`, `(^bytes key [])`, `(datom [])` (may be reused/flyweight; valid until next `advance`), `(^long retractTx [])` (for history cursors; `-1` otherwise), `(close [])`.
- `mnemonic.index/IDb` (protocol):
  - `(-seek-current [db index prefix-bytes])` → ICursor over **current** datoms whose key starts with prefix (each datom `added` true).
  - `(-seek-history [db index prefix-bytes])` → ICursor over **history** entries (assert keys; `retractTx` = retracting tx).
  - `(-view [db index prefix-bytes])` → ICursor implementing this db's semantics (Base: = current).
  - `(-seek-view [db index start-bytes])` → ICursor from first key ≥ start to end of index (for `seek-datoms`).
  - `(-basis-t [db])`, `(-schema [db])`, `(-as-of-t [db])`, `(-since-t [db])`, `(-history? [db])`, `(-filter-pred [db])`, `(-raw [db])` (the db whose `-seek-current/-seek-history` are raw storage: Base or With).
- Cursor combinators: `(prefix-cursor raw-cursor prefix index history?)`, `(merge-cursors [c1 c2 ...])` (k-way, by key bytes), `(filter-cursor c pred)` (`pred` on Datom), `(empty-cursor)`.
- `(datoms-iterable db index prefix) -> Iterable` — `reify` `Iterable`, `Seqable` (lazy, chunks of 512, closes cursor at exhaustion; Cleaner fallback), `IReduceInit` (closes deterministically), `Sequential`. Yields **copied** immutable Datoms.
- `mnemonic.schema`: `Attr` record `{:id :ident :value-type :cardinality :unique :index? :component? :no-history? :fulltext? :tuple-attrs :tuple-type :tuple-types :doc}`; `Schema` record `{:attrs {id Attr} :idents {kw id} :ids {id kw}}`. `(schema-from-datoms datoms) -> Schema` (all `:db/ident` entities go into idents; entities with `:db/valueType` become Attrs); `(attr schema id-or-ident) -> Attr|nil`; `(entid schema x)` for longs/keywords; `(indexed? attr)` = `index?` or `unique`.
- `mnemonic.db/Base` (deftype) `[store snap basis-t schema meta]` implementing `IDb`; `(base-db store) -> Base` takes a fresh snapshot, reads basis-t from last `log` key (bootstrap → 72), loads schema by scanning EAVT. Snapshot released via Cleaner; `(release-db db)` explicit.
- `mnemonic.bootstrap/(bootstrap! store)` — writes `resources/mnemonic/bootstrap.edn` datoms to the 4 current CFs (VAET only when attr value-type is ref) and log entries grouped by tx; writes meta `{:format 1 :next-t 1000 :next-db-n 73}` to `:default` under key `"meta"` (EDN bytes). `(read-meta store)`, `(meta-op meta) -> [:put :default ...]`.

- [ ] **Step 1: failing tests**:
  - `schema_test`: from bootstrap datoms, `(attr s :db/ident)` has id 10, value-type `:db.type/keyword`, unique `:db.unique/identity`; 64 idents.
  - `db_test`: bootstrap a mem store; `(seq (datoms-iterable db :eavt (encode-prefix :eavt [])))` has 272 datoms equal (as `[e a v tx]`) to bootstrap.edn in EAVT order; AEVT prefix for `:db/ident` (a=10) yields 64 datoms; basis-t = 72.
  - `index_test`: merge-cursors over 3 sorted sources yields sorted union; filter-cursor; prefix-cursor stops at prefix end; datoms-iterable `reduce` closes cursor (count open cursors via a test hook) and lazy `seq` closes on exhaustion.
  - Review Focus 4: take an iterable over db A, write 1000 puts via `rocks/write!`, iteration from A unchanged; create 10,000 Base dbs in a loop with `System/gc` and assert no failure (OOM/handle exhaustion).
- [ ] **Step 2–4:** fail, implement, pass.
- [ ] **Step 5:** report.

---

### Task 5: Transactor core, connection, API facade, Datomic oracle (wave 3)

**Files:** Create `src/mnemonic/tx.clj`, `src/mnemonic/conn.clj`, `src/mnemonic/api.clj`, stub namespaces `src/mnemonic/layers.clj`, `src/mnemonic/entity.clj`, `src/mnemonic/pull.clj`, `src/mnemonic/query.clj`, `src/mnemonic/log.clj`, `src/mnemonic/tx_fns.clj`; Test `test/mnemonic/oracle.clj` (helper), `test/mnemonic/tx_test.clj`, `test/mnemonic/api_test.clj`, `test/mnemonic/diff_m1_test.clj`.

**Interfaces — Consumes:** Tasks 1, 2, 4.

**Produces:**
- `(mnemonic.tx/prepare db-before meta tx-data) -> {:tx-data [Datom...] :tempids {tempid eid} :ops [rocks-ops] :meta meta' :tx tx-id}` — **pure** w.r.t. storage (reads only via `db-before`). `ops` includes current puts, current→history moves (`:db/noHistory` → delete only), log put, meta put. `:tx-data` includes retraction datoms (`added` false).
- M1 scope of `prepare`: map forms (nested maps, `:db/id`, reverse refs `:_attr`, component sub-maps), `[:db/add e a v]`, `[:db/retract e a v]`, `[:db/retract e a]`; entity refs: eid, ident, lookup ref, string tempid, `mnemonic.api/tempid` record (`TempId {part idx}`), `"datomic.tx"`; tempid allocation per Pinned facts; upsert fixpoint + tempid unification; value coercion (`Integer`→`Long`, keyword ref values → ident resolution); cardinality-one implicit retract; redundant-assert elision; retract of absent datom = no-op; `:db.error/datoms-conflict`, `:db.error/unique-conflict`, `:db.error/not-an-entity`, `:db.error/wrong-type-for-attribute` (match Datomic's keywords by probing); `:db/txInstant` assertion (monotonic; caller-supplied `[:db/add "datomic.tx" :db/txInstant d]` honored if ≥ previous); auto `[0 :db.install/attribute aid]` for new attrs.
  - Extension hook for Task 9: `prepare` dispatches list-form ops through multimethod `(mnemonic.tx/expand-op op-kw ctx op)`; `:db/add`/`:db/retract` implemented here; unknown keyword → resolve as tx fn (Task 9) — in M1 throw `:db.error/not-a-data-function`.
- `mnemonic.conn/Connection` record `{:uri :store :state (atom {:db Base :meta meta}) :executor (single-thread ExecutorService) :queues (atom #{})}`; `(transact-async conn tx-data) -> future` (runs `prepare` + `rocks/write!` + publishes new `base-db` on the executor; result map `{:db-before :db-after :tx-data :tempids}`; failures → future throws `ExecutionException` wrapping the `ex-info`, as Datomic). `deref` on `d/transact` result rethrows like Datomic.
- URI registry: `mnemonic:mem://name` (store kept in a registry until `delete-database`), `mnemonic:file:///abs/path`. `connect` caches one Connection per URI.
- `mnemonic.api` — every function listed in spec §9 plus `q`, `query`, `qseq`, `pull`, `pull-many`, `entity`, `touch`, `entity-db`, `datoms`, `seek-datoms`, `index-range`, `transact`, `transact-async`, `with`, `db`, `as-of`, `since`, `history`, `filter`, `log`, `tx-range`, `basis-t`, `t->tx`, `tx->t`, `part`, `entid-at`, `tempid`, `resolve-tempid`, `squuid`, `squuid-time-millis`, `entid`, `ident`, `attribute`. Implemented in this task: lifecycle, transact(-async), db, datoms, seek-datoms, index-range (with `:db.error/attribute-not-indexed` check for `:avet`), basis-t, next-t, ids, tempid, resolve-tempid, squuid(s), entid, ident, attribute, sync, release, request-index. Components given to `datoms` may be idents/lookup refs; resolve to ids first, encode with `encode-prefix`.
- Others delegate to stub fns that throw `:db.error/unsupported`, with these exact names (Wave 4 fills them):
  - `mnemonic.layers`: `as-of [db t]`, `since [db t]`, `history [db]`, `filter [db pred]`, `with [db tx-data]` (returns tx-report map like Datomic), `as-of-t [db]`, `since-t [db]`, `history? [db]`.
  - `mnemonic.log`: `log [conn]`, `tx-range [log start end]`, `tx-report-queue [conn]`, `remove-tx-report-queue [conn]`.
  - `mnemonic.entity`: `entity [db eid]`, `touch [e]`, `entity-db [e]`.
  - `mnemonic.pull`: `pull [db pattern eid]`, `pull-many [db pattern eids]`.
  - `mnemonic.query`: `q [query & inputs]`, `query [query-map]`, `qseq [query & inputs]`.
  - `mnemonic.tx-fns`: `function [m]`, `invoke [db eid-or-ident & args]`.
- `test/mnemonic/oracle.clj`: `(with-both [sym-d sym-m] & body)` creating fresh `datomic:mem://<uuid>` and `mnemonic:mem://<uuid>` connections; `(normalize-datoms ds)` → vectors `[e a v tx added]` with `:db/txInstant` values replaced by `:inst`; `(transact-both conns tx-data)` → both reports normalized, or both errors' `:db/error` keywords.

- [ ] **Step 1: failing tests**:
  - `tx_test`: schema tx then data tx yields exactly the Pinned-facts ids (t 1000, attrs 73/74, t 1001, user eids `(4<<42)|1002`/`1003`); card-one replace emits retract+assert; card-many accumulates; redundant assert elided; retract absent no-op; unique-conflict; datoms-conflict; lookup-ref resolution; reverse-ref map; nested component maps.
  - Review Focus 5: two tempids with same identity value unify to one new eid; tempid with identity value matching existing entity upserts.
  - `api_test`: create/connect/delete mem db; `d/datoms` with ident components; `:avet` on non-indexed attr throws `:db.error/attribute-not-indexed`; tx future throws on error and writes nothing (basis-t unchanged).
  - Review Focus 3: on-disk temp dir db, 3 txs, `release` + reopen via new `connect` (clear registry): basis-t, schema, `next-t`/db-n counters, all datoms restored; next tx ids continue the sequence.
  - Review Focus 4: future on old db iterates stably while 200 txs commit concurrently.
  - `diff_m1_test`: ≥ 20 scripted tx scenarios (schema, maps, upserts, card-one/many, retracts, refs, all value types, lookup refs, errors) run via oracle: tx-data, tempids, and `d/datoms` for all 4 indexes (AVET only indexed attrs) equal after normalization.
- [ ] **Step 2–4:** fail, implement, pass. Record any intentional deviation in `docs/deviations.md`.
- [ ] **Step 5:** report.

---

### Task 6: Layers, log API, tx-report-queue (wave 4)

**Files:** Modify `src/mnemonic/layers.clj`, `src/mnemonic/log.clj`, `src/mnemonic/conn.clj` (tx-report-queue publish only); Test `test/mnemonic/layers_test.clj`, `test/mnemonic/log_test.clj`, `test/mnemonic/diff_layers_test.clj`.

**Interfaces — Consumes:** `IDb`, cursor combinators (Task 4), `tx/prepare` (Task 5).

**Produces** (fill the Task 5 stubs):
- `TimeView` deftype `[inner as-of since history?]` implementing `IDb`; `-raw` = inner's raw. `as-of`/`since`/`history` on a TimeView return a new TimeView combining params (as-of = min, since = max).
  - Non-history view: `current where (since < tx ≤ as-of)` ∪ `history where tx_a ≤ as-of < tx_r and tx_a > since` (as Datomic: since keeps only datoms asserted after `since` that are visible at as-of).
  - History view: current datoms (filtered by range) plus, per history entry, an assert datom at `tx_a` and a retract datom (`added false`) at `tx_r`, each kept if its tx is within range; emitted in index order using synthesized keys (key with tx replaced); added-before-retract on ties.
- `Filter` deftype `[inner pred]`: view = inner view filtered by `(pred unfiltered-db datom)`; `filter` of a Filter composes preds.
- `With` deftype `[inner overlay basis-t schema]`: `overlay` = per CF `java.util.TreeMap` keyed by bytes with `Arrays/compareUnsigned`; raw sources merge inner raw sources with overlay puts and hide overlay deletes. `with` = `tx/prepare` on the db, apply ops to a new overlay (no RocksDB write); `:db-after` is the With db. `with` on a TimeView/Filter throws `:db.error/unsupported`.
- `log`/`tx-range`: `(tx-range log start end)` → seq of `{:t t :data [Datom...]}` from the `log` CF, `start`/`end` t or tx ids or nil, end exclusive.
- `tx-report-queue`: `java.util.concurrent.LinkedBlockingQueue` per call; connection publishes each tx report to all queues.

- [ ] **Step 1: failing tests**:
  - Review Focus 2: card-many attr: assert v (t1), retract (t2), reassert (t3), retract (t4); card-one: v1→v2→v1. For every t, `as-of t` datoms equal a db rebuilt by replaying `tx-range` up to t; history view contains exactly the 4 assert/retract datoms in EAVT order.
  - since: only datoms added after t; filter: pred hides datoms in datoms/seek; with: speculative datoms visible, base db and conn unchanged; with + as-of throws.
  - Composition: `(filter (as-of (history db) t) pred)` equals manual computation.
  - log: tx-range bounds; tx-report-queue receives reports.
  - `diff_layers_test`: oracle comparison of as-of/since/history datoms for all indexes over 10 scripted histories.
- [ ] **Step 2–4:** fail, implement, pass.
- [ ] **Step 5:** report.

---

### Task 7: Entity and pull (wave 4)

**Files:** Modify `src/mnemonic/entity.clj`, `src/mnemonic/pull.clj`; Test `test/mnemonic/entity_test.clj`, `test/mnemonic/pull_test.clj`, `test/mnemonic/diff_pull_test.clj`.

**Interfaces — Consumes:** `datoms-iterable`, `IDb`, schema (Task 4); api datoms resolution helpers (Task 5).

**Produces:**
- `Entity` deftype `[db eid cache]`: `ILookup` (keyword attrs; reverse `:ns/_name` via VAET → set of entities, or single entity for component reverse), `:db/id`, `Associative` read-only (`containsKey`, `entryAt`), `Seqable`/`keySet` after touch, `IFn`, equality `[db-basis eid]`, `print-method` like Datomic (`{:db/id n}`). Values: card-many → set; ref → Entity (idents for enum entities: Datomic returns keyword when target has `:db/ident`); `touch` loads all attrs and touches components recursively. `entity` returns nil for... match Datomic (`d/entity` returns an entity for any long; probe).
- `(parse-pattern pattern) -> pattern-ast` and `pull`/`pull-many`: full grammar per spec §8.2, results match Datomic (e.g., refs without nested pattern → `{:db/id n}`; idents enums; reverse refs; `:limit` default 1000 for card-many; `:default`; `:as`; `:xform`; recursion `...`/n with cycle detection; wildcard includes `:db/id` and expands components).
- `(pull-from-ast db pattern-ast eid)` exported for Task 10 (pull in `:find`).

- [ ] **Step 1: failing tests** — entity lookups, reverse refs, component touch, equality; pull grammar cases (each option); `diff_pull_test`: ≥ 30 pull patterns × entity graph (with components, cycles, enums) equal to Datomic.
- [ ] **Step 2–4:** fail, implement, pass.
- [ ] **Step 5:** report.

---

### Task 8: Query engine core (wave 4)

**Files:** Create `src/mnemonic/query/engine.clj`, `src/mnemonic/query/fns.clj`; Modify `src/mnemonic/query.clj`; Test `test/mnemonic/query/engine_test.clj`, `test/mnemonic/diff_query_test.clj`.

**Interfaces — Consumes:** parser AST (`docs/query-ast.md`, Task 3), `IDb`/`datoms-iterable`/codec prefixes (Task 4), schema.

**Produces:**
- `(mnemonic.query/q query & inputs)`, `(query {:query :args :timeout})`, `(qseq ...)` (eager seq in M4; lazy in Task 10).
- Engine: relation = `{:vars [sym...] :tuples java.util.ArrayList of Object[]}`. Clauses executed **in written order**. Data pattern against db: choose index from bound positions (`e` bound → EAVT; `a`+`v` bound and attr indexed or ref → AVET/VAET; `a` only → AEVT; none → EAVT full scan; constants as prefix); if a var is bound in current relation and relation size ≤ 1000 → per-binding prefix seeks; else scan + hash join. Pattern against collection source (`$` bound to a coll of tuples) → in-memory matching. Ident/lookup-ref constants in entity and ref-value positions resolved via schema. Predicates & fn exprs with all binding forms; fn resolution: built-ins in `fns.clj` (Datomic allowlist: `= == not= < > <= >= + - * / quot rem mod inc dec max min zero? pos? neg? even? odd? compare rand rand-int true? false? nil? some? not identity str subs get count range vector list hash-map hash-set keyword symbol name namespace type ground tuple untuple re-find re-matches re-seq re-pattern -differ? get-else get-some missing?` — verify list against Datomic docs/probing), else qualified symbol via `requiring-resolve`. All `:in` binding forms; multiple sources; all find specs; `:keys/:syms/:strs`; `:with`. `:find` aggregates and pull deferred to Task 10 (throw `:db.error/unsupported`).
- Results: relation find → `java.util.HashSet` of vectors (Datomic returns a set); other specs as Datomic.

- [ ] **Step 1: failing tests** — engine_test per construct; `diff_query_test`: ≥ 50 queries (joins, constants, idents, lookup refs, predicates, fn bindings, all `:in` forms, collection sources, multiple dbs, as-of/history dbs once Task 6 lands — guard with `when` resolvable) equal to Datomic.
- [ ] **Step 2–4:** fail, implement, pass.
- [ ] **Step 5:** report.

---

### Task 9: Advanced transactions (wave 4)

**Files:** Modify `src/mnemonic/tx.clj`, `src/mnemonic/tx_fns.clj`; Test `test/mnemonic/tx_advanced_test.clj`, `test/mnemonic/diff_tx_advanced_test.clj`.

**Interfaces — Consumes:** `mnemonic.tx/expand-op` multimethod and `prepare` (Task 5).

**Produces:** `expand-op` methods / `prepare` extensions for:
- `:db/cas` and `:db.fn/cas` (`:db.error/cas-failed`), `:db/retractEntity` and `:db.fn/retractEntity` (retract all datoms of e and refs to e from VAET; cascade components).
- Tx functions: qualified-symbol classpath fns (`(f db & args)` → tx-data, expanded recursively); stored `:db/fn` entities (`d/function` creates fn objects `{:lang :clojure :params [...] :code "..." :requires [] :imports []}` evaluated once and cached by `[eid tx]`); `function [m]` and `invoke [db eid-or-ident & args]` filled in `mnemonic.tx-fns` (Task 5 stubs; `mnemonic.api` already delegates). Tx fns receive db-before.
- Tuples: `:db.type/tuple` with `:db/tupleType` / `:db/tupleTypes` validation; composite `:db/tupleAttrs` auto-maintained on every assert/retract of a component attr (nil for missing).
- `:db.attr/preds` (qualified symbols, called with value; failure `:db.error/attr-pred`), `:db/ensure` with `:db.entity/attrs` and `:db.entity/preds` (`:db.error/entity-attrs`, `:db.error/entity-pred`).
- Schema alteration rules as Datomic (allowed: cardinality one→many, many→one only if no conflicts, add/remove `:db/index`, `:db/unique` addition only if values unique, `:db/isComponent`, `:db/noHistory`, `:db/doc`, renaming `:db/ident`; disallowed: valueType change → `:db.error/incompatible-schema-install` (probe exact keyword)).
- `:db/fulltext true` accepted in schema; `fulltext` query fn and excision attrs → `:db.error/unsupported`.

- [ ] **Step 1: failing tests** — one per feature + `diff_tx_advanced_test` oracle scenarios (cas success/failure, retractEntity with components and inbound refs, classpath tx fn, `:db/fn`, composite tuples, attr preds, ensure, schema alterations allowed/denied).
- [ ] **Step 2–4:** fail, implement, pass.
- [ ] **Step 5:** report.

---

### Task 10: Query complete (wave 5)

**Files:** Modify `src/mnemonic/query/engine.clj`, `src/mnemonic/query.clj`; Create `src/mnemonic/query/rules.clj`, `src/mnemonic/query/aggregates.clj`; Test `test/mnemonic/query/rules_test.clj`, `test/mnemonic/query/aggregates_test.clj`, `test/mnemonic/diff_query_full_test.clj`.

**Interfaces — Consumes:** Task 8 engine, Task 7 `parse-pattern`/`pull-from-ast`, Task 3 rules AST.

**Produces:**
- Rules: semi-naive bottom-up evaluation per rule-call (seeded with bound args when present), recursion, multiple rule bodies, required vars `[[?a] ?b]`, rule `$` source binding.
- `not`, `not-join`, `or`, `or-join`, `and` (anti-join / union semantics as Datomic).
- Aggregates: `count count-distinct sum avg min max (min n ?x) (max n ?x) median variance stddev distinct rand (rand n ?x) sample` + custom aggregate fns via qualified symbol; grouping by non-aggregate find vars + `:with`.
- Pull in `:find` (`(pull ?e pattern)` and `(pull $src ?e pattern)`; pattern may be an input var).
- `qseq` lazy (lazy pull realization); `query` `:timeout` (ms) → `ex-info` `:db.error/timeout` (match Datomic keyword) via checks in the join loops.

- [ ] **Step 1: failing tests** — rules (recursive ancestor, mutual recursion, bound args), not/or variants, every aggregate, pull in find, timeout, qseq laziness; `diff_query_full_test` ≥ 40 queries vs Datomic.
- [ ] **Step 2–4:** fail, implement, pass.
- [ ] **Step 5:** report.

---

### Task 11: CI and generative differential suite (wave 6)

**Files:** Create `.github/workflows/ci.yml`, `test/mnemonic/diff_gen_test.clj`, `README.md`.

- [ ] `ci.yml`: matrix `ubuntu-latest macos-latest windows-latest`, Temurin 27 via `actions/setup-java`, `DeLaGuardo/setup-clojure` CLI 1.12.6.1673, run `clojure -M:test`.
- [ ] `diff_gen_test`: test.check generators for schema (all value types, cardinalities, unique, component, index) and tx sequences (maps, adds, retracts, upserts, retractEntity, cas) over a small value domain to force collisions; property: after each tx, normalized tx-data/tempids/error kind and all-index datoms equal Datomic; plus as-of at random t and a fixed query corpus. 200 trials default, `MNEMONIC_DIFF_TRIALS` env override.
- [ ] Fix discrepancies found (or record in `docs/deviations.md`). README: usage, URI forms, deviations link.
- [ ] Full `clojure -M:test` PASS; report.

---

### Task 12: Performance pass (later, on request)

Benchmarks (`:bench` alias, criterium) for bulk transact, `d/datoms` scan, entity, pull, joins; flyweight cursors over direct `ByteBuffer` (`RocksIterator.key(ByteBuffer)`), reuse `ReadOptions`, move hot classes to Java if profiling shows benefit. Not part of the current execution.
