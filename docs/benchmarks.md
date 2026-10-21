# Benchmarks

A repeatable suite that runs the same workloads, on identical data, against MnemonicDB
in memory (`mnemonic:mem://`), MnemonicDB on RocksDB files (`mnemonic:file://` in a temp
dir) and Datomic's in-memory peer (`datomic:mem://`) as a reference point. Code lives in
`bench/` (`bench/mnemonic/bench.clj`, helpers in `bench/mnemonic/bench/`).

## Running

```
mise exec -- clojure -M:bench -m mnemonic.bench                 # everything, scale 1.0 (about 4 minutes)
mise exec -- clojure -M:bench -m mnemonic.bench queries pull    # selected workloads
MNEMONIC_BENCH_SCALE=0.1 mise exec -- clojure -M:bench -m mnemonic.bench   # quick run
```

Workload names: `bulk-load small-tx datoms-scan entity-lookup pull lookup-ref queries
as-of-history open-reopen cold-lookup mt-lookup`. Results print to stdout as markdown and are appended to
`bench/results/<date>-<git sha>.md` (with git sha, date, JVM). A `-dirty` suffix means
uncommitted changes in the tree.

Environment variables:

| variable | default | meaning |
|---|---|---|
| `MNEMONIC_BENCH_SCALE` | 1.0 | multiplier on every count below |
| `MNEMONIC_BENCH_BACKENDS` | all three | comma list of `mnemonic-mem,mnemonic-file,datomic-mem` |
| `MNEMONIC_BENCH_ENTITIES` | 100000 | people loaded by bulk-load and used by the read workloads |
| `MNEMONIC_BENCH_BATCH` | 1000 | bulk-load batch size (not scaled) |
| `MNEMONIC_BENCH_SMALLTX` | 2000 | small transactions |
| `MNEMONIC_BENCH_EAVT_ENTITIES` | 10000 | entities in the EAVT scan |
| `MNEMONIC_BENCH_LOOKUPS` / `_PULLS` / `_LOOKUP_REFS` | 100000 / 10000 / 100000 | operation counts |
| `MNEMONIC_BENCH_TREE` | 10000 | nodes in the ancestor-rule tree |
| `MNEMONIC_BENCH_IN_VALUES` | 1000 | size of the `:in` collection binding |
| `MNEMONIC_BENCH_HIST_ENTITIES` / `_HIST_UPDATES` / `_HIST_BATCH` | 1000 / 10000 / 10 | as-of/history setup (HIST_BATCH = updates per tx, not scaled) |
| `MNEMONIC_BENCH_QUERY_MS` | 3000 | measurement time per query (warmup is up to 2000 ms) |
| `MNEMONIC_BENCH_ONLY` | all | restrict sub-benchmarks, e.g. `c` (query letter) or `sh,sa` (as-of-history); for profiling |

The Datomic peer comes in through the `:bench` alias (`com.datomic/peer`, `slf4j-nop`,
`-Xmx10g`, `--enable-native-access=ALL-UNNAMED`). `datomic:mem://` needs no license.

## Data

Schema: `:p/email` (string, unique identity), `:p/age` (long, indexed, 0-999), `:p/name`
(string), `:p/status` (ref to one of 4 enum idents), `:p/tags` (card-many ref to 20 enum
idents, 1-4 per person), `:p/created` (instant); plus `:n/id` (unique long) and `:n/parent`
(ref) for the tree. Every value is a pure function of the entity index and a fixed seed
(`bench/mnemonic/bench/data.clj`), so all backends hold identical data. Random picks use
fixed `java.util.Random` seeds. The read workloads share one 100k-person dataset per
backend, loaded by `bulk-load` (or untimed on demand).

## Workloads

Warmup: each workload does an untimed warmup first (20 percent of the operations, at least
1000, for the op-count loops; at least 5 iterations or 1.5 to 2 s for scans and queries),
then a forced GC, then the timed pass.

| workload | what it measures |
|---|---|
| `bulk-load` | 100k people (6 attrs, about 5.5 datoms each) in transactions of 1000. entities/sec, total, per-batch p50/p99. |
| `small-tx` | 2000 sequential transactions each adding one entity with 1-5 attributes (500 warmup txs on the same db). tx/sec and p50/p99 latency. |
| `datoms-scan` | full AEVT scan of `:p/age` (100k datoms) and EAVT scans of 10k entities (about 100k datoms), forcing each value. datoms/sec. Uses `reduce` over `d/datoms`. |
| `entity-lookup` | 100k random `d/entity` plus reading `:p/age`, `:p/name`, `:p/email`. lookups/sec. |
| `pull` | 10k random `d/pull` with `[*]` and with `[:p/email :p/age {:p/status [:db/ident]} {:p/tags [:db/ident]}]`. pulls/sec. |
| `lookup-ref` | 100k `(d/entid db [:p/email "..."])` and 20k `(d/entity db [:p/email "..."])` plus an attr read. ops/sec. |
| `queries` | mean time of: (a) single pattern by indexed value, (b) 3-way join age -> tags -> ident, (c) join + two range predicates + constant ref, (d) count/sum/max group-by status over 100k, (e) recursive `ancestor` rule over a 10k-node ternary tree (all descendants of the root, 9999 rows), (f) `:in [?em ...]` with 1000 emails. Warmed loop with `System/nanoTime` (not criterium). |
| `as-of-history` | fresh db, 1000 people, 10k updates to `:p/age` (batches of 10). Then, on the as-of db at the halfway t: a join query and a 3-attr AEVT scan; on the history db: an AEVT scan of `:p/age` (21000 datoms) and a one-entity query; plus the same join query on the current db for comparison. |
| `cold-lookup` | mnemonic-file only. Flushes the loaded dataset to SST files (add `MNEMONIC_BENCH_COMPACT=1` to compact them; by default the flushed L0 files stay), reconnects (empty block cache), then random entity lookups (cold pass, warm pass), lookup-ref hits (cold, warm) and lookup-ref misses (emails that do not exist). Shows what prefix bloom filters and the iterator pool buy on the read path. |
| `mt-lookup` | Entity lookups and `d/entid` lookup refs from 1, 4 and 8 threads (`MNEMONIC_BENCH_THREADS=1,2,...`) on one shared db value. Total ops/sec; measures contention in the lifecycle guard and cursor open. |
| `open-reopen` | mnemonic-file only: release, then `connect` + `db` + first entity read + first query on the loaded 125 MB database, mean of 5. |

Correctness check: for every query and scan the row count and a hash of the (eid-independent)
result set are compared between each mnemonic backend and Datomic; the results file ends
with an OK/MISMATCH table. Queries that return entity ids are normalized through each
entity's `:p/email`.

## Baseline results

Scale 1.0, one run, 32-core Linux box, Temurin 27, 10 GB heap, commit `fbc31df` (raw file:
`bench/results/2026-10-20-fbc31df.md`). The last two columns are the factor by which
MnemonicDB is slower than Datomic mem (above 1 = slower, below 1 = faster). Run-to-run noise
on this machine is roughly 10 percent, more on the sub-millisecond rows. All 28 correctness
checks matched (no mismatches found).

#### bulk-load

| metric | mnemonic-mem | mnemonic-file | datomic-mem | mem vs datomic | file vs datomic |
|---|---:|---:|---:|---:|---:|
| entities/sec | 22,139 | 21,441 | 64,182 | 2.90x | 2.99x |
| total s | 4.52 | 4.66 | 1.56 | 2.90x | 2.99x |
| batch p50 ms | 43.5 | 46.8 | 14.3 | 3.03x | 3.26x |
| batch p99 ms | 110 | 63.5 | 55.7 | 1.97x | 1.14x |

#### small-tx

| metric | mnemonic-mem | mnemonic-file | datomic-mem | mem vs datomic | file vs datomic |
|---|---:|---:|---:|---:|---:|
| tx/sec | 4,378 | 3,806 | 33,453 | 7.64x | 8.79x |
| p50 ms | 0.223 | 0.249 | 0.027 | 8.18x | 9.12x |
| p99 ms | 0.326 | 0.423 | 0.091 | 3.59x | 4.67x |

#### datoms-scan

| metric | mnemonic-mem | mnemonic-file | datomic-mem | mem vs datomic | file vs datomic |
|---|---:|---:|---:|---:|---:|
| AEVT :p/age datoms/sec | 2,885,518 | 2,796,437 | 36,563,567 | 12.67x | 13.08x |
| AEVT scan ms | 34.7 | 35.8 | 2.73 | 12.67x | 13.08x |
| EAVT datoms/sec | 1,983,381 | 1,736,032 | 13,201,262 | 6.66x | 7.60x |
| EAVT scan ms | 39.9 | 45.6 | 6.00 | 6.66x | 7.60x |

#### entity-lookup

| metric | mnemonic-mem | mnemonic-file | datomic-mem | mem vs datomic | file vs datomic |
|---|---:|---:|---:|---:|---:|
| entity+3 attrs/sec | 165,724 | 140,984 | 768,733 | 4.64x | 5.45x |

#### pull

| metric | mnemonic-mem | mnemonic-file | datomic-mem | mem vs datomic | file vs datomic |
|---|---:|---:|---:|---:|---:|
| pull [*] /sec | 153,855 | 149,365 | 188,508 | 1.23x | 1.26x |
| pull nested /sec | 76,791 | 62,620 | 231,780 | 3.02x | 3.70x |

#### lookup-ref

| metric | mnemonic-mem | mnemonic-file | datomic-mem | mem vs datomic | file vs datomic |
|---|---:|---:|---:|---:|---:|
| entid [:p/email x] /sec | 410,693 | 353,505 | 1,285,728 | 3.13x | 3.64x |
| entity [:p/email x] /sec | 197,851 | 173,518 | 538,426 | 2.72x | 3.10x |

#### queries

| metric | mnemonic-mem | mnemonic-file | datomic-mem | mem vs datomic | file vs datomic |
|---|---:|---:|---:|---:|---:|
| a single pattern, indexed value (age=500) ms | 0.034 | 0.035 | 0.046 | 0.74x | 0.78x |
| b 3-way join (age=42 -> tags -> ident) ms | 0.646 | 0.769 | 0.260 | 2.48x | 2.96x |
| c join + predicate + range (age in [100,110), active) ms | 58.7 | 60.8 | 0.912 | 64.29x | 66.67x |
| d aggregate count/sum/max group-by status ms | 168 | 169 | 183 | 0.92x | 0.92x |
| e recursive rule ancestor (tree) ms | 111 | 117 | 62.6 | 1.78x | 1.87x |
| f :in collection binding (1000 emails) ms | 2.11 | 2.51 | 0.775 | 2.72x | 3.23x |

#### as-of-history

| metric | mnemonic-mem | mnemonic-file | datomic-mem | mem vs datomic | file vs datomic |
|---|---:|---:|---:|---:|---:|
| updates/sec (10 per tx) | 17,369 | 23,033 | 76,942 | 4.43x | 3.34x |
| as-of query (email,age join) ms | 7.46 | 8.97 | 0.790 | 9.43x | 11.34x |
| current-db same query ms | 2.76 | 3.18 | 0.660 | 4.18x | 4.82x |
| as-of 3-attr AEVT scan datoms/sec | 570,599 | 595,677 | 7,885,437 | 13.82x | 13.24x |
| history :p/age AEVT scan datoms/sec | 2,381,502 | 2,429,608 | 51,014,196 | 21.42x | 21.00x |
| history scan ms | 8.82 | 8.64 | 0.412 | 21.42x | 21.00x |
| history query (one entity) ms | 0.020 | 0.021 | 0.051 | 0.39x | 0.41x |

#### open-reopen

| metric | mnemonic-mem | mnemonic-file | datomic-mem | mem vs datomic | file vs datomic |
|---|---:|---:|---:|---:|---:|
| connect ms (mean of 5) | n/a | 335 | n/a | n/a | n/a |
| first db ms | n/a | 0.009 | n/a | n/a | n/a |
| first entity+query ms | n/a | 0.472 | n/a | n/a | n/a |
| db dir size MB | n/a | 125 | n/a | n/a | n/a |

Slowdown columns: factor by which mnemonic is slower than Datomic mem (>1 = slower, <1 = faster).

#### Result checks (mnemonic vs Datomic)

| workload | check | status |
|---|---|---|
| bulk-load | email datoms | OK |
| small-tx | age datoms | OK |
| small-tx | email datoms | OK |
| datoms-scan | AEVT count | OK |
| datoms-scan | EAVT count | OK |
| entity-lookup | checksum | OK |
| pull | [*] key count | OK |
| pull | nested checksum | OK |
| lookup-ref | entity resolved | OK |
| lookup-ref | resolved | OK |
| queries | query a single pattern, indexed value (age=500) result hash | OK |
| queries | query a single pattern, indexed value (age=500) rows | OK |
| queries | query b 3-way join (age=42 -> tags -> ident) result hash | OK |
| queries | query b 3-way join (age=42 -> tags -> ident) rows | OK |
| queries | query c join + predicate + range (age in [100,110), active) result hash | OK |
| queries | query c join + predicate + range (age in [100,110), active) rows | OK |
| queries | query d aggregate count/sum/max group-by status result hash | OK |
| queries | query d aggregate count/sum/max group-by status rows | OK |
| queries | query e recursive rule ancestor (tree) result hash | OK |
| queries | query e recursive rule ancestor (tree) rows | OK |
| queries | query f :in collection binding (1000 emails) result hash | OK |
| queries | query f :in collection binding (1000 emails) rows | OK |
| as-of-history | as-of query hash | OK |
| as-of-history | as-of query rows | OK |
| as-of-history | as-of scan datoms | OK |
| as-of-history | history query hash | OK |
| as-of-history | history query rows | OK |
| as-of-history | history scan datoms | OK |



Observations:

- Datoms scans and as-of/history are the largest gaps (7-21x): every datom is decoded from
  its RocksDB key through a generic codec, and the as-of/history layers add a merge and a
  per-datom re-encode.
- Query (c) is 64x slower: the range predicates run after a full AEVT scan of `:p/age`
  (see hotspots); Datomic answers it in under a millisecond.
- Point operations (entity, lookup-ref, small tx) are 3-9x slower.
- MnemonicDB is on par or faster for single-pattern queries, group-by aggregates and
  one-entity history queries.
- `file` is within noise of `mem` almost everywhere (the OS page cache and RocksDB block
  cache hold the whole 125 MB database), and `small-tx` on file does not appear to pay for
  an fsync per transaction.
- Datomic mem is the in-process object cache, so these numbers are a reference ceiling, not
  a claim about Datomic on a storage backend.

## Initial hotspots

Sampled with JFR (`settings=profile`, 10 ms execution samples) on the mnemonic-mem backend
only, at scale 1.0, with the measurement window stretched so the benchmark loop dominates:

```
MNEMONIC_BENCH_BACKENDS=mnemonic-mem MNEMONIC_BENCH_ONLY=c MNEMONIC_BENCH_QUERY_MS=20000 \
  clojure -J-XX:StartFlightRecording=filename=qc.jfr,settings=profile -M:bench -m mnemonic.bench queries
MNEMONIC_BENCH_BACKENDS=mnemonic-mem MNEMONIC_BENCH_ONLY=sh,sa MNEMONIC_BENCH_QUERY_MS=15000 \
  clojure -J-XX:StartFlightRecording=filename=hist.jfr,settings=profile -M:bench -m mnemonic.bench as-of-history
bench/jfr-top.sh qc.jfr 15 self      # top leaf frames
bench/jfr-top.sh qc.jfr 60 incl      # inclusive: frames present in the stack
```

Profiled the two worst relative gaps: query (c) (64x) and the history/as-of scans (21x/14x).
The first is a query-engine plan problem; the second is cursor and codec overhead that also
sits under the plain `datoms-scan` (13x). Datomic itself was not profiled.

### Query (c): join + predicate + range (1014 samples)

Leaf frames (self time):

| self % | samples | frame |
|---:|---:|---|
| 9.8% | 99 | `mnemonic.query.engine$fn_caller$fn__12848.invoke` |
| 7.7% | 78 | `clojure.lang.Numbers.ops` |
| 5.4% | 55 | `clojure.lang.Numbers$LongOps.lt` |
| 5.1% | 52 | `clojure.lang.PersistentVector$TransientVector.conj` |
| 3.6% | 37 | `clojure.core$reduce.invokeStatic` |
| 3.6% | 36 | `clojure.lang.Util.equiv` |
| 3.4% | 34 | `clojure.lang.RT.get` |
| 3.1% | 31 | `clojure.lang.RT.nth` |
| 2.8% | 28 | `clojure.lang.KeywordLookupSite$1.get` |
| 2.5% | 25 | `clojure.core$mapv$fn__8565.invoke` |
| 2.1% | 21 | `mnemonic.codec$write_escaped_BANG_.invokeStatic` |
| 1.8% | 18 | `mnemonic.query.engine$scan_BANG_$fn__12680.invoke` |
| 1.7% | 17 | `clojure.lang.PersistentVector.nth` |
| 1.6% | 16 | `clojure.lang.RT.first` |
| 1.6% | 16 | `clojure.lang.RT.count` |

Inclusive, selected mnemonic frames:

| inclusive % | samples | frame |
|---:|---:|---|
| 62.0% | 629 | `mnemonic.query.engine$execute.invoke` |
| 62.0% | 629 | `mnemonic.query$run.invoke` |
| 62.0% | 629 | `mnemonic.query$q.doInvoke` |
| 61.2% | 621 | `mnemonic.query.engine$run_clauses.invoke` |
| 36.6% | 371 | `mnemonic.query.engine$exec_db_pattern.invoke` |
| 36.6% | 371 | `mnemonic.index$datoms_iterable$reify__10035.reduce` |
| 36.0% | 365 | `mnemonic.query.engine$scan_BANG_.invoke` |
| 22.8% | 231 | `mnemonic.index.RangeCursor.datom` |
| 22.8% | 231 | `mnemonic.codec$decode_key.invoke` |
| 7.7% | 78 | `mnemonic.query.fns$lte.invoke` |
| 5.4% | 55 | `mnemonic.query.fns$lt.invoke` |
| 4.6% | 47 | `mnemonic.codec$encode_prefix.invoke` |

Reading: 36 percent of the time is one `exec_db_pattern` scan, and the biggest part of that
is key decoding (`RangeCursor.datom` / `decode_key`, 23 percent). The query scans every
`:p/age` datom (100k) and then filters with `[(<= ?lo ?a)] [(< ?a ?hi)]`; the predicate
calls go through `fn_caller` and generic `Numbers.ops`/`lt`/`lte` (about 25 percent in
total), plus vector conj/nth churn from building a result row per datom. Likely wins: turn
`[?e :p/age ?a]` plus range predicates into an AVET range seek (the attr is indexed), and
specialise or inline comparison predicates.

### As-of / history scans (1209 samples)

Leaf frames (self time):

| self % | samples | frame |
|---:|---:|---|
| 6.0% | 73 | `clojure.lang.RT.first` |
| 6.0% | 73 | `clojure.lang.PersistentVector$TransientVector.conj` |
| 4.5% | 55 | `java.io.ByteArrayOutputStream.ensureCapacity` |
| 4.5% | 54 | `clojure.lang.Util.equiv` |
| 3.7% | 45 | `clojure.core$reduce.invokeStatic` |
| 3.2% | 39 | `clojure.lang.RT.nth` |
| 3.1% | 38 | `jdk.internal.util.ArraysSupport.mismatch` |
| 3.1% | 38 | `java.lang.Long.valueOf` |
| 2.6% | 32 | `mnemonic.index$min_index.invokeStatic` |
| 2.5% | 30 | `clojure.lang.RT.get` |
| 2.5% | 30 | `clojure.lang.PersistentVector$TransientVector.persistent` |
| 2.4% | 29 | `clojure.lang.RT.seqFrom` |
| 2.3% | 28 | `clojure.lang.RT.next` |
| 2.2% | 27 | `mnemonic.codec$decode_value.invoke` |
| 2.2% | 26 | `org.rocksdb.RocksIterator.value` |

Inclusive, selected mnemonic frames:

| inclusive % | samples | frame |
|---:|---:|---|
| 87.0% | 1052 | `mnemonic.index$datoms_iterable$reify__10035.reduce` |
| 57.9% | 700 | `mnemonic.layers.HistoryCursor.advance` |
| 35.5% | 429 | `mnemonic.index.RangeCursor.datom` |
| 35.5% | 429 | `mnemonic.codec$decode_key.invoke` |
| 35.4% | 428 | `mnemonic.index.MergeCursor.datom` |
| 24.4% | 295 | `mnemonic.layers.VisibleCursor.advance` |
| 20.4% | 247 | `mnemonic.codec$encode_key.invoke` |
| 18.6% | 225 | `mnemonic.codec$encode_prefix.invoke` |
| 7.4% | 90 | `mnemonic.index.MergeCursor.advance` |
| 5.5% | 67 | `mnemonic.codec$write_component_BANG_.invoke` |
| 4.7% | 57 | `mnemonic.codec$encode_value.invoke` |
| 4.6% | 56 | `mnemonic.index.RangeCursor.advance` |

Reading: `HistoryCursor.advance` / `read_group` is 58 percent of the time and
`VisibleCursor.advance` another 24 percent. Inside that, `codec/encode_key` and
`encode_prefix` (about 20 percent) re-encode keys per datom while merging the current and
history indexes, `decode_key` takes 34 percent, and the rest is transient-vector
conj/persistent and seq churn (`RT.first`, `TransientVector.conj`,
`ByteArrayOutputStream.ensureCapacity`). Per-datom byte-array and vector allocation, not
RocksDB itself (`RocksIterator.value` is about 2 percent), is the cost.

## Progress across performance rounds

Factor = how many times slower than Datomic mem (below 1 = faster). mnemonic-mem unless noted. Scale 1.0, same machine.

| workload | baseline (fbc31df) | round 1 (98e9d4a) | round 2 (6ba6bd3) |
|---|---:|---:|---:|
| bulk-load entities/sec | 2.90x | 1.52x | 1.55x (41.9k/s) |
| small-tx tx/sec | 7.64x | 2.64x | 1.56x (21.5k/s) |
| AEVT scan | 12.67x | 7.75x | 8.18x (5.1M datoms/s) |
| EAVT scan | 6.66x | 3.48x | 3.43x (3.8M datoms/s) |
| entity + 3 attrs | 4.64x | 2.80x | 2.53x (308k/s) |
| pull [*] | 1.23x | 0.96x | 0.82x |
| pull nested | 3.02x | 1.82x | 2.08x |
| lookup-ref entid | 3.13x | 1.54x | 1.85x |
| query a single pattern | 0.74x | 0.39x | 0.38x |
| query b 3-way join | 2.48x | 0.73x | 0.64x |
| query c join + range | 64.3x | 4.83x | 1.48x (1.35 ms) |
| query d aggregates | 0.92x | 0.52x | 0.44x |
| query e recursive rule | 1.78x | 1.05x | 1.03x |
| query f :in collection | 2.72x | 1.70x | 1.32x |
| as-of query | 9.43x | 5.30x | 3.82x |
| as-of 3-attr scan | 13.8x | 9.42x | 7.87x |
| history scan | 21.4x | 8.91x | 8.89x |
| entity, 8 threads | — | 2.82x | 2.46x (2.3M/s) |

Durable writes (file db on a real filesystem, round 2): fsync on — 309 tx/s with 1 submitting thread, 628 with 4, 2,445 with 16 (group commit); `?sync=false` — 25.6k / 32.9k / 33.9k tx/s.

Scans and as-of/history scans are near the JNI floor (raw RocksIterator next+isValid ≈ 90–120 ns/entry); see the round-1 second opinions for measurements.

## Round 2.5: storage configuration and startup

File db on a real filesystem (`target/r25`, btrfs, not tmpfs), 1.2M entities (3 attributes each plus tags/status/created, 409 MB of SST files, larger than the 256 MB default cache), loaded in 1000-entity transactions, then flushed and fully compacted. One fresh JVM per measurement (`bench/mnemonic/r25.clj`: `build`, `measure`, `wal`, `reconnect`, `durable`; `r25_startup.clj` for start-to-first-answer). Before = the round 2 defaults (commit 4efb69b: no explicit cache, one 8 MB cache per table config, 64 MB x 2 memtables per column family); after = this round's defaults. Other agents used the machine at the same time, so differences under about 5 percent are noise. Three before runs, two after runs.

| row | before | after | change |
|---|---:|---:|---:|
| bulk-load entities/s | 34.2k, 34.3k | 33.6k | same |
| flush + full compaction | 6.4 s | 6.0 s | same |
| db size | 408.7 MB | 408.9 MB | same (LZ4 was already the default) |
| entity + 3 attrs, 300k random eids /s | 183k, 192k, 193k | 184k, 190k | same |
| lookup-ref + entity, warm /s | 120k, 132k, 135k | 135k, 147k | +10% |
| lookup-ref + entity, first pass after connect /s | 103k, 131k, 130k | 120k, 113k | same (noise) |
| entid miss /s (bloom only) | 994k, 1027k, 996k | 800k, 847k | -17% |
| pull nested /s | 84.8k, 88.5k, 88.2k | 87.9k, 88.6k | same |
| query b (3-way join) ms | 4.41, 4.40, 4.09 | 2.56, 2.61 | -39% |
| query c (join + range) ms | 50.6, 48.8, 47.8 | 37.9, 38.2 | -22% |
| query f (:in 1000 values) ms | 1.62, 1.82, 1.66 | 1.69, 1.77 | same |
| durable small tx, 1 thread, sync on /s | 293, 289 | 236, 292 | same |
| durable small tx, 16 threads /s | 2455, 2244 | 2273, 2266 | same |
| connect, 100k-entity db (cold JVM / warm) | 98 / 23 ms | 98 / 23 ms | same |
| reopen after a 100k-entity `?sync=false` load | 1662 ms (127 MB WAL replayed) | 39 ms (0 bytes WAL) | -98% |
| close after that load | 1.9 ms | 290 ms (flushes the memtables) | +288 ms |

What moved: the shared 256 MB cache helps the multi-pattern queries (b, c) that revisit the same index blocks, and warm point lookups; single random entity lookups were already at the page cache + LZ4 floor on this machine, so they do not change. Putting filters through the cache costs misses about 17 percent (below). The big difference is restart time: close now flushes, so reopening never replays a large WAL.

Cache size (same db, `-Dmnemonic.rocks.cache-mb`): 32 MB: entity 161k/s, query c 49 ms; 256 MB: 187k/s, 38 ms; 1024 MB: 204k/s, 37.5 ms.

Variants that were measured and not adopted:

- **Cached index/filter blocks, default shard count (64 shards of 4 MB):** bulk load fell from 34k to 5.7k entities/s. A filter block of a 64 MB SST is several MB and a block larger than a cache shard is never kept, so every lookup re-read it. Shards are now at least 16 MB (`shard-bits`); bulk load is back to 34k/s.
- **Partitioned index and filters** (`-Dmnemonic.rocks.partitioned=true`): fixes the above differently, but costs about 20 percent on point lookups (entity 157k/s, misses 775k/s). Off by default.
- **Not caching index/filter blocks** (`-Dmnemonic.rocks.cache-index-filter=false`): fastest on this size (entity 195k/s, misses 0.95-1.0M/s, warm lookup-ref 144-150k/s) but index and filter memory is outside any budget and grows with the db (about 9 percent of the SST size here: 35 MB at 409 MB). Default stays bounded; set the property to false for databases that are small relative to RAM.
- **Compression** (`-Dmnemonic.rocks.compression`): `none` 1211 MB, `lz4` (default, and what RocksDB already used) 409 MB, `lz4+zstd` (LZ4 above, ZSTD on the bottom level) 244 MB. ZSTD costs 5-8 percent on point lookups (entity 169-175k/s, warm 121-126k/s) and nothing on queries or bulk load; use it when disk is the constraint. `none` reads like LZ4 but is three times the size.
- **Memtable budget charged to the block cache** (`-Dmnemonic.rocks.wbm-charge-cache=true`): not needed, off.

Startup (JVM start to first answer, 100k-entity file db, JDK 27; `bench/mnemonic/r25_startup.clj`, wall clock of the whole process):

| setup | wall | `require mnemonic.api` | connect |
|---|---:|---:|---:|
| source, as shipped | 1.29-1.40 s | 830-900 ms | 105 ms |
| AOT-compiled classes in a jar | 0.41-0.43 s | 84 ms | 100 ms |
| + AppCDS dynamic archive | 0.26-0.30 s | 35-43 ms | 100 ms |
| + JDK AOT cache (JEP 483) | 0.22-0.23 s | 25-28 ms | 90 ms |

The cold 100 ms connect is the native library extraction and load (62 ms) plus class initialisation; a warm connect (second one in the same JVM) is 22 ms, of which opening RocksDB with 10 column families is 19 ms and loading the schema 0.5-2 ms, so schema caching was not worth adding. See README "Tuning" for how to build the AOT jar and the CDS/AOT archives in an application.

## Index cache

The heap index segment cache (`mnemonic.segcache`, README "Tuning"), cache off
(`-Dmnemonic.index-cache-mb=0`) against on (default 256 MB), commit ef1910a. File
databases on a real filesystem (`target/`, btrfs, not tmpfs). Off and on runs
interleaved, 3 each, medians; the machine was otherwise idle. Suite at scale 1.0
(`MNEMONIC_BENCH_BACKENDS=mnemonic-mem,mnemonic-file`); controlled hit rates and the
larger-than-cache rows from `bench/mnemonic/cache_bench.clj` (100k people; `big-file`:
150k people, about 4x a 32 MB budget).

| row | mem off | mem on | file off | file on |
|---|---:|---:|---:|---:|
| AEVT :p/age scan datoms/s | 8.17M | 121M (14.9x) | 8.03M | 92.3M (11.5x) |
| EAVT scan (10k entities) datoms/s | 3.27M | 25.1M (7.7x) | 3.54M | 23.6M (6.7x) |
| entity + 3 attrs /s (suite: 100k random, each read once) | 238k | 299k (1.26x) | 276k | 324k (1.18x) |
| pull nested /s | 91k | 296k (3.3x) | 104k | 347k (3.4x) |
| pull [*] /s | 207k | 238k (1.15x) | 279k | 346k (1.24x) |
| entid lookup-ref (hit) /s | 563k | 787k (1.4x) | 597k | 794k (1.3x) |
| entity via lookup ref /s | 246k | 398k (1.6x) | 276k | 435k (1.6x) |
| entid miss, distinct (cold-lookup, file) /s | | | 1.24M | 1.05M (0.85x) |
| query a / b / c ms | 0.017 / 0.222 / 1.92 | 0.006 / 0.042 / 0.398 | 0.017 / 0.199 / 1.69 | 0.006 / 0.041 / 0.401 |
| query d / e / f ms | 53.7 / 58.8 / 1.51 | 27.7 / 43.7 / 0.236 | 57.7 / 58.9 / 1.46 | 27.0 / 43.5 / 0.232 |
| as-of query ms | 2.03 | 0.447 (4.5x) | 2.22 | 0.516 (4.3x) |
| as-of 3-attr AEVT scan datoms/s | 1.48M | 7.36M (5.0x) | 1.33M | 6.74M (5.1x) |
| history :p/age AEVT scan datoms/s | 7.16M | 18.5M (2.6x) | 6.76M | 17.1M (2.5x) |
| entity + 3 attrs, 8 threads /s | 906k | 7.65M (8.4x) | 1.82M | 7.90M (4.3x) |
| entid, 8 threads /s | 1.77M | 7.05M (4.0x) | 3.16M | 13.2M (4.2x) |
| bulk-load entities/s | 47.3k | 44.9k (-5%) | 39.4k | 38.2k (-3%) |
| small-tx tx/s (file: fsync per tx) | 22.2k | 21.6k (-3%) | 318 | 300 (-6%, noise level) |

Controlled hit rates (`cache_bench mem`; file within 10 percent):

| row | off | on |
|---|---:|---:|
| entity, one attribute, uniform, cold | 520k | 422k (0.81x) |
| entity + 3, uniform pass 1 / 2 / 3 | 247k / 251k / 254k | 261k / 504k / 1.27M |
| entity + 3, 90 percent of reads on 1 percent | 278k | 1.19M (4.3x) |
| same, writer at ~2.8k tx/s, fresh `d/db` per 10 reads | 236k | 1.03M (4.4x) |
| pull nested pass 1 / 3 | 95k / 95k | 154k / 449k |
| entid lookup-ref pass 1 / 3 | 593k / 598k | 506k / 1.84M |
| entid miss (same misses again) | 995k | 2.08M |
| as-of db, entity + 3, warm | 185k | 594k (3.2x) |
| heap with every entity and email cached | | 112 MB, 147 B/datom (estimate 134 MB) |

Larger than the cache (`cache_bench big-file`, 32 MB budget, 150k people):

| row | off | on |
|---|---:|---:|
| entity + 3, uniform, pass 1 / 2 | 252k / 255k | 261k / 284k |
| entity + 3, 90 percent of reads on 10 percent | 274k | 813k (3.0x) |
| same, after two full AEVT scans of :p/age | 269k | 780k (2.9x) |
| AEVT :p/age full scan (larger than the scan pool: streamed) | 6.96M | 7.09M |
| pull [*] uniform | 236k | 224k |

Reading the numbers: a hit costs no JNI call, decode or lock, so warm reads are 3-5x
faster and reader threads scale; the suite's single-pass rows (entity, pull [*]) gain
less because a segment is only admitted when a later read wants it again. First reads
of uncached data cost 10-20 percent more (hash, map probe, admission bookkeeping,
copying a granule when it is admitted). Writers pay one stamp per touched granule
(3-5 percent of a bulk load). Variants measured and dropped: eager entity reads (one
`[e]` read on the first attribute; 2.4x slower warm than slicing `[e a]` out of the
cached segment), admission on the second probe of the same read (filled and evicted on
every read of a too-large working set: -16 percent uniform), a 4096-entry chunk cap
(history chunks of updated attributes did not fit: as-of query 1.6 ms instead of 0.5).
