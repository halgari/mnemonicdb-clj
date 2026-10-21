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
