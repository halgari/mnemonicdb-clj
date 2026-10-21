# MnemonicDB

An embedded database for Clojure that implements the Datomic Peer API on top of
RocksDB. Same data model (datoms, schema, tempids, upserts, history, as-of/since,
entities, pull, Datalog), no transactor and no network: the database lives in your
process, in memory or in a directory.

Datomic (`com.datomic/peer`) is the reference implementation: behavior is checked
against it by differential tests. Where MnemonicDB intentionally differs, see
[docs/deviations.md](docs/deviations.md).

## Install

As a git dependency in `deps.edn` (use the latest commit sha):

```clojure
{:deps {io.github.halgari/mnemonicdb-clj {:git/url "https://github.com/halgari/mnemonicdb-clj"
                                        :git/sha "<commit sha>"}}}
```

Developed and tested on JDK 27 (Temurin) and Clojure 1.12. RocksDB comes in
through `org.rocksdb/rocksdbjni`. On recent JDKs add
`--enable-native-access=ALL-UNNAMED` to `:jvm-opts` to silence native-access warnings.

## Database URIs

| URI | Storage |
|---|---|
| `mnemonic:mem://name` | in memory, gone when the process exits |
| `mnemonic:file:///absolute/path` | RocksDB directory on disk |
| `mnemonic:file:///absolute/path?sync=false` | same, without fsync per commit (see Durability) |
| `mnemonic:file:///absolute/path?cache-mb=512&compression=lz4` | same, with storage options (see Tuning) |

Query parameters are `sync` (`true` or `false`) and the storage options `cache-mb`,
`write-buffer-mb` and `compression` (`none`, `lz4` or `lz4+zstd`); combine them with `&`.
An unknown key or a bad value is rejected with `:db.error/invalid-db-uri`. They belong to the connection, not the database's identity: the
first `create-database` / `connect` per process decides. `mnemonic:mem://name` accepts the
same storage options (`sync` is accepted and ignored); they apply when the database is
created.

## Example

```clojure
(require '[mnemonic.api :as m])

(def uri "mnemonic:mem://demo")
(m/create-database uri)
(def conn (m/connect uri))

@(m/transact conn
   [{:db/ident :person/name :db/valueType :db.type/string
     :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
    {:db/ident :person/friends
     :db/valueType :db.type/ref :db/cardinality :db.cardinality/many}])

(def r1 @(m/transact conn [{:person/name "Ann"} {:person/name "Bob"}]))

@(m/transact conn [{:person/name "Ann" :person/friends [[:person/name "Bob"]]}])

(m/q '[:find ?n :where [?e :person/friends ?f] [?e :person/name ?n]] (m/db conn))
;; => #{["Ann"]}

(m/pull (m/db conn) '[:person/name {:person/friends [:person/name]}]
        [:person/name "Ann"])
;; => {:person/name "Ann", :person/friends [{:person/name "Bob"}]}

;; the database before the friendship was added
(m/q '[:find ?n :where [?e :person/friends _] [?e :person/name ?n]]
     (m/as-of (m/db conn) (m/basis-t (:db-after r1))))
;; => #{}

(m/release conn)
(m/delete-database uri)
```

The API mirrors `datomic.api`: `create-database`, `connect`, `db`, `transact`,
`with`, `as-of`, `since`, `history`, `filter`, `datoms`, `seek-datoms`, `index-range`,
`entity`, `touch`, `pull`, `pull-many`, `index-pull`, `q`, `query`, `qseq`, `log`,
`tx-range`, `tx-report-queue`, `tempid`, `entid`, `ident`, `squuid`, `function`,
`invoke`, `release`, `shutdown`, `get-database-names`, plus `release-db` (frees a db's
RocksDB snapshot early).

### Lifecycle

- `release` stops a connection's transactor and, for file databases, closes the store.
  Afterwards every read through that connection's dbs, lazy seqs, entities and log
  throws `:db.error/connection-released` (never a native crash). `delete-database`
  does the same and then deletes the data.
- `release-db` releases the snapshot a db reads from. Reads already in progress
  complete; later reads of that db throw `:db.error/db-released`. Releasing a
  connection's current db is safe: the connection publishes a fresh one.
- `shutdown` releases every connection; call it from a JVM shutdown hook so file
  stores are closed cleanly.
- The on-disk format is versioned (currently 2). Connecting to a store written in
  another format fails with `:db.error/incompatible-format`.

### Durability and group commit

For file databases a transaction's future completes only after the transaction is
fsynced to the write-ahead log, so a completed `@(m/transact ...)` survives power loss.
The transactor does group commit: transactions that queue up while it works (for
example from concurrent submitters, or `transact` calls not yet dereferenced) are
prepared and written one after the other, each as its own atomic batch and each seeing
the one before it, and then share a single fsync before any of their futures complete.
Results, ordering and per-transaction errors are the same as one-at-a-time transacting;
a transaction rejected while being prepared fails alone. A serial caller that waits for
each future still pays one fsync per transaction (several ms on typical disks), so
submit concurrently or without waiting to benefit. A new db value is published to
readers only once its transaction is durable. (A transaction function that calls `d/db`
on its own connection sees the preceding transaction of the group, as in Datomic; that
value is visible only on the transactor thread until the group is durable.)

If the fsync itself fails, the group's db is not published and the futures of the
transactions in that group that were already written fail with
`:db.error/commit-status-unknown` (category fault): they may or may not survive a restart.
No tx-report-queue entries are made for them. The connection is then marked failed: further
transactions fail at once with `:db.error/connection-failed` (reads keep serving the last
durable db) until you `m/release` it and `m/connect` again, which rebuilds the state from
storage.

`?sync=false` on the file URI turns the fsync off: writes still reach the OS before
the future completes, so data survives a process crash but not an OS crash or power
loss, and transactions are several times to tens of times faster. The default is
`?sync=true`. The option belongs to the connection: the first `connect` to a database
(per process) decides, and the query string is not part of the database's identity.
In-memory databases have no durability and ignore the question.

### Pull `:xform`

As in Datomic, a symbol used as `:xform` in a pull pattern must be allowed: the
built-ins (`str`, `name`, `namespace`, `keyword`, `symbol`, `clojure.edn/read-string`),
symbols listed under `:xforms` in a `mnemonic/extensions.edn` resource on the classpath
(`{:xforms [my.ns/my-fn]}`), or symbols bound in `mnemonic.pull/*xforms*`. Function
and var objects passed from code are used as given.

## Tests

```
clojure -M:test                       # everything
clojure -M:test -n mnemonic.codec-test  # one namespace
```

The suite includes differential tests that run the same operations against Datomic
(`datomic:mem://`) and MnemonicDB and compare tx reports, error kinds, all four
indexes, as-of/since/history views, entities, pull and queries. The generative one
(`mnemonic.diff-gen-test`) runs 200 random schema and transaction sequences by default;
set `MNEMONIC_DIFF_TRIALS` to run more:

```
MNEMONIC_DIFF_TRIALS=5000 clojure -M:test -n mnemonic.diff-gen-test
```


## Tuning

### Storage (file databases)

Settings are system properties (`-Dmnemonic.rocks.<name>=...`) read once at startup, keys of the map passed to `mnemonic.rocks/open-store`, or, for `cache-mb`, `write-buffer-mb` and `compression`, query parameters of the database URI (`mnemonic:file:///data/db?cache-mb=1024&compression=lz4+zstd`; see "Database URIs"). They change memory use and speed only; nothing here changes the on-disk key format, and existing databases open unchanged.

| property | default | meaning |
|---|---|---|
| `cache-mb` | 256 | One LRU block cache shared by all column families of a database (off-heap, not part of `-Xmx`). It holds data, index and filter blocks; level-0 index/filter blocks are pinned. Size it to the hot part of the database: on a 409 MB database, 32 MB gave 161k entity lookups/s, 256 MB 187k/s, 1024 MB 204k/s. |
| `write-buffer-mb` | 128 | Total memtable budget for the database (ten column families would otherwise use up to 64 MB x 2 each). The WAL is capped at twice this, so a restart never replays more than that. |
| `compression` | `lz4` | `none`, `lz4`, or `lz4+zstd` (ZSTD on the bottom level: 40 percent smaller files, 5-8 percent slower point lookups). Applies to files written from now on; mixed files read fine. |
| `cache-index-filter` | true | Keep index and filter blocks inside the cache budget. `false` is about 10-17 percent faster on point lookups and misses but their memory (about 9 percent of the db size) is unbounded. |

`bytes_per_sync` and `wal_bytes_per_sync` are set to 1 MB so background writeback does not pile up before a commit's fsync. Closing a connection flushes the memtables (a close after a large `?sync=false` load takes a few hundred milliseconds), so the next connect does not replay the WAL: a 100k-entity database that took 1.7 s to reopen after an unclean-looking load now reopens in 40 ms.

For write throughput see "Durability and group commit": batch several changes into one transaction, or submit from several threads with `transact-async`.

### Startup

`require`-ing the library from source takes about 0.9 s (Clojure compiling its namespaces); the whole JVM-start-to-first-answer is about 1.3 s. For an application, AOT-compile it and optionally add a class-data archive:

```
# 1. AOT-compile your entry namespace (compiles mnemonic.* transitively) and jar the classes
clojure -M -e "(compile 'my.app.main)"            # with -Dclojure.compile.path=classes, "classes" on the classpath
(cd classes && jar cf ../app-aot.jar .)

# 2. optional: record a class-data archive on a training run (jars only, no directories on -cp)
java -XX:ArchiveClassesAtExit=app.jsa -cp app-aot.jar:deps/* clojure.main -m my.app.main --exit-after-startup
java -XX:SharedArchiveFile=app.jsa     -cp app-aot.jar:deps/* clojure.main -m my.app.main

# JDK 24+: one-step AOT cache (class loading + linking), a bit faster again
java -XX:AOTCacheOutput=app.aot -cp app-aot.jar:deps/* clojure.main -m my.app.main --exit-after-startup
java -XX:AOTCache=app.aot       -cp app-aot.jar:deps/* clojure.main -m my.app.main
```

Measured on JDK 27 with a 100k-entity database (JVM start to first answer): 1.3 s from source, 0.42 s AOT-compiled, 0.27 s with the CDS archive, 0.23 s with the AOT cache. The archives must be rebuilt when the jars change. Add `--enable-native-access=ALL-UNNAMED` to silence the RocksDB native-access warning.
