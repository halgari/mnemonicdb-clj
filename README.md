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

