(ns mnemonic.api
  "Peer API facade, mirroring datomic.api."
  (:refer-clojure :exclude [filter sync])
  (:require [mnemonic.codec :as codec]
            [mnemonic.conn :as conn]
            [mnemonic.db :as dbns]
            [mnemonic.entity :as entityns]
            [mnemonic.index :as index]
            [mnemonic.layers :as layers]
            [mnemonic.log :as logns]
            [mnemonic.pull :as pullns]
            [mnemonic.query :as query*]
            [mnemonic.schema :as schema]
            [mnemonic.tx :as tx]
            [mnemonic.tx-fns :as tx-fns])
  (:import (java.util UUID)
           (java.util.concurrent CompletableFuture)
           (mnemonic.datom Datom)
           (mnemonic.index ICursor)
           (mnemonic.schema Attr)))

(set! *warn-on-reflection* true)

;; ------------------------------------------------------------------ lifecycle

(defn create-database
  "Creates the database at `uri`. Returns true if created, false if it already exists."
  [uri] (conn/create-database uri))

(defn delete-database
  "Deletes the database at `uri`. Returns true if it existed."
  [uri] (conn/delete-database uri))

(defn connect
  "Connection to the database at `uri` (mnemonic:mem://name or mnemonic:file:///path)."
  [uri] (conn/connect uri))

(defn release
  "Releases a connection."
  [connection] (conn/release connection))

(defn shutdown
  "Releases every connection (closing file stores). With `shutdown-clojure?` true, also
  shuts down Clojure's agent thread pools."
  [shutdown-clojure?] (conn/shutdown shutdown-clojure?))

(defn get-database-names
  "Names of the databases matching `uri`, which has `*` in place of the database name
  (mnemonic:mem://* or mnemonic:file:///dir/*); nil when there are none."
  [uri] (conn/get-database-names uri))

(defn release-db
  "Releases the RocksDB snapshot `db` reads from (for views and with-dbs: the snapshot
  of the underlying db) now instead of when it is collected. Later reads of a released
  db throw :db.error/db-released; iterations already started complete. Releasing a
  connection's current db is safe: the connection publishes a fresh db."
  [db*] (dbns/release-db (index/-raw db*)))

(defn db
  "The latest db value of `connection`."
  [connection] (conn/current-db connection))

(defn sync
  "A future of the latest db value. Writes are applied before transact returns, so
  this completes immediately."
  ([connection] (CompletableFuture/completedFuture (conn/current-db connection)))
  ([connection _t] (CompletableFuture/completedFuture (conn/current-db connection))))

(defn request-index
  "No-op; there is no background indexing."
  [connection]
  (conn/current-db connection)
  true)

;; ------------------------------------------------------------------ transactions

(defn transact-async
  "Submits a transaction; returns a future of {:db-before :db-after :tx-data :tempids}."
  [connection tx-data] (conn/transact-async connection tx-data))

(defn transact
  "Submits a transaction; returns a future of the tx report. Deref throws an
  ExecutionException wrapping the ex-info on failure."
  [connection tx-data] (conn/transact-async connection tx-data))

(defn with
  "Applies tx-data speculatively; returns a tx report. (layers task)"
  [db* tx-data] (layers/with db* tx-data))

;; ------------------------------------------------------------------ db views

(defn as-of [db* t] (layers/as-of db* t))
(defn since [db* t] (layers/since db* t))
(defn history [db*] (layers/history db*))
(defn filter [db* pred] (layers/filter db* pred))
(defn as-of-t [db*] (layers/as-of-t db*))
(defn since-t [db*] (layers/since-t db*))
(defn is-history [db*] (layers/history? db*))

(defn basis-t
  "The t of the most recent transaction visible in `db`."
  [db*] (index/-basis-t db*))

(defn next-t
  "The next t that will be allocated (t and entity ids share one counter)."
  [db*] (long (:next-t (dbns/db-meta (index/-raw db*)))))

;; ------------------------------------------------------------------ ids

(defn t->tx [t] (codec/t->tx t))
(defn tx->t [tx*] (codec/tx->t tx*))

(defn part
  "Partition id of entity id `eid`."
  [eid] (codec/part-of eid))

(defn entid
  "Entity id for an id, ident, or lookup ref; nil when it does not resolve."
  [db* x]
  (let [sch (index/-schema db*)]
    (cond
      (nil? x) nil
      (integer? x) (long x)
      (keyword? x) (get (:idents sch) x)
      (vector? x) (tx/lookup-ref-eid {:db db* :schema sch} x)
      (string? x) (throw (tx/error :db.error/not-a-keyword
                                   (str "Cannot interpret as a keyword: " x ", no leading :")
                                   {:value x}))
      :else nil)))

(defn ident
  "The ident keyword of entity id `x`, or nil. Non-ids are returned as given."
  [db* x]
  (if (integer? x)
    (get (:ids (index/-schema db*)) (long x))
    x))

(defn attribute
  "Attribute info map for an attribute id or ident, or nil."
  [db* x]
  (when-let [^Attr a (schema/attr (index/-schema db*) (if (integer? x) (long x) x))]
    {:id (:id a)
     :ident (:ident a)
     :value-type (:value-type a)
     :cardinality (:cardinality a)
     :indexed (boolean (:index? a))
     :has-avet (schema/indexed? a)
     :unique (:unique a)
     :is-component (boolean (:component? a))
     :no-history (boolean (:no-history? a))
     :fulltext (boolean (:fulltext? a))
     :tuple-discontinued false}))

(defn entid-at
  "Entity id in partition `part*` (ident or id) built from t (or a tx id)."
  [db* part* t]
  (let [pid (entid db* part*)]
    (when-not pid
      (throw (tx/error :db.error/not-a-db-id (str "Invalid db/id: " (pr-str part*)) {:part part*})))
    (codec/eid pid (bit-and (long t) (dec (bit-shift-left 1 42))))))

(defn tempid
  "A tempid in `part*`, optionally with a given negative n."
  ([part*] (tx/tempid part*))
  ([part* n] (tx/tempid part* n)))

(defn resolve-tempid
  "The entity id a tempid resolved to in a tx report's :tempids, or nil."
  [db* tempids tid]
  (cond
    (string? tid) (get tempids tid)
    (instance? mnemonic.tx.TempId tid)
    (when-let [pid (entid db* (:part tid))]
      (get tempids (tx/tempid-key pid (:idx tid))))
    :else (get tempids tid)))

(defn squuid
  "A sequential UUID: the first 32 bits are the current time in seconds."
  []
  (let [u (UUID/randomUUID)
        secs (quot (System/currentTimeMillis) 1000)]
    (UUID. (bit-or (bit-shift-left secs 32)
                   (bit-and (.getMostSignificantBits u) 0xFFFFFFFF))
           (.getLeastSignificantBits u))))

(defn squuid-time-millis
  "Time in milliseconds (second resolution) embedded in a squuid."
  [^UUID u]
  (* 1000 (unsigned-bit-shift-right (.getMostSignificantBits u) 32)))

;; ------------------------------------------------------------------ index access

(def ^:private index-order
  {:eavt [:e :a :v :tx] :aevt [:a :e :v :tx] :avet [:a :v :e :tx] :vaet [:v :a :e :tx]})

(defn- bad-index [index]
  (throw (tx/error :db.error/invalid-index (str "No matching clause: " (pr-str index))
                   {:index index})))

(defn- not-indexed [^Attr a]
  (throw (tx/error :db.error/attribute-not-indexed
                   (str "attribute: " (:ident a) " is not indexed")
                   {:attribute (:ident a)})))

(def ^:private empty-result ())

(defn- resolve-attr-component
  "Attr for a datoms attr component: keyword -> Attr or ::none when unknown;
  integer ids must be attributes."
  [db* x]
  (let [sch (index/-schema db*)]
    (cond
      (keyword? x) (if-let [id (get (:idents sch) x)]
                     (or (get (:attrs sch) id) (tx/resolve-attr sch x))
                     ::none)
      (integer? x) (tx/resolve-attr sch x)
      :else (tx/resolve-attr sch x))))

(defn- resolve-components
  "Index components -> vector of storage values in index order, or nil when some
  component cannot match anything."
  [db* idx components]
  (let [order (get index-order idx)
        slots (map vector order components)
        attr (some (fn [[slot x]] (when (= slot :a) (resolve-attr-component db* x))) slots)]
    (when-not (= attr ::none)
      (when (and (= idx :avet) attr (not (schema/indexed? attr)))
        (not-indexed attr))
      (let [res (reduce
                 (fn [acc [slot x]]
                   (let [r (case slot
                             :e (entid db* x)
                             :a (:id (resolve-attr-component db* x))
                             :tx (when (some? x) (long x))
                             :v (cond
                                  (= idx :vaet) (entid db* x)
                                  (and attr (= :db.type/ref (:value-type attr))) (entid db* x)
                                  attr (try (tx/coerce-value attr x)
                                            (catch clojure.lang.ExceptionInfo _ ::none))
                                  :else x))]
                     (if (or (nil? r) (= r ::none))
                       (reduced ::none)
                       (conj acc r))))
                 [] slots)]
        (when-not (= res ::none) res)))))

(defn- indexed-attr-pred [db*]
  (let [attrs (:attrs (index/-schema db*))]
    (fn [^Datom d] (boolean (some-> (get attrs (.-a d)) schema/indexed?)))))

(defn- view-db
  "A minimal IDb whose -view is `f`; keeps `db*` reachable for the iteration."
  [db* f]
  (reify index/IDb
    (-view [_ idx prefix] (f db* idx prefix))))

(defn- indexed-view
  "View function; for :avet, drops datoms of attributes that have no AVET entries."
  [idx view-fn]
  (if (= idx :avet)
    (fn [db* i p] (index/filter-cursor (view-fn db* i p) (indexed-attr-pred db*)))
    view-fn))

(defn datoms
  "Datoms of `index` (:eavt :aevt :avet :vaet) matching the leading `components`."
  [db* index & components]
  (when-not (contains? index-order index) (bad-index index))
  (if-let [comps (resolve-components db* index components)]
    (index/datoms-iterable (view-db db* (indexed-view index index/-view))
                           index (codec/encode-prefix index comps))
    empty-result))

(defn seek-datoms
  "Like `datoms`, but continues from the first matching position to the end of the index."
  [db* index & components]
  (when-not (contains? index-order index) (bad-index index))
  (if-let [comps (resolve-components db* index components)]
    (index/datoms-iterable (view-db db* (indexed-view index index/-seek-view))
                           index (codec/encode-prefix index comps))
    empty-result))

(deftype UntilCursor [^ICursor c ^bytes stop]
  ICursor
  (valid [_] (and (.valid c) (neg? (codec/compare-bytes (.key c) stop))))
  (advance [_] (.advance c))
  (key [_] (.key c))
  (datom [_] (.datom c))
  (retractTx [_] (.retractTx c))
  (close [_] (.close c)))

(defn index-range
  "AVET datoms of `attrid` with start <= value < end (nil bounds are open)."
  [db* attrid start end]
  (let [^Attr attr (tx/resolve-attr (index/-schema db*) attrid)
        aid (:id attr)
        _ (when-not (schema/indexed? attr) (not-indexed attr))
        norm (fn [v]
               (cond (nil? v) nil
                     (= :db.type/ref (:value-type attr)) (entid db* v)
                     :else (try (tx/coerce-value attr v) (catch clojure.lang.ExceptionInfo _ v))))
        s (norm start)
        e (norm end)
        start-key (codec/encode-prefix :avet (if (nil? s) [aid] [aid s]))
        stop-key (codec/encode-prefix :avet (if (nil? e) [(inc aid)] [aid e]))]
    (index/datoms-iterable
     (view-db db* (fn [d _ _] (->UntilCursor (index/-seek-view d :avet start-key) stop-key)))
     :avet start-key)))

;; ------------------------------------------------------------------ delegates

(defn entity [db* eid] (entityns/entity db* eid))
(defn touch [e] (entityns/touch e))
(defn entity-db [e] (entityns/entity-db e))

(defn pull [db* pattern eid] (pullns/pull db* pattern eid))
(defn pull-many [db* pattern eids] (pullns/pull-many db* pattern eids))

(defn- invalid-pull [msg data]
  (throw (tx/error :db.error/invalid-pull msg data)))

(defn index-pull
  "Walks :avet (pulling each datom's :e) or :aevt (pulling each datom's :v) from the
  position `start` (a vector in index order; at least the attribute), limited to the
  datoms of that attribute, and returns a lazy seq of `(pull db selector eid)`. With
  `:reverse true`, walks backwards from `start` (the datoms are collected first)."
  [db* {:keys [index selector start reverse]}]
  (when (or (nil? selector) (nil? index) (nil? (first start)))
    (throw (tx/error :db.error/nil-input "selector, index, and start attribute must be specified"
                     {})))
  (when-not (contains? #{:avet :aevt} index)
    (invalid-pull (str (if (keyword? index) (name index) (pr-str index))
                       " is an invalid index, must be :avet or :aevt")
                  {:index index}))
  (when (index/-history? db*)
    (invalid-pull "Can't pull from history" {}))
  (let [^Attr a (tx/resolve-attr (index/-schema db*) (first start))
        aid (long (:id a))
        many? (= :db.cardinality/many (:cardinality a))]
    (case index
      :avet (do (when (and (== 1 (count start)) many?)
                  (invalid-pull (str (:ident a) " is not card-one, as required for :avet when start has only A specified")
                                {:attribute (:ident a)}))
                (when-not (schema/indexed? a) (not-indexed a)))
      :aevt (do (when-not many?
                  (invalid-pull (str (:ident a) " is not card-many, as required for :aevt")
                                {:attribute (:ident a)}))
                (when-not (= :db.type/ref (:value-type a))
                  (invalid-pull (str (:ident a) " is not a ref, as required for :aevt")
                                {:attribute (:ident a)}))))
    (let [comps (or (resolve-components db* index start) [aid])
          attr-prefix (codec/encode-prefix index [aid])
          start-key (codec/encode-prefix index comps)
          in-attr (fn [^Datom d] (== aid (.-a d)))
          ds (if reverse
               (let [stop (codec/prefix-end start-key)]
                 (->> (seq (index/datoms-iterable (view-db db* index/-view) index attr-prefix))
                      (take-while (fn [^Datom d]
                                    (or (nil? stop)
                                        (neg? (codec/compare-bytes
                                               (codec/encode-key index (.-e d) (.-a d) (.-v d) (.-tx d))
                                               stop)))))
                      vec
                      rseq))
               (take-while in-attr
                           (seq (index/datoms-iterable (view-db db* index/-seek-view) index start-key))))
          ast (pullns/parse-pattern selector)
          eid-of (if (= index :avet) (fn [^Datom d] (.-e d)) (fn [^Datom d] (.-v d)))]
      (map #(pullns/pull-from-ast db* ast (eid-of %)) ds))))

(defn q [query & inputs] (apply query*/q query inputs))
(defn query [query-map] (query*/query query-map))
(defn qseq [query & inputs] (apply query*/qseq query inputs))

(defn log [connection] (logns/log connection))
(defn tx-range [log* start end] (logns/tx-range log* start end))
(defn tx-report-queue [connection] (logns/tx-report-queue connection))
(defn remove-tx-report-queue [connection] (logns/remove-tx-report-queue connection))

(defn function [m] (tx-fns/function m))
(defn invoke [db* eid-or-ident & args] (apply tx-fns/invoke db* eid-or-ident args))
