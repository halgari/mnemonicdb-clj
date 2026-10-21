(ns mnemonic.conn
  "Connections: URI registry, the single-writer transactor executor, and db publishing."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [mnemonic.bootstrap :as bootstrap]
            [mnemonic.codec :as codec]
            [mnemonic.db :as db]
            [mnemonic.index :as index]
            [mnemonic.rocks :as rocks]
            [mnemonic.rocks-sync :as rocks-sync]
            [mnemonic.schema :as schema]
            [mnemonic.tx :as tx])
  (:import (java.io File)
           (mnemonic.datom Datom)
           (java.util.concurrent CompletableFuture ConcurrentLinkedQueue ExecutionException
                                 ExecutorService Executors RejectedExecutionException
                                 ThreadFactory TimeUnit)
           (java.util.concurrent.atomic AtomicLong)))

(set! *warn-on-reflection* true)

;; state: (atom {:db Base :meta meta :released? bool :stale? bool})
;; key: the normalized registry key, [:mem name] or [:file canonical-path]
;; pending: ConcurrentLinkedQueue of submitted-but-not-started transactions (group commit
;; drains it); sync?: fsync before a transaction's future completes (file databases)
(defrecord Connection [uri key store state executor queues pending sync?])

(def ^:private mem-prefix "mnemonic:mem://")
(def ^:private file-prefix "mnemonic:file://")

(defonce ^:private lock (Object.))

;; {:mem {name store}, :conns {key Connection},
;;  :closing {key CompletableFuture}}  ; stores of `key` still being closed / destroyed
(defonce ^:private registry (atom {:mem {} :conns {} :closing {}}))

(defn- fail
  ([kind msg] (fail kind msg :cognitect.anomalies/incorrect nil))
  ([kind msg category data]
   (throw (tx/error kind msg category data))))

(defn- canonical-path [^String p]
  (.getCanonicalPath (File. p)))

;; "?sync=true|false" on a file URI (default true): see README "Durability"
(defn- parse-sync [uri query]
  (if (str/blank? query)
    true
    (let [kvs (into {} (map #(let [[k v] (str/split % #"=" 2)] [k v])) (str/split query #"&"))
          bad (fn [] (fail :db.error/invalid-db-uri
                           (str "Invalid database URI " uri " (supported option: sync=true|false)")))]
      (when-not (every? #{"sync"} (keys kvs)) (bad))
      (case (get kvs "sync")
        "true" true
        "false" false
        (bad)))))

(defn- parse-uri
  "{:kind :mem :name n :key k} or {:kind :file :path p :key k :sync? b}. File paths are
  canonicalized so one database written two ways maps to one key; a `?sync=false` query
  is stripped from the path."
  [uri]
  (cond
    (not (string? uri)) (fail :db.error/invalid-db-uri (str "Invalid database URI " (pr-str uri)))
    (str/starts-with? uri mem-prefix)
    (let [n (subs uri (count mem-prefix))]
      (when (str/blank? n) (fail :db.error/invalid-db-uri (str "Invalid database URI " uri)))
      {:kind :mem :name n :key [:mem n]})
    (str/starts-with? uri file-prefix)
    (let [p (subs uri (count file-prefix))
          qi (str/index-of p "?")
          query (when qi (subs p (inc (long qi))))
          p (if qi (subs p 0 qi) p)
          p (if (re-matches #"/[A-Za-z]:.*" p) (subs p 1) p)]
      (when (str/blank? p) (fail :db.error/invalid-db-uri (str "Invalid database URI " uri)))
      (let [p (canonical-path p)]
        {:kind :file :path p :key [:file p] :sync? (parse-sync uri query)}))
    :else (fail :db.error/invalid-db-uri (str "Invalid database URI " uri))))

(defn- file-db-exists? [path]
  (.exists (io/file path "CURRENT")))

(defn- not-found [uri]
  (fail :db.error/db-not-found (str "Could not find " (or (some-> uri (str/split #"/") last) uri) " in catalog")
        :cognitect.anomalies/not-found nil))

(defonce ^:private ^AtomicLong thread-counter (AtomicLong.))

(defn- executor ^ExecutorService []
  (Executors/newSingleThreadExecutor
   (reify ThreadFactory
     (newThread [_ r]
       (doto (Thread. ^Runnable r (str "mnemonic-transactor-" (.incrementAndGet thread-counter)))
         (.setDaemon true))))))

(defn- file-uri? [^Connection conn]
  (= :file (first (:key conn))))

(defn- check-format!
  "Refuses stores written by another on-disk format, and validates the counters against
  the log (spec 5.3)."
  [meta basis-t]
  (let [fmt (:format meta)]
    (when-not (= fmt codec/format-version)
      (fail :db.error/incompatible-format
            (str "Database format " (pr-str fmt) " is not supported (this version reads format "
                 codec/format-version "); the database was written by an incompatible MnemonicDB version")
            :cognitect.anomalies/unsupported {:format fmt :supported codec/format-version})))
  (let [next-t (:next-t meta)
        next-db-n (:next-db-n meta)]
    (when-not (and (integer? next-t) (integer? next-db-n)
                   (> (long next-t) (long basis-t)))
      (fail :db.error/invalid-metadata
            (str "Database counters are inconsistent with the log (next-t " (pr-str next-t)
                 ", last t " basis-t ")")
            :cognitect.anomalies/fault {:meta meta :basis-t basis-t}))))

(defn- make-connection [uri key store sync?]
  (when-not (bootstrap/read-meta store)
    (not-found uri))
  (let [d (db/base-db store)]
    (try (check-format! (db/db-meta d) (index/-basis-t d))
         (catch Throwable t (db/release-db d) (throw t)))
    (->Connection uri key store (atom {:db d :meta (db/db-meta d) :released? false}) (executor) (atom #{})
                  (ConcurrentLinkedQueue.) (boolean sync?))))

;; ------------------------------------------------------------------ lifecycle

(defn- under-lock
  "Runs `f` (taking the parsed uri) under the registry lock once no store of the same
  database is still being closed or destroyed (that wait happens outside the lock)."
  [uri f]
  (let [parsed (parse-uri uri)]
    (loop []
      (let [r (locking lock
                (if-let [pending (get-in @registry [:closing (:key parsed)])]
                  [::wait pending]
                  [::done (f parsed)]))]
        (if (= ::wait (nth r 0))
          (do (try (.get ^CompletableFuture (nth r 1)) (catch Throwable _ nil))
              (recur))
          (nth r 1))))))

(defn create-database
  "Creates the database at `uri`; true if created, false if it already exists."
  [uri]
  (under-lock
   uri
   (fn [{:keys [kind name path key]}]
     (case kind
       :mem (if (contains? (:mem @registry) name)
              false
              (let [store (rocks/open-store {:path name :mem? true})]
                (try (bootstrap/bootstrap! store)
                     (catch Throwable t (rocks/close-store store) (throw t)))
                (swap! registry assoc-in [:mem name] store)
                true))
       :file (if (or (file-db-exists? path) (contains? (:conns @registry) key))
               false
               (do (.mkdirs (io/file path))
                   (let [store (rocks/open-store {:path path})]
                     (try (bootstrap/bootstrap! store)
                          (finally (rocks/close-store store))))
                   true))))))

(defn connect
  "Connection to the database at `uri` (one cached Connection per database)."
  [uri]
  (under-lock
   uri
   (fn [{:keys [kind name path key sync?]}]
     (or (get-in @registry [:conns key])
         (let [store (case kind
                       :mem (or (get-in @registry [:mem name]) (not-found uri))
                       :file (if (file-db-exists? path)
                               (rocks/open-store {:path path})
                               (not-found uri)))
               conn (try (make-connection uri key store (if (= kind :file) sync? false))
                         (catch Throwable t
                           (when (= kind :file) (rocks/close-store store))
                           (throw t)))]
           (swap! registry assoc-in [:conns key] conn)
           conn)))))

(defn- release*
  "Unregisters and stops `conn` (call under the registry lock). Returns a fn that waits
  for the transactor to drain and then runs `after` (closing / destroying the store);
  call it OUTSIDE the lock. While it runs, the database's key is marked as closing so
  that connect / create / delete of the same database wait for it."
  [^Connection conn after]
  (let [key (:key conn)]
    (when (identical? conn (get-in @registry [:conns key]))
      (swap! registry update :conns dissoc key))
    (swap! (:state conn) assoc :released? true)
    (let [^ExecutorService ex (:executor conn)]
      (.shutdown ex)
      (fn []
        (try (.awaitTermination ex 30 TimeUnit/SECONDS)
             (finally (when after (after))))
        nil))))

(defn- closing!
  "Marks `key` as closing (under the lock); returns a fn that runs `finish` and then
  clears the mark."
  [key finish]
  (let [f (CompletableFuture.)]
    (swap! registry assoc-in [:closing key] f)
    (fn []
      (try (finish)
           (finally
             (locking lock
               (when (identical? f (get-in @registry [:closing key]))
                 (swap! registry update :closing dissoc key)))
             (.complete f true))))))

(defn release
  "Releases `conn`: stops its transactor and, for file databases, closes the store.
  In-memory databases stay available to later `connect` calls."
  [^Connection conn]
  (when-let [finish (locking lock
                      (when-not (:released? @(:state conn))
                        (if (file-uri? conn)
                          (closing! (:key conn)
                                    (release* conn #(rocks/close-store (:store conn))))
                          (release* conn nil))))]
    (finish))
  nil)

(defn delete-database
  "Deletes the database at `uri`; true if it existed."
  [uri]
  (let [[existed? finish]
        (under-lock
         uri
         (fn [{:keys [kind name path key]}]
           (let [conn (get-in @registry [:conns key])]
             (case kind
               :mem (if-let [store (get-in @registry [:mem name])]
                      (let [fin (when conn (release* conn nil))]
                        (swap! registry update :mem dissoc name)
                        [true (fn [] (try (when fin (fin)) (finally (rocks/close-store store))))])
                      [false nil])
               :file (if (or conn (file-db-exists? path))
                       [true (closing! key
                                       (if conn
                                         (release* conn (fn []
                                                          (rocks/close-store (:store conn))
                                                          (rocks/destroy-store path)))
                                         #(rocks/destroy-store path)))]
                       [false nil])))))]
    (when finish (finish))
    existed?))

(defn- release-all!
  "Releases every connection (closing file stores); with `drop-mem?` also drops every
  in-memory database."
  [drop-mem?]
  (let [fins (locking lock
               (let [fins (vec (for [^Connection conn (vals (:conns @registry))]
                                 (if (file-uri? conn)
                                   (closing! (:key conn)
                                             (release* conn #(rocks/close-store (:store conn))))
                                   (release* conn nil))))
                     stores (when drop-mem? (vec (vals (:mem @registry))))]
                 (swap! registry assoc :conns {})
                 (when drop-mem? (swap! registry assoc :mem {}))
                 (conj fins (fn [] (doseq [st stores] (rocks/close-store st))))))]
    (let [errs (volatile! nil)]
      (doseq [f fins]
        (try (f) (catch Throwable t (vswap! errs #(or % t)))))
      (when-let [t @errs] (throw t)))
    nil))

(defn reset-registry!
  "Test hook: releases every connection and drops all in-memory databases."
  []
  (release-all! true))

(defn shutdown
  "Releases every connection (file stores are closed; in-memory databases stay
  available). With `shutdown-clojure?` true, also calls `shutdown-agents`."
  [shutdown-clojure?]
  (release-all! false)
  (when shutdown-clojure? (shutdown-agents))
  nil)

(defn get-database-names
  "Names of the databases matching `uri`, which must have `*` in place of the database
  name: mnemonic:mem://* lists the in-memory databases, mnemonic:file:///dir/* the
  databases that are direct subdirectories of /dir. nil when there are none."
  [uri]
  (let [bad #(fail :db.error/invalid-db-uri
                   "Invalid URI. Note URI must have '*' in place of database name.")]
    (when-not (and (string? uri) (str/ends-with? uri "*")) (bad))
    (cond
      (= uri (str mem-prefix "*")) (seq (sort (keys (:mem @registry))))
      (and (str/starts-with? uri file-prefix) (str/ends-with? uri "/*"))
      (let [dir (subs uri (count file-prefix) (- (count uri) 2))
            dir (if (re-matches #"/[A-Za-z]:.*" dir) (subs dir 1) dir)
            ^"[Ljava.io.File;" kids (.listFiles (File. (if (str/blank? dir) "/" ^String dir)))]
        (seq (sort (for [^File f (seq kids)
                         :when (and (.isDirectory f) (file-db-exists? (.getPath f)))]
                     (.getName f)))))
      :else (bad))))

;; ------------------------------------------------------------------ reads

(defn- released-ex []
  (rocks/connection-released-ex))

;; The connection whose transactor runs on the current thread (nil elsewhere).
(def ^:private ^ThreadLocal writer-of (ThreadLocal.))

(defn- on-writer? [^Connection conn]
  (identical? conn (.get writer-of)))

;; While a group runs: {:db volatile :meta volatile} of the group's chain (the db built
;; from the group's latest written tx, not yet published because not yet fsynced). Only
;; the writer thread reads it, so `d/db` called from a transaction function sees the
;; preceding tx of the group, exactly like Datomic, without ever publishing unsynced data.
(def ^:private ^ThreadLocal writer-chain (ThreadLocal.))

(defn- connection-failed-error ^Throwable [^Throwable cause]
  (ex-info (str "The connection is failed: a write-ahead log sync did not succeed, so the commit "
                "status of recent transactions is unknown. Release the connection and reconnect "
                "to rebuild its state from storage")
           {:db/error :db.error/connection-failed
            :cognitect.anomalies/category :cognitect.anomalies/fault}
           cause))

(defn- chain-view
  "The db `d/db` returns on the writer thread inside a group, or nil when the group has
  written nothing yet (then the published db is current). Never published."
  [^Connection conn]
  (when-let [g (.get writer-chain)]
    (when @(:meta g)
      (let [cdb @(:db g)]
        (if (and cdb (not (db/released? cdb)))
          cdb
          (let [nb (db/base-db (:store conn))]
            (vreset! (:db g) nb)
            nb))))))

(defn- rebuild-db!
  "Rebuilds the published db from the store (writer thread only)."
  [^Connection conn]
  (let [d (db/base-db (:store conn))]
    (swap! (:state conn) assoc :db d :stale? false)
    d))

(defn- needs-rebuild? [s]
  (or (:stale? s) (db/released? (:db s))))

(defn current-db
  "The latest published db of `conn`. If a post-write rebuild failed earlier or the
  published db was released (d/release-db), it is rebuilt on the writer thread first
  (directly when called on the writer thread, e.g. from a transaction function)."
  [^Connection conn]
  (let [s @(:state conn)
        chain (when (on-writer? conn) (chain-view conn))]
    (when (:released? s) (throw (released-ex)))
    (cond
      chain chain
      (not (needs-rebuild? s)) (:db s)
      (:failed s) (throw (connection-failed-error (:failed s)))
      (on-writer? conn) (rebuild-db! conn)
      :else
      (let [f (CompletableFuture.)]
        (try
          (.execute ^ExecutorService (:executor conn)
                    ^Runnable (fn []
                                (.set writer-of conn)
                                (try (.complete f (let [s @(:state conn)]
                                                    (cond
                                                      (not (needs-rebuild? s)) (:db s)
                                                      (:failed s) (throw (connection-failed-error (:failed s)))
                                                      :else (rebuild-db! conn))))
                                     (catch Throwable t (.completeExceptionally f t))
                                     (finally (.remove writer-of)))))
          (catch RejectedExecutionException _ (throw (released-ex))))
        (try (.get f)
             (catch ExecutionException e (throw (or (.getCause e) e))))))))

(defn- post-commit-error [r ^Throwable t]
  (let [txid (long (:tx r))]
    (ex-info (str "Transaction " txid " (t " (codec/tx->t txid) ") was committed, but building "
                  "its db-after failed; the next d/db or transaction rebuilds it")
             {:db/error :db.error/db-after-unavailable
              :cognitect.anomalies/category :cognitect.anomalies/fault
              :committed? true :tx txid :t (codec/tx->t txid) :tempids (:tempids r)}
             t)))

(defn- schema-touched?
  "True when `tx-data` (Datoms) asserts or retracts a schema-defining attribute."
  [tx-data]
  (boolean (some (fn [^Datom d] (contains? schema/schema-attr-ids (.-a d))) tx-data)))

(defn- db-released-error? [^Throwable t]
  (= :db.error/db-released (:db/error (ex-data t))))

(defn- prepare-tx
  "Prepares `tx-data` against the published db. The published db may be released by a
  caller (d/release-db) at any moment, which makes reads through it fail; nothing has
  been written yet, so rebuild it and retry."
  [^Connection conn tx-data]
  (loop [attempt 0]
    (let [s @(:state conn)]
      (when (:released? s) (throw (released-ex)))
      (when (needs-rebuild? s) (rebuild-db! conn))
      (let [{:keys [db meta]} @(:state conn)
            r (try (tx/prepare db meta tx-data)
                   (catch Throwable t
                     (if (and (db-released-error? t) (< attempt 3))
                       ::retry
                       (throw t))))]
        (if (identical? r ::retry)
          (do (rebuild-db! conn) (recur (inc attempt)))
          [db r])))))

(defn- prepare-chain
  "Prepares `tx-data` against `db`, the db built from the previous tx of the same group
  (not yet published), with that tx's `meta`. A nil or released `db` is rebuilt from
  the store, which already holds every tx written so far."
  [^Connection conn db meta tx-data]
  (loop [attempt 0 db db]
    (when (:released? @(:state conn)) (throw (released-ex)))
    (let [db (if (or (nil? db) (db/released? db)) (db/base-db (:store conn)) db)
          r (try (tx/prepare db meta tx-data)
                 (catch Throwable t
                   (if (and (db-released-error? t) (< attempt 3)) ::retry (throw t))))]
      (if (identical? r ::retry)
        (recur (inc attempt) nil)
        [db r]))))

;; db/retire-pool! (perf round 2, piece R2-A) drops the idle pooled iterators of a db that
;; was superseded, without invalidating it. Resolved lazily so this namespace works
;; with or without that piece merged.
(def ^:private retire-fn (delay (resolve 'mnemonic.db/retire-pool!)))

(defn- retire! [db]
  (when db
    (when-let [f @retire-fn]
      (try (f db) (catch Throwable _ nil)))))

(defn- unknown-status-error ^Throwable [txid ^Throwable cause]
  (ex-info (str "Transaction " txid " was written but the write-ahead log could not be synced: "
                "its commit status is unknown")
           {:db/error :db.error/commit-status-unknown
            :cognitect.anomalies/category :cognitect.anomalies/fault
            :tx txid}
           cause))

(defn- complete-entries!
  "Completes the futures of `pend` ([f outcome txid written?] in submission order).
  `sync-err` (or nil) is the failure of the group's fsync: it fails every tx that
  was written, never one that was rejected before writing."
  [^Connection conn ^java.util.ArrayList pend sync-err]
  (doseq [[^CompletableFuture f outcome txid written?] pend]
    (cond
      (and written? sync-err) (.completeExceptionally f (unknown-status-error txid sync-err))
      (instance? Throwable outcome) (.completeExceptionally f ^Throwable outcome)
      :else (do (doseq [^java.util.Queue q @(:queues conn)]
                  (.add q outcome))
                (.complete f outcome)))))

(defn- flush-group!
  "Group commit point: fsyncs the WAL when anything was written (sync on), publishes the
  newest db (or marks the state stale when no db could be built), then completes the
  group's futures in submission order. Nothing completes before the fsync returns.
  If the fsync fails nothing is published: the connection is marked failed (further
  transactions fail fast until it is released and reconnected, which rebuilds the state from
  storage), the group's written txs fail with :db.error/commit-status-unknown and no
  tx-report-queue entry is made for them."
  [^Connection conn ^java.util.ArrayList pend chain-db]
  (when-not (.isEmpty pend)
    (let [written? (boolean (some (fn [e] (nth e 3)) pend))
          sync-err (when (and written? (:sync? conn))
                     (try (rocks-sync/sync-wal! (:store conn)) nil
                          (catch Throwable t t)))]
      (cond
        sync-err (swap! (:state conn) assoc :failed sync-err)
        written? (if chain-db
                   (swap! (:state conn) assoc :db chain-db :stale? false)
                   (swap! (:state conn) assoc :stale? true)))
      (complete-entries! conn pend sync-err)
      (.clear pend))))

(defn- run-group!
  "Runs on the transactor thread. Processes `entries` ([tx-data future] pairs) in order:
  each tx is prepared against the db of the previous one (so it sees it, exactly as
  with one-at-a-time transactions) and written as its own atomic WriteBatch, without
  fsync. One fsync then covers the whole group before ANY of its futures completes
  (with sync off every tx completes right after its write). A tx rejected in prepare
  fails alone; the others are unaffected. If the fsync fails the written txs fail with
  :db.error/commit-status-unknown and the connection is marked failed. Once a batch is
  written the new meta is recorded immediately, so a failure while building the new db
  cannot make the next tx reuse ids; the db is then built incrementally from the prepare
  result, else rebuilt, else the state is marked stale and rebuilt lazily. `pend` collects
  [future outcome txid written?] per entry (the caller needs it if this throws)."
  [^Connection conn entries ^java.util.ArrayList pend]
  (let [store (:store conn)
        sync? (:sync? conn)
        chain-db (volatile! nil)
        chain-meta (volatile! nil)]
    (.set writer-chain {:db chain-db :meta chain-meta})
    (doseq [[tx-data ^CompletableFuture f] entries]
      (let [outcome
            (try
              (when-let [cause (:failed @(:state conn))]
                (throw (connection-failed-error cause)))
              (let [[db r] (if-let [m @chain-meta]
                             (prepare-chain conn @chain-db m tx-data)
                             (prepare-tx conn tx-data))]
                (try
                  (rocks/write! store (:ops r) {:sync? false})
                  (catch Throwable t (retire! db) (throw t)))
                (swap! (:state conn) assoc :meta (:meta r))
                (vreset! chain-meta (:meta r))
                (let [new-db (try (db/successor-db store db (codec/tx->t (:tx r)) (:meta r)
                                                   (schema-touched? (:tx-data r)))
                                  (catch Throwable _
                                    ;; the tx is written: retry the db build once
                                    (try (db/base-db store) (catch Throwable t t))))]
                  (retire! db)
                  (if (instance? Throwable new-db)
                    (do (vreset! chain-db nil)
                        [(post-commit-error r new-db) (:tx r) true])
                    (do (vreset! chain-db new-db)
                        [{:db-before db :db-after new-db :tx-data (:tx-data r)
                          :tempids (:tempids r)}
                         (:tx r) true]))))
              (catch Throwable t [t nil false]))]
        (.add pend [f (nth outcome 0) (nth outcome 1) (nth outcome 2)])
        (when-not sync?
          (flush-group! conn pend @chain-db))))
    (flush-group! conn pend @chain-db)))

(def ^:private ^:const max-group 256)

(defn- fail-unfinished!
  "Last-resort completion after `run-group!` threw `t`: every future of `entries` that is
  not done yet fails. One whose tx was already written (its outcome is in `pend`) gets
  :db.error/commit-status-unknown, never a raw error, because it may be durable."
  [entries ^java.util.ArrayList pend ^Throwable t]
  (doseq [[_ ^CompletableFuture f] entries
          :when (not (.isDone f))]
    (let [[_ outcome txid written?] (first (filter (fn [e] (identical? f (nth e 0))) pend))]
      (.completeExceptionally
       f (cond written? (unknown-status-error txid t)
               (instance? Throwable outcome) outcome
               :else t)))))

(defn- drain!
  "Executor task: takes up to `max-group` queued txs and runs them as one group."
  [^Connection conn]
  (let [^ConcurrentLinkedQueue q (:pending conn)
        entries (loop [acc (transient []) n 0]
                  (if (< n max-group)
                    (if-let [e (.poll q)] (recur (conj! acc e) (inc n)) (persistent! acc))
                    (persistent! acc)))]
    (when (seq entries)
      (.set writer-of conn)
      (let [pend (java.util.ArrayList.)]
        (try (run-group! conn entries pend)
             (catch Throwable t
               ;; defensive: never leave a caller hanging; the state is rebuilt lazily
               (swap! (:state conn) assoc :stale? true)
               (fail-unfinished! entries pend t))
             (finally (.remove writer-of)
                      (.remove writer-chain)))))))

(defn transact-async
  "Enqueues `tx-data`; returns a future of the tx report {:db-before :db-after :tx-data
  :tempids}. Failures complete the future exceptionally, so deref throws an
  ExecutionException wrapping the ex-info. For file databases with sync on (default) the
  future completes only after the transaction is fsynced; transactions queued
  together share one fsync (group commit). On a failed connection (see flush-group!) the
  future fails at once."
  [^Connection conn tx-data]
  (let [f (CompletableFuture.)
        entry [tx-data f]
        ^ConcurrentLinkedQueue q (:pending conn)
        ;; If submission fails after the entry is queued, a drain task may already own it
        ;; (it then completes the future with the real outcome); only an entry we can
        ;; still take back out of the queue is ours to fail.
        abandon! (fn [^Throwable t] (when (.remove q entry) (.completeExceptionally f t)))]
    (try
      (let [s @(:state conn)]
        (when (:released? s) (throw (released-ex)))
        (when-let [cause (:failed s)] (throw (connection-failed-error cause))))
      (catch Throwable t (.completeExceptionally f t)))
    (when-not (.isDone f)
      (try
        (.add q entry)
        (.execute ^ExecutorService (:executor conn) ^Runnable (fn [] (drain! conn)))
        (catch RejectedExecutionException _ (abandon! (released-ex)))
        (catch Throwable t (abandon! t))))
    f))
