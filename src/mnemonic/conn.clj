(ns mnemonic.conn
  "Connections: URI registry, the single-writer transactor executor, and db publishing."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [mnemonic.bootstrap :as bootstrap]
            [mnemonic.codec :as codec]
            [mnemonic.db :as db]
            [mnemonic.index :as index]
            [mnemonic.rocks :as rocks]
            [mnemonic.tx :as tx])
  (:import (java.io File)
           (java.util.concurrent CompletableFuture ExecutionException ExecutorService Executors
                                 RejectedExecutionException ThreadFactory TimeUnit)
           (java.util.concurrent.atomic AtomicLong)))

(set! *warn-on-reflection* true)

;; state: (atom {:db Base :meta meta :released? bool :stale? bool})
;; key: the normalized registry key, [:mem name] or [:file canonical-path]
(defrecord Connection [uri key store state executor queues])

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

(defn- parse-uri
  "{:kind :mem :name n :key k} or {:kind :file :path p :key k}. File paths are
  canonicalized so one database written two ways maps to one key."
  [uri]
  (cond
    (not (string? uri)) (fail :db.error/invalid-db-uri (str "Invalid database URI " (pr-str uri)))
    (str/starts-with? uri mem-prefix)
    (let [n (subs uri (count mem-prefix))]
      (when (str/blank? n) (fail :db.error/invalid-db-uri (str "Invalid database URI " uri)))
      {:kind :mem :name n :key [:mem n]})
    (str/starts-with? uri file-prefix)
    (let [p (subs uri (count file-prefix))
          p (if (re-matches #"/[A-Za-z]:.*" p) (subs p 1) p)]
      (when (str/blank? p) (fail :db.error/invalid-db-uri (str "Invalid database URI " uri)))
      (let [p (canonical-path p)]
        {:kind :file :path p :key [:file p]}))
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

(defn- make-connection [uri key store]
  (when-not (bootstrap/read-meta store)
    (not-found uri))
  (let [d (db/base-db store)]
    (try (check-format! (db/db-meta d) (index/-basis-t d))
         (catch Throwable t (db/release-db d) (throw t)))
    (->Connection uri key store (atom {:db d :meta (db/db-meta d) :released? false}) (executor) (atom #{}))))

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
   (fn [{:keys [kind name path key]}]
     (or (get-in @registry [:conns key])
         (let [store (case kind
                       :mem (or (get-in @registry [:mem name]) (not-found uri))
                       :file (if (file-db-exists? path)
                               (rocks/open-store {:path path})
                               (not-found uri)))
               conn (try (make-connection uri key store)
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
  (let [s @(:state conn)]
    (when (:released? s) (throw (released-ex)))
    (cond
      (not (needs-rebuild? s)) (:db s)
      (on-writer? conn) (rebuild-db! conn)
      :else
      (let [f (CompletableFuture.)]
        (try
          (.execute ^ExecutorService (:executor conn)
                    ^Runnable (fn []
                                (.set writer-of conn)
                                (try (.complete f (let [s @(:state conn)]
                                                    (if (needs-rebuild? s)
                                                      (rebuild-db! conn)
                                                      (:db s))))
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

(defn- do-transact
  "Runs on the transactor thread. Once the batch is written the new meta is recorded
  immediately, so a failure while building the new db cannot make the next tx reuse
  ids; the db is then rebuilt lazily (:stale?) at the start of the next tx."
  [^Connection conn tx-data]
  (let [s @(:state conn)]
    (when (:released? s) (throw (released-ex)))
    (when (needs-rebuild? s) (rebuild-db! conn))
    (let [{:keys [db meta]} @(:state conn)
          r (tx/prepare db meta tx-data)
          _ (rocks/write! (:store conn) (:ops r) {:sync? (file-uri? conn)})
          _ (swap! (:state conn) assoc :meta (:meta r) :stale? true)
          ;; the tx is durable: retry the db build once instead of failing the caller
          new-db (try (rebuild-db! conn)
                      (catch Throwable _
                        (try (rebuild-db! conn)
                             (catch Throwable t
                               (throw (post-commit-error r t))))))
          report {:db-before db :db-after new-db :tx-data (:tx-data r) :tempids (:tempids r)}]
      (doseq [^java.util.Queue q @(:queues conn)]
        (.add q report))
      report)))

(defn transact-async
  "Enqueues `tx-data`; returns a future of the tx report {:db-before :db-after :tx-data
  :tempids}. Failures complete the future exceptionally, so deref throws an
  ExecutionException wrapping the ex-info."
  [^Connection conn tx-data]
  (let [f (CompletableFuture.)
        task ^Runnable (fn []
                         (.set writer-of conn)
                         (try (.complete f (do-transact conn tx-data))
                              (catch Throwable t (.completeExceptionally f t))
                              (finally (.remove writer-of))))]
    (try
      (when (:released? @(:state conn)) (throw (released-ex)))
      (.execute ^ExecutorService (:executor conn) task)
      (catch RejectedExecutionException _
        (.completeExceptionally f (released-ex)))
      (catch Throwable t
        (.completeExceptionally f t)))
    f))
