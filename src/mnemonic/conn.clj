(ns mnemonic.conn
  "Connections: URI registry, the single-writer transactor executor, and db publishing."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [mnemonic.bootstrap :as bootstrap]
            [mnemonic.db :as db]
            [mnemonic.rocks :as rocks]
            [mnemonic.tx :as tx])
  (:import (java.util.concurrent CompletableFuture ExecutorService Executors
                                 RejectedExecutionException ThreadFactory TimeUnit)
           (java.util.concurrent.atomic AtomicLong)))

(set! *warn-on-reflection* true)

;; state: (atom {:db Base :meta meta :released? bool})
(defrecord Connection [uri store state executor queues])

(def ^:private mem-prefix "mnemonic:mem://")
(def ^:private file-prefix "mnemonic:file://")

(defonce ^:private lock (Object.))

;; {:mem {name store} :conns {uri Connection}}
(defonce ^:private registry (atom {:mem {} :conns {}}))

(defn- fail
  ([kind msg] (fail kind msg :cognitect.anomalies/incorrect nil))
  ([kind msg category data]
   (throw (tx/error kind msg category data))))

(defn- parse-uri
  "{:kind :mem :name n} or {:kind :file :path p}."
  [uri]
  (cond
    (not (string? uri)) (fail :db.error/invalid-db-uri (str "Invalid database URI " (pr-str uri)))
    (str/starts-with? uri mem-prefix)
    (let [n (subs uri (count mem-prefix))]
      (when (str/blank? n) (fail :db.error/invalid-db-uri (str "Invalid database URI " uri)))
      {:kind :mem :name n})
    (str/starts-with? uri file-prefix)
    (let [p (subs uri (count file-prefix))
          p (if (re-matches #"/[A-Za-z]:.*" p) (subs p 1) p)]
      (when (str/blank? p) (fail :db.error/invalid-db-uri (str "Invalid database URI " uri)))
      {:kind :file :path p})
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
  (str/starts-with? (:uri conn) file-prefix))

(defn- make-connection [uri store]
  (when-not (bootstrap/read-meta store)
    (rocks/close-store store)
    (not-found uri))
  (let [d (db/base-db store)]
    (->Connection uri store (atom {:db d :meta (db/db-meta d) :released? false}) (executor) (atom #{}))))

;; ------------------------------------------------------------------ lifecycle

(defn create-database
  "Creates the database at `uri`; true if created, false if it already exists."
  [uri]
  (locking lock
    (let [{:keys [kind name path]} (parse-uri uri)]
      (case kind
        :mem (if (contains? (:mem @registry) name)
               false
               (let [store (rocks/open-store {:path name :mem? true})]
                 (bootstrap/bootstrap! store)
                 (swap! registry assoc-in [:mem name] store)
                 true))
        :file (if (or (file-db-exists? path) (contains? (:conns @registry) uri))
                false
                (do (.mkdirs (io/file path))
                    (let [store (rocks/open-store {:path path})]
                      (try (bootstrap/bootstrap! store)
                           (finally (rocks/close-store store))))
                    true))))))

(defn connect
  "Connection to the database at `uri` (one cached Connection per uri)."
  [uri]
  (locking lock
    (let [{:keys [kind name path]} (parse-uri uri)]
      (or (get-in @registry [:conns uri])
          (let [store (case kind
                        :mem (or (get-in @registry [:mem name]) (not-found uri))
                        :file (if (file-db-exists? path)
                                (rocks/open-store {:path path})
                                (not-found uri)))
                conn (try (make-connection uri store)
                          (catch Throwable t
                            (when (= kind :file) (rocks/close-store store))
                            (throw t)))]
            (swap! registry assoc-in [:conns uri] conn)
            conn)))))

(defn- release* [^Connection conn close-store?]
  (when (identical? conn (get-in @registry [:conns (:uri conn)]))
    (swap! registry update :conns dissoc (:uri conn)))
  (swap! (:state conn) assoc :released? true)
  (let [^ExecutorService ex (:executor conn)]
    (.shutdown ex)
    (.awaitTermination ex 30 TimeUnit/SECONDS))
  (when close-store?
    (rocks/close-store (:store conn)))
  nil)

(defn release
  "Releases `conn`: stops its transactor and, for file databases, closes the store.
  In-memory databases stay available to later `connect` calls."
  [^Connection conn]
  (locking lock
    (when-not (:released? @(:state conn))
      (release* conn (file-uri? conn))))
  nil)

(defn delete-database
  "Deletes the database at `uri`; true if it existed."
  [uri]
  (locking lock
    (let [{:keys [kind name path]} (parse-uri uri)
          conn (get-in @registry [:conns uri])]
      (case kind
        :mem (if-let [store (get-in @registry [:mem name])]
               (do (when conn (release* conn false))
                   (swap! registry update :mem dissoc name)
                   (rocks/close-store store)
                   true)
               false)
        :file (if (or conn (file-db-exists? path))
                (do (when conn (release* conn true))
                    (rocks/destroy-store path)
                    true)
                false)))))

(defn reset-registry!
  "Test hook: releases every connection and drops all in-memory databases."
  []
  (locking lock
    (doseq [conn (vals (:conns @registry))]
      (release* conn (file-uri? conn)))
    (doseq [store (vals (:mem @registry))]
      (rocks/close-store store))
    (reset! registry {:mem {} :conns {}})
    nil))

;; ------------------------------------------------------------------ reads

(defn- released-ex []
  (tx/error :db.error/connection-released "The connection has been released."
            :cognitect.anomalies/conflict nil))

(defn current-db
  "The latest published db of `conn`."
  [^Connection conn]
  (let [s @(:state conn)]
    (when (:released? s) (throw (released-ex)))
    (:db s)))

;; ------------------------------------------------------------------ writes

(defn- do-transact
  "Runs on the transactor thread."
  [^Connection conn tx-data]
  (let [{:keys [db meta released?]} @(:state conn)]
    (when released? (throw (released-ex)))
    (let [r (tx/prepare db meta tx-data)
          _ (rocks/write! (:store conn) (:ops r) {:sync? (file-uri? conn)})
          new-db (db/base-db (:store conn))
          report {:db-before db :db-after new-db :tx-data (:tx-data r) :tempids (:tempids r)}]
      (swap! (:state conn) assoc :db new-db :meta (:meta r))
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
                         (try (.complete f (do-transact conn tx-data))
                              (catch Throwable t (.completeExceptionally f t))))]
    (try
      (when (:released? @(:state conn)) (throw (released-ex)))
      (.execute ^ExecutorService (:executor conn) task)
      (catch RejectedExecutionException _
        (.completeExceptionally f (released-ex)))
      (catch Throwable t
        (.completeExceptionally f t)))
    f))
