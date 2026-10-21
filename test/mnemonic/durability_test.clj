(ns mnemonic.durability-test
  "Group commit and sync semantics for file databases: a tx's future completes only
  after the WAL is fsynced; queued txs share one fsync; order and per-tx atomicity are
  unchanged."
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [mnemonic.api :as m]
            [mnemonic.conn :as conn]
            [mnemonic.rocks-sync :as rocks-sync])
  (:import (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)
           (java.util.concurrent CompletableFuture CountDownLatch ExecutionException TimeUnit)
           (java.util.concurrent.atomic AtomicLong)))

(use-fixtures :each (fn [f] (try (f) (finally (conn/reset-registry!)))))

(defn- tmp-dir []
  (str (Files/createTempDirectory "mnemonic-dur-test" (make-array FileAttribute 0))))

(defn- delete-tree [dir]
  (doseq [f (reverse (file-seq (io/file dir)))] (.delete ^java.io.File f)))

(def ^:private schema
  [{:db/ident :t/n :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])

(defn- setup [opts]
  (let [dir (tmp-dir)
        uri (str "mnemonic:file://" dir "/db" opts)]
    (m/create-database uri)
    (let [c (m/connect uri)]
      @(m/transact c schema)
      [dir uri c])))

(defn- ns-of [db]
  (into (sorted-set) (map #(.-v ^mnemonic.datom.Datom %)) (m/datoms db :aevt :t/n)))

(defn- cause-error [^Throwable t]
  (loop [x t] (cond (nil? x) nil (:db/error (ex-data x)) (:db/error (ex-data x)) :else (recur (.getCause x)))))

(deftest concurrent-submitters-test
  (let [[dir uri c] (setup "")]
    (try
      (let [threads 8 per 40
            results (vec (for [th (range threads)]
                           (future
                             (vec (for [i (range per)]
                                    (let [n (+ (* th 1000) i)
                                          r @(m/transact c [{:t/n n}])]
                                      [n (m/basis-t (:db-before r)) (m/basis-t (:db-after r))
                                       (ns-of (:db-after r))]))))))
            all (mapv deref results)]
        (testing "every future sees its own tx, and per-thread order is preserved"
          (doseq [rs all]
            (is (apply < (map #(nth % 2) rs)))
            (doseq [[n before after seen] rs]
              (is (> after before) "tx follows its db-before")
              (is (contains? seen n)))))
        (testing "txs are serial: t values are unique and contiguous"
          (let [ts (sort (mapcat #(map (fn [x] (nth x 2)) %) all))]
            (is (= (count ts) (count (set ts))))
            (is (= (range (first ts) (+ 2 (last ts)) 2) ts) "no gaps (this store allocates t in steps of 2)")))
        (testing "everything is durable and visible"
          (is (= (* threads per) (count (ns-of (m/db c)))))))
      (finally (conn/reset-registry!) (delete-tree dir)))))

(deftest queued-txs-see-each-other-test
  (let [[dir uri c] (setup "")]
    (try
      ;; submit without deref: they queue behind the transactor and run as groups
      (let [fs (vec (for [i (range 200)] (m/transact c [{:t/n i}])))
            rs (mapv deref fs)]
        (doseq [[i r] (map-indexed vector rs)]
          (is (= (set (range (inc i))) (ns-of (:db-after r))) "tx i sees exactly txs 0..i")
          (is (= (set (range i)) (ns-of (:db-before r))))
          (when (pos? i)
            (is (= (m/basis-t (:db-after (nth rs (dec i)))) (m/basis-t (:db-before r)))))))
      (finally (conn/reset-registry!) (delete-tree dir)))))

(deftest rejected-tx-in-group-is-isolated-test
  (let [[dir uri c] (setup "")]
    (try
      (let [fs (vec (for [i (range 100)]
                      (m/transact c (if (zero? (mod i 7))
                                      [[:db/add 17592186045418 :no/such-attr i]]
                                      [{:t/n i}]))))
            outcomes (mapv (fn [^CompletableFuture f]
                             (try @f (catch ExecutionException _ ::rejected))) fs)]
        (doseq [[i o] (map-indexed vector outcomes)]
          (if (zero? (mod i 7))
            (is (= ::rejected o) (str "tx " i " is rejected"))
            (is (map? o) (str "tx " i " commits"))))
        (is (= (set (remove #(zero? (mod % 7)) (range 100))) (ns-of (m/db c)))))
      (finally (conn/reset-registry!) (delete-tree dir)))))

(deftest future-completes-only-after-fsync-test
  (let [[dir uri c] (setup "")]
    (try
      (let [syncs (AtomicLong.)
            entered (CountDownLatch. 1)
            release (CountDownLatch. 1)
            orig rocks-sync/sync-wal!]
        (with-redefs [rocks-sync/sync-wal! (fn [store]
                                             (when (= 1 (.incrementAndGet syncs))
                                               (.countDown entered)
                                               (.await release 10 TimeUnit/SECONDS))
                                             (orig store))]
          (let [f1 (m/transact c [{:t/n 1}])]
            (is (.await entered 10 TimeUnit/SECONDS))
            ;; tx 1 is written but its fsync is still pending: not complete, and not
            ;; yet visible to readers
            (is (not (.isDone ^CompletableFuture f1)))
            (is (= #{} (ns-of (m/db c))))
            ;; these queue up behind the pending fsync and share the next one
            (let [fs (vec (for [i (range 2 22)] (m/transact c [{:t/n i}])))]
              (Thread/sleep 100)
              (is (not-any? #(.isDone ^CompletableFuture %) fs))
              (.countDown release)
              (is (some? (deref f1 10000 nil)))
              (doseq [f fs] (is (some? (deref f 10000 nil))))
              (is (= 2 (.get syncs)) "21 txs, 2 fsyncs: 1 + a group of 20")
              (is (= (set (range 1 22)) (ns-of (m/db c))))))))
      (finally (conn/reset-registry!) (delete-tree dir)))))

(deftest fsync-failure-fails-the-group-test
  (let [[dir uri c] (setup "")]
    (try
      (let [q (m/tx-report-queue c)
            before (m/db c)
            before-t (m/basis-t before)]
        (with-redefs [rocks-sync/sync-wal! (fn [_] (throw (ex-info "disk on fire" {})))]
          (let [fs (vec (for [i (range 5)] (m/transact c (if (= i 2)
                                                            [[:db/add 17592186045418 :no/such-attr 1]]
                                                            [{:t/n i}]))))
                errs (mapv #(try @% :ok (catch ExecutionException e (cause-error e))) fs)]
            (is (= [:db.error/commit-status-unknown :db.error/commit-status-unknown
                    :db.error/not-an-entity
                    :db.error/commit-status-unknown :db.error/commit-status-unknown]
                   errs)
                "written txs fail as unknown; the rejected tx keeps its own error")))
        (testing "nothing was published and the report queue is silent"
          (is (= before-t (m/basis-t (m/db c))))
          (is (= #{} (ns-of (m/db c))))
          (is (nil? (.poll ^java.util.Queue q))))
        (testing "the connection is failed: later txs fail fast, even with a healthy disk"
          (let [e (try @(m/transact c [{:t/n 100}]) :ok
                       (catch ExecutionException e (cause-error e)))]
            (is (= :db.error/connection-failed e)))
          (is (= before-t (m/basis-t (m/db c))) "reads keep serving the last durable db")
          (is (nil? (.poll ^java.util.Queue q))))
        (testing "release + reconnect rebuilds from storage and works again"
          (m/release c)
          (let [c2 (m/connect uri)]
            (is (some? @(m/transact c2 [{:t/n 200}])))
            (is (contains? (ns-of (m/db c2)) 200)))))
      (finally (conn/reset-registry!) (delete-tree dir)))))

;; ------------------------------------------------------------------ d/db on the writer

(def ^:private seen (atom []))
(def ^:private the-conn (atom nil))

(defn probe
  "Transaction function that closes over the connection (through a global), as some codebases do."
  [db]
  (swap! seen conj {:arg (m/basis-t db)
                    :conn (m/basis-t (m/db @the-conn))
                    :conn-ns (ns-of (m/db @the-conn))
                    :arg-ns (ns-of db)})
  [])

(deftest writer-db-inside-group-is-the-chain-db-test
  (let [[dir uri c] (setup "")]
    (try
      (reset! seen [])
      (reset! the-conn c)
      (let [syncs (AtomicLong.)
            entered (CountDownLatch. 1)
            release (CountDownLatch. 1)
            orig rocks-sync/sync-wal!]
        (with-redefs [rocks-sync/sync-wal! (fn [store]
                                             ;; hold the group's own fsync (2nd call)
                                             (when (= 2 (.incrementAndGet syncs))
                                               (.countDown entered)
                                               (.await release 10 TimeUnit/SECONDS))
                                             (orig store))]
          (let [f0 (m/transact c [{:t/n 0}])
                _ @f0
                ;; the published db is released by the application: a naive on-writer
                ;; rebuild would publish the store's unsynced state mid-group
                _ (m/release-db (m/db c))
                fs [(m/transact c [{:t/n 1}])
                    (m/transact c [['mnemonic.durability-test/probe]])
                    (m/transact c [{:t/n 2}])
                    (m/transact c [['mnemonic.durability-test/probe]])]]
            (is (.await entered 10 TimeUnit/SECONDS))
            (testing "readers see no unsynced data while the group's fsync is pending"
              (is (= #{0} (ns-of (m/db c)))))
            (.countDown release)
            (doseq [f fs] (is (some? (deref f 10000 nil))))
            (is (= 2 (count @seen)))
            (doseq [{:keys [arg conn conn-ns arg-ns]} @seen]
              (is (= arg conn) "d/db on the writer is the preceding tx's db, as in Datomic")
              (is (= arg-ns conn-ns)))
            (is (= [#{0 1} #{0 1 2}] (mapv :conn-ns @seen)))
            (is (= #{0 1 2} (ns-of (m/db c)))))))
      (finally (reset! the-conn nil) (conn/reset-registry!) (delete-tree dir)))))

;; ------------------------------------------------------------------ minors

(defn- rejecting-executor
  "An executor whose execute runs `on-execute`, then rejects like a shut-down executor."
  [on-execute]
  (proxy [java.util.concurrent.AbstractExecutorService] []
    (execute [r] (on-execute r) (throw (java.util.concurrent.RejectedExecutionException. "shut down")))))

(deftest rejected-execute-does-not-fail-a-taken-entry-test
  (let [[dir uri c] (setup "")]
    (try
      (let [entered (CountDownLatch. 1)
            release (CountDownLatch. 1)
            orig rocks-sync/sync-wal!
            real-ex (:executor c)]
        (with-redefs [rocks-sync/sync-wal! (fn [store]
                                             (.countDown entered)
                                             (.await release 10 TimeUnit/SECONDS)
                                             (orig store))]
          ;; the drain runs on the real executor and takes the entry; then this
          ;; submission's execute is reported as rejected
          (let [fake (rejecting-executor
                      (fn [r] (.execute ^java.util.concurrent.Executor real-ex r)
                        (.await entered 10 TimeUnit/SECONDS)))
                f (conn/transact-async (assoc c :executor fake) [{:t/n 7}])]
            (Thread/sleep 100)
            (is (not (.isDone ^CompletableFuture f)) "the tx is in flight, not reported released")
            (.countDown release)
            (is (some? (deref f 10000 nil)) "and it commits")
            (is (contains? (ns-of (m/db c)) 7)))))
      (testing "an entry nobody took is failed as released"
        (let [f (conn/transact-async (assoc c :executor (rejecting-executor (fn [_]))) [{:t/n 8}])]
          (is (= :db.error/connection-released
                 (try @f (catch ExecutionException e (cause-error e)))))
          (is (not (contains? (ns-of (m/db c)) 8)))
          (is (.isEmpty ^java.util.Queue (:pending c)))))
      (finally (conn/reset-registry!) (delete-tree dir)))))

(deftest drain-catch-all-reports-unknown-status-for-written-txs-test
  (let [[dir uri c] (setup "")]
    (try
      (with-redefs [conn/complete-entries! (fn [& _] (throw (Error. "boom")))]
        (let [fs [(m/transact c [{:t/n 1}])
                  (m/transact c [[:db/add 17592186045418 :no/such-attr 1]])]
              errs (mapv #(try @% :ok (catch ExecutionException e (cause-error e))) fs)]
          (is (= :db.error/commit-status-unknown (first errs)) "written: never a raw Error")
          (is (= :db.error/not-an-entity (second errs)) "rejected before writing: its own error")))
      (finally (conn/reset-registry!) (delete-tree dir)))))

(deftest sync-option-test
  (testing "sync=false never fsyncs, and the data survives reconnect"
    (let [[dir uri c] (setup "?sync=false")
          syncs (AtomicLong.)]
      (try
        (with-redefs [rocks-sync/sync-wal! (fn [_] (.incrementAndGet syncs) nil)]
          (dotimes [i 20] @(m/transact c [{:t/n i}])))
        (is (zero? (.get syncs)))
        (m/release c)
        (let [c2 (m/connect (str "mnemonic:file://" dir "/db"))]
          (is (= (set (range 20)) (ns-of (m/db c2)))))
        (finally (conn/reset-registry!) (delete-tree dir)))))
  (testing "default is sync=true: one fsync per serial tx"
    (let [[dir uri c] (setup "?sync=true")
          syncs (AtomicLong.)
          orig rocks-sync/sync-wal!]
      (try
        (with-redefs [rocks-sync/sync-wal! (fn [s] (.incrementAndGet syncs) (orig s))]
          (dotimes [i 5] @(m/transact c [{:t/n i}])))
        (is (= 5 (.get syncs)))
        (finally (conn/reset-registry!) (delete-tree dir)))))
  (testing "invalid options"
    (is (= :db.error/invalid-db-uri
           (try (m/create-database "mnemonic:file:///tmp/x?sync=maybe") (catch Throwable t (cause-error t)))))
    (is (= :db.error/invalid-db-uri
           (try (m/create-database "mnemonic:file:///tmp/x?foo=1") (catch Throwable t (cause-error t)))))))
