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
      (testing "the connection keeps working afterwards"
        (is (some? @(m/transact c [{:t/n 100}])))
        (is (contains? (ns-of (m/db c)) 100)))
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
