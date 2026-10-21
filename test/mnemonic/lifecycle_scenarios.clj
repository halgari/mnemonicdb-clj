(ns mnemonic.lifecycle-scenarios
  "Lifecycle scenarios that used to crash the JVM (use after release / delete / release-db,
  concurrent close). Run in a subprocess by mnemonic.lifecycle-test so that a native
  crash cannot take down the test runner. Prints one `SCENARIO <name> OK|FAIL ...` line
  per scenario; exits 0 when every scenario passed."
  (:require [mnemonic.api :as m])
  (:import (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)
           (java.util.concurrent CountDownLatch ExecutionException TimeUnit TimeoutException)))

(def ^:private schema
  [{:db/ident :p/name :db/valueType :db.type/string :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :p/n :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :p/tags :db/valueType :db.type/string :db/cardinality :db.cardinality/many}])

(defn- tmp-dir []
  (str (Files/createTempDirectory "mnemonic-lifecycle" (make-array FileAttribute 0))))

(defn- seed! [c n]
  @(m/transact c schema)
  (doseq [part (partition-all 500 (range n))]
    @(m/transact c (for [i part] {:p/name (str "p" i) :p/n i}))))

(defn- fresh [kind]
  (let [u (case kind
            :mem (str "mnemonic:mem://" (java.util.UUID/randomUUID))
            :file (str "mnemonic:file://" (tmp-dir) "/db"))]
    (m/create-database u)
    [u (m/connect u)]))

(defn err-kind
  "Runs `f`: :ok, or the :db/error of the first ex-info in the cause chain, or the
  exception itself when there is none."
  [f]
  (try (f) :ok
       (catch Throwable t
         (loop [^Throwable x t]
           (cond
             (nil? x) t
             (:db/error (ex-data x)) (:db/error (ex-data x))
             :else (recur (.getCause x)))))))

(def ^:private problems (atom []))

(defn- check [what expected actual]
  (when-not (if (set? expected) (contains? expected actual) (= expected actual))
    (swap! problems conj [what :expected expected :actual actual])))

(def ^:private released #{:db.error/connection-released})

;; ------------------------------------------------------------------ scenarios

(defn release-db-current []
  (let [[u c] (fresh :mem)]
    (seed! c 10)
    (let [db (m/db c)]
      (m/release-db db)
      (m/release-db db)
      (check "datoms on released db" :db.error/db-released (err-kind #(count (seq (m/datoms db :eavt)))))
      (check "query on released db" :db.error/db-released
             (err-kind #(m/q '[:find ?e :where [?e :p/name]] db)))
      (check "entity on released db" :db.error/db-released (err-kind #(:p/n (m/entity db [:p/name "p1"]))))
      (check "transact after release-db" :ok (err-kind #(deref (m/transact c [{:p/name "x"}]))))
      (check "d/db after release-db" 11
             (count (seq (m/datoms (m/db c) :avet :p/name)))))
    (let [db (m/db c)]
      (m/release-db db)
      (check "d/db rebuilds after release of the current db" 11
             (count (seq (m/datoms (m/db c) :avet :p/name))))
      (check "transact after release of the current db" :ok
             (err-kind #(deref (m/transact c [{:p/name "y"}])))))
    (m/release c)
    (m/delete-database u)))

(defn release-db-mid-iteration []
  (let [[u c] (fresh :mem)]
    (seed! c 2000)
    (let [db (m/db c)
          s (seq (m/datoms db :eavt))
          head (doall (take 10 s))]
      @(m/transact c [{:p/name "later"}])
      (m/release-db db)
      (check "seq started before release-db completes" true (> (count s) 4000))
      (check "head" 10 (count head))
      (check "new read of released db" :db.error/db-released
             (err-kind #(first (seq (m/datoms db :eavt))))))
    (m/release c)
    (m/delete-database u)))

(defn- use-after-close [kind close!]
  (let [[u c] (fresh kind)]
    (seed! c 2000)
    (let [db (m/db c)
          s (seq (m/datoms db :eavt))
          _ (first s)
          e (m/entity db [:p/name "p5"])
          lg (m/log c)
          txs (m/tx-range lg nil nil)
          _ (first txs)
          hist (m/history db)
          asof (m/as-of db 1001)]
      (close! u c)
      (check "lazy datoms seq after close" released (err-kind #(count s)))
      (check "new datoms after close" released (err-kind #(count (seq (m/datoms db :eavt)))))
      (check "entity after close" released (err-kind #(:p/n e)))
      (check "query after close" released (err-kind #(m/q '[:find ?e :where [?e :p/name]] db)))
      (check "pull after close" released (err-kind #(m/pull db '[*] [:p/name "p1"])))
      (check "started tx-range after close" (conj released :ok) (err-kind #(count txs)))
      (check "new tx-range after close" released (err-kind #(count (m/tx-range lg nil nil))))
      (check "history after close" released (err-kind #(count (seq (m/datoms hist :eavt)))))
      (check "as-of after close" released (err-kind #(count (seq (m/datoms asof :eavt)))))
      (check "with after close" released (err-kind #(m/with db [{:p/name "w"}])))
      (check "release-db after close" :ok (err-kind #(m/release-db db)))
      (check "d/db after close" released (err-kind #(m/db c)))
      (check "transact after close" released (err-kind #(deref (m/transact c [{:p/name "z"}])))))))

(defn use-after-release-file []
  (use-after-close :file (fn [_ c] (m/release c))))

(defn use-after-delete-file []
  (use-after-close :file (fn [u _] (m/delete-database u))))

(defn use-after-delete-mem []
  (use-after-close :mem (fn [u _] (m/delete-database u))))

(def ^:private stats (atom {}))

(def ^:private allowed-in-stress
  #{:db.error/connection-released :db.error/db-released})

(defn- stress-round [kind]
  (let [[u c] (fresh kind)
        _ (seed! c 600)
        old (m/db c)
        stop (volatile! false)
        start (CountDownLatch. 1)
        record! (fn [what t] (when-not (contains? allowed-in-stress t)
                               (swap! problems conj [what t])))
        reader (fn [pick]
                 (Thread.
                  (fn []
                    (.await start)
                    (loop [i 0]
                      (when-not @stop
                        (let [r (err-kind
                                 #(let [db (pick)]
                                    (count (seq (m/datoms db :eavt)))
                                    (m/q '[:find (count ?e) :where [?e :p/name]] db)
                                    (:p/n (m/entity db [:p/name "p3"]))
                                    (reduce (fn [n _] (inc n)) 0 (m/datoms (m/history db) :aevt :p/n))
                                    (count (take 3 (m/tx-range (m/log c) nil nil)))
                                    (let [s (seq (m/datoms db :aevt))]
                                      (first s)
                                      (Thread/sleep 1)
                                      (count s))))]
                          (swap! stats update r (fnil inc 0))
                          (when-not (= :ok r) (record! "reader" r)))
                        (recur (inc i)))))))
        threads [(reader (constantly old))
                 (reader #(m/db c))
                 (reader #(m/as-of (m/db c) 1001))
                 (Thread. (fn []
                            (.await start)
                            (loop [i 0]
                              (when-not @stop
                                (let [r (err-kind #(deref (m/transact c [{:p/name (str "w" i) :p/n i}])))]
                                  (when-not (= :ok r) (record! "writer" r)))
                                (recur (inc i))))))
                 (Thread. (fn []
                            (.await start)
                            (loop []
                              (when-not @stop
                                (let [r (err-kind #(let [d (m/db c)]
                                                     (when (> (m/basis-t d) (m/basis-t old))
                                                       (m/release-db d))))]
                                  (when-not (= :ok r) (record! "release-db" r)))
                                (Thread/sleep 5)
                                (recur)))))]]
    (doseq [^Thread t threads] (.setDaemon t true) (.start t))
    (.countDown start)
    (Thread/sleep (long (+ 20 (rand-int 130))))
    (if (zero? (rand-int 2)) (m/release c) (m/delete-database u))
    (Thread/sleep 10)
    (vreset! stop true)
    (doseq [^Thread t threads]
      (.join t 20000)
      (when (.isAlive t) (swap! problems conj ["thread did not stop" (.getName t)])))
    (m/delete-database u)))

(defn stress []
  (reset! stats {})
  (dotimes [i 60]
    (stress-round (if (even? i) :mem :file)))
  (println "stress reader outcomes" @stats))

(defn connect-race []
  (let [u (str "mnemonic:file://" (tmp-dir) "/db")]
    (m/create-database u)
    (dotimes [_ 20]
      (let [go (CountDownLatch. 1)
            fs (doall (for [j (range 4)]
                        (future
                          (.await go)
                          (err-kind #(let [c (m/connect u)]
                                       @(m/transact c [{:db/ident (keyword "k" (str (rand-int 100000)))}])
                                       (when (even? j) (m/release c)))))))]
        (.countDown go)
        (doseq [f fs]
          (check "concurrent connect/release" #{:ok :db.error/connection-released} (deref f 30000 :timeout)))))
    (dotimes [_ 100]
      (let [c (m/connect u)
            rel (future (m/release c))]
        (check "connect while the previous connection closes" #{:ok :db.error/connection-released}
               (err-kind #(deref (m/transact (m/connect u) [{:db/ident :k/x}]))))
        @rel))
    (let [go (CountDownLatch. 1)
          fs [(future (.await go) (err-kind #(m/delete-database u)))
              (future (.await go) (err-kind #(m/connect u)))
              (future (.await go) (err-kind #(m/create-database u)))]]
      (.countDown go)
      (doseq [f fs]
        (check "concurrent delete/connect/create" #{:ok :db.error/db-not-found} (deref f 30000 :timeout))))
    (m/delete-database u)))

(defn uri-normalization []
  (let [dir (tmp-dir)
        u1 (str "mnemonic:file://" dir "/db")
        u2 (str "mnemonic:file://" dir "/./x/../db")]
    (check "create" true (m/create-database u1))
    (check "create via other spelling" false (m/create-database u2))
    (let [c1 (m/connect u1)
          r (err-kind #(m/connect u2))]
      (check "connect via other spelling" :ok r)
      (when (= :ok r)
        (check "same connection" true (identical? c1 (m/connect u2)))))
    (check "delete via other spelling" true (m/delete-database u2))
    (check "gone" false (m/delete-database u1))))

(def the-conn (atom nil))

(defn db-on-writer [_db]
  (let [c @the-conn]
    (swap! (:state c) assoc :stale? true)
    (m/basis-t (m/db c))
    (m/release-db (m/db c))
    (m/basis-t (m/db c))
    [{:p/name "from-writer"}]))

(defn current-db-on-writer-thread []
  (let [[u c] (fresh :mem)]
    (seed! c 1)
    (reset! the-conn c)
    (let [f (m/transact c [['mnemonic.lifecycle-scenarios/db-on-writer]])
          r (try (.get ^java.util.concurrent.Future f 10 TimeUnit/SECONDS) :ok
                 (catch TimeoutException _ :deadlock)
                 (catch ExecutionException e (err-kind #(throw e))))]
      (check "d/db on the writer thread while stale" :ok r)
      (when (= :ok r)
        (check "tx applied" 1 (count (seq (m/datoms (m/db c) :avet :p/name "from-writer"))))))
    (m/delete-database u)))

(def scenarios
  [#'release-db-current #'release-db-mid-iteration #'use-after-release-file
   #'use-after-delete-file #'use-after-delete-mem #'uri-normalization
   #'current-db-on-writer-thread #'connect-race #'stress])

(defn -main [& names]
  (let [sel (if (seq names) (filter #(contains? (set names) (name (.sym ^clojure.lang.Var %))) scenarios) scenarios)
        failed (atom 0)]
    (doseq [v sel]
      (reset! problems [])
      (println "START" (.sym ^clojure.lang.Var v))
      (flush)
      (let [r (err-kind @v)]
        (when-not (= :ok r) (swap! problems conj ["scenario threw" r])))
      (if (empty? @problems)
        (println "SCENARIO" (.sym ^clojure.lang.Var v) "OK")
        (do (swap! failed inc)
            (println "SCENARIO" (.sym ^clojure.lang.Var v) "FAIL" (pr-str (take 10 @problems)))))
      (flush))
    (shutdown-agents)
    (System/exit (if (zero? @failed) 0 1))))
