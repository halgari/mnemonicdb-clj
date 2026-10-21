(ns mnemonic.r25
  "Round 2.5 storage-configuration benchmark, file db on a REAL filesystem (target/r25).

  clojure -M:bench -m mnemonic.r25 build   <dir> <entities>   bulk load + flush + compact + close
  clojure -M:bench -m mnemonic.r25 measure <dir> [uri-query]  connect time, reads, queries, cold lookups
  clojure -M:bench -m mnemonic.r25 wal     <dir> <entities>   load with sync=false, close, time reopen
  clojure -M:bench -m mnemonic.r25 durable <dir> [uri-query]  small-tx rate, 1 and 16 threads

  Configuration under test comes from system properties (mnemonic.rocks.*) or the uri query."
  (:require [clojure.java.io :as io]
            [mnemonic.api :as m]
            [mnemonic.bench.data :as data]
            [mnemonic.bench.util :as u])
  (:import (java.util Random)
           (org.rocksdb FlushOptions RocksDB)))

(set! *warn-on-reflection* true)

(defn- out [k v] (println (format "%-34s %s" k (if (float? v) (format "%.2f" (double v)) v))) (flush))
(defn- now ^long [] (System/nanoTime))
(defn- ms ^double [^long ns] (/ (double ns) 1e6))
(defn- rate [n ns] (/ (double n) (/ (double ns) 1e9)))

(defn- du-bytes ^long [dir]
  (reduce + (map #(.length ^java.io.File %) (filter #(.isFile ^java.io.File %) (file-seq (io/file dir))))))

(defn- uri [dir q] (str "mnemonic:file://" (.getCanonicalPath (io/file dir)) q))

(defn- flush-compact! [conn]
  (let [store (:store conn) ^RocksDB db (:db store)]
    (with-open [fo (doto (FlushOptions.) (.setWaitForFlush true))]
      (.flush db fo ^java.util.List (vec (vals (:cfs store)))))
    (doseq [h (vals (:cfs store))] (.compactRange db ^org.rocksdb.ColumnFamilyHandle h))))

(defn- load! [conn n batch]
  (let [t0 (now)]
    (doseq [from (range 0 n batch)]
      @(m/transact conn (data/people 1 from (min n (+ from batch)))))
    (- (now) t0)))

(defn build [dir n]
  (let [u (uri dir "")]
    (m/delete-database u)
    (m/create-database u)
    (let [conn (m/connect u)]
      @(m/transact conn data/schema)
      @(m/transact conn data/enum-idents)
      (let [ns (load! conn n 1000)]
        (out "bulk-load entities/s" (rate n ns))
        (let [t0 (now)] (flush-compact! conn) (out "flush+compact ms" (ms (- (now) t0)))))
      (let [t0 (now)] (m/release conn) (out "close ms" (ms (- (now) t0)))))
    (out "db size MB" (/ (du-bytes dir) 1048576.0))))

(defn wal [dir n]
  (let [u (uri dir "?sync=false")]
    (m/delete-database u)
    (m/create-database u)
    (let [conn (m/connect u)]
      @(m/transact conn data/schema)
      @(m/transact conn data/enum-idents)
      (load! conn n 1000)
      (let [t0 (now)] (m/release conn) (out "close ms (after nosync load)" (ms (- (now) t0)))))
    (out "wal bytes after close"
         (reduce + (map #(.length ^java.io.File %)
                        (filter #(.endsWith (.getName ^java.io.File %) ".log") (file-seq (io/file dir))))))
    (dotimes [i 3]
      (let [t0 (now) conn (m/connect u) t1 (now)]
        (out (str "connect #" (inc i) " ms") (ms (- t1 t0)))
        (let [t2 (now)]
          (m/entity (m/db conn) [:p/email (data/email 5)])
          (out "  first entity ms" (ms (- (now) t2))))
        (m/release conn)))))

(defn- rand-ids ^longs [^long n ^long bound seed]
  (let [r (Random. (long seed))] (long-array (repeatedly n #(.nextInt r (int bound))))))

(defn- pass [^longs xs f]
  (let [t0 (now)] (dotimes [i (alength xs)] (f (aget xs i))) (- (now) t0)))

(defn measure [dir q]
  (let [u (uri dir q)
        t0 (now) conn (m/connect u) t1 (now)]
    (out "connect #1 ms" (ms (- t1 t0)))
    (let [ents (long (ffirst (m/q '[:find (count ?e) :where [?e :p/email]] (m/db conn))))]
      (out "first query ms (incl count)" (ms (- (now) t1)))
      (out "entities" ents)
      (let [db (m/db conn)
            nl 100000
            ids (rand-ids nl ents 51)
            ent (fn [i] (let [e (m/entity db [:p/email (data/email i)])]
                          (+ (long (:p/age e)) (count ^String (:p/name e)))))
            ent2 (fn [eid] (let [e (m/entity db eid)]
                             (+ (long (:p/age e)) (count ^String (:p/name e)) (count ^String (:p/email e)))))
            c1 (pass ids ent)]
        (out "cold lookup-ref+entity /s" (rate nl c1))
        (out "warm lookup-ref+entity /s" (rate nl (pass ids ent)))
        (let [miss (rand-ids 100000 1000000 52)
              f (fn [i] (m/entid db [:p/email (str "nobody-" i "@x.example")]))]
          (out "entid miss /s" (rate 100000 (pass miss f))))
        (let [big (rand-ids 300000 ents 53)
              ee (long-array (map #(m/entid db [:p/email (data/email %)]) big))
              _ (pass ee ent2)
              t (pass ee ent2)]
          (out "entity+3 attrs /s (300k rand)" (rate 300000 t))
          (let [pl '[:p/email :p/age {:p/status [:db/ident]} {:p/tags [:db/ident]}]
                pe (java.util.Arrays/copyOf ee 30000)]
            (pass pe #(count (m/pull db pl %)))
            (out "pull nested /s" (rate 30000 (pass pe #(count (m/pull db pl %)))))))
        (let [r (Random. 45)
              in-emails (vec (repeatedly 1000 #(data/email (.nextInt r (int ents)))))
              qs [["q-b 3-way join" '[:find ?e ?i :in $ ?a :where [?e :p/age ?a] [?e :p/tags ?t] [?t :db/ident ?i]] [42]]
                  ["q-c join+range+pred" '[:find ?n ?a :in $ ?lo ?hi
                                           :where [?e :p/age ?a] [(<= ?lo ?a)] [(< ?a ?hi)]
                                           [?e :p/status :status/active] [?e :p/name ?n]] [100 110]]
                  ["q-f in-collection 1000" '[:find ?e :in $ [?em ...] :where [?e :p/email ?em]] [in-emails]]]]
          (doseq [[label q args] qs]
            (let [res (u/run-for #(count (apply m/q q db args)) {:warm-ms 1500 :run-ms 3000 :min-iters 5})]
              (out (str label " ms") (:mean-ms res)))))))
    (let [t (now)] (m/release conn) (out "close ms" (ms (- (now) t))))
    (let [t0 (now) conn (m/connect u) t1 (now)]
      (out "connect #2 (same jvm) ms" (ms (- t1 t0)))
      (m/release conn))))

(defn durable [dir q]
  (let [u (uri dir q)]
    (m/delete-database u) (m/create-database u)
    (let [conn (m/connect u)]
      @(m/transact conn data/schema)
      @(m/transact conn data/enum-idents)
      (dotimes [i 50] @(m/transact conn (data/small-tx (+ 9000000 i))))
      (doseq [threads [1 16]]
        (let [per (quot 1600 threads)
              start (java.util.concurrent.CountDownLatch. 1)
              ws (vec (for [th (range threads)]
                        (future (.await start)
                                (dotimes [j per] @(m/transact conn (data/small-tx (+ 20000000 (* th 100000) j)))))))
              t0 (now)]
          (.countDown start) (run! deref ws)
          (out (str "durable tx/s, " threads " thr") (rate (* per threads) (- (now) t0)))))
      (m/release conn))))

(defn reconnect
  "Five connect/first-answer/release cycles in one JVM."
  [dir q]
  (let [u (uri dir q)]
    (dotimes [i 5]
      (let [t0 (now) conn (m/connect u) t1 (now)
            e (m/entity (m/db conn) [:p/email (data/email 5)])
            _ (:p/age e)
            t2 (now)]
        (out (str "connect #" (inc i) " ms / +first entity ms") (format "%.1f / %.1f" (ms (- t1 t0)) (ms (- t2 t1))))
        (let [t3 (now)] (m/release conn) (out "  release ms" (ms (- (now) t3))))))))

(defn -main [& [cmd dir a]]
  (case cmd
    "build" (build dir (Long/parseLong a))
    "wal" (wal dir (Long/parseLong a))
    "measure" (measure dir (or a ""))
    "reconnect" (reconnect dir (or a ""))
    "durable" (durable dir (or a "")))
  (shutdown-agents))
