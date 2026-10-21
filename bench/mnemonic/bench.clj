(ns mnemonic.bench
  "Benchmark suite: the same workloads against mnemonic mem, mnemonic file and Datomic mem.

  clojure -M:bench -m mnemonic.bench [workload-names...]

  Env: MNEMONIC_BENCH_SCALE (size multiplier, default 1.0), MNEMONIC_BENCH_BACKENDS
  (comma list of mnemonic-mem,mnemonic-file,datomic-mem), per-size overrides
  MNEMONIC_BENCH_<ENTITIES|BATCH|SMALLTX|EAVT_ENTITIES|LOOKUPS|PULLS|LOOKUP_REFS|TREE|IN_VALUES|
  HIST_ENTITIES|HIST_UPDATES|HIST_BATCH|QUERY_MS>. See docs/benchmarks.md."
  (:require [clojure.java.io :as io]
            mnemonic.index
            [clojure.string :as str]
            [datomic.api :as d]
            [mnemonic.api :as m]
            [mnemonic.bench.backend :as be :refer [call tx! db]]
            [mnemonic.bench.data :as data]
            [mnemonic.bench.util :as u :refer [size timed-ns]])
  (:import (java.util Random)
           (org.rocksdb FlushOptions RocksDB)))

(set! *warn-on-reflection* true)

(defn- log [& xs] (binding [*out* *err*] (apply println xs) (flush)))

;; ------------------------------------------------------------------ sizes

(defn sizes []
  (let [entities (size "ENTITIES" 100000)]
    {:entities entities
     :batch (long (or (some-> (System/getenv "MNEMONIC_BENCH_BATCH") Long/parseLong) 1000))
     :small-tx (size "SMALLTX" 2000)
     :durable-tx (size "DURABLE_TX" 800)
     :eavt-entities (min entities (size "EAVT_ENTITIES" 10000))
     :lookups (size "LOOKUPS" 100000)
     :pulls (size "PULLS" 10000)
     :lookup-refs (size "LOOKUP_REFS" 100000)
     :tree (size "TREE" 10000)
     :in-values (min entities (size "IN_VALUES" 1000))
     :hist-entities (size "HIST_ENTITIES" 1000)
     :hist-updates (size "HIST_UPDATES" 10000)
     :hist-batch (long (or (some-> (System/getenv "MNEMONIC_BENCH_HIST_BATCH") Long/parseLong) 10))
     :query-ms (size "QUERY_MS" 3000)}))

;; ------------------------------------------------------------------ result collection

(def ^:private results (atom []))  ; [{:workload :metric :dir :values {backend v}}]
(def ^:private checks (atom []))   ; [{:workload :check :values {backend v}}]

(defn- record! [workload b {:keys [metrics checks-m]}]
  (doseq [[metric v dir] metrics]
    (swap! results
           (fn [rs]
             (let [i (first (keep-indexed (fn [i r] (when (and (= (:workload r) workload) (= (:metric r) metric)) i)) rs))]
               (if i
                 (assoc-in rs [i :values (:name b)] v)
                 (conj rs {:workload workload :metric metric :dir dir :values {(:name b) v}}))))))
  (doseq [[k v] checks-m]
    (swap! checks
           (fn [cs]
             (let [i (first (keep-indexed (fn [i r] (when (and (= (:workload r) workload) (= (:check r) k)) i)) cs))]
               (if i
                 (assoc-in cs [i :values (:name b)] v)
                 (conj cs {:workload workload :check k :values {(:name b) v}})))))))

;; ------------------------------------------------------------------ dataset (shared between workloads)

(def ^:private datasets (atom {})) ; backend-name -> {:ctx :eids}

(defn- scan-count
  "Counts datoms, forcing the value of each."
  ^long [datoms]
  (reduce (fn [^long n datom] (if (nil? (:v datom)) n (inc n))) 0 datoms))

(defn- load-schema! [ctx]
  (tx! ctx data/schema)
  (tx! ctx data/enum-idents))

(defn- people-batches [{:keys [entities batch]}]
  (vec (for [from (range 0 entities batch)]
         (data/people 1 from (min entities (+ from batch))))))

(defn- eids-of [b db*]
  (reduce (fn [acc datom] (conj! acc (:e datom))) (transient []) (call b :datoms db* :aevt :p/email)))

(defn- load-dataset!
  "Creates the benchmark db, loads schema and people; returns [ctx batch-times-ns total-ns]."
  [b batches]
  (when-let [old (:ctx (get @datasets (:name b)))] (be/drop-db! old))
  (swap! datasets dissoc (:name b))
  (let [ctx (be/new-db! b)]
    (load-schema! ctx)
    (u/gc!)
    (let [times (long-array (count batches))
          t0 (System/nanoTime)]
      (dorun (map-indexed (fn [i batch]
                            (let [s (System/nanoTime)]
                              (tx! ctx batch)
                              (aset times i (- (System/nanoTime) s))))
                          batches))
      (let [total (- (System/nanoTime) t0)
            eids (persistent! (eids-of b (db ctx)))]
        (swap! datasets assoc (:name b) {:ctx ctx :eids eids})
        [ctx (vec times) total]))))

(defn- dataset! [b sz]
  (or (get @datasets (:name b))
      (do (log "  [" (:name b) "] loading dataset (untimed)...")
          (load-dataset! b (people-batches sz))
          (get @datasets (:name b)))))

(defn- ms [ns] (/ (double ns) 1e6))
(defn- per-sec [n ns] (/ (double n) (/ (double ns) 1e9)))

;; ------------------------------------------------------------------ workloads
;; Each (fn [b sz]) returns {:metrics [[name value :hi|:lo]...] :checks-m {name value}}.

(defn bulk-load [b sz]
  (let [batches (people-batches sz)
        [ctx times total] (load-dataset! b batches)
        sorted (vec (sort times))
        n (:entities sz)]
    {:metrics [["entities/sec" (per-sec n total) :hi]
               ["total s" (/ total 1e9) :lo]
               ["batch p50 ms" (ms (u/percentile sorted 0.5)) :lo]
               ["batch p99 ms" (ms (u/percentile sorted 0.99)) :lo]]
     :checks-m {"email datoms" (scan-count (call b :datoms (db ctx) :aevt :p/email))}}))

(defn small-tx [b sz]
  (let [ctx (be/new-db! b)
        n (:small-tx sz)
        warm (min 500 n)]
    (try
      (load-schema! ctx)
      (dotimes [i warm] (tx! ctx (data/small-tx (+ 10000000 i))))
      (u/gc!)
      (let [txs (mapv data/small-tx (range n))
            lat (long-array n)
            t0 (System/nanoTime)]
        (dotimes [i n]
          (let [s (System/nanoTime)]
            (tx! ctx (nth txs i))
            (aset lat i (- (System/nanoTime) s))))
        (let [total (- (System/nanoTime) t0)
              sorted (vec (sort (vec lat)))]
          {:metrics [["tx/sec" (per-sec n total) :hi]
                     ["p50 ms" (ms (u/percentile sorted 0.5)) :lo]
                     ["p99 ms" (ms (u/percentile sorted 0.99)) :lo]]
           :checks-m {"email datoms" (scan-count (call b :datoms (db ctx) :aevt :p/email))
                      "age datoms" (scan-count (call b :datoms (db ctx) :aevt :p/age))}}))
      (finally (be/drop-db! ctx)))))

(defn- durable-case
  "`n` small txs, split over `threads` submitting threads that each deref their own
  transact, on a REAL filesystem (target/bench-durable; /tmp is tmpfs here, which hides
  fsync). `opts` is the URI query (\"\" = sync on, \"?sync=false\")."
  [b n threads opts]
  (let [root (doto (io/file "target" "bench-durable") (.mkdirs))
        dir (str (.getCanonicalPath root) "/db-" (System/nanoTime))
        uri (str "mnemonic:file://" dir opts)]
    (call b :create-database uri)
    (let [ctx {:uri uri :backend b :conn (call b :connect uri)}]
      (try
        (load-schema! ctx)
        (dotimes [i 20] (tx! ctx (data/small-tx (+ 10000000 i))))
        (let [per (quot n threads)
              total-n (* per threads)
              lat (long-array total-n)
              start (java.util.concurrent.CountDownLatch. 1)
              ws (vec (for [th (range threads)]
                        (future
                          (.await start)
                          (dotimes [j per]
                            (let [i (+ (* th per) j)
                                  tx-data (data/small-tx (+ 20000000 i))
                                  s (System/nanoTime)]
                              (tx! ctx tx-data)
                              (aset lat i (- (System/nanoTime) s)))))))
              t0 (System/nanoTime)]
          (.countDown start)
          (run! deref ws)
          (let [total (- (System/nanoTime) t0)
                sorted (vec (sort (vec lat)))]
            [(per-sec total-n total) (ms (u/percentile sorted 0.5)) (ms (u/percentile sorted 0.99))]))
        (finally (be/drop-db! ctx))))))

(defn durable-tx
  "Durability cost on a real filesystem: sync on (default; group commit shares one fsync
  between concurrent submitters) vs ?sync=false, with 1/4/16 submitting threads."
  [b sz]
  (if-not (= "mnemonic-file" (:name b))
    {:metrics []}
    (let [n (:durable-tx sz)
          n-off (* 5 n)]
      {:metrics
       (vec (for [[label opts nn] (filter (fn [[label]] (let [o (System/getenv "MNEMONIC_BENCH_DURABLE_ONLY")]
                                                           (or (nil? o) (= o label))))
                                           [["sync" "" n] ["no-sync" "?sync=false" n-off]])
                  threads [1 4 16]
                  :let [[tps p50 p99] (durable-case b nn threads opts)]
                  m [[(str label ", " threads " thr: tx/sec") tps :hi]
                     [(str label ", " threads " thr: p50 ms") p50 :lo]
                     [(str label ", " threads " thr: p99 ms") p99 :lo]]]
              m))})))

(defn- fly-count
  "Like scan-count, but over flyweight views (internal path)."
  ^long [datoms]
  (mnemonic.index/reduce-views datoms
                               (fn [^long n ^mnemonic.datom.IDatomView d] (if (nil? (.value d)) n (inc n)))
                               0))

(defn datoms-scan [b sz]
  (let [{:keys [ctx eids]} (dataset! b sz)
        db* (db ctx)
        aevt #(scan-count (call b :datoms db* :aevt :p/age))
        sample (subvec eids 0 (:eavt-entities sz))
        eavt (fn [] (reduce (fn [^long n eid] (+ n (scan-count (call b :datoms db* :eavt eid)))) 0 sample))
        opts {:warm-ms 2000 :run-ms 3000 :min-iters 3}
        a (u/run-for aevt opts)
        e (u/run-for eavt opts)
        fly? (and (= :mnemonic (:kind b)) (System/getProperty "bench.fly"))
        fa (when fly? (u/run-for #(fly-count (call b :datoms db* :aevt :p/age)) opts))
        fe (when fly? (u/run-for (fn [] (reduce (fn [^long n eid] (+ n (fly-count (call b :datoms db* :eavt eid)))) 0 sample)) opts))]
    {:metrics (cond-> [["AEVT :p/age datoms/sec" (/ (:result a) (/ (:mean-ms a) 1000.0)) :hi]
                       ["AEVT scan ms" (:mean-ms a) :lo]
                       ["EAVT datoms/sec" (/ (:result e) (/ (:mean-ms e) 1000.0)) :hi]
                       ["EAVT scan ms" (:mean-ms e) :lo]]
                fly? (into [["AEVT fly datoms/sec" (/ (:result fa) (/ (:mean-ms fa) 1000.0)) :hi]
                            ["EAVT fly datoms/sec" (/ (:result fe) (/ (:mean-ms fe) 1000.0)) :hi]]))
     :checks-m (cond-> {"AEVT count" (:result a) "EAVT count" (:result e)}
                 fly? (assoc "AEVT fly count" (:result fa) "EAVT fly count" (:result fe)))}))

(defn- random-eids [eids n seed]
  (let [r (Random. (long seed)) cnt (count eids)]
    (long-array (repeatedly n #(nth eids (.nextInt r cnt))))))

(defn- timed-ops
  "Warms up with the first 20% of eids (min 1000 ops), then times all of `eids`.
  f: (fn [eid]) -> long; returns [ops/sec checksum]."
  [^longs eids f]
  (let [n (alength eids)
        warm (min n (max 1000 (quot n 5)))]
    (dotimes [i warm] (f (aget eids i)))
    (u/gc!)
    (let [[sum ns] (timed-ns (loop [i 0 acc 0]
                               (if (< i n) (recur (inc i) (+ acc (long (f (aget eids i))))) acc)))]
      [(per-sec n ns) sum])))

(defn entity-lookup [b sz]
  (let [{:keys [ctx eids]} (dataset! b sz)
        db* (db ctx)
        ids (random-eids eids (:lookups sz) 42)
        [rate sum] (timed-ops ids
                              (fn [eid]
                                (let [e (call b :entity db* eid)]
                                  (+ (long (:p/age e)) (count ^String (:p/name e)) (count ^String (:p/email e))))))]
    {:metrics [["entity+3 attrs/sec" rate :hi]]
     :checks-m {"checksum" sum}}))

(defn pull [b sz]
  (let [{:keys [ctx eids]} (dataset! b sz)
        db* (db ctx)
        ids (random-eids eids (:pulls sz) 43)
        [r1 s1] (timed-ops ids (fn [eid] (count (call b :pull db* '[*] eid))))
        nested '[:p/email :p/age {:p/status [:db/ident]} {:p/tags [:db/ident]}]
        [r2 s2] (timed-ops ids (fn [eid]
                                 (let [r (call b :pull db* nested eid)]
                                   (+ (count r) (count (:p/tags r))))))]
    {:metrics [["pull [*] /sec" r1 :hi]
               ["pull nested /sec" r2 :hi]]
     :checks-m {"[*] key count" s1 "nested checksum" s2}}))

(defn lookup-ref [b sz]
  (let [{:keys [ctx]} (dataset! b sz)
        db* (db ctx)
        n (:lookup-refs sz)
        r (Random. 44)
        ents (:entities sz)
        refs (vec (repeatedly n #(vector :p/email (data/email (.nextInt r (int ents))))))
        eid-of (fn [ref] (call b :entity db* ref))
        warm (min n 5000)]
    ;; resolve through d/entid (lookup-ref resolution) and through d/entity
    (let [entid-fn (if (= :datomic (:kind b)) d/entid m/entid)
          run (fn [] (reduce (fn [^long acc ref] (if (entid-fn db* ref) (inc acc) acc)) 0 refs))]
      (dotimes [_ 1] (run))
      (u/gc!)
      (let [[cnt ns] (timed-ns (run))
            ;; entity via lookup ref
            sub (subvec refs 0 (min n 20000))
            _ (doseq [ref (subvec refs 0 warm)] (eid-of ref))
            [cnt2 ns2] (timed-ns (reduce (fn [^long acc ref] (if (:p/age (eid-of ref)) (inc acc) acc)) 0 sub))]
        {:metrics [["entid [:p/email x] /sec" (per-sec n ns) :hi]
                   ["entity [:p/email x] /sec" (per-sec (count sub) ns2) :hi]]
         :checks-m {"resolved" cnt "entity resolved" cnt2}}))))

;; --- queries

(def ^:private ancestor-rules
  '[[(ancestor ?a ?d) [?d :n/parent ?a]]
    [(ancestor ?a ?d) [?d :n/parent ?m] (ancestor ?a ?m)]])

(defn- email-set [b db* res]
  (set (map (fn [row] (:p/email (call b :entity db* (if (coll? row) (first row) row)))) res)))

(defn- load-tree! [b ctx n]
  (when-not (:tree-loaded (get @datasets (:name b)))
    (let [nodes (into [{:db/id "n0" :n/id 0}]
                      (map (fn [i] {:db/id (str "n" i) :n/id (long i) :n/parent (str "n" (quot (dec i) 3))}))
                      (range 1 n))]
      (tx! ctx nodes)
      (swap! datasets assoc-in [(:name b) :tree-loaded] true))))

(def ^:private only
  "MNEMONIC_BENCH_ONLY=a,c,sh restricts sub-benchmarks (query letters a-f; as-of-history
  keys qa sa sh qh qc). Meant for profiling a single piece."
  (some-> (System/getenv "MNEMONIC_BENCH_ONLY") (str/split #",") set))

(defn- on? [k] (or (nil? only) (contains? only k)))

(defn queries [b sz]
  (let [{:keys [ctx]} (dataset! b sz)
        _ (load-tree! b ctx (:tree sz))
        db* (db ctx)
        r (Random. 45)
        in-emails (vec (repeatedly (:in-values sz) #(data/email (.nextInt r (int (:entities sz))))))
        root (call b :entity db* [:n/id 0])
        root-id (:db/id root)
        qs [["a single pattern, indexed value (age=500)"
             '[:find ?e :in $ ?a :where [?e :p/age ?a]] [500]
             (fn [res] (email-set b db* res))]
            ["b 3-way join (age=42 -> tags -> ident)"
             '[:find ?e ?i :in $ ?a :where [?e :p/age ?a] [?e :p/tags ?t] [?t :db/ident ?i]] [42]
             (fn [res] (set (map (fn [[e i]] [(:p/email (call b :entity db* e)) i]) res)))]
            ["c join + predicate + range (age in [100,110), active)"
             '[:find ?n ?a :in $ ?lo ?hi
               :where [?e :p/age ?a] [(<= ?lo ?a)] [(< ?a ?hi)] [?e :p/status :status/active] [?e :p/name ?n]]
             [100 110] identity]
            ["d aggregate count/sum/max group-by status"
             '[:find ?i (count ?e) (sum ?a) (max ?a)
               :where [?e :p/status ?s] [?s :db/ident ?i] [?e :p/age ?a]] [] identity]
            ["e recursive rule ancestor (tree)"
             '[:find ?id :in $ % ?root :where (ancestor ?root ?d) [?d :n/id ?id]]
             [ancestor-rules root-id] identity]
            ["f :in collection binding (1000 emails)"
             '[:find ?e :in $ [?em ...] :where [?e :p/email ?em]] [in-emails]
             (fn [res] (email-set b db* res))]]
        out (volatile! {:metrics [] :checks-m {}})]
    (doseq [[label q args norm] qs :when (on? (subs label 0 1))]
      (let [f #(apply call b :q q db* args)
            ;; long queries need fewer warm iterations
            probe (second (timed-ns (f)))
            opts {:warm-ms (min 2000 (:query-ms sz)) :run-ms (:query-ms sz)
                  :min-iters (if (> probe 2e9) 2 5)}
            t (u/run-for f opts)
            key (subs label 0 1)]
        (log "   " label (format "%.2f ms" (:mean-ms t)))
        (vswap! out update :metrics conj [(str label " ms") (:mean-ms t) :lo])
        (vswap! out update-in [:checks-m] assoc (str "query " label " rows") (count (:result t)))
        (vswap! out update-in [:checks-m] assoc (str "query " label " result hash") (hash (set (norm (:result t)))))))
    @out))

;; --- as-of / history

(defn as-of-history [b sz]
  (let [ctx (be/new-db! b)
        n (:hist-entities sz)
        updates (:hist-updates sz)
        hb (:hist-batch sz)]
    (try
      (load-schema! ctx)
      (doseq [batch (partition-all 1000 (data/people 3 0 n))] (tx! ctx (vec batch)))
      (let [half (quot updates 2)
            update-tx (fn [from to]
                        (vec (for [j (range from to)]
                               {:p/email (data/email (mod (* (long j) 7919) n))
                                :p/age (+ 1000 (long j))})))
            run-updates (fn [from to]
                          (doseq [s (range from to hb)]
                            (tx! ctx (update-tx s (min to (+ s hb))))))
            [_ upd-ns] (timed-ns (run-updates 0 half))
            t-mid (call b :basis-t (db ctx))
            _ (run-updates half updates)
            now (db ctx)
            asof (call b :as-of now t-mid)
            hist (call b :history now)
            q-asof '[:find ?em ?a :where [?e :p/email ?em] [?e :p/age ?a]]
            q-hist '[:find ?a ?added :in $ ?em :where [?e :p/email ?em] [?e :p/age ?a _ ?added]]
            em (data/email (mod (* 17 7919) n))
            opts {:warm-ms 1500 :run-ms (max 2500 (:query-ms sz)) :min-iters 5}
            scan3 (fn [db*] (+ (scan-count (call b :datoms db* :aevt :p/age))
                               (scan-count (call b :datoms db* :aevt :p/name))
                               (scan-count (call b :datoms db* :aevt :p/email))))
            run (fn [k f] (when (on? k) (u/run-for f opts)))
            qa (run "qa" #(call b :q q-asof asof))
            sa (run "sa" #(scan3 asof))
            sh (run "sh" #(scan-count (call b :datoms hist :aevt :p/age)))
            qh (run "qh" #(call b :q q-hist hist em))
            qc (run "qc" #(call b :q q-asof now))
            rate (fn [t] (when t (/ (:result t) (/ (:mean-ms t) 1000.0))))
            mean (fn [t] (:mean-ms t))
            res {:metrics [[(str "updates/sec (" hb " per tx)") (per-sec half upd-ns) :hi]
                           ["as-of query (email,age join) ms" (mean qa) :lo]
                           ["current-db same query ms" (mean qc) :lo]
                           ["as-of 3-attr AEVT scan datoms/sec" (rate sa) :hi]
                           ["history :p/age AEVT scan datoms/sec" (rate sh) :hi]
                           ["history scan ms" (mean sh) :lo]
                           ["history query (one entity) ms" (mean qh) :lo]]
                 :checks-m (cond-> {}
                             qa (assoc "as-of query rows" (count (:result qa))
                                       "as-of query hash" (hash (set (:result qa))))
                             sa (assoc "as-of scan datoms" (:result sa))
                             sh (assoc "history scan datoms" (:result sh))
                             qh (assoc "history query rows" (count (:result qh))
                                       "history query hash" (hash (set (:result qh)))))}]
        (if only (update res :metrics #(filterv second %)) res))
      (finally (be/drop-db! ctx)))))

;; --- open / reopen

(defn open-reopen [b sz]
  (if-not (= "mnemonic-file" (:name b))
    {:metrics [["connect ms (mean of 5)" nil :lo] ["first db ms" nil :lo]
               ["first entity+query ms" nil :lo] ["db dir size MB" nil :lo]]}
    (let [{:keys [ctx eids]} (dataset! b sz)
          uri (:uri ctx)
          dir (subs uri (count "mnemonic:file://"))
          size-mb (/ (reduce + (map #(.length ^java.io.File %) (filter #(.isFile ^java.io.File %) (file-seq (io/file dir))))) 1048576.0)
          rounds 5
          eid (first eids)
          one (fn []
                (call b :release (:conn ctx))
                (u/gc!)
                (let [[conn t1] (timed-ns (call b :connect uri))
                      [db* t2] (timed-ns (call b :db conn))
                      [r t3] (timed-ns [(:p/age (call b :entity db* eid))
                                        (count (call b :q '[:find ?e :in $ ?a :where [?e :p/age ?a]] db* 500))])]
                  (swap! datasets assoc-in [(:name b) :ctx :conn] conn)
                  [(ms t1) (ms t2) (ms t3) r]))
          rs (vec (repeatedly rounds one))
          mean (fn [i] (/ (reduce + (map #(nth % i) rs)) rounds))]
      {:metrics [["connect ms (mean of 5)" (mean 0) :lo]
                 ["first db ms" (mean 1) :lo]
                 ["first entity+query ms" (mean 2) :lo]
                 ["db dir size MB" size-mb :lo]]
       :checks-m {}})))

;; --- cold point lookups (flushed SSTs, empty block cache) and multi-threaded readers

(defn- flush-store!
  "Flushes every column family to SSTs (plain RocksDB calls: works on any version)."
  [store]
  (with-open [fo (doto (FlushOptions.) (.setWaitForFlush true))]
    (.flush ^RocksDB (:db store) fo ^java.util.List (vec (vals (:cfs store))))))

(defn- compact-store! [store]
  (doseq [h (vals (:cfs store))] (.compactRange ^RocksDB (:db store) ^org.rocksdb.ColumnFamilyHandle h)))

(defn- entity-sum
  "Reads three attributes of entity `eid`; the sum keeps the work observable."
  ^long [b db* ^long eid]
  (let [e (call b :entity db* eid)]
    (+ (long (:p/age e)) (count ^String (:p/name e)) (count ^String (:p/email e)))))

(defn- pass-ns
  "Nanoseconds to run (f x) over every element of `xs` (a long array)."
  ^long [^longs xs f]
  (let [t0 (System/nanoTime)]
    (dotimes [i (alength xs)] (f (aget xs i)))
    (- (System/nanoTime) t0)))

(defn cold-lookup
  "mnemonic-file only. The loaded dataset is flushed to SST files (and compacted when
  MNEMONIC_BENCH_COMPACT is set; otherwise the L0 files stay as flushed, where bloom
  filters matter most), the connection is reopened (empty block cache), then random
  point lookups run: entities and lookup refs that hit and that miss, first pass cold,
  second pass warm."
  [b sz]
  (let [labels ["cold entity+3 attrs/sec" "warm entity+3 attrs/sec"
                "cold entid hit/sec" "warm entid hit/sec" "entid miss/sec (distinct)"
                "entid miss/sec (same shape)"]]
    (if-not (= "mnemonic-file" (:name b))
      {:metrics (mapv (fn [l] [l nil :hi]) labels)}
      (let [{:keys [ctx eids]} (dataset! b sz)
            uri (:uri ctx)]
        (flush-store! (:store (:conn ctx)))
        (when (System/getenv "MNEMONIC_BENCH_COMPACT") (compact-store! (:store (:conn ctx))))
        (call b :release (:conn ctx))
        (u/gc!)
        (let [conn (call b :connect uri)
              _ (swap! datasets assoc-in [(:name b) :ctx :conn] conn)
              db* (call b :db conn)
              n (:lookups sz)
              ids (random-eids eids n 51)
              ent (fn [eid] (entity-sum b db* eid))
              ents (:entities sz)
              r (Random. 52)
              hit-ids (long-array (repeatedly n #(.nextInt r (int ents))))
              miss-ids (long-array (repeatedly n #(+ ents 10 (.nextInt r 1000000))))
              hit (fn [i] (m/entid db* [:p/email (data/email i)]))
              miss (fn [i] (m/entid db* [:p/email (str "nobody-" i "@nowhere.example")]))
              miss2 (fn [i] (m/entid db* [:p/email (data/email i)]))
              c1 (pass-ns ids ent)
              c2 (pass-ns ids ent)
              h1 (pass-ns hit-ids hit)
              h2 (pass-ns hit-ids hit)
              m1 (pass-ns miss-ids miss)
              m2 (pass-ns miss-ids miss2)]
          {:metrics [[(first labels) (per-sec n c1) :hi]
                     [(second labels) (per-sec n c2) :hi]
                     [(nth labels 2) (per-sec n h1) :hi]
                     [(nth labels 3) (per-sec n h2) :hi]
                     [(nth labels 4) (per-sec n m1) :hi]
                     [(nth labels 5) (per-sec n m2) :hi]]})))))

(defn mt-lookup
  "Entity lookups and lookup-ref resolution from 1, 4 and 8 reader threads on one db
  value (override with MNEMONIC_BENCH_THREADS=1,2,4,...). Total ops/sec."
  [b sz]
  (let [{:keys [ctx eids]} (dataset! b sz)
        db* (db ctx)
        threads (mapv #(Long/parseLong %) (str/split (u/env "MNEMONIC_BENCH_THREADS" "1,4,8") #","))
        n (:lookups sz)
        ^longs ids (random-eids eids n 53)
        entid-fn (if (= :datomic (:kind b)) d/entid m/entid)
        r (Random. 54)
        refs (vec (repeatedly n #(vector :p/email (data/email (.nextInt r (int (:entities sz)))))))
        run (fn [t work]
              (let [chunk (quot n t)
                    sums (long-array t)
                    ts (mapv (fn [k]
                               (Thread. ^Runnable
                                        (fn [] (aset sums k (long (work (* k chunk) (* (inc k) chunk)))))))
                             (range t))
                    t0 (System/nanoTime)]
                (doseq [^Thread th ts] (.start th))
                (doseq [^Thread th ts] (.join th))
                [(per-sec (* chunk t) (- (System/nanoTime) t0)) (reduce + sums)]))
        ent-work (fn [^long from ^long to]
                   (loop [i from acc 0] (if (< i to) (recur (inc i) (+ acc (entity-sum b db* (aget ids i)))) acc)))
        ref-work (fn [^long from ^long to]
                   (loop [i from acc 0] (if (< i to) (recur (inc i) (if (entid-fn db* (nth refs i)) (inc acc) acc)) acc)))]
    (ent-work 0 (min n 20000))
    (ref-work 0 (min n 20000))
    {:metrics (vec (concat
                    (for [t threads] [(str "entity+3 attrs/sec, " t " threads") (first (run t ent-work)) :hi])
                    (for [t threads] [(str "entid [:p/email x]/sec, " t " threads") (first (run t ref-work)) :hi])))}))

(def workloads
  [["bulk-load" bulk-load]
   ["small-tx" small-tx]
   ["durable-tx" durable-tx]
   ["datoms-scan" datoms-scan]
   ["entity-lookup" entity-lookup]
   ["pull" pull]
   ["lookup-ref" lookup-ref]
   ["queries" queries]
   ["as-of-history" as-of-history]
   ["open-reopen" open-reopen]
   ["cold-lookup" cold-lookup]
   ["mt-lookup" mt-lookup]])

;; ------------------------------------------------------------------ reporting

(defn- slowdown
  "Factor by which mnemonic is slower than datomic (>1 = slower)."
  [dir mv dv]
  (when (and mv dv (pos? (double mv)) (pos? (double dv)))
    (if (= dir :hi) (/ (double dv) (double mv)) (/ (double mv) (double dv)))))

(defn- fx [x] (if x (format "%.2fx" x) "n/a"))

(defn- md-table [header rows]
  (str "| " (str/join " | " header) " |\n"
       "|" (str/join "|" (map (fn [i] (if (or (zero? i) (= (count header) 3)) "---" "---:")) (range (count header)))) "|\n"
       (str/join "\n" (map #(str "| " (str/join " | " %) " |") rows)) "\n"))

(defn- check-status [vals]
  (let [d (get vals "datomic-mem" ::none)
        ms (remove nil? (map vals ["mnemonic-mem" "mnemonic-file"]))]
    (cond (= d ::none) "n/a (no datomic run)"
          (every? #(= % d) ms) "OK"
          :else (str "MISMATCH " (pr-str vals)))))

(defn report
  [selected-backends]
  (let [names (mapv :name selected-backends)
        by-w (group-by :workload @results)
        sb (with-out-str
             (doseq [[w _] workloads :when (contains? by-w w)]
               (println (str "\n### " w "\n"))
               (print (md-table
                       (concat ["metric"] names ["mem vs datomic" "file vs datomic"])
                       (for [{:keys [metric dir values]} (get by-w w)]
                         (concat [metric]
                                 (map #(u/fmt-num (get values %)) names)
                                 [(fx (slowdown dir (get values "mnemonic-mem") (get values "datomic-mem")))
                                  (fx (slowdown dir (get values "mnemonic-file") (get values "datomic-mem")))])))))
             (println "\nSlowdown columns: factor by which mnemonic is slower than Datomic mem (>1 = slower, <1 = faster).")
             (println "\n### Result checks (mnemonic vs Datomic)\n")
             (print (md-table ["workload" "check" "status"]
                              (for [{:keys [workload check values]}
                                    (sort-by (juxt #(.indexOf ^java.util.List (mapv first workloads) (:workload %)) :check) @checks)]
                                [workload check (check-status values)]))))]
    sb))

(defn- header [workloads-run]
  (let [sha (or (u/sh "git" "rev-parse" "--short" "HEAD") "unknown")
        dirty (not (str/blank? (u/sh "git" "status" "--porcelain" "--untracked-files=no")))]
    {:sha (str sha (when dirty "-dirty"))
     :text (str "## Benchmark run " (java.time.LocalDateTime/now) "\n\n"
                "- git: " sha (when dirty " (dirty tree)") "\n"
                "- JVM: " (System/getProperty "java.vm.name") " " (System/getProperty "java.version")
                " (" (.availableProcessors (Runtime/getRuntime)) " cpus, max heap "
                (quot (.maxMemory (Runtime/getRuntime)) 1048576) " MB)\n"
                "- scale: " u/scale "; sizes: " (pr-str (sizes)) "\n"
                "- workloads: " (str/join ", " workloads-run) "\n")}))

(defn -main [& args]
  (let [wanted (if (seq args) (set args) (set (map first workloads)))
        unknown (remove (set (map first workloads)) wanted)
        _ (when (seq unknown)
            (println "Unknown workloads:" unknown "known:" (map first workloads))
            (System/exit 2))
        bset (when-let [e (System/getenv "MNEMONIC_BENCH_BACKENDS")] (set (str/split e #",")))
        backends (filterv #(or (nil? bset) (bset (:name %))) (be/all-backends))
        sz (sizes)
        run (filterv #(wanted (first %)) workloads)
        hdr (header (map first run))]
    (log "sizes" (pr-str sz))
    (try
      (doseq [[w f] run]
        (log "== workload" w)
        (doseq [b backends]
          (log "  --" (:name b))
          (let [r (try (f b sz)
                       (catch Throwable t
                         (log "  FAILED" (:name b) w (ex-message t))
                         (.printStackTrace t)
                         {:metrics [["FAILED" nil :lo]]}))]
            (record! w b r))))
      (let [text (report backends)
            date (str (java.time.LocalDate/now))
            file (io/file "bench" "results" (str date "-" (:sha hdr) ".md"))]
        (println (str "# MnemonicDB benchmarks\n\n" (:text hdr)) text)
        (io/make-parents file)
        (spit file (str (:text hdr) text "\n") :append true)
        (log "appended results to" (str file)))
      (finally
        (doseq [{:keys [ctx]} (vals @datasets)] (be/drop-db! ctx))
        (be/delete-tmp!)
        (shutdown-agents)
        (d/shutdown true)))
    (System/exit 0)))
