(ns mnemonic.cache-bench
  "Read workloads with controlled hit rates for the heap index segment cache
  (mnemonic.segcache), plus a larger-than-cache workload. One backend per JVM:

    clojure -M:bench -m mnemonic.cache-bench mem|file|datomic        cold / warm / hot rows
    clojure -M:bench -m mnemonic.cache-bench big-file|big-mem        dataset ~4x the cache

  Cache on/off and budget: -Dmnemonic.index-cache-mb=N (0 = off; default 256). File
  databases live under CB_DIR (default target/cache-bench: a real filesystem, not tmpfs).
  CB_N people (default 100000; big: CB_BIG_N, default 150000)."
  (:require [clojure.java.io :as io]
            [datomic.api :as d]
            [mnemonic.api :as m]
            [mnemonic.bench.data :as data]
            [mnemonic.segcache :as sc])
  (:import (java.util Random)))

(set! *warn-on-reflection* true)

(defn- env-long [k default] (Long/parseLong (or (System/getenv k) (str default))))

(defn- gc! [] (dotimes [_ 3] (System/gc)) (Thread/sleep 200))

(defn- api [kind]
  (if (= kind :datomic)
    {:create d/create-database :connect d/connect :transact d/transact :db d/db
     :entity d/entity :pull d/pull :entid d/entid :as-of d/as-of :basis-t d/basis-t :datoms d/datoms}
    {:create m/create-database :connect m/connect :transact m/transact :db m/db
     :entity m/entity :pull m/pull :entid m/entid :as-of m/as-of :basis-t m/basis-t :datoms m/datoms}))

(defn- rate [n f]
  (let [t0 (System/nanoTime)
        r (f)
        dt (- (System/nanoTime) t0)]
    [(/ (double n) (/ dt 1e9)) r]))

(defn- ids-uniform ^longs [^longs eids n seed]
  (let [r (Random. seed) c (alength eids)]
    (long-array (repeatedly n #(aget eids (.nextInt r c))))))

(defn- ids-hot
  "`frac-hot` of the ops on the first `hot` eids (random among them), the rest uniform."
  ^longs [^longs eids n seed hot frac-hot]
  (let [r (Random. seed) c (alength eids)]
    (long-array (repeatedly n #(aget eids (if (< (.nextDouble r) frac-hot)
                                            (.nextInt r (int hot))
                                            (.nextInt r c)))))))

(defn- uri-for [kind tag]
  (case kind
    :mem (str "mnemonic:mem://" tag "-" (System/nanoTime))
    :file (let [dir (io/file (or (System/getenv "CB_DIR") "target/cache-bench"))]
            (.mkdirs dir)
            (str "mnemonic:file://" (.getCanonicalPath (io/file dir (str tag "-" (System/nanoTime))))
                 "?sync=false"))
    :datomic (str "datomic:mem://" tag "-" (System/nanoTime))))

(defn- delete-tree! [uri]
  (when-let [p (second (re-find #"^mnemonic:file://([^?]+)" uri))]
    (doseq [f (reverse (file-seq (io/file p)))] (.delete ^java.io.File f))))

(defn- harness [kind tag n]
  (let [A (api kind)
        uri (uri-for kind tag)
        _ ((:create A) uri)
        conn ((:connect A) uri)
        tx! (fn [data] @((:transact A) conn data))
        out (atom [])
        cache (fn [] (when (not= kind :datomic) (:segcache (:store conn))))]
    (tx! data/schema)
    (tx! data/enum-idents)
    (doseq [from (range 0 n 1000)]
      (tx! (data/people 1 from (min n (+ from 1000)))))
    {:A A :uri uri :conn conn :tx! tx! :out out
     :clear! #(when-let [c (cache)] (sc/clear! c) (sc/reset-stats! c))
     :reset! #(when-let [c (cache)] (sc/reset-stats! c))
     :stats #(when-let [c (cache)] (sc/stats c))
     :hitrate #(when-let [c (cache)]
                 (let [{:keys [hits misses stale]} (sc/stats c)
                       tot (+ hits misses stale)]
                   (when (pos? tot) (format "hit %.0f%%" (* 100.0 (/ hits (double tot)))))))
     :rec! (fn [label v & [extra]]
             (swap! out conj [label v extra])
             (binding [*out* *err*]
               (println (format "%-52s %,14.0f %s" label (double v) (or extra "")))))}))

(defn- finish! [{:keys [out uri]}]
  (println "RESULT" (pr-str (mapv (fn [[l v _]] [l (Math/round (double v))]) @out)))
  (delete-tree! uri)
  (shutdown-agents)
  (System/exit 0))

(defn- readers [A]
  (let [ent3 (fn [db* eid] (let [e ((:entity A) db* eid)]
                             (+ (long (:p/age e)) (count ^String (:p/name e)) (count ^String (:p/email e)))))]
    {:ent3 ent3
     :run-ent (fn [db* ^longs ids]
                (loop [i 0 acc 0] (if (< i (alength ids)) (recur (inc i) (+ acc (long (ent3 db* (aget ids i))))) acc)))
     :run-pull (fn [db* pat ^longs ids]
                 (loop [i 0 acc 0] (if (< i (alength ids)) (recur (inc i) (+ acc (count ((:pull A) db* pat (aget ids i))))) acc)))
     :run-entid (fn [db* refs] (reduce (fn [^long acc ref] (if ((:entid A) db* ref) (inc acc) acc)) 0 refs))}))

(def ^:private nested '[:p/email :p/age {:p/status [:db/ident]} {:p/tags [:db/ident]}])

(defn- standard [kind]
  (let [N (env-long "CB_N" 100000)
        {:keys [A conn tx! clear! hitrate rec!] :as h} (harness kind "cb" N)
        reset-stats! (:reset! h)
        {:keys [ent3 run-ent run-pull run-entid]} (readers A)
        db* ((:db A) conn)
        eids (long-array (map :e ((:datoms A) db* :aevt :p/email)))
        refs (let [r (Random. 44)] (vec (repeatedly N #(vector :p/email (data/email (.nextInt r (int N)))))))
        u1 (ids-uniform eids N 42)
        hot (ids-hot eids (* 2 N) 7 (quot N 100) 0.9)]
    ;; JIT warmup on a separate id stream, then cleared
    (run-ent db* (ids-uniform eids 50000 1))
    (run-pull db* nested (ids-uniform eids 10000 2))
    (run-entid db* (subvec refs 0 20000))
    (clear!) (gc!)
    (let [ids (ids-uniform eids N 77)
          one (fn [] (loop [i 0 acc 0] (if (< i (alength ids)) (recur (inc i) (+ acc (long (:p/age ((:entity A) db* (aget ids i)))))) acc)))
          [r _] (rate N one)]
      (rec! "entity 1 attr uniform, cold /s" r (hitrate)))
    (clear!) (gc!)
    (let [[r _] (rate N #(run-ent db* u1))] (rec! "entity+3 uniform, pass 1 (cold) /s" r (hitrate)))
    (reset-stats!)
    (let [[r _] (rate N #(run-ent db* u1))] (rec! "entity+3 uniform, pass 2 /s" r (hitrate)))
    (reset-stats!)
    (let [[r _] (rate N #(run-ent db* u1))] (rec! "entity+3 uniform, pass 3 (warm) /s" r (hitrate)))
    (clear!) (gc!)
    (let [[r _] (rate (alength hot) #(run-ent db* hot))] (rec! "entity+3 hot 90/1 /s" r (hitrate)))
    (clear!) (gc!)
    (let [ids (ids-uniform eids 20000 43)]
      (let [[r _] (rate 20000 #(run-pull db* nested ids))] (rec! "pull nested pass 1 /s" r (hitrate)))
      (run-pull db* nested ids)
      (reset-stats!)
      (let [[r _] (rate 20000 #(run-pull db* nested ids))] (rec! "pull nested pass 3 /s" r (hitrate)))
      (clear!) (gc!)
      (let [[r _] (rate 20000 #(run-pull db* '[*] ids))] (rec! "pull [*] pass 1 /s" r (hitrate)))
      (run-pull db* '[*] ids)
      (reset-stats!)
      (let [[r _] (rate 20000 #(run-pull db* '[*] ids))] (rec! "pull [*] pass 3 /s" r (hitrate))))
    (clear!) (gc!)
    (let [[r _] (rate N #(run-entid db* refs))] (rec! "entid lookup-ref pass 1 /s" r (hitrate)))
    (run-entid db* refs)
    (reset-stats!)
    (let [[r _] (rate N #(run-entid db* refs))] (rec! "entid lookup-ref pass 3 /s" r (hitrate)))
    (let [miss (vec (for [i (range N)] [:p/email (str "nobody" i)]))]
      (run-entid db* miss) (run-entid db* miss)
      (reset-stats!)
      (let [[r _] (rate N #(run-entid db* miss))] (rec! "entid miss, warm /s" r (hitrate))))
    ;; memory: every entity and email cached
    (when ((:stats h))
      (clear!) (gc!)
      (let [rt (Runtime/getRuntime)
            used0 (- (.totalMemory rt) (.freeMemory rt))]
        (dotimes [_ 2] (run-ent db* eids) (run-entid db* refs))
        (gc!)
        (let [used1 (- (.totalMemory rt) (.freeMemory rt))
              nd (+ (reduce (fn [n _] (inc n)) 0 ((:datoms A) db* :eavt)) (count (distinct refs)))
              st ((:stats h))]
          (rec! "heap MB with all entities+emails cached" (/ (- used1 used0) 1048576.0)
                (str "estimate " (quot (long (:bytes st)) 1048576) " MB"))
          (rec! "  heap bytes per cached datom" (/ (double (- used1 used0)) nd)))))
    ;; as-of db: entity reads (current + history segments)
    (let [t ((:basis-t A) ((:db A) conn))
          _ (dotimes [i 200] (tx! [[:db/add (aget eids i) :p/age (long i)]]))
          adb ((:as-of A) ((:db A) conn) t)
          ids (ids-uniform eids 50000 9)]
      (run-ent adb ids) (run-ent adb ids)
      (reset-stats!)
      (let [[r _] (rate 50000 #(run-ent adb ids))] (rec! "as-of entity+3, warm /s" r (hitrate))))
    ;; app-like: a fresh db per 'request' of 10 reads (hot 90/1) while a writer updates
    (clear!) (gc!)
    (let [stop (atom false)
          writes (atom 0)
          w (future
              (let [r (Random. 5)]
                (while (not @stop)
                  (tx! [[:db/add (aget eids (.nextInt r (alength eids))) :p/age (long (.nextInt r 1000))]])
                  (swap! writes inc)
                  (Thread/sleep 0 200000))))
          ids (ids-hot eids 400000 8 (quot N 100) 0.9)
          t0 (System/nanoTime)
          _ (loop [i 0]
              (when (< i (alength ids))
                (let [db* ((:db A) conn)]
                  (dotimes [j 10] (ent3 db* (aget ids (+ i j)))))
                (recur (+ i 10))))
          dt (- (System/nanoTime) t0)]
      (reset! stop true) @w
      (rec! "entity+3 hot 90/1, concurrent writer /s" (/ (alength ids) (/ dt 1e9))
            (str (hitrate) ", writer tx/s " (format "%.0f" (/ @writes (/ dt 1e9))))))
    (let [n 3000
          _ (dotimes [i 2000] (tx! (data/small-tx (+ 6000000 i))))
          [r _] (rate n #(dotimes [i n] (tx! (data/small-tx (+ 5000000 i)))))]
      (rec! "small-tx tx/s (cache populated)" r))
    (finish! h)))

(defn- big
  "Dataset of CB_BIG_N people (~4x a 32 MB cache budget at the default size), uniform and
  skewed random reads, and a full attribute scan larger than the scan budget."
  [kind]
  (let [N (env-long "CB_BIG_N" 150000)
        {:keys [A conn hitrate rec!] :as h} (harness kind "cbig" N)
        reset-stats! (:reset! h)
        {:keys [run-ent run-pull]} (readers A)
        db* ((:db A) conn)
        eids (long-array (map :e ((:datoms A) db* :aevt :p/email)))
        n 200000
        uni (ids-uniform eids n 61)
        skew (ids-hot eids n 62 (quot N 10) 0.9)
        scan #(reduce (fn [^long k _] (inc k)) 0 ((:datoms A) db* :aevt :p/age))]
    (run-ent db* (ids-uniform eids 50000 60))
    (gc!)
    (let [[r _] (rate n #(run-ent db* uni))] (rec! "big: entity+3 uniform, pass 1 /s" r (hitrate)))
    (reset-stats!)
    (let [[r _] (rate n #(run-ent db* uni))] (rec! "big: entity+3 uniform, pass 2 /s" r (hitrate)))
    (reset-stats!)
    (run-ent db* skew)
    (reset-stats!)
    (let [[r _] (rate n #(run-ent db* skew))] (rec! "big: entity+3 skewed 90/10 /s" r (hitrate)))
    (scan) (scan)
    (reset-stats!)
    (let [[r c] (rate 1 scan)] (rec! "big: AEVT :p/age full scan datoms/s" (* r (long c)) (hitrate)))
    (reset-stats!)
    (let [[r _] (rate n #(run-ent db* skew))] (rec! "big: entity+3 skewed 90/10 after scans /s" r (hitrate)))
    (reset-stats!)
    (let [ids (ids-uniform eids 50000 63)]
      (let [[r _] (rate 50000 #(run-pull db* '[*] ids))] (rec! "big: pull [*] uniform /s" r (hitrate))))
    (when-let [st ((:stats h))]
      (rec! "big: cache MB (estimate)" (/ (double (:bytes st)) 1048576.0)
            (str "budget " (quot (long (:budget-bytes st)) 1048576) " MB, evictions " (:evictions st))))
    (finish! h)))

(defn -main [& [k]]
  (case (or k "mem")
    "mem" (standard :mem)
    "file" (standard :file)
    "datomic" (standard :datomic)
    "big-file" (big :file)
    "big-mem" (big :mem)))
