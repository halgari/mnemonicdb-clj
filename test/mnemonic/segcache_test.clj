(ns mnemonic.segcache-test
  "The index segment cache must never serve anything a fresh RocksDB read of the same
  snapshot would not: while a writer hammers a small key space, readers compare every
  cached read (raw current and history segments of all four indexes, as-of/history
  views, entities and pulls) with the same read on the same db value with the cache
  bypassed, on current and older db values. Run under several cache configurations
  that force the big-granule paths, stamp collisions, constant eviction and immediate
  admission. Plus lifecycle and configuration checks."
  (:require [clojure.java.io :as io]
            [clojure.walk :as walk]
            [mnemonic.rocks :as rocks]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [mnemonic.api :as m]
            [mnemonic.codec :as codec]
            [mnemonic.conn :as conn]
            [mnemonic.index :as index]
            [mnemonic.segcache :as sc])
  (:import (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)
           (java.util Random)
           (java.util.concurrent CopyOnWriteArrayList)
           (mnemonic.index ICursor)))

(use-fixtures :each (fn [f] (try (f) (finally (conn/reset-registry!)))))

(defn- uncached
  "Runs `f` with the segment cache bypassed on this thread (same db, same snapshot)."
  [f]
  (.set sc/bypass true)
  (try (f) (finally (.remove sc/bypass))))

(defn- drain
  "[key-bytes-vec datom-vec retract-tx] of every entry of `c`; closes it."
  [^ICursor c]
  (try
    (loop [acc []]
      (if (.valid c)
        (let [d (.datom c)
              x [(vec (.key c)) [(:e d) (:a d) (:v d) (:tx d) (:added d)] (.retractTx c)]]
          (.advance c)
          (recur (conj acc x)))
        acc))
    (finally (.close c))))

(defn- raw-read [db hist? idx ^bytes prefix]
  (let [b (index/-raw db)]
    (drain (if hist? (index/-seek-history b idx prefix) (index/-view b idx prefix)))))

(def ^:private schema
  [{:db/ident :k/id :db/valueType :db.type/long :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/ident :k/v :db/valueType :db.type/long :db/cardinality :db.cardinality/one :db/index true}
   {:db/ident :k/tags :db/valueType :db.type/long :db/cardinality :db.cardinality/many}
   {:db/ident :k/ref :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/ident :k/refs :db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   {:db/ident :k/name :db/valueType :db.type/string :db/cardinality :db.cardinality/one :db/unique :db.unique/value}])

(def ^:private n-ents 3000)

;; these tests exercise the cache: on even when the suite runs with it off by default
(def ^:private on "?index-cache-mb=256")

(defn- with-props
  "Runs `f` with system properties `props` set (restored after)."
  [props f]
  (let [old (into {} (map (fn [[k _]] [k (System/getProperty k)])) props)]
    (try
      (doseq [[k v] props] (System/setProperty k v))
      (f)
      (finally
        (doseq [[k v] old] (if v (System/setProperty k v) (System/clearProperty k)))))))

(defn- random-tx [^Random r]
  (let [i (if (.nextBoolean r) (.nextInt r 40) (.nextInt r n-ents))
        e [:k/id i]]
    (case (.nextInt r 9)
      0 [[:db/add e :k/v (long (.nextInt r 100))]]
      1 [[:db/add e :k/tags (long (.nextInt r 10))]]
      2 [[:db/retract e :k/tags (long (.nextInt r 10))]]
      3 [[:db/add e :k/ref [:k/id (.nextInt r 40)]]]
      4 [[:db/add e :k/name (str "n" (.nextInt r 60))]]
      5 [[:db/retractEntity e]]
      6 [{:k/id i :k/v 1 :k/refs [[:k/id (.nextInt r 40)]]}]
      7 (vec (for [_ (range 5)] [:db/add [:k/id (.nextInt r 40)] :k/v (long (.nextInt r 100))]))
      8 [{:k/id (+ n-ents (.nextInt r 2000)) :k/v (long (.nextInt r 100)) :k/tags 3}])))

(defn- probes
  "[label thunk] reads to compare; each thunk reads `db`."
  [db ^Random r]
  (let [sch (index/-schema db)
        aid #(get (:idents sch) %)
        i (if (.nextBoolean r) (.nextInt r 40) (.nextInt r n-ents))
        e (m/entid db [:k/id i])
        hot (m/entid db [:k/id (.nextInt r 40)])
        raw (fn [hist? idx comps] [[hist? idx comps] #(raw-read db hist? idx (codec/encode-prefix idx comps))])
        t (m/basis-t db)
        old-t (max 1 (- t (.nextInt r 50)))]
    (cond-> [(raw false :aevt [(aid :k/v)])
             (raw false :aevt [(aid :k/tags)])
             (raw false :aevt [(aid :k/ref)])
             (raw true :aevt [(aid :k/v)])
             (raw true :aevt [(aid :k/tags)])
             (raw false :avet [(aid :k/name) (str "n" (.nextInt r 60))])
             (raw false :avet [(aid :k/id) (long i)])
             (raw false :avet [(aid :k/v) (long (.nextInt r 100))])
             (raw true :avet [(aid :k/v) (long (.nextInt r 100))])
             [:as-of-scan #(vec (m/datoms (m/as-of db old-t) :aevt :k/v))]
             [:history-scan #(vec (m/datoms (m/history db) :aevt :k/tags))]
             (let [nm (str "n" (.nextInt r 60))] [:lookup-ref #(m/entid db [:k/name nm])])]
      hot (into [(raw false :vaet [hot])
                 (raw false :vaet [hot (aid :k/ref)])
                 (raw true :vaet [hot])])
      e (into [(raw false :eavt [e])
               (raw false :eavt [e (aid :k/tags)])
               (raw false :eavt [e (aid :k/v)])
               (raw false :eavt [e (aid :k/tags) (long (.nextInt r 10))])
               (raw true :eavt [e])
               (raw false :aevt [(aid :k/v) e])
               (raw false :aevt [(aid :k/tags) e])
               (raw true :aevt [(aid :k/tags) e])
               (raw false :avet [(aid :k/id) (long i) e])
               [:entity #(into {} (m/touch (m/entity db e)))]
               [:pull #(m/pull db '[* {:k/ref [:k/id]} {:k/_ref [:k/id]}] e)]
               [:as-of-entity #(into {} (m/entity (m/as-of db old-t) e))]
               [:history-entity #(vec (m/datoms (m/history db) :eavt e))]]))))

(defn- stress!
  "Writer + 4 readers for `ms`; returns {:checks n :mismatches [...] :stats {...}}."
  [uri ms]
  (m/create-database uri)
  (let [c (m/connect uri)
        tx! #(deref (m/transact c %))]
    (tx! schema)
    (tx! (vec (for [i (range n-ents)] {:k/id i :k/v 0 :k/tags (long (mod i 7)) :k/name (str "x" i)})))
    (let [dbs (CopyOnWriteArrayList. [(m/db c)])
          stop (atom false)
          checks (atom 0)
          bad (atom [])
          readers (vec (for [th (range 4)]
                         (future
                           (let [r (Random. th)]
                             (while (not @stop)
                               (let [^objects a (.toArray dbs) n (alength a)
                                     db (aget a (if (.nextBoolean r) (dec n) (.nextInt r n)))]
                                 (doseq [[label f] (probes db r)]
                                   ;; three times: the first two may only be admitted/filled
                                   (let [want (uncached f)]
                                     (dotimes [_ 3]
                                       (let [got (f)]
                                         (swap! checks inc)
                                         (when (and (not= want got) (< (count @bad) 5))
                                           (swap! bad conj {:probe label :t (m/basis-t db)
                                                            :cached got :uncached want}))))))))))))
          r (Random. 99)
          t0 (System/currentTimeMillis)]
      (while (< (- (System/currentTimeMillis) t0) ms)
        (try (tx! (random-tx r)) (catch Exception _ nil))
        (.add dbs (m/db c))
        (when (> (.size dbs) 60) (.remove dbs 1))
        (Thread/sleep 1))
      (reset! stop true)
      (run! deref readers)
      (let [st (m/index-cache-stats c)]
        (m/release c)
        (m/delete-database uri)
        {:checks @checks :mismatches @bad :stats st}))))

(defn- tmp-dir ^String []
  (let [d (io/file "target" "segcache-test")]
    (.mkdirs d)
    (str (Files/createTempDirectory (.toPath d) "db" (make-array FileAttribute 0)))))

(deftest cache-agrees-with-rocksdb-under-concurrent-writes
  (doseq [[label props uri-opts kind]
          [["default" {} on :mem]
           ["point-cap 1, chunk-cap 2 (big granules)"
            {"mnemonic.index-cache.point-cap" "1" "mnemonic.index-cache.chunk-cap" "2"} on :mem]
           ["3-bit stamps (collisions)" {"mnemonic.index-cache.stripe-bits" "3"} on :mem]
           ["256 KB budget (eviction)" {"mnemonic.index-cache.doorkeeper" "false"
                                        "mnemonic.index-cache.budget-kb" "256"} on :mem]
           ["no doorkeeper" {"mnemonic.index-cache.doorkeeper" "false"} on :mem]
           ["file store" {} on :file]]]
    (testing label
      (let [uri (if (= kind :file)
                  (str "mnemonic:file://" (tmp-dir) uri-opts)
                  (str "mnemonic:mem://segcache-" (System/nanoTime) uri-opts))
            {:keys [checks mismatches stats]} (with-props props #(stress! uri 1800))]
        (is (empty? mismatches) (pr-str mismatches))
        (is (< 1000 checks))
        (is (pos? (:hits stats)) (pr-str stats))
        (is (pos? (:fills stats)) (pr-str stats))
        (is (pos? (:stale stats)) (str "invalidation exercised " (pr-str stats)))
        (when (= label "256 KB budget (eviction)")
          (is (pos? (:evictions stats)))
          (is (<= (:bytes stats) (* 2 262144))))))))

(deftest hit-serves-exact-state-test
  (let [uri (str "mnemonic:mem://segcache-exact-" (System/nanoTime) on)]
    (m/create-database uri)
    (let [c (m/connect uri)]
      @(m/transact c [{:db/ident :t/n :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])
      (let [e (get (:tempids @(m/transact c [{:db/id "e" :t/n 1}])) "e")
            d1 (m/db c)
            vs #(mapv :v (m/datoms % :eavt e :t/n))]
        (dotimes [_ 12] (is (= [1] (vs d1))))
        (is (pos? (:hits (m/index-cache-stats c))))
        @(m/transact c [[:db/add e :t/n 2]])
        (let [d2 (m/db c)]
          (dotimes [_ 3] (is (= [2] (vs d2)) "the write invalidated the segment"))
          (dotimes [_ 3] (is (= [1] (vs d1)) "the older db keeps its version"))
          (is (= [1] (vs (m/as-of d2 (m/basis-t d1)))) "as-of over cached current + history")
          (is (= [[1 true] [1 false] [2 true]]
                 (sort-by (juxt first (comp not second))
                          (map (juxt :v :added) (m/datoms (m/history d2) :eavt e :t/n))))))))))

(deftest lifecycle-test
  (testing "a hit on a released db throws like a cursor open"
    (let [uri (str "mnemonic:mem://segcache-rel-" (System/nanoTime) on)]
      (m/create-database uri)
      (let [c (m/connect uri)
            d (m/db c)
            read #(count (seq (m/datoms % :eavt :db/ident)))]
        (dotimes [_ 12] (read d))
        (m/release-db d)
        (is (thrown-with-msg? Exception #"released" (read d))))))
  (testing "a hit after the store closed throws connection-released, never touches native code"
    (let [uri (str "mnemonic:file://" (tmp-dir) on)]
      (m/create-database uri)
      (let [c (m/connect uri)
            d (m/db c)
            read #(vec (m/datoms % :eavt :db/ident))]
        (dotimes [_ 12] (read d))
        (is (pos? (:hits (m/index-cache-stats c))))
        (m/release c)
        (is (= :db.error/connection-released
               (try (read d) nil (catch Exception e (:db/error (ex-data e))))))
        (is (zero? (:bytes (m/index-cache-stats d))) "close-store cleared the cache")))))

(deftest configuration-test
  (testing "index-cache-mb URI option"
    (let [uri (str "mnemonic:mem://segcache-off-" (System/nanoTime) "?index-cache-mb=0")]
      (m/create-database uri)
      (is (nil? (m/index-cache-stats (m/connect uri)))))
    (let [uri (str "mnemonic:mem://segcache-8-" (System/nanoTime) "?index-cache-mb=8")]
      (m/create-database uri)
      (is (= (* 8 1048576) (:budget-bytes (m/index-cache-stats (m/connect uri)))))))
  (testing "bad values"
    (doseq [v ["-1" "abc" ""]]
      (is (= :db.error/invalid-db-uri
             (try (m/create-database (str "mnemonic:mem://segcache-bad?index-cache-mb=" v)) nil
                  (catch Exception e (:db/error (ex-data e)))))
          v))))

;; ------------------------------------------------------------------ mixed types, async
;; writer, views, irregular writes, close during reads

(def ^:private mixed-schema
  [{:db/ident :x/id :db/valueType :db.type/long :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/ident :x/s :db/valueType :db.type/string :db/cardinality :db.cardinality/one :db/index true}
   {:db/ident :x/k :db/valueType :db.type/keyword :db/cardinality :db.cardinality/many}
   {:db/ident :x/b :db/valueType :db.type/bytes :db/cardinality :db.cardinality/one}
   {:db/ident :x/i :db/valueType :db.type/instant :db/cardinality :db.cardinality/one :db/index true}
   {:db/ident :x/m :db/valueType :db.type/bigdec :db/cardinality :db.cardinality/one :db/index true}
   {:db/ident :x/d :db/valueType :db.type/double :db/cardinality :db.cardinality/one :db/index true}
   {:db/ident :x/u :db/valueType :db.type/uuid :db/cardinality :db.cardinality/one :db/index true}
   {:db/ident :x/t :db/valueType :db.type/tuple :db/tupleTypes [:db.type/keyword :db.type/string]
    :db/cardinality :db.cardinality/one :db/index true}
   {:db/ident :x/r :db/valueType :db.type/ref :db/cardinality :db.cardinality/many}])

(defn- mixed-value [^Random r a]
  (let [n (.nextInt r 8)]
    (case a
      :x/s (nth ["" "a" "a\u0000" "a\u0000b" "été" "😀" "ab" (str "s" n)] n)
      :x/k (nth [:a :b/a (keyword "" "e") :c/d :x (keyword "n" "") :b/b :z] n)
      :x/b (byte-array (take n (cycle [0 -1 127 -128 1])))
      :x/i (java.util.Date. (long (* n 1000)))
      :x/m (nth [1M 1.0M 1.00M -0M 0M 2.5M 1E+2M 100M] n)
      :x/d (nth [0.0 -0.0 Double/NaN 1.5 -1.5 Double/POSITIVE_INFINITY 1e300 2.0] n)
      :x/u (java.util.UUID. 0 n)
      :x/t [(nth [:a :b :c/d :e] (mod n 4)) (str "t" (mod n 3))])))

(def ^:private mixed-attrs [:x/s :x/k :x/b :x/i :x/m :x/d :x/u :x/t])

(defn- norm
  "Comparable form: byte arrays as vectors, Datoms as vectors, NaN-safe (printed)."
  [x]
  (pr-str (walk/prewalk (fn [v] (cond (bytes? v) (vec v)
                                      (instance? mnemonic.datom.Datom v) (vec (seq v))
                                      (instance? mnemonic.entity.Entity v) [:entity (:db/id v)]
                                      :else v))
                        x)))

(defn- attempt
  "(f), or [:error message] (an unindexed AVET read throws on both paths alike)."
  [f]
  (try (f) (catch clojure.lang.ExceptionInfo ex [:error (ex-message ex)])))

(defn- mixed-tx [^Random r]
  (let [i (.nextInt r 60) e [:x/id i] a (nth mixed-attrs (.nextInt r (count mixed-attrs)))]
    (case (.nextInt r 4)
      0 [[:db/add e a (mixed-value r a)]]
      1 [[:db/retract e a (mixed-value r a)]]
      2 [{:x/id i a (mixed-value r a) :x/r [[:x/id (.nextInt r 60)]]}]
      3 [[:db/retractEntity e]])))

(defn- mixed-probes [db ^Random r]
  (let [sch (index/-schema db)
        aid #(get (:idents sch) %)
        e (m/entid db [:x/id (.nextInt r 60)])
        a (nth mixed-attrs (.nextInt r (count mixed-attrs)))
        v (mixed-value r a)
        t (m/basis-t db)
        old-t (max 1 (- t (.nextInt r 30)))
        raw (fn [hist? idx comps] [[hist? idx comps] #(raw-read db hist? idx (codec/encode-prefix idx comps))])
        datoms (fn [db* & args] (mapv (juxt :e :a :v :tx :added) (apply m/datoms db* args)))]
    (cond-> [(raw false :aevt [(aid a)])
             (raw true :aevt [(aid a)])
             [:avet-value #(datoms db :avet a v)]
             (raw false :avet [(aid a) v])
             (raw true :avet [(aid a) v])
             [:since-scan #(datoms (m/since db old-t) :aevt a)]
             [:as-of-lookup #(datoms (m/as-of db old-t) :avet a v)]
             (let [txd [(assoc {:x/id 1000 :x/s "w"} a v)]]
               [:with #(datoms (:db-after (m/with db txd)) :avet a v)])]
      e (into [(raw false :eavt [e])
               (raw true :eavt [e])
               (raw false :eavt [e (aid a)])
               (raw false :vaet [e])
               [:entity #(into {} (m/touch (m/entity db e)))]
               [:pull #(m/pull db '[* {:x/r [:x/id]} {:x/_r [:x/id]}] e)]
               [:as-of-pull #(m/pull (m/as-of db old-t) '[*] e)]
               [:history #(datoms (m/history db) :eavt e)]
               (let [txd [[:db/add e a v]]]
                 [:with-entity #(datoms (:db-after (m/with db txd)) :eavt e)])]))))

(deftest mixed-types-async-writer-test
  ;; file store with sync on: several transact-async in flight form groups sharing one
  ;; fsync; now and then an irregular raw write (no :log entry) to an index CF
  (let [uri (str "mnemonic:file://" (tmp-dir) on)]
    (m/create-database uri)
    (let [c (m/connect uri)
          store (:store c)
          _ @(m/transact c mixed-schema)
          _ @(m/transact c (vec (for [i (range 60)] {:x/id i :x/s (str "s" i)})))
          dbs (CopyOnWriteArrayList. [(m/db c)])
          stop (atom false)
          checks (atom 0)
          bad (atom [])
          readers (vec (for [th (range 3)]
                         (future
                           (let [r (Random. (+ 10 th))]
                             (while (not @stop)
                               (let [^objects arr (.toArray dbs) n (alength arr)
                                     db (aget arr (if (.nextBoolean r) (dec n) (.nextInt r n)))]
                                 (doseq [[label f] (mixed-probes db r)]
                                   (let [want (norm (attempt #(uncached f)))]
                                     (dotimes [_ 3]
                                       (let [got (norm (attempt f))]
                                         (swap! checks inc)
                                         (when (and (not= want got) (< (count @bad) 5))
                                           (swap! bad conj {:probe label :t (m/basis-t db)
                                                            :cached got :uncached want}))))))))))))
          r (Random. 7)
          t0 (System/currentTimeMillis)]
      (while (< (- (System/currentTimeMillis) t0) 4000)
        (let [fs (doall (for [_ (range (inc (.nextInt r 6)))] (m/transact-async c (mixed-tx r))))]
          (doseq [f fs] (try @f (catch Exception _ nil))))
        (when (zero? (.nextInt r 40))
          ;; irregular: a raw index entry with no :log entry
          (let [db (m/db c)
                k (codec/encode-key :eavt (m/entid db [:x/id 0]) (m/entid db :x/s) (str "raw" (.nextInt r 5))
                                    (codec/t->tx (m/basis-t db)))]
            (rocks/write! store [[:put :eavt k (byte-array 0)]] {})
            ;; the connection's db does not see raw writes; a fresh base db does
            (.add dbs (mnemonic.db/base-db store))))
        (.add dbs (m/db c))
        (when (> (.size dbs) 40) (.remove dbs 1)))
      (reset! stop true)
      (run! deref readers)
      (let [st (m/index-cache-stats c)]
        (is (empty? @bad) (pr-str @bad))
        (is (< 1000 @checks))
        (is (pos? (:hits st)) (pr-str st))
        (is (pos? (:stale st)) (pr-str st)))
      (m/release c))))

(deftest close-during-reads-test
  ;; readers hammer reads while the file store closes: every read either answers
  ;; correctly or fails with connection-released / db-released
  (let [uri (str "mnemonic:file://" (tmp-dir) on)]
    (m/create-database uri)
    (let [c (m/connect uri)
          _ @(m/transact c schema)
          _ @(m/transact c (vec (for [i (range 3000)] {:k/id i :k/v i :k/name (str "x" i)})))
          db (m/db c)
          stop (atom false)
          wrong (atom [])
          readers (vec (for [th (range 4)]
                         (future
                           (let [r (Random. th)]
                             (while (not @stop)
                               (try
                                 (let [i (.nextInt r 3000)
                                       e (m/entid db [:k/id i])]
                                   (when-not (= i (:k/v (m/entity db e))) (swap! wrong conj [:entity i]))
                                   (when (zero? (.nextInt r 50))
                                     (when-not (= 3000 (count (seq (m/datoms db :aevt :k/v))))
                                       (swap! wrong conj [:scan]))))
                                 (catch Exception ex
                                   (when-not (#{:db.error/connection-released :db.error/db-released}
                                              (:db/error (ex-data ex)))
                                     (swap! wrong conj [:error (str ex)])))))))))]
      (Thread/sleep 1200)
      (m/release c)
      (Thread/sleep 300)
      (reset! stop true)
      (run! deref readers)
      (is (empty? @wrong) (pr-str (take 3 @wrong)))
      (is (zero? (:bytes (m/index-cache-stats db))) "a closed store keeps no segments"))))
