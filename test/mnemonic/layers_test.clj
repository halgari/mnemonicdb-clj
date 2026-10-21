(ns mnemonic.layers-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [mnemonic.api :as m]
            [mnemonic.codec :as codec]
            [mnemonic.conn :as conn]
            [mnemonic.index :as index]))

(use-fixtures :each (fn [f] (try (f) (finally (conn/reset-registry!)))))

(defn- fresh []
  (let [u (str "mnemonic:mem://" (java.util.UUID/randomUUID))]
    (m/create-database u)
    (m/connect u)))

(defn- tx! [c data] @(m/transact c data))

(def schema
  [{:db/ident :a/one :db/valueType :db.type/string :db/cardinality :db.cardinality/one :db/index true}
   {:db/ident :a/many :db/valueType :db.type/string :db/cardinality :db.cardinality/many :db/index true}
   {:db/ident :a/ref :db/valueType :db.type/ref :db/cardinality :db.cardinality/many}])

(defn- vec5 [d] [(:e d) (:a d) (:v d) (:tx d) (:added d)])

(defn- user-datoms
  "Datoms of the user part (tx >= t 1000) as [e a v tx added]."
  [db idx & comps]
  (->> (apply m/datoms db idx comps)
       (map vec5)
       (filter (fn [[_ a _ tx _]] (and (not= 50 a) (>= (m/tx->t tx) 1000))))
       vec))

(defn- history-fixture
  "Returns {:conn :eid :ts [t1 t2 t3 t4]}: many x asserted t1, retracted t2, reasserted t3,
  retracted t4; one v1 -> v2 -> v1 (t1 t2 t3)."
  []
  (let [c (fresh)
        _ (tx! c schema)
        r1 (tx! c [{:db/id "e" :a/one "v1" :a/many "x"}])
        eid (get (:tempids r1) "e")
        r2 (tx! c [[:db/add eid :a/one "v2"] [:db/retract eid :a/many "x"]])
        r3 (tx! c [[:db/add eid :a/one "v1"] [:db/add eid :a/many "x"]])
        r4 (tx! c [[:db/retract eid :a/many "x"]])]
    {:conn c :eid eid
     :ts (mapv #(m/basis-t (:db-after %)) [r1 r2 r3 r4])}))

(defn- replay
  "Set of [e a v tx] visible after applying the log up to and including t."
  [c t]
  (let [lg (m/log c)]
    (->> (m/tx-range lg nil (inc t))
         (mapcat :data)
         (remove #(= 50 (:a %)))
         (reduce (fn [acc d]
                   (if (:added d)
                     (assoc acc [(:e d) (:a d) (:v d)] (:tx d))
                     (dissoc acc [(:e d) (:a d) (:v d)])))
                 {})
         (map (fn [[[e a v] tx]] [e a v tx]))
         set)))

(deftest as-of-matches-log-replay-test
  (let [{:keys [conn]} (history-fixture)
        db (m/db conn)]
    (doseq [t (range 999 (inc (m/basis-t db)))]
      (let [adb (m/as-of db t)
            eavt (set (map #(subvec % 0 4) (user-datoms adb :eavt)))]
        (is (= (replay conn t) eavt) (str "eavt as-of " t))
        (is (= eavt (set (map #(subvec % 0 4) (user-datoms adb :aevt)))) (str "aevt as-of " t))
        (is (= t (m/as-of-t adb)))
        (is (= (m/basis-t db) (m/basis-t adb)) "basis-t of an as-of db is the underlying basis")))))

(deftest as-of-accepts-tx-id-test
  (let [{:keys [conn ts]} (history-fixture)
        db (m/db conn)]
    (is (= (user-datoms (m/as-of db (second ts)) :eavt)
           (user-datoms (m/as-of db (m/t->tx (second ts))) :eavt)))
    (is (= (second ts) (m/as-of-t (m/as-of db (m/t->tx (second ts))))))))

(deftest history-view-test
  (let [{:keys [conn eid ts]} (history-fixture)
        [t1 t2 t3 t4] ts
        db (m/db conn)
        hist (m/history db)
        a-one (m/entid db :a/one)
        a-many (m/entid db :a/many)]
    (is (true? (m/is-history hist)))
    (is (false? (m/is-history db)))
    (is (nil? (m/as-of-t hist)))
    (testing "EAVT order: within e a v, newest tx first (as Datomic)"
      (is (= [[eid a-one "v1" (m/t->tx t3) true]
              [eid a-one "v1" (m/t->tx t2) false]
              [eid a-one "v1" (m/t->tx t1) true]
              [eid a-one "v2" (m/t->tx t3) false]
              [eid a-one "v2" (m/t->tx t2) true]
              [eid a-many "x" (m/t->tx t4) false]
              [eid a-many "x" (m/t->tx t3) true]
              [eid a-many "x" (m/t->tx t2) false]
              [eid a-many "x" (m/t->tx t1) true]]
             (user-datoms hist :eavt eid))))
    (testing "AVET history"
      (is (= [["v1" true] ["v1" false] ["v1" true] ["v2" false] ["v2" true]]
             (mapv (juxt :v :added) (m/datoms hist :avet :a/one)))))
    (testing "history of as-of is bounded by as-of"
      (is (= (set (filter (fn [[_ _ _ tx _]] (<= (m/tx->t tx) t2)) (user-datoms hist :eavt)))
             (set (user-datoms (m/history (m/as-of db t2)) :eavt)))))
    (testing "as-of of history"
      (is (= (set (user-datoms (m/history (m/as-of db t2)) :eavt))
             (set (user-datoms (m/as-of hist t2) :eavt)))))
    (testing "since of history keeps only events after t"
      (is (= (set (filter (fn [[_ _ _ tx _]] (> (m/tx->t tx) t2)) (user-datoms hist :eavt)))
             (set (user-datoms (m/since hist t2) :eavt)))))
    (testing "seek-datoms and index-range on history"
      (is (= (mapv vec5 (m/datoms hist :avet :a/one))
             (take 5 (map vec5 (m/seek-datoms hist :avet :a/one)))))
      (is (= #{"v1" "v2"} (set (map :v (m/index-range hist :a/one "v1" "v3"))))))))

(deftest since-test
  (let [{:keys [conn eid ts]} (history-fixture)
        [t1 t2 t3] ts
        db (m/db conn)]
    (is (= [[eid (m/entid db :a/one) "v1" (m/t->tx t3) true]]
           (user-datoms (m/since db t1) :eavt)))
    (is (= (user-datoms (m/since db t1) :eavt) (user-datoms (m/since db t2) :eavt)))
    (is (= [] (user-datoms (m/since db t3) :eavt)))
    (is (= t2 (m/since-t (m/since db t2))))
    (is (nil? (m/since-t db)))
    (is (= (m/basis-t db) (m/basis-t (m/since db t2))))
    (testing "since + as-of: asserted after since and visible at as-of"
      (is (= (set (user-datoms (m/since (m/as-of db t3) t1) :eavt))
             (set (user-datoms (m/as-of (m/since db t1) t3) :eavt))))
      (is (= #{[eid (m/entid db :a/one) "v1" (m/t->tx t3) true]
               [eid (m/entid db :a/many) "x" (m/t->tx t3) true]}
             (set (user-datoms (m/since (m/as-of db t3) t1) :eavt)))))))

(deftest nested-time-views-outer-wins-test
  (let [{:keys [conn ts]} (history-fixture)
        [_ t2 t3] ts
        db (m/db conn)]
    (is (= (user-datoms (m/as-of db t3) :eavt)
           (user-datoms (m/as-of (m/as-of db t2) t3) :eavt)))
    (is (= t2 (m/as-of-t (m/as-of (m/as-of db t3) t2))))))

(deftest filter-test
  (let [{:keys [conn eid]} (history-fixture)
        db (m/db conn)
        a-one (m/entid db :a/one)
        seen (atom #{})
        fdb (m/filter db (fn [d datom] (swap! seen conj d) (not= a-one (:a datom))))]
    (is (empty? (filter #(= a-one (:a %)) (m/datoms fdb :eavt eid))))
    (is (seq (m/datoms fdb :eavt 0)) "other attributes remain")
    (is (empty? (m/datoms fdb :aevt :a/one)))
    (is (empty? (m/seek-datoms fdb :avet :a/one)))
    (is (seq (m/datoms db :eavt eid :a/one)) "base unchanged")
    (is (= (m/basis-t db) (m/basis-t fdb)))
    (testing "pred receives a db"
      (is (some? (seq @seen)))
      (is (every? #(some? (m/basis-t %)) @seen)))
    (testing "filter of filter composes"
      (let [a-many (m/entid db :a/many)
            f2 (m/filter fdb (fn [_ datom] (not= a-many (:a datom))))]
        (is (empty? (filter #(#{a-one a-many} (:a %)) (m/datoms f2 :eavt eid))))))
    (testing "schema and attribute lookups pass through"
      (is (= (m/attribute db :a/one) (m/attribute fdb :a/one)))
      (is (= (m/entid db :a/one) (m/entid fdb :a/one))))))

(deftest composition-test
  (let [{:keys [conn ts]} (history-fixture)
        [t1 _ t3] ts
        db (m/db conn)
        pred (fn [_ d] (:added d))
        composed (m/filter (m/as-of (m/history db) t3) pred)
        manual (->> (user-datoms (m/history db) :eavt)
                    (filter (fn [[_ _ _ tx added]] (and (<= (m/tx->t tx) t3) added))))]
    (is (= manual (user-datoms composed :eavt)))
    (is (true? (m/is-history composed)))
    (is (= t3 (m/as-of-t composed)))
    (testing "as-of over a filter"
      (is (= (user-datoms (m/filter (m/as-of db t1) pred) :eavt)
             (user-datoms (m/as-of (m/filter db pred) t1) :eavt))))))

(deftest with-test
  (let [{:keys [conn eid]} (history-fixture)
        db (m/db conn)
        before (user-datoms db :eavt)
        r (m/with db [[:db/add eid :a/one "spec"] {:db/id "new" :a/one "fresh"}])
        wdb (:db-after r)]
    (is (= db (:db-before r)))
    (is (seq (:tx-data r)))
    (is (= #{"spec" "fresh"} (set (map :v (m/datoms wdb :aevt :a/one)))))
    (is (= (inc (m/basis-t db)) (m/basis-t wdb)))
    (testing "base db and connection unchanged"
      (is (= before (user-datoms db :eavt)))
      (is (= before (user-datoms (m/db conn) :eavt)))
      (is (= #{"v1"} (set (map :v (m/datoms (m/db conn) :aevt :a/one))))))
    (testing "avet and index-range see the overlay"
      (is (= ["fresh"] (mapv :v (m/index-range wdb :a/one "f" "g"))))
      (is (= ["spec"] (mapv :v (m/datoms wdb :avet :a/one "spec")))))
    (testing "retraction in the overlay hides base datoms"
      (let [w2 (:db-after (m/with db [[:db/retract eid :a/one "v1"]]))]
        (is (empty? (m/datoms w2 :eavt eid :a/one)))
        (is (= 1 (count (seq (m/datoms db :eavt eid :a/one)))))
        (is (some #(and (= "v1" (:v %)) (not (:added %)))
                  (m/datoms (m/history w2) :eavt eid :a/one)))))
    (testing "with chains"
      (let [w3 (:db-after (m/with wdb [[:db/add eid :a/one "spec2"]]))]
        (is (= ["spec2"] (mapv :v (m/datoms w3 :eavt eid :a/one))))
        (is (= ["spec"] (mapv :v (m/datoms wdb :eavt eid :a/one))))))
    (testing "schema installed in a with tx is usable in the with db"
      (let [w4 (:db-after (m/with db [{:db/ident :b/new :db/valueType :db.type/long
                                       :db/cardinality :db.cardinality/one}]))]
        (is (some? (m/attribute w4 :b/new)))
        (is (nil? (m/attribute db :b/new)))
        (is (= [5] (mapv :v (m/datoms (:db-after (m/with w4 [[:db/add eid :b/new 5]]))
                                      :eavt eid :b/new))))))
    (testing "as-of over a with db"
      (is (= before (user-datoms (m/as-of wdb (m/basis-t db)) :eavt))))
    (testing "with on a time view or filter throws"
      (doseq [v [(m/as-of db 1001) (m/since db 1001) (m/history db) (m/filter db (fn [_ _] true))]]
        (is (= :db.error/unsupported
               (try (m/with v [[:db/add eid :a/one "x"]])
                    (catch clojure.lang.ExceptionInfo e (:db/error (ex-data e))))))))
    (testing "failed tx throws"
      (is (thrown? clojure.lang.ExceptionInfo (m/with db [[:db/add eid :nope/attr 1]]))))))

(deftest cursors-closed-test
  (let [before (.get ^java.util.concurrent.atomic.AtomicLong index/open-cursors)
        {:keys [conn eid ts]} (history-fixture)
        db (m/db conn)
        wdb (:db-after (m/with db [[:db/add eid :a/one "z"]]))]
    (doseq [d [(m/history db) (m/as-of db (first ts)) (m/since db (first ts)) wdb
               (m/history wdb) (m/as-of (m/history wdb) (second ts))]]
      (doall (m/datoms d :eavt))
      (doall (m/seek-datoms d :aevt))
      (doall (m/datoms d :avet :a/one)))
    (is (= before (.get ^java.util.concurrent.atomic.AtomicLong index/open-cursors)))))

(deftest key-tx-bytes
  (doseq [idx [:eavt :aevt :avet :vaet]
          tx [0 1 536870912001 Long/MAX_VALUE]]
    (let [k (codec/encode-key idx 17 18 19 5)
          kt #'mnemonic.layers/key-tx
          kw #'mnemonic.layers/key-with-tx]
      (is (= 5 (kt k)))
      (is (= tx (kt (kw k tx))))
      (is (java.util.Arrays/equals ^bytes (kw k tx) ^bytes (codec/encode-key idx 17 18 19 tx))))))
