(ns mnemonic.db-test
  (:require [clojure.test :refer [deftest is testing]]
            [mnemonic.bootstrap :as bootstrap]
            [mnemonic.codec :as codec]
            [mnemonic.datom :as datom]
            [mnemonic.db :as db]
            [mnemonic.index :as ix]
            [mnemonic.rocks :as rocks]
            [mnemonic.schema :as schema]))

(def ^:private counter (atom 0))

(defn- with-db [f]
  (let [store (rocks/open-store {:path (str "db-test-" (swap! counter inc)) :mem? true})]
    (try
      (bootstrap/bootstrap! store)
      (f store (db/base-db store))
      (finally (rocks/close-store store)))))

(defn- edn-datoms []
  (sort-by (fn [d] (codec/encode-key :eavt (:e d) (:a d) (:v d) (:tx d)))
           codec/compare-bytes
           (bootstrap/bootstrap-datoms)))

(def ^:private empty-prefix (byte-array 0))

(defn- eavt [d] [(:e d) (:a d) (:v d) (:tx d)])

(deftest bootstrap-db-test
  (with-db
    (fn [store db]
      (is (= 72 (ix/-basis-t db)))
      (is (= {:format 2 :next-t 1000 :next-db-n 73} (bootstrap/read-meta store)))
      (testing "EAVT holds all 272 datoms in order"
        (let [ds (seq (ix/datoms-iterable db :eavt (codec/encode-prefix :eavt [])))]
          (is (= 272 (count ds)))
          (is (= (map eavt (edn-datoms)) (map eavt ds)))
          (is (every? :added ds))))
      (testing "AEVT :db/ident"
        (is (= 66 (count (seq (ix/datoms-iterable db :aevt (codec/encode-prefix :aevt [10])))))))
      (testing "VAET only has datoms of ref attributes"
        (let [all (bootstrap/bootstrap-datoms)
              ref-attrs (set (map :e (filter #(and (= 40 (:a %)) (= 20 (:v %))) all)))
              expected (filter #(contains? ref-attrs (:a %)) all)
              ds (seq (ix/datoms-iterable db :vaet empty-prefix))]
          (is (pos? (count expected)))
          (is (= (set (map eavt expected)) (set (map eavt ds))))))
      (testing "history is empty, protocol accessors"
        (is (nil? (seq (ix/datoms-iterable (reify ix/IDb (-view [_ i p] (ix/-seek-history db i p)))
                                           :eavt empty-prefix))))
        (is (nil? (ix/-as-of-t db)))
        (is (nil? (ix/-since-t db)))
        (is (nil? (ix/-filter-pred db)))
        (is (false? (ix/-history? db)))
        (is (identical? db (ix/-raw db))))
      (testing "seek-view from a start key"
        (let [c (ix/-seek-view db :eavt (codec/encode-prefix :eavt [10]))]
          (is (= 10 (:e (.datom c))))
          (.close c)))
      (testing "schema"
        (is (= 66 (count (:idents (ix/-schema db)))))))))

(deftest isolation-test
  (with-db
    (fn [store db]
      (let [it (ix/datoms-iterable db :eavt empty-prefix)
            before (vec it)]
        (rocks/write! store (for [i (range 1000)]
                              [:put :eavt (codec/encode-key :eavt (+ 1000 i) 10 "x" 1) (byte-array 0)])
                      {})
        (is (= before (vec it)) "snapshot unaffected by later writes")
        (is (= 272 (count (seq (ix/datoms-iterable db :eavt empty-prefix)))))
        (is (= 1272 (count (seq (ix/datoms-iterable (db/base-db store) :eavt empty-prefix)))))))))

(deftest release-test
  (with-db
    (fn [_ db]
      (db/release-db db)
      (db/release-db db)))
  (testing "store closed before cursor close / release"
    (let [store (rocks/open-store {:path "db-test-closed" :mem? true})]
      (bootstrap/bootstrap! store)
      (let [d (db/base-db store)
            c (ix/-view d :eavt empty-prefix)]
        (rocks/close-store store)
        (.close c)
        (db/release-db d)))))

(deftest many-dbs-test
  (with-db
    (fn [store _]
      (dotimes [i 10000]
        (db/base-db store)
        (when (zero? (mod i 1000)) (System/gc)))
      (System/gc)
      (is (= 272 (count (seq (ix/datoms-iterable (db/base-db store) :eavt empty-prefix))))))))

(deftest user-schema-test
  (with-db
    (fn [store _]
      (let [tx (codec/t->tx 1000)
            ds [(datom/datom 73 10 :p/name tx true)
                (datom/datom 73 40 23 tx true)
                (datom/datom 73 41 35 tx true)
                (datom/datom 73 42 38 tx true)
                (datom/datom 74 10 :p/tags tx true)
                (datom/datom 74 40 21 tx true)
                (datom/datom 74 41 36 tx true)
                (datom/datom 74 44 true tx true)]
            refs #{40 41 42}]
        (rocks/write! store
                      (concat (mapcat #(bootstrap/datom-ops % refs) ds)
                              [(bootstrap/log-op tx ds)])
                      {})
        (let [d2 (db/base-db store)
              s (ix/-schema d2)
              n (schema/attr s :p/name)
              t (schema/attr s :p/tags)]
          (is (= 1000 (ix/-basis-t d2)))
          (is (= 73 (:id n)))
          (is (= :db.type/string (:value-type n)))
          (is (= :db.cardinality/one (:cardinality n)))
          (is (= :db.unique/identity (:unique n)))
          (is (schema/indexed? n))
          (is (= 74 (schema/entid s :p/tags)))
          (is (= :db.type/keyword (:value-type t)))
          (is (= :db.cardinality/many (:cardinality t)))
          (is (true? (:index? t)))
          (is (nil? (:unique t)))
          (is (schema/indexed? t)))))))

(deftest history-cursor-test
  (with-db
    (fn [store _]
      (let [assert-tx (codec/t->tx 1000)
            retract-tx (codec/t->tx 1001)
            k (codec/encode-key :eavt 500 10 "gone" assert-tx)
            cur-k (codec/encode-key :eavt 500 10 "here" assert-tx)]
        (rocks/write! store [[:put :eavt-h k (codec/long->bytes retract-tx)]
                             [:put :eavt cur-k (byte-array 0)]]
                      {})
        (let [d (db/base-db store)
              prefix (codec/encode-prefix :eavt [500])
              h (ix/-seek-history d :eavt prefix)]
          (is (.valid h))
          (is (= (datom/datom 500 10 "gone" assert-tx true) (.datom h)))
          (is (= retract-tx (.retractTx h)))
          (.advance h)
          (is (not (.valid h)))
          (.close h)
          (let [c (ix/-seek-current d :eavt prefix)]
            (is (= -1 (.retractTx c)))
            (.close c))
          (let [m (ix/merge-cursors [(ix/-seek-current d :eavt prefix)
                                     (ix/-seek-history d :eavt prefix)])]
            (is (= ["gone" "here"] (loop [acc []]
                                     (if (.valid m)
                                       (let [dv (:v (.datom m))
                                             rt (.retractTx m)]
                                         (is (= (if (= dv "gone") retract-tx -1) rt))
                                         (.advance m)
                                         (recur (conj acc dv)))
                                       acc))))
            (.close m)))))))

(deftest retire-pool-bounds-pooled-iterators
  (testing "retiring each superseded db keeps live pooled iterators bounded"
    (with-db
      (fn [store db0]
        (let [prefix (codec/encode-prefix :eavt [])
              read! (fn [db] (count (seq (ix/datoms-iterable db :eavt (codec/encode-prefix :eavt [0])))))
              baseline (count (:cursors store))
              final (loop [i 0 prev db0]
                      (if (< i 2000)
                        (let [nxt (db/successor-db store prev (+ 72 i 1) (db/db-meta prev) false)]
                          (read! prev)                     ; the writer reads through the published db
                          (read! prev)
                          (db/retire-pool! prev)
                          (recur (inc i) nxt))
                        prev))]
          (is (<= (- (count (:cursors store)) baseline) 8)
              "only the current db's pool holds iterators")
          (testing "a retired db is still readable and re-pools"
            (is (= (read! db0) (read! db0)))
            (is (not (db/released? db0))))
          (db/release-db final))))))
