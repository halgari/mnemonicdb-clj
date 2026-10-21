(ns mnemonic.db-test
  (:require [clojure.test :refer [deftest is testing]]
            [mnemonic.bootstrap :as bootstrap]
            [mnemonic.codec :as codec]
            [mnemonic.db :as db]
            [mnemonic.index :as ix]
            [mnemonic.rocks :as rocks]))

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
      (is (= {:format 1 :next-t 1000 :next-db-n 73} (bootstrap/read-meta store)))
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
