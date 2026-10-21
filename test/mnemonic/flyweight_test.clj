(ns mnemonic.flyweight-test
  "Flyweight datom views (index/reduce-views) must agree with the copying path for every
  index, value type and key length (including keys longer than the initial 64-byte
  cursor buffer), and a view's toDatom copy must survive the cursor advancing."
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [mnemonic.api :as m]
            [mnemonic.conn :as conn]
            [mnemonic.index :as index])
  (:import (mnemonic.datom IDatomView)))

(use-fixtures :each (fn [f] (try (f) (finally (conn/reset-registry!)))))

(def ^:private schema
  [{:db/ident :t/s :db/valueType :db.type/string :db/cardinality :db.cardinality/many :db/index true}
   {:db/ident :t/n :db/valueType :db.type/long :db/cardinality :db.cardinality/one :db/index true}
   {:db/ident :t/d :db/valueType :db.type/double :db/cardinality :db.cardinality/one}
   {:db/ident :t/r :db/valueType :db.type/ref :db/cardinality :db.cardinality/many}])

(defn- fixture [uri]
  (m/create-database uri)
  (let [c (m/connect uri)]
    @(m/transact c schema)
    @(m/transact c (for [i (range 50)]
                     {:t/s [(str "short-" i) (apply str "long-" i "-" (repeat 300 "x")) (str "é" i)]
                      :t/n (* i 1000003)
                      :t/d (* i 1.5)}))
    @(m/transact c (let [ids (map :e (m/datoms (m/db c) :aevt :t/n))]
                     (for [[a b] (partition 2 1 ids)] {:db/id a :t/r [b]})))
    c))

(defn- fly [coll]
  (index/reduce-views coll
                      (fn [acc ^IDatomView d]
                        (conj acc [(.eid d) (.attr d) (.value d) (.txid d) (.isAdded d)]))
                      []))

(defn- plain [coll]
  (mapv (fn [d] [(:e d) (:a d) (:v d) (:tx d) (:added d)]) coll))

(deftest views-equal-copies-all-indexes
  (doseq [uri [(str "mnemonic:mem://" (random-uuid))
               (str "mnemonic:file://" (System/getProperty "java.io.tmpdir") "/fw-" (random-uuid))]]
    (let [c (fixture uri)
          db (m/db c)]
      (doseq [idx [:eavt :aevt :avet :vaet]]
        (let [ds (m/datoms db idx)]
          (is (pos? (count (plain ds))) (str idx))
          (is (= (plain ds) (fly ds)) (str idx " " uri))))
      (let [r (m/index-range db :t/n 1000003 20000000)]
        (is (= (plain r) (fly r))))
      (m/release c)
      (m/delete-database uri))))

(deftest value-long-and-copies-survive
  (let [uri (str "mnemonic:mem://" (random-uuid))
        c (fixture uri)
        db (m/db c)
        ds (m/datoms db :aevt :t/n)
        copies (index/reduce-views ds (fn [acc ^IDatomView d] (conj acc (.toDatom d))) [])
        sum (index/reduce-views ds (fn [^long acc ^IDatomView d] (+ acc (.valueLong d))) 0)]
    (is (= (mapv :v copies) (mapv :v ds)))
    (is (= (reduce + (map :v copies)) sum))
    (is (= (vec ds) copies))
    (is (= (count copies) (count (set copies))))))

(deftest entity-and-pull-through-views
  (let [uri (str "mnemonic:mem://" (random-uuid))
        c (fixture uri)
        db (m/db c)
        e (:e (first (m/datoms db :avet :t/n 7000021)))
        ent (m/entity db e)]
    (is (= 7000021 (:t/n ent)))
    (is (= 3 (count (:t/s ent))))
    (is (some #(> (count ^String %) 300) (:t/s ent)))
    (is (= (:t/n ent) (:t/n (m/pull db [:t/n] e))))
    (is (= (set (:t/s ent)) (set (:t/s (m/pull db [:t/s] e)))))
    (is (= (:t/d ent) 10.5))
    ))

(deftest views-equal-copies-time-views-and-merges
  ;; history / as-of / since views merge current and history cursors; with-dbs merge an
  ;; overlay. Each read three times (the later ones through cached segments).
  (let [uri (str "mnemonic:mem://" (random-uuid))
        c (fixture uri)
        t1 (m/basis-t (m/db c))
        ids (vec (map :e (m/datoms (m/db c) :aevt :t/n)))
        _ @(m/transact c (for [e (take 20 ids)] [:db/add e :t/n (+ 7 e)]))
        _ @(m/transact c (for [[i e] (map-indexed vector (take 10 ids))] [:db/retract e :t/s (str "é" i)]))
        db (m/db c)
        w (:db-after (m/with db [{:t/n 42 :t/s ["w"]} [:db/add (first ids) :t/d 99.5]]))]
    (doseq [[label d] [["history" (m/history db)] ["as-of" (m/as-of db t1)]
                       ["since" (m/since db t1)] ["with" w]
                       ["filter" (m/filter db (fn [_ dt] (even? (:e dt))))]]
            idx [:eavt :aevt :avet :vaet]
            _ (range 3)]
      (let [ds (m/datoms d idx)]
        (is (= (plain ds) (fly ds)) (str label " " idx))))
    (is (seq (plain (m/datoms (m/since db t1) :aevt :t/n))))
    (m/release c)
    (m/delete-database uri)))

(deftest view-after-close-throws
  (let [uri (str "mnemonic:mem://" (random-uuid))
        c (fixture uri)
        ^mnemonic.db.Base b (index/-raw (m/db c))
        ^mnemonic.index.ICursor cur
        (index/prefix-cursor (mnemonic.rocks/raw-cursor (.-store b) :aevt (.-snap b))
                             (byte-array 0) :aevt false)
        ^IDatomView v (index/view-of cur)]
    (is (some? (.value v)) "readable while the cursor is on it")
    (.close cur)
    (is (thrown? IllegalStateException (.eid v)))
    (is (thrown? IllegalStateException (.value v)))
    (m/release c)))
