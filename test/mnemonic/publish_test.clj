(ns mnemonic.publish-test
  "Incremental db publishing after a transaction, and the writer surviving a concurrent
  release of the published db."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [mnemonic.api :as m]
            [mnemonic.bootstrap :as bootstrap]
            [mnemonic.conn :as conn]
            [mnemonic.db :as db]
            [mnemonic.index :as index]
            [mnemonic.tx :as tx])
  (:import (java.util Random)))

(use-fixtures :each (fn [f] (try (f) (finally (conn/reset-registry!)))))

(defn- fresh []
  (let [u (str "mnemonic:mem://" (java.util.UUID/randomUUID))]
    (m/create-database u)
    (m/connect u)))

(defn- check-equivalent!
  "The published db equals a db freshly built from storage."
  [c]
  (let [pub (m/db c)
        fresh (db/base-db (:store c))]
    (try
      (is (= (index/-basis-t fresh) (index/-basis-t pub)))
      (is (= (db/db-meta fresh) (db/db-meta pub)))
      (is (= (bootstrap/read-meta (:store c)) (db/db-meta pub)))
      (is (= (index/-schema fresh) (index/-schema pub)) "incremental schema == rebuilt schema")
      (finally (db/release-db fresh)))))

(defn- rand-tx [^Random r n]
  (let [pick (fn [coll] (nth coll (.nextInt r (count coll))))
        attr (fn [i] (keyword "g" (str "a" i)))
        i (.nextInt r 6)]
    (case (.nextInt r 8)
      0 [{:db/ident (attr i) :db/valueType (pick [:db.type/string :db.type/long :db.type/ref])
          :db/cardinality (pick [:db.cardinality/one :db.cardinality/many])}]
      1 [{:db/ident (attr i) :db/index true}]
      2 [{:db/ident (attr i) :db/doc (str "doc" n)}]
      3 [[:db/add (attr i) :db/ident (attr (+ 6 (.nextInt r 3)))]]            ; rename / alias
      4 [[:db/retract (attr i) :db/ident (attr i)]]
      5 [{:db/ident (keyword "e" (str "x" (.nextInt r 4))) :db/doc (str n)}]
      6 [{:db/id "t" (attr i) (pick [(str "s" n) n])}]
      7 [{:db/id (str "u" n) :db/doc "plain"}])))

(deftest incremental-publish-matches-rebuilt-test
  (dotimes [seed 6]
    (let [c (fresh)
          r (Random. (long seed))]
      (dotimes [n 40]
        (try @(m/transact c (rand-tx r n)) (catch Exception _ nil))
        (check-equivalent! c))
      @(m/transact c [{:db/id "d" :db/doc "final"}])
      (check-equivalent! c))))

(deftest plain-data-tx-reuses-schema-object-test
  (let [c (fresh)]
    @(m/transact c [{:db/ident :p/n :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])
    (let [s1 (index/-schema (m/db c))
          _ @(m/transact c [{:p/n "a"}])
          s2 (index/-schema (m/db c))]
      (is (identical? s1 s2)))
    (let [s2 (index/-schema (m/db c))]
      @(m/transact c [{:db/ident :p/m :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])
      (is (not (identical? s2 (index/-schema (m/db c))))))))

(deftest tx-survives-release-of-published-db-test
  (let [c (fresh)
        real tx/prepare
        n (atom 0)]
    @(m/transact c [{:db/ident :p/n :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])
    (testing "published db released between the rebuild check and prepare"
      (with-redefs [tx/prepare (fn [d meta data]
                                 (when (= 1 (swap! n inc)) (m/release-db (m/db c)))
                                 (real d meta data))]
        (let [rep @(m/transact c [{:p/n "a"}])]
          (is (= 2 @n) "prepared twice")
          (is (not (db/released? (index/-raw (:db-before rep)))))
          (is (= ["a"] (map :v (m/datoms (m/db c) :aevt :p/n)))))))
    (testing "release of the current db after a tx"
      (m/release-db (m/db c))
      @(m/transact c [{:p/n "b"}])
      (is (= ["a" "b"] (map :v (m/datoms (m/db c) :aevt :p/n)))))
    (check-equivalent! c)))
