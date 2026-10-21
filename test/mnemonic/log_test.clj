(ns mnemonic.log-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [mnemonic.api :as m]
            [mnemonic.conn :as conn])
  (:import (java.util.concurrent LinkedBlockingQueue TimeUnit)))

(use-fixtures :each (fn [f] (try (f) (finally (conn/reset-registry!)))))

(defn- fresh []
  (let [u (str "mnemonic:mem://" (java.util.UUID/randomUUID))]
    (m/create-database u)
    (m/connect u)))

(defn- tx! [c data] @(m/transact c data))

(def schema [{:db/ident :a/one :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])

(deftest tx-range-test
  (let [c (fresh)
        r0 (tx! c schema)
        r1 (tx! c [{:db/id "e" :a/one "v1"}])
        eid (get (:tempids r1) "e")
        r2 (tx! c [[:db/add eid :a/one "v2"]])
        r3 (tx! c [[:db/retract eid :a/one "v2"]])
        [t0 t1 t2 t3] (mapv #(m/basis-t (:db-after %)) [r0 r1 r2 r3])
        lg (m/log c)
        ts (fn [s e] (mapv :t (m/tx-range lg s e)))]
    (is (= [t0 t1 t2 t3] (ts nil nil)) "bootstrap txs are not in the log range")
    (is (= [t1 t2] (ts t1 t3)) "end exclusive")
    (is (= [t1 t2] (ts (m/t->tx t1) (m/t->tx t3))) "tx ids accepted")
    (is (= [t2 t3] (ts t2 nil)))
    (is (= [t0 t1] (ts nil t2)))
    (is (= [] (ts t2 t2)))
    (is (= [] (ts 0 5)))
    (let [e (first (m/tx-range lg t2 t3))
          a-one (m/entid (m/db c) :a/one)]
      (is (= #{:t :data} (set (keys e))))
      (is (vector? (:data e)))
      (is (= #{[eid a-one "v1" (m/t->tx t2) false]
               [eid a-one "v2" (m/t->tx t2) true]}
             (set (map (juxt :e :a :v :tx :added)
                       (remove #(= 50 (:a %)) (:data e)))))))
    (let [instant (first (:data (first (m/tx-range lg t1 t2))))]
      (is (= 50 (:a instant)))
      (is (instance? java.util.Date (:v instant))))
    (is (= (mapv :tx-data [r0 r1 r2 r3])
           (mapv :data (m/tx-range lg nil nil))))
    (testing "large logs are read in chunks"
      (dotimes [i 150] (tx! c [[:db/add eid :a/one (str "n" i)]]))
      (is (= 154 (count (m/tx-range lg nil nil))))
      (is (apply < (map :t (m/tx-range lg nil nil)))))))

(deftest tx-report-queue-test
  (let [c (fresh)
        q (m/tx-report-queue c)]
    (is (instance? LinkedBlockingQueue q))
    (is (identical? q (m/tx-report-queue c)) "one queue per connection, as Datomic")
    (let [r (tx! c schema)
          rep (.poll ^LinkedBlockingQueue q 2 TimeUnit/SECONDS)]
      (is (some? rep))
      (is (= #{:db-before :db-after :tx-data :tempids} (set (keys rep))))
      (is (= (:tx-data r) (:tx-data rep))))
    (tx! c [{:a/one "x"}])
    (is (some? (.poll ^LinkedBlockingQueue q 2 TimeUnit/SECONDS)))
    (is (nil? (.poll ^LinkedBlockingQueue q 100 TimeUnit/MILLISECONDS)))
    (m/remove-tx-report-queue c)
    (tx! c [{:a/one "y"}])
    (is (nil? (.poll ^LinkedBlockingQueue q 100 TimeUnit/MILLISECONDS)))
    (testing "failed txs are not published"
      (let [q2 (m/tx-report-queue c)]
        (is (thrown? Exception (tx! c [[:db/add 1 :nope 2]])))
        (is (nil? (.poll ^LinkedBlockingQueue q2 100 TimeUnit/MILLISECONDS)))))))
