(ns mnemonic.diff-index-pull-test
  "Differential test: d/index-pull must equal Datomic's (results and error kinds)."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datomic.api :as d]
            [mnemonic.api :as m]
            [mnemonic.conn :as conn]
            [mnemonic.oracle :as o]))

(use-fixtures :each (fn [f] (try (f) (finally (conn/reset-registry!)))))

(def ^:private schema
  [{:db/ident :p/name :db/valueType :db.type/string :db/cardinality :db.cardinality/one :db/index true}
   {:db/ident :p/age :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :p/friend :db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   {:db/ident :p/tags :db/valueType :db.type/string :db/cardinality :db.cardinality/many}])

(defn- run [api db arg]
  (try {:ok (vec ((if (= api :d) d/index-pull m/index-pull) db arg))}
       (catch Throwable t
         {:error (or (:db/error (ex-data t)) (class t))})))

(defn- eid [api db n]
  ((if (= api :d) d/q m/q) '[:find ?e . :in $ ?n :where [?e :p/name ?n]] db n))

(deftest index-pull-matches-datomic
  (o/with-both [cd cm]
    (doseq [c [cd cm]
            :let [tx (if (= c cd) d/transact m/transact)]]
      @(tx c schema)
      @(tx c [{:db/id "a" :p/name "Al" :p/age 3} {:db/id "b" :p/name "Bo" :p/age 5 :p/friend "a"}
              {:db/id "c" :p/name "Cy" :p/age 4}]))
    (let [dbd (d/db cd) dbm (m/db cm)]
      @(d/transact cd [{:db/id (eid :d dbd "Cy") :p/friend [(eid :d dbd "Al") (eid :d dbd "Bo")] :p/tags ["x" "y"]}])
      @(m/transact cm [{:db/id (eid :m dbm "Cy") :p/friend [(eid :m dbm "Al") (eid :m dbm "Bo")] :p/tags ["x" "y"]}]))
    (let [dbd (d/db cd) dbm (m/db cm)
          strip (fn [r] (if (:ok r)
                          {:ok (mapv (fn [x] (if (map? x) (dissoc x :db/id) x)) (:ok r))}
                          r))
          cases
          (fn [api db]
            (let [cy (eid api db "Cy") bo (eid api db "Bo")]
              [{:index :avet :selector [:p/name :p/age] :start [:p/name]}
               {:index :avet :selector [:p/name] :start [:p/name "Bo"]}
               {:index :avet :selector [:p/name] :start [:p/name "Bo"] :reverse true}
               {:index :avet :selector [:p/name] :start [:p/name] :reverse true}
               {:index :avet :selector [:p/name] :start [:p/name "Zed"]}
               {:index :avet :selector [:p/name] :start [:p/name 5]}
               {:index :avet :selector [:p/name] :start [:p/name] :offset 1 :limit 1}
               {:index :avet :selector [:db/ident] :start [:db/ident :db/add]}
               {:index :aevt :selector [:p/name] :start [:p/friend]}
               {:index :aevt :selector [:p/name] :start [:p/friend cy]}
               {:index :aevt :selector [:p/name] :start [:p/friend cy bo]}
               {:index :aevt :selector [:p/name] :start [:p/friend] :reverse true}
               ;; errors
               {:index :aevt :selector [:p/name] :start [:p/age]}
               {:index :aevt :selector [:p/name] :start [:p/tags]}
               {:index :avet :selector [:p/name] :start [:p/age]}
               {:index :avet :selector [:p/name] :start [:p/friend]}
               {:index :avet :selector [:p/name] :start [:p/nope]}
               {:index :eavt :selector [:p/name] :start [:p/name]}
               {:index :vaet :selector [:p/name] :start [:p/friend]}
               {:index :avet :selector [:p/name]}]))]
      (doseq [[cd* cm*] (map vector (cases :d dbd) (cases :m dbm))]
        (testing (pr-str cm*)
          (is (= (strip (run :d dbd cd*)) (strip (run :m dbm cm*))))))
      (testing "pull of * includes the same keys"
        (let [arg {:index :avet :selector '[*] :start [:p/name "Cy"]}]
          (is (= (mapv keys (d/index-pull dbd arg)) (mapv keys (m/index-pull dbm arg))))))
      (testing "as-of db"
        (let [arg {:index :avet :selector [:p/name] :start [:p/name]}]
          (is (= (vec (d/index-pull (d/as-of dbd 1001) arg)) (vec (m/index-pull (m/as-of dbm 1001) arg))))))
      (testing "history is rejected"
        (is (thrown? Exception (vec (m/index-pull (m/history dbm) {:index :avet :selector [:p/name] :start [:p/name]}))))))))
