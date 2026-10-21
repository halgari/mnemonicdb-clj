(ns mnemonic.diff-explicit-partition-ids-test
  "Explicit entity ids in :db.part/db and :db.part/tx that were never allocated: Datomic
  accepts those inside its allocation window (db: below last allocated + 928; tx: below 1000
  or up to the current tx) and treats them as ordinary entities (a second card-one assert
  replaces the first, retract works), and rejects ids far beyond it."
  (:require [clojure.test :refer [deftest is]]
            [mnemonic.oracle :as o]))

(def ^:private tx-base 13194139533312)

(defn- step! [conns data expect]
  (let [r (o/transact-both conns data)
        kind (fn [x] (if (contains? x :error) (:error x) :ok))]
    (is (= (:datomic r) (:mnemonic r)) (pr-str data))
    (is (= expect (kind (:datomic r))) (pr-str data))))

(deftest explicit-db-and-tx-partition-ids
  (o/with-both [cd cm]
    (let [conns [cd cm]
          far-db 123456789
          far-tx (+ tx-base 123456789)]
      (step! conns [[:db/add far-db :db/doc "a"]] :db.error/invalid-entity-id)
      (step! conns [[:db/add far-tx :db/doc "a"]] :db.error/invalid-entity-id)
      (step! conns [[:db/retract far-db :db/doc "a"]] :db.error/invalid-entity-id)
      (doseq [e [500 (+ tx-base 500)]]
        (step! conns [[:db/add e :db/doc "a"]] :ok)
        (step! conns [[:db/add e :db/doc "b"]] :ok)
        (step! conns [[:db/retract e :db/doc "b"]] :ok)
        (step! conns [[:db/retract e :db/doc "b"]] :ok)))))
