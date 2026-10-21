(ns mnemonic.diff-two-identity-test
  "Pinned differential cases for tempid upsert with two :db.unique/identity attributes
  (u1, u2). Cases where MnemonicDB matches Datomic are compared; the one known
  deviation is pinned on both sides (see docs/deviations.md)."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [mnemonic.conn :as conn]
            [mnemonic.oracle :as o]))

(use-fixtures :each (fn [f] (try (f) (finally (conn/reset-registry!)))))

(def ^:private schema
  [{:db/ident :u1 :db/valueType :db.type/string :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/ident :u2 :db/valueType :db.type/string :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/ident :n :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])

(def ^:private seed [{:u1 "a" :u2 "A" :n 1} {:u1 "b" :n 2} {:u2 "C" :n 3}])

(def matching-cases
  "Transactions (against the seed) whose outcome equals Datomic's."
  [;; one tempid, both identities of one existing entity
   [[:db/add "t0" :u1 "a"] [:db/add "t0" :u2 "A"] [:db/add "t0" :n 10]]
   ;; one tempid, identities of two different existing entities
   [[:db/add "t0" :u1 "a"] [:db/add "t0" :u2 "C"]]
   ;; one tempid, an existing u1 plus a new u2
   [[:db/add "t0" :u1 "b"] [:db/add "t0" :u2 "B"]]
   ;; two tempids sharing a new u1, different new u2
   [[:db/add "t0" :u1 "x"] [:db/add "t1" :u1 "x"] [:db/add "t0" :u2 "X"] [:db/add "t1" :n 5]]
   ;; two tempids joined through u1 and u2 in the same order
   [[:db/add "t0" :u1 "x"] [:db/add "t1" :u1 "x"] [:db/add "t0" :u2 "y"] [:db/add "t1" :u2 "y"]]
   ;; a tempid upserting via u2 while another new tempid takes the same u1
   [[:db/add "t0" :u2 "A"] [:db/add "t1" :u1 "z"] [:db/add "t0" :n 7]]
   ;; two tempids with conflicting existing identities
   [[:db/add "t0" :u1 "a"] [:db/add "t1" :u1 "b"] [:db/add "t0" :u2 "C"]]
   ;; map form with both identities of one entity
   [{:db/id "t0" :u1 "a" :u2 "A" :n 4}]
   ;; map form joining a new u1 to an existing u2
   [{:db/id "t0" :u1 "new" :u2 "C"}]
   ;; retraction of an identity value through a tempid upsert
   [[:db/add "t0" :u1 "a"] [:db/retract "t0" :u2 "A"]]])

(def ^:private crossed-case
  "Two tempids that each carry one value of the other's identity pair."
  [[:db/add "t0" :u1 "x"] [:db/add "t1" :u2 "y"] [:db/add "t0" :u2 "y"] [:db/add "t1" :u1 "x"]])

(defn- run-case [tx]
  (o/with-both [cd cm]
    (let [conns [cd cm]]
      (o/transact-both conns schema)
      (o/transact-both conns seed)
      (o/transact-both conns tx))))

(deftest two-identity-upserts-match-datomic
  (doseq [tx matching-cases]
    (testing (pr-str tx)
      (let [{:keys [datomic mnemonic]} (run-case tx)]
        (is (= datomic mnemonic))))))

(deftest crossed-two-identity-case-is-a-pinned-deviation
  (let [{:keys [datomic mnemonic]} (run-case crossed-case)]
    (is (= {:error :db.error/datoms-conflict} datomic)
        "Datomic reports a conflict (pinned; if this changes, revisit the deviation)")
    (is (not (contains? mnemonic :error))
        "MnemonicDB unifies t0 and t1 into one new entity (deviation)")
    (is (= #{[:u1 "x"] [:u2 "y"]}
           (set (keep (fn [[_ a v]] (when (#{:u1 :u2} a) [a v]))
                      (map (fn [[e a v tx added]] [e ({73 :u1 74 :u2} a a) v tx added])
                           (:tx-data mnemonic))))))))
