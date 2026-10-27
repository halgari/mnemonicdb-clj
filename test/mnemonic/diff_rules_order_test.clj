(ns mnemonic.diff-rules-order-test
  "Differential tests for recursive rules whose bodies run in a different order than
  written (a pattern with nothing bound waits for a rule call that binds it): the same
  rules in both clause orders, bound from either end, mutually recursive, with required
  arguments and under negation, must give Datomic's results."
  (:require [clojure.test :refer [deftest is testing]]
            [datomic.api :as d]
            [mnemonic.api :as m]
            [mnemonic.oracle :as o]))

(def ^:private schema
  [{:db/ident :n/id :db/valueType :db.type/long :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/ident :n/parent :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/ident :n/link :db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   {:db/ident :n/hidden :db/valueType :db.type/boolean :db/cardinality :db.cardinality/one}])

(def ^:private n-nodes 300)

(def ^:private data
  ;; a tree (each node's parent is (i-1)/3), some cross links, and a cycle among 3 nodes
  (vec (concat
        [{:db/id "n0" :n/id 0}]
        (for [i (range 1 n-nodes)]
          (cond-> {:db/id (str "n" i) :n/id i :n/parent (str "n" (quot (dec i) 3))}
            (zero? (mod i 17)) (assoc :n/link [(str "n" (mod (* i 7) n-nodes))])
            (zero? (mod i 5)) (assoc :n/hidden true)))
        [{:db/id "c1" :n/id 1001 :n/link ["c2"]} {:db/id "c2" :n/id 1002 :n/link ["c3"]}
         {:db/id "c3" :n/id 1003 :n/link ["c1"]}])))

(def ^:private rulesets
  {:written-recursive-last
   '[[(anc ?a ?d) [?d :n/parent ?a]]
     [(anc ?a ?d) [?d :n/parent ?m] (anc ?a ?m)]]
   :written-recursive-first
   '[[(anc ?a ?d) [?d :n/parent ?a]]
     [(anc ?a ?d) (anc ?a ?m) [?d :n/parent ?m]]]
   :middle-pattern
   '[[(anc ?a ?d) [?d :n/parent ?a]]
     [(anc ?a ?d) [?m :n/parent ?a] (anc ?m ?d)]]
   :links
   '[[(anc ?a ?d) [?a :n/link ?d]]
     [(anc ?a ?d) [?m :n/link ?d] (anc ?a ?m)]]
   :required-arg
   '[[(anc [?a] ?d) [?d :n/parent ?a]]
     [(anc [?a] ?d) [?d :n/parent ?m] (anc ?a ?m)]]})

(def ^:private mutual
  '[[(even ?a ?d) [?d :n/parent ?a]]
    [(even ?a ?d) [?d :n/parent ?m] (odd ?a ?m)]
    [(odd ?a ?d) [?d :n/parent ?m] (even ?a ?m)]])

(defn- setup [cd cm]
  @(d/transact cd schema) @(m/transact cm schema)
  (let [rd @(d/transact cd data) rm @(m/transact cm data)
        named (fn [r] (into {} (filter (comp string? key)) (:tempids r)))]
    (is (= (named rd) (named rm)) "same entity ids on both sides")
    [(d/db cd) (m/db cm)]))

(defn- run [f]
  (try (f)
       (catch Throwable t
         [:error (or (:db/error (ex-data t)) [:unclassified (class t)])])))

(defn- same [dd mm q & args]
  (let [r-d (run #(apply d/q q dd args))
        r-m (run #(apply m/q q mm args))]
    (is (= r-d r-m) (pr-str q))))

(deftest recursive-rules-match-datomic-in-any-clause-order
  (o/with-both [cd cm]
    (let [[dd mm] (setup cd cm)]
      (doseq [[label rules] rulesets]
        (testing label
          (same dd mm '[:find ?id :in $ % ?root :where (anc ?root ?d) [?d :n/id ?id]] rules [:n/id 0])
          (same dd mm '[:find ?id :in $ % ?root :where (anc ?root ?d) [?d :n/id ?id]] rules [:n/id 4])
          (same dd mm '[:find ?id :in $ % ?root :where (anc ?root ?d) [?d :n/id ?id]] rules [:n/id 1001])
          (same dd mm '[:find ?id :in $ % ?leaf :where (anc ?a ?leaf) [?a :n/id ?id]] rules [:n/id 250])
          (same dd mm '[:find ?ai ?di :in $ % :where [?a :n/id 13] (anc ?a ?d) [?a :n/id ?ai] [?d :n/id ?di]] rules)
          (same dd mm '[:find (count ?d) :in $ % :where (anc ?a ?d)] rules)
          (same dd mm '[:find ?id :in $ % :where [?r :n/id 1] (anc ?r ?d) (not [?d :n/hidden true]) [?d :n/id ?id]] rules)
          (same dd mm '[:find ?id :in $ % :where [?d :n/id ?id] [(< ?id 40)] (not-join [?d] [?r :n/id 2] (anc ?r ?d))] rules)))
      (testing "mutual recursion"
        (same dd mm '[:find ?id :in $ % :where [?r :n/id 0] (odd ?r ?d) [?d :n/id ?id]] mutual)
        (same dd mm '[:find ?id :in $ % :where [?r :n/id 1] (even ?r ?d) [?d :n/id ?id]] mutual)))))
