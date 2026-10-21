(ns mnemonic.bench.data
  "Deterministic schema and data generation (identical for every backend)."
  (:import (java.util Date Random)))

(def schema
  [{:db/ident :p/email :db/valueType :db.type/string :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :p/age :db/valueType :db.type/long :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :p/name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :p/status :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/ident :p/tags :db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   {:db/ident :p/created :db/valueType :db.type/instant :db/cardinality :db.cardinality/one}
   ;; tree for the recursive-rule query
   {:db/ident :n/id :db/valueType :db.type/long :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :n/parent :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}])

(def statuses [:status/active :status/inactive :status/pending :status/banned])
(def n-tags 20)
(def tags (mapv #(keyword "tag" (str "t" %)) (range n-tags)))

(def enum-idents (mapv (fn [k] {:db/ident k}) (concat statuses tags)))

(def base-ms 1700000000000)

(defn email [i] (str "user" i "@example.com"))

(defn person
  "Person i; all attribute values are a pure function of i and the seed."
  [seed i]
  (let [r (Random. (+ (* 1000003 (long seed)) (long i)))
        ntags (+ 1 (.nextInt r 4))
        tg (vec (distinct (repeatedly ntags #(.nextInt r (int n-tags)))))]
    {:p/email (email i)
     :p/age (long (.nextInt r 1000))
     :p/name (str "Person " i " " (Long/toString (.nextLong r) 36))
     :p/status (nth statuses (.nextInt r 4))
     :p/tags (mapv tags tg)
     :p/created (Date. (long (+ base-ms (* (long i) 1000))))}))

(defn people [seed from to]
  (mapv #(person seed %) (range from to)))

(defn tree-nodes
  "n nodes, ternary tree: node i (>0) has parent (i-1)/3. Parents as lookup refs."
  [n]
  (into [{:n/id 0}]
        (map (fn [i] {:n/id (long i) :n/parent [:n/id (long (quot (dec i) 3))]}))
        (range 1 n)))

(defn small-tx
  "Transaction i for the small-tx workload: a new person with 1-5 attributes."
  [i]
  (let [k (inc (mod (* i 7) 5))
        p (person 2 i)
        ks (take k [:p/email :p/age :p/name :p/created :p/status])]
    [(select-keys p ks)]))
