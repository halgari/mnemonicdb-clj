(ns mnemonic.diff-query-range-test
  "Differential tests for range / equality predicates on a pattern's value variable
  (AVET range pushdown): long, double, string and instant attributes, mixed-type and nil
  bounds, bounds bound from :in, and the as-of / since / history views."
  (:require [clojure.test :refer [deftest is]]
            [datomic.api :as d]
            [mnemonic.api :as m]
            [mnemonic.oracle :as o]))

(def ^:private schema
  [{:db/id "a" :db/ident :r/n :db/valueType :db.type/string :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/id "b" :db/ident :r/age :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/id "c" :db/ident :r/ages :db/valueType :db.type/long :db/cardinality :db.cardinality/many
    :db/index true}
   {:db/id "d" :db/ident :r/score :db/valueType :db.type/double :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/id "e" :db/ident :r/born :db/valueType :db.type/instant :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/id "f" :db/ident :r/city :db/valueType :db.type/string :db/cardinality :db.cardinality/one
    :db/index true}])

(def ^:private ages [Long/MIN_VALUE -5 -1 0 1 7 7 10 20 20 20 30 99 1000 Long/MAX_VALUE])
(def ^:private scores [-1.5 -0.0 0.0 0.5 2.5 2.5 3.5 1e10 Double/POSITIVE_INFINITY])
(def ^:private cities ["" "A" "Ab" "b" "～" "😀" "zz" "X"])

(def ^:private people
  (vec (for [i (range 40)]
         (cond-> {:db/id (str "p" i) :r/n (str "n" i)
                  :r/age (nth ages (mod i (count ages)))
                  :r/score (nth scores (mod (* 3 i) (count scores)))
                  :r/born (java.util.Date. (* 86400000 (long (- (mod (* 7 i) 23) 5))))
                  :r/city (nth cities (mod i (count cities)))}
           (even? i) (assoc :r/ages [(nth ages (mod (+ i 3) (count ages)))
                                     (nth ages (mod (+ i 5) (count ages)))])))))

(def ^:private d1 #inst "1970-01-03")
(def ^:private d2 #inst "1970-01-12")

(def ^:private queries
  (concat
   ;; long, constants
   (for [[op k] [['< 10] ['<= 10] ['> 10] ['>= 10] ['= 20] ['< 0] ['> -1] ['>= Long/MIN_VALUE]
                 ['<= Long/MAX_VALUE] ['> Long/MAX_VALUE] ['< Long/MIN_VALUE] ['= 7]]
         attr [:r/age :r/ages]]
     [`[:find ~'?e ~'?a :where [~'?e ~attr ~'?a] [(~op ~'?a ~k)]]])
   ;; flipped operands
   (for [[op k] [['< 10] ['<= 10] ['> 10] ['>= 10] ['= 20]]]
     [`[:find ~'?e ~'?a :where [~'?e :r/age ~'?a] [(~op ~k ~'?a)]]])
   [['[:find ?e ?a :where [?e :r/age ?a] [(<= 5 ?a)] [(< ?a 30)]]]
    ['[:find ?e ?a :where [?e :r/age ?a] [(> ?a 5)] [(< ?a 5)]]]
    ['[:find ?e ?a :where [?e :r/age ?a] [(>= ?a 20)] [(<= ?a 20)] [?e :r/n ?n]]]
    ['[:find ?n :where [?e :r/age ?a] [(> ?a 0)] [(< ?a 25)] [?e :r/n ?n]]]
    ;; mixed-type bounds: no pushdown, same results or same errors. (Cross-class compares
    ;; such as long vs string or date vs long throw here but order by type in Datomic: a
    ;; pre-existing divergence outside this test.)
    ['[:find ?e ?a :where [?e :r/age ?a] [(< ?a 10.5)]]]
    ['[:find ?e ?a :where [?e :r/age ?a] [(>= ?a 10.0)] [(< ?a 30.5)]]]
    ['[:find ?e ?a :where [?e :r/age ?a] [(< ?a nil)]]]
    ['[:find ?e ?a :where [?e :r/age ?a] [(= ?a nil)]]]
    ['[:find ?e ?a :where [?e :r/age ?a] [(= ?a 20.0)]]]
    ['[:find ?e ?a :where [?e :r/age ?a] [(< ?a 3N)]]]
    ['[:find ?e ?a :where [?e :r/age ?a] [(foo ?a)] [(< ?a 10)]]]
    ;; doubles
    ['[:find ?e ?s :where [?e :r/score ?s] [(< ?s 2.5)]]]
    ['[:find ?e ?s :where [?e :r/score ?s] [(<= ?s 2.5)]]]
    ['[:find ?e ?s :where [?e :r/score ?s] [(> ?s 0.0)]]]
    ['[:find ?e ?s :where [?e :r/score ?s] [(>= ?s -0.0)]]]
    ['[:find ?e ?s :where [?e :r/score ?s] [(= ?s 0.0)]]]
    ['[:find ?e ?s :where [?e :r/score ?s] [(< ?s 3)]]]
    ['[:find ?e ?s :where [?e :r/score ?s] [(> ?s -1.5)] [(< ?s 1e10)]]]
    ;; strings
    ['[:find ?e ?c :where [?e :r/city ?c] [(< ?c "b")]]]
    ['[:find ?e ?c :where [?e :r/city ?c] [(>= ?c "A")] [(<= ?c "Ab")]]]
    ['[:find ?e ?c :where [?e :r/city ?c] [(> ?c "～")]]]
    ['[:find ?e ?c :where [?e :r/city ?c] [(< ?c "😀")]]]
    ['[:find ?e ?c :where [?e :r/city ?c] [(= ?c "X")]]]
    ;; instants
    [`[:find ~'?e ~'?b :where [~'?e :r/born ~'?b] [(~'< ~'?b ~d1)]]]
    [`[:find ~'?e ~'?b :where [~'?e :r/born ~'?b] [(~'>= ~'?b ~d1)] [(~'< ~'?b ~d2)]]]
    [`[:find ~'?e ~'?b :where [~'?e :r/born ~'?b] [(~'= ~'?b ~d1)]]]
    [`[:find ~'?e ~'?b :where [~'?e :r/born ~'?b] [(~'> ~d2 ~'?b)]]]
    ;; bounds from :in
    ['[:find ?e ?a :in $ ?lo ?hi :where [?e :r/age ?a] [(<= ?lo ?a)] [(< ?a ?hi)]] 5 25]
    ['[:find ?e ?a :in $ ?lo ?hi :where [?e :r/age ?a] [(<= ?lo ?a)] [(< ?a ?hi)]] 25 5]
    ['[:find ?e ?a :in $ ?lo ?hi :where [?e :r/age ?a] [(<= ?lo ?a)] [(< ?a ?hi)]] 5 nil]
    ['[:find ?e ?a :in $ ?lo ?hi :where [?e :r/age ?a] [(<= ?lo ?a)] [(< ?a ?hi)]] 5 25.5]
    ['[:find ?e ?a :in $ ?lo :where [?e :r/age ?a] [(< ?a ?lo)]] 10]
    ['[:find ?e ?a :in $ [?lo ...] :where [?e :r/age ?a] [(<= ?lo ?a)] [(< ?a 21)]] [0 20 100]]
    ['[:find ?lo ?e ?a :in $ [?lo ...] :where [?e :r/age ?a] [(<= ?lo ?a)] [(< ?a 21)]] [0 20 100]]
    ['[:find ?lo ?hi ?e :in $ [[?lo ?hi]] :where [?e :r/age ?a] [(<= ?lo ?a)] [(<= ?a ?hi)]]
     [[0 10] [20 30] [1000 2000]]]
    ['[:find ?e ?s :in $ ?lo ?hi :where [?e :r/score ?s] [(<= ?lo ?s)] [(< ?s ?hi)]] 0.0 3.0]
    ['[:find ?e ?b :in $ ?lo ?hi :where [?e :r/born ?b] [(<= ?lo ?b)] [(< ?b ?hi)]] :D1 :D2]
    ['[:find ?e ?c :in $ ?lo ?hi :where [?e :r/city ?c] [(<= ?lo ?c)] [(< ?c ?hi)]] "A" "b"]
    ;; predicates after other patterns, bounds from functions
    ['[:find ?e ?a :where [?e :r/age ?a] [?e :r/n ?n] [(< ?a 10)]]]
    ['[:find ?e ?a :where [?e :r/age ?a] [(< ?a 10)] [?e :r/n ?n] [(> ?a 0)]]]
    ['[:find ?e ?a :where [?e :r/age ?a] [(+ 5 5) ?hi] [(< ?a ?hi)]]]
    ['[:find ?e ?a :where [?e :r/age ?a] [(< ?a ?hi)] [(+ 5 5) ?hi]]]
    ['[:find ?e ?a ?a2 :where [?e :r/age ?a] [?e :r/ages ?a2] [(< ?a ?a2)]]]
    ['[:find ?e ?a :where [?e :r/age ?a] [(< ?a 10) ?r]]]
    ;; repeated keys (per-key seek dedupe) and odd values in :in
    ['[:find ?e ?n :in $ [?a ...] :where [?e :r/age ?a] [?e :r/n ?n]] [7 7 20 20 5]]
    ['[:find ?e ?t :in $ [?a ...] :where [?e :r/age ?a] [?e :r/ages ?t]] [7 7 20 20 5]]
    ['[:find ?e ?n :in $ [?a ...] :where [?e :r/age ?a] [?e :r/n ?n]] [7 7.0 20 "20" nil]]
    ['[:find ?e ?c :in $ [?a ...] :where [?e :r/age ?a] [?e :r/city ?c]] [1 1 :x]]]))

(defn- subst [x db]
  (case x :DB db :D1 d1 :D2 d2 x))

(defn- row [r]
  (cond (and (coll? r) (not (map? r))) (mapv row r)
        (instance? java.util.Collection r) (mapv row r)
        (instance? Float r) (double r)
        :else r))

(defn- canon [r]
  (cond (instance? java.util.Set r) [:set (set (map row r))]
        (vector? r) [:vec (frequencies (map row r))]
        :else [:val (row r)]))

(defn- error-kind [^Throwable t]
  (or (:db/error (ex-data t)) (some-> (.getCause t) ex-data :db/error)))

(defn- run-with [q-fn q args db]
  (try {:ok (canon (apply q-fn q (map #(subst % db) args)))}
       (catch Throwable t {:error (error-kind t) :msg (ex-message t)})))

(defn- compare-one [[q & args] db-d db-m]
  (let [args (cons :DB args)
        d (run-with d/q q args db-d)
        mm (run-with m/q q args db-m)]
    (cond
      (and (contains? d :ok) (contains? mm :ok))
      (or (= d mm) {:q q :args args :datomic d :mnemonic mm})

      (and (contains? d :error) (contains? mm :error))
      (or (nil? (:error d)) (= (:error d) (:error mm)) {:q q :datomic d :mnemonic mm})

      :else {:q q :args args :datomic d :mnemonic mm})))

(defn- check-all [db-d db-m]
  (doseq [qa queries
          :let [r (compare-one qa db-d db-m)]]
    (is (true? r) (pr-str r))))

(deftest range-queries-equal-datomic
  (o/with-both [cd cm]
    (doseq [tx [schema people]]
      (let [{:keys [datomic mnemonic]} (o/transact-both [cd cm] tx)]
        (assert (not (:error datomic)) (pr-str datomic))
        (assert (not (:error mnemonic)) (pr-str mnemonic))))
    (check-all (d/db cd) (m/db cm))))

(deftest range-queries-equal-datomic-over-time-views
  (o/with-both [cd cm]
    (doseq [tx [schema people]]
      (o/transact-both [cd cm] tx))
    (let [t0 (d/basis-t (d/db cd))
          upd [[:db/add [:r/n "n1"] :r/age 15] [:db/retract [:r/n "n3"] :r/age 30]
               [:db/add [:r/n "n4"] :r/age 20] [:db/add [:r/n "n5"] :r/score 2.5]]]
      (o/transact-both [cd cm] upd)
      (check-all (d/as-of (d/db cd) t0) (m/as-of (m/db cm) t0))
      (check-all (d/since (d/db cd) t0) (m/since (m/db cm) t0))
      (check-all (d/history (d/db cd)) (m/history (m/db cm)))
      (check-all (d/db cd) (m/db cm)))))
