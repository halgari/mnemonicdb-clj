(ns mnemonic.diff-query-range-test
  "Differential tests for range / equality predicates on a pattern's value variable
  (AVET range pushdown): long, double, string and instant attributes, mixed-type and nil
  bounds, bounds bound from :in, and the as-of / since / history views."
  (:require [clojure.test :refer [deftest is]]
            [clojure.walk]
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
    :db/index true}
   {:db/id "g" :db/ident :r/friend :db/valueType :db.type/ref :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/id "h" :db/ident :r/friends :db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   {:db/id "i" :db/ident :r/kw :db/valueType :db.type/keyword :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/id "j" :db/ident :r/uid :db/valueType :db.type/uuid :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/id "k" :db/ident :r/flag :db/valueType :db.type/boolean :db/cardinality :db.cardinality/one}
   {:db/id "l" :db/ident :r/big :db/valueType :db.type/bigint :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/id "n" :db/ident :r/nan :db/valueType :db.type/double :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/id "m" :db/ident :r/scores :db/valueType :db.type/double :db/cardinality :db.cardinality/many
    :db/index true}])

(def ^:private ages [Long/MIN_VALUE -5 -1 0 1 7 7 10 20 20 20 30 99 1000 Long/MAX_VALUE])
(def ^:private scores [-1.5 -0.0 0.0 0.5 2.5 2.5 3.5 1e10 Double/POSITIVE_INFINITY
                       Double/NEGATIVE_INFINITY])
(def ^:private cities ["" "A" "Ab" "b" "～" "😀" "zz" "X" "a\u0000b" "a" "ab" "\ud83d" "\uffff"])
;; Datomic's own as-of / since / history reads are unreliable for attributes holding NaN
;; (its index order breaks), so NaN lives in :r/nan and is queried on the current db only
(def ^:private nans [Double/NaN 1.0 -0.0 0.0 Double/NaN Double/POSITIVE_INFINITY 2.5 Double/NEGATIVE_INFINITY])
(def ^:private kws [:a :b/c :b/d :zz])
(def ^:private uids [#uuid "00000000-0000-0000-0000-000000000001" #uuid "00000000-0000-0000-0000-000000000009"
                     #uuid "ffffffff-ffff-ffff-ffff-ffffffffffff"])

(def ^:private people
  (vec (for [i (range 40)]
         (cond-> {:db/id (str "p" i) :r/n (str "n" i)
                  :r/age (nth ages (mod i (count ages)))
                  :r/score (nth scores (mod (* 3 i) (count scores)))
                  :r/born (java.util.Date. (* 86400000 (long (- (mod (* 7 i) 23) 5))))
                  :r/city (nth cities (mod i (count cities)))}
           (even? i) (assoc :r/ages [(nth ages (mod (+ i 3) (count ages)))
                                     (nth ages (mod (+ i 5) (count ages)))])
           (not (zero? (mod i 3))) (assoc :r/nan (nth nans (mod i (count nans))))
           (not (zero? (mod i 3))) (assoc :r/friend (str "p" (mod (* 7 i) 40)))
           (even? i) (assoc :r/friends [(str "p" (mod (+ i 1) 40)) (str "p" (mod (+ i 9) 40))])
           (not (zero? (mod i 4))) (assoc :r/kw (nth kws (mod i (count kws))))
           (not (zero? (mod i 5))) (assoc :r/uid (nth uids (mod i (count uids))))
           (not (zero? (mod i 7))) (assoc :r/flag (even? (quot i 3)))
           (not (zero? (mod i 6))) (assoc :r/big (nth [-5N 0N 7N 100N 12345678901234567890N] (mod i 5)))
           (odd? i) (assoc :r/scores [(nth scores (mod (+ i 2) (count scores)))
                                      (nth scores (mod (* 5 i) (count scores)))])))))

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


(def ^:private class-values
  "One value per class of the comparison domain, for cross-class comparisons via :in."
  [1 -7 2.5 ##NaN -0.0 0.0 3N 1.5M "a" "b" "" :k :j/l 'sym true false
   #inst "1970-01-01T00:00:00.005-00:00" #uuid "00000000-0000-0000-0000-000000000001" [1 2] [1 3] [1]
   (java.net.URI. "http://a")])

(def ^:private cross-class-queries
  (concat
   ;; every pair of the domain through the four ordering built-ins and =
   (for [op '[< <= > >= =]]
     [`[:find ~'?x ~'?y :in ~'$ ~'[?x ...] ~'[?y ...] :where [(~op ~'?x ~'?y)]] class-values class-values])
   ;; attribute values against bounds of every class (post-filter and no pushdown)
   (for [attr [:r/age :r/ages :r/score :r/scores :r/city :r/born :r/friend :r/friends :r/kw :r/uid :r/big]
         op '[< <= > >= =]
         bound [5 2.5 "b" :b/c #inst "1970-01-05" #uuid "00000000-0000-0000-0000-000000000009" [1 2] true 7N]]
     [`[:find ~'?e ~'?v :where [~'?e ~attr ~'?v] [(~op ~'?v ~bound)]]])
   ;; both operand orders, bounds from :in
   (for [attr [:r/age :r/city :r/born :r/score]
         op '[< >=]]
     [`[:find ~'?e ~'?v :in ~'$ [~'?b ~'...] :where [~'?e ~attr ~'?v] [(~op ~'?b ~'?v)]]
      [5 "b" :b/c #inst "1970-01-05" 2.5]])))

(def ^:private range-queries-2
  [;; strings: bounds, prefix relations, NUL, supplementary / BMP-high / lone surrogate chars
   ['[:find ?e ?c :where [?e :r/city ?c] [(< ?c "a")]]]
   ['[:find ?e ?c :where [?e :r/city ?c] [(<= ?c "a")]]]
   ['[:find ?e ?c :where [?e :r/city ?c] [(> ?c "a")] [(< ?c "ab")]]]
   ['[:find ?e ?c :where [?e :r/city ?c] [(>= ?c "a")] [(<= ?c "ab")]]]
   ['[:find ?e ?c :where [?e :r/city ?c] [(> ?c "a\u0000")]]]
   ['[:find ?e ?c :where [?e :r/city ?c] [(< ?c "a\u0000c")]]]
   ['[:find ?e ?c :where [?e :r/city ?c] [(>= ?c "")]]]
   ['[:find ?e ?c :where [?e :r/city ?c] [(> ?c "")]]]
   ['[:find ?e ?c :where [?e :r/city ?c] [(< ?c "")]]]
   ['[:find ?e ?c :where [?e :r/city ?c] [(= ?c "")]]]
   ['[:find ?e ?c :where [?e :r/city ?c] [(>= ?c "\ud83d")]]]
   ['[:find ?e ?c :where [?e :r/city ?c] [(< ?c "\ud83d")]]]
   ['[:find ?e ?c :where [?e :r/city ?c] [(> ?c "😀")]]]
   ['[:find ?e ?c :where [?e :r/city ?c] [(>= ?c "😀")] [(<= ?c "\uffff")]]]
   ['[:find ?e ?c :where [?e :r/city ?c] [(> ?c "\uffff")]]]
   ['[:find ?e ?c :where [?e :r/city ?c] [(<= "b" ?c)] [(< ?c "zz")]]]
   ['[:find ?e ?c :where [?e :r/city ?c] [(> "b" ?c)]]]
   ['[:find ?e ?c ?n :where [?e :r/city ?c] [(>= ?c "A")] [(< ?c "b")] [?e :r/n ?n]]]
   ['[:find ?e ?c :in $ ?lo ?hi :where [?e :r/city ?c] [(<= ?lo ?c)] [(< ?c ?hi)]] "Ab" "zz"]
   ['[:find ?e ?c :in $ [[?lo ?hi]] :where [?e :r/city ?c] [(<= ?lo ?c)] [(< ?c ?hi)]]
    [["" "a"] ["a" "b"] ["～" "😀"] ["zz" "zzz"]]]
   ;; refs (plain longs in the index)
   ['[:find ?e ?f :in $ ?lo :where [?e :r/friend ?f] [(< ?f ?lo)]] :E3]
   ['[:find ?e ?f :in $ ?lo :where [?e :r/friend ?f] [(< ?f ?lo)]] :E10]
   ['[:find ?e ?f :in $ ?lo :where [?e :r/friend ?f] [(>= ?f ?lo)]] :E10]
   ['[:find ?e ?f :in $ ?lo ?hi :where [?e :r/friend ?f] [(> ?f ?lo)] [(<= ?f ?hi)]] :E3 :E20]
   ['[:find ?e ?f :in $ ?lo :where [?e :r/friends ?f] [(> ?f ?lo)]] :E10]
   ['[:find ?e ?f :in $ ?lo :where [?e :r/friend ?f] [(= ?f ?lo)]] :E5]
   ['[:find ?e ?f :in $ ?lo :where [?e :r/friend ?f] [(< ?f ?lo)]] 5]
   ['[:find ?e ?f :in $ ?lo :where [?e :r/friend ?f] [(< ?f ?lo)]] :a/b]
   ['[:find ?e ?f :in $ ?lo :where [?e :r/friend ?f] [(> ?f ?lo)]] Long/MAX_VALUE]
   ['[:find ?e ?f :in $ ?lo :where [?e :r/friend ?f] [(< ?f ?lo)]] Long/MIN_VALUE]
   ;; doubles: NaN, -0.0 / 0.0, infinities
   ['[:find ?e ?s :where [?e :r/score ?s] [(<= ?s 2.5)]]]
   ['[:find ?e ?s :where [?e :r/score ?s] [(>= ?s 2.5)]]]
   ['[:find ?e ?s :where [?e :r/score ?s] [(<= ?s 0.0)]]]
   ['[:find ?e ?s :where [?e :r/score ?s] [(<= ?s -0.0)]]]
   ['[:find ?e ?s :where [?e :r/score ?s] [(>= ?s 0.0)] [(<= ?s 0.5)]]]
   ['[:find ?e ?s :where [?e :r/score ?s] [(>= ?s -0.0)] [(<= ?s 0.0)]]]
   ['[:find ?e ?s :where [?e :r/score ?s] [(> ?s -0.0)]]]
   ['[:find ?e ?s :where [?e :r/score ?s] [(< ?s 0.0)]]]
   ['[:find ?e ?s :where [?e :r/score ?s] [(< ?s -0.0)]]]
   ['[:find ?e ?s :where [?e :r/score ?s] [(= ?s -0.0)]]]
   ['[:find ?e ?s :where [?e :r/score ?s] [(= ?s 0.0)]]]
   ['[:find ?e ?s :where [?e :r/score ?s] [(< ?s ##Inf)]]]
   ['[:find ?e ?s :where [?e :r/score ?s] [(<= ?s ##Inf)]]]
   ['[:find ?e ?s :where [?e :r/score ?s] [(> ?s ##-Inf)]]]
   ['[:find ?e ?s :where [?e :r/score ?s] [(>= ?s ##-Inf)]]]
   ['[:find ?e ?s :where [?e :r/score ?s] [(= ?s ##Inf)]]]
   ['[:find ?e ?s :where [?e :r/score ?s] [(< ?s ##NaN)]]]
   ['[:find ?e ?s :where [?e :r/score ?s] [(<= ?s ##NaN)]]]
   ['[:find ?e ?s :where [?e :r/score ?s] [(> ?s ##NaN)]]]
   ['[:find ?e ?s :where [?e :r/score ?s] [(>= ?s ##NaN)]]]
   ['[:find ?e ?s :where [?e :r/score ?s] [(= ?s ##NaN)]]]
   ['[:find ?e ?s :where [?e :r/score ?s] [(>= ?s 1.0)] [(< ?s ##NaN)]]]
   ['[:find ?e ?s :where [?e :r/score ?s] [(>= ?s 1.0)] [(<= ?s 1e10)]]]
   ['[:find ?e ?s :where [?e :r/score ?s] [(> ?s 1.0)] [(<= ?s 1e10)]]]
   ['[:find ?e ?s :where [?e :r/scores ?s] [(<= ?s 2.5)]]]
   ['[:find ?e ?s :where [?e :r/scores ?s] [(>= ?s 0.0)] [(< ?s 3.0)]]]
   ['[:find ?e ?s :in $ ?lo :where [?e :r/score ?s] [(>= ?s ?lo)]] -0.0]
   ['[:find ?e ?s :in $ ?lo :where [?e :r/score ?s] [(<= ?s ?lo)]] 0.0]
   ['[:find ?e ?s :in $ ?lo :where [?e :r/score ?s] [(<= ?s ?lo)]] ##NaN]
   ['[:find ?e ?s :in $ [?lo ...] :where [?e :r/score ?s] [(>= ?s ?lo)] [(< ?s 3.0)]] [0.0 -0.0 ##NaN 1.0]]
   ['[:find ?e ?s :in $ [?lo ...] :where [?e :r/score ?s] [(>= ?s ?lo)] [(< ?s 3.0)]] [0.0 -0.0 1.0]]
   ['[:find ?e ?s :in $ ?lo :where [?e :r/score ?s] [(>= ?s ?lo)]] 1]
   ['[:find ?e ?s :in $ ?lo :where [?e :r/score ?s] [(>= ?s ?lo)]] (float 1.0)]
   ;; joins: the pushed range plus a relation larger than the hash-join minimum
   ['[:find ?e ?c ?a :where [?e :r/n ?n] [?e :r/age ?a] [?e :r/city ?c] [(< ?c "b")] [(> ?a 0)]]]
   ['[:find ?e ?s ?a :where [?e :r/age ?a] [(> ?a 0)] [?e :r/score ?s] [(<= ?s 2.5)]]]
   ['[:find ?e ?f ?a :in $ ?hi :where [?e :r/age ?a] [(>= ?a 7)] [?e :r/friend ?f] [(< ?f ?hi)]] :E20]
   ['[:find ?e ?b :where [?e :r/age ?a] [?e :r/born ?b] [(< ?b #inst "1970-01-10")]]]
   ['[:find ?e ?k :where [?e :r/age ?a] [(> ?a 0)] [?e :r/kw ?k]]]
   ['[:find ?e ?k ?u :where [?e :r/kw ?k] [?e :r/uid ?u]]]
   ['[:find ?e ?k ?u :where [?e :r/kw ?k] [?e :r/uid ?u] [(< ?k :b/d)]]]
   ['[:find ?e ?u :where [?e :r/uid ?u] [(> ?u #uuid "00000000-0000-0000-0000-000000000005")]]]
   ['[:find ?e ?bg :where [?e :r/big ?bg] [(> ?bg 6N)]]]
   ['[:find ?e ?bg :where [?e :r/big ?bg] [(< ?bg 10)]]]])

(defn- nan-query
  "`q` over :r/nan instead of :r/score."
  [q]
  (clojure.walk/postwalk-replace {:r/score :r/nan} q))

(defn- with-barrier
  "`q` with a function clause right after its first data pattern, which stops the range
  pushdown (the hints only look at the predicates directly following the pattern)."
  [q]
  (let [[head where] (split-with #(not= :where %) q)
        where (rest where)
        [pre [pat & post]] (split-with #(not (and (vector? %) (not (list? (first %))))) where)
        e (first pat)]
    (vec (concat head [:where] pre [pat [(list 'identity e) '?barrier]] post))))

(defn- subst [x db entid-fn]
  (cond
    (= x :DB) db
    (= x :D1) d1
    (= x :D2) d2
    (and (keyword? x) (re-matches #"E\d+" (name x))) (entid-fn db [:r/n (str "n" (subs (name x) 1))])
    :else x))

(defn- row [r]
  (cond (and (coll? r) (not (map? r))) (mapv row r)
        (instance? java.util.Collection r) (mapv row r)
        (instance? Float r) (double r)
        (and (instance? Double r) (Double/isNaN r)) :nan
        :else r))

(defn- canon [r]
  (cond (instance? java.util.Set r) [:set (set (map row r))]
        (vector? r) [:vec (frequencies (map row r))]
        :else [:val (row r)]))

(defn- error-kind [^Throwable t]
  (or (:db/error (ex-data t)) (some-> (.getCause t) ex-data :db/error)))

(defn- run-with [q-fn entid-fn q args db]
  (try {:ok (canon (apply q-fn q (map #(subst % db entid-fn) args)))}
       (catch Throwable t {:error (error-kind t) :msg (ex-message t)})))

(defn- compare-one [[q & args] db-d db-m]
  (let [args (cons :DB args)
        d (run-with d/q d/entid q args db-d)
        mm (run-with m/q m/entid q args db-m)]
    (cond
      (and (contains? d :ok) (contains? mm :ok))
      (or (= d mm) {:q q :args args :datomic d :mnemonic mm})

      (and (contains? d :error) (contains? mm :error))
      (or (nil? (:error d)) (= (:error d) (:error mm)) {:q q :datomic d :mnemonic mm})

      :else {:q q :args args :datomic d :mnemonic mm})))

(defn- summarize
  "A failure report small enough to read: the difference between the two sides."
  [r]
  (if (map? r)
    (let [side (fn [k] (let [x (get-in r [k :ok])]
                         (if (and (vector? x) (#{:set :vec} (first x)))
                           (let [c (second x)] (if (map? c) (set (keys c)) c))
                           x)))
          d (side :datomic) m (side :mnemonic)]
      (cond-> {:q (:q r) :args (:args r)}
        (and (set? d) (set? m)) (assoc :only-datomic (take 5 (remove m d)) :only-mnemonic (take 5 (remove d m)))
        (not (and (set? d) (set? m))) (assoc :datomic (:datomic r) :mnemonic (:mnemonic r))))
    r))

(defn- check-all [label db-d db-m & [qs]]
  (doseq [qa (or qs (concat queries range-queries-2 cross-class-queries))
          :let [r (compare-one qa db-d db-m)]]
    (is (true? r) (str label " " (pr-str (summarize r))))))

(deftest range-queries-equal-datomic
  (o/with-both [cd cm]
    (doseq [tx [schema people]]
      (let [{:keys [datomic mnemonic]} (o/transact-both [cd cm] tx)]
        (assert (not (:error datomic)) (pr-str datomic))
        (assert (not (:error mnemonic)) (pr-str mnemonic))))
    (check-all :current (d/db cd) (m/db cm))))

(deftest range-queries-equal-datomic-over-time-views
  (o/with-both [cd cm]
    (doseq [tx [schema people]]
      (o/transact-both [cd cm] tx))
    (let [t0 (d/basis-t (d/db cd))
          upd [[:db/add [:r/n "n1"] :r/age 15] [:db/retract [:r/n "n3"] :r/age 30]
               [:db/add [:r/n "n4"] :r/age 20] [:db/add [:r/n "n5"] :r/score 2.5]]]
      (o/transact-both [cd cm] upd)
      (check-all :as-of (d/as-of (d/db cd) t0) (m/as-of (m/db cm) t0))
      (check-all :since (d/since (d/db cd) t0) (m/since (m/db cm) t0))
      (check-all :history (d/history (d/db cd)) (m/history (m/db cm)))
      (check-all :current-after (d/db cd) (m/db cm)))))

(deftest pushdown-equals-plain-scan
  ;; Datomic's own results are unreliable for NaN in an index, so the NaN cases (and every
  ;; other range query) are checked against the same query with the pushdown switched off
  (o/with-both [cd cm]
    (doseq [tx [schema people]]
      (o/transact-both [cd cm] tx))
    (let [db (m/db cm)]
      (doseq [[q & args] (concat queries range-queries-2)
              q (if (some #{:r/score} (flatten q)) [q (nan-query q)] [q])
              :let [args (map #(subst % db m/entid) args)
                    run (fn [q] (try {:ok (canon (apply m/q q db args))}
                                     (catch Throwable t {:error (error-kind t)})))
                    pushed (run q)
                    plain (run (with-barrier q))]]
        (is (= plain pushed) (pr-str q args))))))
