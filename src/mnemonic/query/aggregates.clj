(ns mnemonic.query.aggregates
  "Aggregate functions for :find (`(count ?x)`, `(min 3 ?x)`, `(my.ns/f ?x)`).

  `resolve-agg` turns a parsed aggregate element into a function of the vector of values
  collected for one group. Semantics follow Datomic 1.0.7745 (probed):
  - `count` / `count-distinct` return Integer; `avg`, `variance`, `stddev` return double
    (variance and stddev are population statistics);
  - `median` of an even count is `(quot (+ a b) 2)` of the middle pair;
  - `(min n ?x)` / `(max n ?x)` return a vector of the n smallest / largest values,
    `(sample n ?x)` a vector of up to n distinct values, `(rand n ?x)` n random draws;
  - any other function is called with its constant arguments followed by the collection."
  (:require [mnemonic.query.fns :as fns]))

(set! *warn-on-reflection* true)

(defn- sorted [vs] (sort compare vs))

(defn- median [vs]
  (let [s (vec (sorted vs))
        n (count s)
        h (quot n 2)]
    (if (odd? n)
      (nth s h)
      (quot (+ (nth s (dec h)) (nth s h)) 2))))

(defn- variance ^double [vs]
  (let [n (count vs)
        mean (/ (double (reduce + vs)) n)]
    ;; same left-to-right sum as before, without a lazy seq of the squares
    (/ (double (reduce (fn [acc x] (let [d (- (double x) mean)] (+ (double acc) (* d d)))) 0.0 vs)) n)))

(defn- extreme [cmp vs]
  (reduce (fn [a b] (if (neg? (cmp b a)) b a)) vs))

(def ^:private builtins
  "Aggregate symbol -> {arities (counts of constant args) -> (fn [consts vs])}."
  {'count          {0 (fn [_ vs] (int (count vs)))}
   'count-distinct {0 (fn [_ vs] (int (count (set vs))))}
   'sum            {0 (fn [_ vs] (reduce + vs))}
   'avg            {0 (fn [_ vs] (/ (double (reduce + vs)) (count vs)))}
   'median         {0 (fn [_ vs] (median vs))}
   'variance       {0 (fn [_ vs] (variance vs))}
   'stddev         {0 (fn [_ vs] (Math/sqrt (variance vs)))}
   'distinct       {0 (fn [_ vs] (set vs))}
   'min            {0 (fn [_ vs] (extreme compare vs))
                    1 (fn [[n] vs] (vec (take n (sorted vs))))}
   'max            {0 (fn [_ vs] (extreme #(compare %2 %1) vs))
                    1 (fn [[n] vs] (vec (take n (sort #(compare %2 %1) vs))))}
   'rand           {0 (fn [_ vs] (rand-nth (vec vs)))
                    1 (fn [[n] vs] (let [v (vec vs)] (doall (repeatedly n #(rand-nth v)))))}
   'sample         {1 (fn [[n] vs] (vec (take n (shuffle (vec (distinct vs))))))}})

(defn- invalid-aggregate [sym]
  (fns/qerror :db.error/invalid-aggregate
              (str "Argument " sym " in :find is not an aggregate function")
              {:aggregate sym}))

(defn- custom-fn
  "The function for a non-built-in aggregate symbol: qualified symbols (auto-required),
  or unqualified clojure.core functions."
  [sym]
  (if (namespace sym)
    (fns/resolve-fn sym)
    (let [v (ns-resolve (the-ns 'clojure.core) sym)]
      (if (and v (fn? @v)) @v (throw (invalid-aggregate sym))))))

;; ------------------------------------------------------------------ streaming

;; The built-ins that fold their values one at a time: a group keeps an accumulator
;; instead of the list of its values. Each folds in the order the values come with the
;; same operations as its function above (`reduce` with the first value as the start),
;; so results, and the errors bad values raise, are the same.

(definterface IAcc
  (add [v])
  (result []))

(deftype CountAcc [^:unsynchronized-mutable ^long n]
  IAcc
  (add [_ _] (set! n (inc n)) nil)
  (result [_] (int n)))

(deftype SumAcc [^:unsynchronized-mutable s ^:unsynchronized-mutable ^long n]
  IAcc
  (add [_ v] (set! s (if (zero? n) v (+ s v))) (set! n (inc n)) nil)
  (result [_] s))

(deftype AvgAcc [^:unsynchronized-mutable s ^:unsynchronized-mutable ^long n]
  IAcc
  (add [_ v] (set! s (if (zero? n) v (+ s v))) (set! n (inc n)) nil)
  (result [_] (/ (double s) n)))

;; `lo?`: keeps the smallest (min), else the largest (max), first one on ties
(deftype ExtremeAcc [^boolean lo? ^:unsynchronized-mutable x ^:unsynchronized-mutable ^boolean any]
  IAcc
  (add [_ v]
    (if any
      (when (neg? (if lo? (compare v x) (compare x v))) (set! x v))
      (do (set! x v) (set! any (boolean true))))
    nil)
  (result [_] x))

(defn accumulator
  "A no-argument function making a fresh accumulator (IAcc) for the parsed aggregate
  element `elem` when it is a built-in that can fold its values one at a time, else nil."
  [{:keys [agg args]}]
  (when (== 1 (count args))
    (case agg
      count #(CountAcc. 0)
      sum #(SumAcc. nil 0)
      avg #(AvgAcc. nil 0)
      min #(ExtremeAcc. true nil false)
      max #(ExtremeAcc. false nil false)
      nil)))

(defn resolve-agg
  "A function of the vector of values for the parsed aggregate element `elem`
  ({:type :agg :agg sym :args [terms]}). Throws for unknown aggregates and bad arities."
  [{:keys [agg args]}]
  (let [consts (mapv :val (butlast args))]
    (if-let [arities (get builtins agg)]
      (if-let [f (get arities (count consts))]
        (fn [vs] (f consts vs))
        (throw (fns/qerror :db.error/invalid-query
                           (str "Wrong number of arguments to aggregate " agg)
                           {:aggregate agg :args (count args)})))
      (let [f (custom-fn agg)]
        (fn [vs] (apply f (conj consts vs)))))))
