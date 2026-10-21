(ns mnemonic.diff-query-full-test
  "Differential tests for the complete query engine (rules, not/or, aggregates, pull in
  :find, qseq): the same queries run against Datomic (oracle) and MnemonicDB must return
  equal results, or both fail (with the same error kind when Datomic reports one)."
  (:require [clojure.test :refer [deftest is testing]]
            [datomic.api :as d]
            [mnemonic.api :as m]
            [mnemonic.oracle :as o]))

(def ^:private one :db.cardinality/one)
(def ^:private many :db.cardinality/many)

(defn- attr-tx [specs]
  (for [[id vt card & {:as opts}] specs]
    (merge {:db/id (str "attr-" id) :db/ident id :db/valueType vt :db/cardinality card} opts)))

(def ^:private schema
  (attr-tx
   [[:p/name :db.type/string one :db/unique :db.unique/identity]
    [:p/age :db.type/long one]
    [:p/city :db.type/string one :db/index true]
    [:p/tags :db.type/string many]
    [:p/boss :db.type/ref one]
    [:p/friends :db.type/ref many]
    [:p/kind :db.type/ref one]
    [:p/score :db.type/double one]
    [:p/born :db.type/instant one]
    [:p/key :db.type/keyword one]]))

(def ^:private idents [{:db/ident :kind/a} {:db/ident :kind/b}])

(def ^:private people
  [{:db/id "al" :p/name "Al" :p/age 10 :p/city "X" :p/tags ["x" "y"] :p/kind :kind/a
    :p/score 2.5 :p/born #inst "1990-01-01" :p/key :a/b}
   {:db/id "bo" :p/name "Bo" :p/age 20 :p/city "X" :p/boss "al" :p/kind :kind/b :p/tags ["y" "z"]
    :p/score 3.5 :p/born #inst "2000-01-01" :p/friends ["al"]}
   {:db/id "cy" :p/name "Cy" :p/age 30 :p/city "Y" :p/boss "bo" :p/friends ["al" "bo"] :p/key :c/d
    :p/score 0.1}
   {:db/id "di" :p/name "Di" :p/age 20 :p/city "Z" :p/boss "cy" :p/tags ["x"] :p/kind :kind/a
    :p/score 0.2}
   {:db/id "ed" :p/name "Ed"}])

(defn my-agg [xs] (reduce + 100 xs))
(defn my-agg-n [n xs] [n (count xs)])

(def ^:private anc
  '[[(anc ?x ?y) [?x :p/boss ?y]]
    [(anc ?x ?y) [?x :p/boss ?z] (anc ?z ?y)]])

(def ^:private rs
  '[[(nm ?e ?n) [?e :p/name ?n]]
    [(adult ?e) [?e :p/age ?a] [(> ?a 15)]]
    [(root ?e) [?e :p/name _] (not [?e :p/boss _])]
    [(special ?e) (or [?e :p/age 30] [?e :p/kind :kind/a])]
    [(tagged ?e) [?e :p/tags _]]
    [(tagged ?e) [?e :p/friends _]]
    [(older [?a] ?b) [?a :p/age ?aa] [?b :p/age ?ba] [(< ?aa ?ba)]]
    [(ev ?x) [?x :p/age 10]]
    [(ev ?x) [?x :p/boss ?y] (od ?y)]
    [(od ?x) [?x :p/boss ?y] (ev ?y)]])

(def ^:private path
  '[[(path ?a ?b) [?a ?b]] [(path ?a ?b) [?a ?c] (path ?c ?b)]])

(def ^:private queries
  "[query & args]; :DB :DB2 :HIST :ASOF stand for dbs under test."
  [;; ---- rules
   ['[:find ?xn ?yn :in $ % :where (anc ?x ?y) [?x :p/name ?xn] [?y :p/name ?yn]] :DB anc]
   ['[:find ?yn :in $ % :where [?x :p/name "Di"] (anc ?x ?y) [?y :p/name ?yn]] :DB anc]
   ['[:find ?xn :in $ % :where [?y :p/name "Al"] (anc ?x ?y) [?x :p/name ?xn]] :DB anc]
   ['[:find ?yn :in $ % :where (anc [:p/name "Di"] ?y) [?y :p/name ?yn]] :DB anc]
   ['[:find ?xn :in $ % :where (anc ?x [:p/name "Al"]) [?x :p/name ?xn]] :DB anc]
   ['[:find ?x :in $ % :where (anc ?x _)] :DB anc]
   ['[:find ?x :in $ % :where (anc _ ?x)] :DB anc]
   ['[:find ?x :in $ % :where (anc ?x ?x)] :DB anc]
   ['[:find ?x :in $ % :where (anc 1 ?x)] :DB anc]
   ['[:find ?n :in $ % :where (ev ?x) [?x :p/name ?n]] :DB rs]
   ['[:find ?n :in $ % :where (od ?x) [?x :p/name ?n]] :DB rs]
   ['[:find ?bn :in $ % :where [?a :p/name "Bo"] (older ?a ?b) [?b :p/name ?bn]] :DB rs]
   ['[:find ?bn :in $ % :where (older ?a ?b) [?a :p/name "Bo"] [?b :p/name ?bn]] :DB rs]
   ['[:find ?bn :in $ % :where (older [:p/name "Cy"] ?b) [?b :p/name ?bn]] :DB rs]
   ['[:find ?bn :in $ % :where (older ?a ?b) [?b :p/name ?bn]] :DB rs]
   ['[:find ?e :in $ % :where (adult ?e)] :DB rs]
   ['[:find ?e :in $ % :where (root ?e)] :DB rs]
   ['[:find ?e :in $ % :where (special ?e)] :DB rs]
   ['[:find ?e :in $ % :where (tagged ?e)] :DB rs]
   ['[:find ?e :in $ % :where [?e :p/name _] (not (adult ?e))] :DB rs]
   ['[:find ?e :in $ % :where (not (adult ?e)) [?e :p/name _]] :DB rs]
   ['[:find ?n :in $ $2 % :where ($2 nm ?e ?n)] :DB :DB2 rs]
   ['[:find ?n :in $ $2 % :where ($2 adult ?e) [?e :p/name ?n]] :DB :DB2 rs]
   ['[:find ?e ?n :in $ % :where (nm ?e ?n)] :DB rs]
   ['[:find ?e :in $ % :where (nm ?e "Cy")] :DB rs]
   ['[:find (count ?x) :in $ % :where (anc ?x ?y)] :DB anc]
   ['[:find ?y (count ?x) :in $ % :where (anc ?x ?y)] :DB anc]
   ['[:find ?b :in $ % :where (path 1 ?b)] [[1 2] [2 3] [3 1]] path]
   ['[:find ?a ?b :in $ % :where (path ?a ?b)] [[1 2] [2 3] [3 4]] path]
   ['[:find ?x :in $ % :where (nope ?x)] :DB rs]
   ['[:find ?x :in $ :where (adult ?x)] :DB]
   ;; ---- not / or
   ['[:find ?n :where [?e :p/name ?n] (not [?e :p/age 20])] :DB]
   ['[:find ?n :where (not [?e :p/age 20]) [?e :p/name ?n]] :DB]
   ['[:find ?n :where [?e :p/name ?n] (not [?e :p/age 20] [?e :p/boss _])] :DB]
   ['[:find ?n :where [?e :p/name ?n] (not [?e :p/age ?a] [(> ?a 15)])] :DB]
   ['[:find ?n :where [?e :p/name ?n] (not-join [?e] [?e :p/age ?a] [(> ?a 15)])] :DB]
   ['[:find ?n :where [?e :p/name ?n] (not-join [?e] [?e :p/boss ?b] [?b :p/age 20])] :DB]
   ['[:find ?n :where [?e :p/name ?n] [?b :p/age 10] (not [?e :p/boss ?b])] :DB]
   ['[:find ?n :where [?e :p/name ?n] (not [?e :p/name])] :DB]
   ['[:find ?n :where [?e :p/name ?n] (not (not [?e :p/age 20]))] :DB]
   ['[:find ?n :where [?e :p/name ?n] (not (or [?e :p/age 20] [?e :p/age 30]))] :DB]
   ['[:find ?n :where [?e :p/name ?n] (not [(= ?n "Al")])] :DB]
   ['[:find ?n :where [?e :p/name ?n] (not [?x :p/age 99])] :DB]
   ['[:find ?n :where [?e :p/name ?n] (not-join [?q] [?q :p/age 99])] :DB]
   ['[:find ?n :in $ $2 :where [?e :p/name ?n] ($2 not [?e :p/name "Al"])] :DB :DB2]
   ['[:find ?n :where [?e :p/name ?n] (or [?e :p/age 10] [?e :p/age 30])] :DB]
   ['[:find ?n :where (or [?e :p/age 10] [?e :p/age 30]) [?e :p/name ?n]] :DB]
   ['[:find ?e :where (or [?e :p/age 10] [?e :p/boss _] [?e :p/city "Z"])] :DB]
   ['[:find ?e ?a :where (or [?e :p/age ?a] [?e :p/boss ?a])] :DB]
   ['[:find ?n :where [?e :p/name ?n] (or [(= ?n "Al")] [(= ?n "Bo")])] :DB]
   ['[:find ?e :where (or (and [?e :p/age 20] [?e :p/boss _]) [?e :p/age 10])] :DB]
   ['[:find ?e :where (or-join [?e] (and [?e :p/age 20] [?e :p/boss ?b]) [?e :p/age 10])] :DB]
   ['[:find ?e :where [?e :p/name _] (or-join [[?e]] [?e :p/age 10] [?e :p/age 30])] :DB]
   ['[:find ?e :where (or-join [[?e]] [?e :p/age 10] [?e :p/age 30]) [?e :p/name _]] :DB]
   ['[:find ?e ?x :where (or-join [[?e] ?x] [?e :p/age ?x] [?e :p/boss ?x]) [?e :p/name _]] :DB]
   ['[:find ?e :where (or-join [[?e]] [?e :p/age 10] [?e :p/age 30])] :DB]
   ['[:find ?e :where [?e :p/name _] (or [?e :p/age 10] [?e :p/boss ?b])] :DB]
   ['[:find ?e :where [?e :p/name _] (or-join [?e] [?e :p/age 99])] :DB]
   ['[:find ?n :with ?e :where [?e :p/name ?n] (or [?e :p/age 20] [?e :p/city "X"])] :DB]
   ['[:find (count ?e) :where (or [?e :p/age 20] [?e :p/boss _])] :DB]
   ['[:find ?n :where [?e :p/name ?n] (or [(= ?n "Al")] [?e :p/age 20])] :DB]
   ;; ---- aggregates
   ['[:find (count ?a) :where [?e :p/age ?a]] :DB]
   ['[:find (count ?a) :with ?e :where [?e :p/age ?a]] :DB]
   ['[:find (count-distinct ?a) :with ?e :where [?e :p/age ?a]] :DB]
   ['[:find (sum ?a) :where [?e :p/age ?a]] :DB]
   ['[:find (sum ?a) :with ?e :where [?e :p/age ?a]] :DB]
   ['[:find (avg ?a) :with ?e :where [?e :p/age ?a]] :DB]
   ['[:find (min ?a) (max ?a) :where [?e :p/age ?a]] :DB]
   ['[:find (min ?n) (max ?n) :where [?e :p/name ?n]] :DB]
   ['[:find (min ?b) (max ?b) :where [?e :p/born ?b]] :DB]
   ['[:find (min 2 ?a) (max 2 ?a) :with ?e :where [?e :p/age ?a]] :DB]
   ['[:find (min 9 ?a) (max 0 ?a) :where [?e :p/age ?a]] :DB]
   ['[:find (median ?a) :with ?e :where [?e :p/age ?a]] :DB]
   ['[:find (median ?a) :where [?e :p/age ?a]] :DB]
   ['[:find (median ?s) :where [?e :p/score ?s]] :DB]
   ['[:find (variance ?a) (stddev ?a) :with ?e :where [?e :p/age ?a]] :DB]
   ['[:find (sum ?s) (avg ?s) :where [?e :p/score ?s]] :DB]
   ['[:find (distinct ?a) :with ?e :where [?e :p/age ?a]] :DB]
   ['[:find (distinct ?c) :where [?e :p/city ?c]] :DB]
   ['[:find ?c (count ?e) :where [?e :p/city ?c]] :DB]
   ['[:find ?c (sum ?a) (max ?a) :where [?e :p/city ?c] [?e :p/age ?a]] :DB]
   ['[:find ?c (count ?e) :with ?a :where [?e :p/city ?c] [?e :p/age ?a]] :DB]
   ['[:find ?n (count ?t) :where [?e :p/name ?n] [?e :p/tags ?t]] :DB]
   ['[:find ?n (count ?t) (distinct ?t) :where [?e :p/name ?n] [?e :p/tags ?t]] :DB]
   ['[:find (count ?e) . :where [?e :p/age ?a]] :DB]
   ['[:find [(count ?e) ...] :where [?e :p/age ?a]] :DB]
   ['[:find [(count ?e) (max ?a)] :where [?e :p/age ?a]] :DB]
   ['[:find (count ?e) (max ?a) :keys c m :where [?e :p/age ?a]] :DB]
   ['[:find (count ?e) :where [?e :p/age 999]] :DB]
   ['[:find ?a (count ?e) :where [?e :p/age ?a] [?e :p/age 999]] :DB]
   ['[:find (count ?e) . :where [?e :p/age 999]] :DB]
   ['[:find (sum ?a) :where [?e :p/age ?a] [(> ?a 15)]] :DB]
   ['[:find (count ?x) :in [?x ...]] [1 2 3 3]]
   ['[:find (sum ?x) (avg ?x) :in [?x ...]] [1 2.5]]
   ['[:find (sum ?x) :in [?x ...]] [1N 2N]]
   ['[:find (median ?x) :in [?x ...]] [1 100]]
   ['[:find (median ?x) :in [?x ...]] [1.0 2.0]]
   ['[:find (count-distinct ?x) :in [?x ...]] [1 1.0]]
   ['[:find (user-agg ?x) :in [?x ...]] [1 2]]
   ['[:find (mnemonic.diff-query-full-test/my-agg ?a) :where [?e :p/age ?a]] :DB]
   ['[:find (mnemonic.diff-query-full-test/my-agg ?a) :with ?e :where [?e :p/age ?a]] :DB]
   ['[:find ?c (mnemonic.diff-query-full-test/my-agg ?a) :where [?e :p/city ?c] [?e :p/age ?a]] :DB]
   ['[:find (mnemonic.diff-query-full-test/my-agg-n 7 ?a) :where [?e :p/age ?a]] :DB]
   ['[:find (clojure.core/count ?a) :where [?e :p/age ?a]] :DB]
   ['[:find (nope ?a) :where [?e :p/age ?a]] :DB]
   ['[:find (sum ?n) :where [?e :p/name ?n]] :DB]
   ['[:find (sum 3 ?a) :where [?e :p/age ?a]] :DB]
   ['[:find (count ?z) :where [?e :p/age ?a]] :DB]
   ;; ---- pull in :find
   ['[:find (pull ?e [:p/name]) :where [?e :p/age 20]] :DB]
   ['[:find (pull ?e [:p/age]) :where [?e :p/age 20]] :DB]
   ['[:find (pull ?e [:p/name :p/age]) :where [?e :p/city "X"]] :DB]
   ['[:find (pull ?e [*]) . :where [?e :p/name "Al"]] :DB]
   ['[:find (pull ?e [*]) :where [?e :p/name "Bo"]] :DB]
   ['[:find [(pull ?e [:p/name]) ...] :where [?e :p/age 20]] :DB]
   ['[:find (pull ?e [:p/name]) . :where [?e :p/age 99]] :DB]
   ['[:find (pull ?e [:p/name]) :where [?e :p/age 99]] :DB]
   ['[:find (pull ?e [:p/name {:p/boss [:p/name]}]) :where [?e :p/age 30]] :DB]
   ['[:find (pull ?e [:p/tags :p/friends]) :where [?e :p/name "Cy"]] :DB]
   ['[:find (pull ?e [(:p/name :as :n)]) . :where [?e :p/name "Cy"]] :DB]
   ['[:find (pull ?e [:p/name]) ?a :where [?e :p/age ?a]] :DB]
   ['[:find ?a (pull ?e [:p/name]) :where [?e :p/age ?a]] :DB]
   ['[:find (pull ?e [:p/name]) (count ?t) :where [?e :p/tags ?t]] :DB]
   ['[:find (pull ?e [:p/name]) ?a :keys who age :where [?e :p/age ?a] [?e :p/city "X"]] :DB]
   ['[:find [(pull ?e [:p/name]) ?a] :where [?e :p/name "Al"] [?e :p/age ?a]] :DB]
   ['[:find (pull $ ?e [:p/name]) . :where [?e :p/name "Al"]] :DB]
   ['[:find (pull $2 ?e [:p/name]) . :in $ $2 :where [$2 ?e :p/name "Al"]] :DB :DB2]
   ['[:find (pull ?e [:p/name]) . :in $ ?e :where] :DB [:p/name "Al"]]
   ['[:find (pull ?e [:p/name]) . :in $ ?e :where] :DB :kind/a]
   ['[:find (pull ?e [:db/ident]) :where [:p/age :db/valueType ?e]] :DB]
   ['[:find (pull ?e [:p/name]) :where [?e :p/age ?a] [(> ?a 15)] (not [?e :p/city "Z"])] :DB]
   ['[:find (pull ?e [:p/name]) :in $ % :where (anc ?e _)] :DB anc]
   ['[:find (pull ?e [:p/name]) . :where [?e :p/name "Al"]] :ASOF]
   ['[:find (pull ?e [:p/name]) . :where [?e :p/name "Al"]] :HIST]
   ['[:find (pull ?e [:p/name]) . :in $c :where [$c ?e]] [[1]]]
   ;; ---- bare-symbol pull pattern inputs
   ['[:find (pull ?e pattern) . :in $ pattern :where [?e :p/age 10]] :DB [:p/name]]
   ['[:find (pull ?e pattern) . :in $ pattern :where [?e :p/age 10]] :DB [:p/name {:p/boss [:p/name]}]]
   ['[:find (pull ?e pattern) :in $ pattern :where [?e :p/age 20]] :DB '[*]]
   ['[:find (pull $ ?e pattern) . :in $ pattern :where [?e :p/age 10]] :DB [:p/name]]
   ['[:find (pull ?e pattern) . :in $ pattern :where [?e :p/age 10]] :DB]
   ['[:find (pull ?e pattern) . :in $ pattern :where [?e :p/age 10]] :DB nil]
   ['[:find (pull ?e pattern) . :in $ ?pattern :where [?e :p/age 10]] :DB [:p/name]]
   ['[:find (pull ?e pattern) . :where [?e :p/age 10]] :DB]
   ['[:find (pull ?e pattern) . :in $ [pattern ...] :where [?e :p/age 10]] :DB [[:p/name]]]
   ['[:find ?e :in $ x :where [?e :p/age 10]] :DB 1]
   ['[:find ?e :in $ x :where [?e :p/age 10]] :DB]
   ['[:find ?e :in $ % :where (r ?e)] :DB '[[(r ?e) [$2 ?e :p/age 10]]]]
   ['[:find ?e :in $ % :where [?e :p/age 10]] :DB '[[(r ?e) [$2 ?e :p/age 10]]]]
   ['[:find ?e :in $ % :where (anc ?x ?e)] :DB '[[(anc ?x ?y) [?x :p/age ?y]]]]
   ['[:find ?e :in $ % :where (anc ?x ?e)] :DB '[[(anc ?x ?y) [?x ?a ?y]]]]])

(defn- subst [x dbs]
  (get dbs x x))

(defn- run-with [q-fn q args dbs]
  (try {:ok (apply q-fn q (map #(subst % dbs) args))}
       (catch Throwable t {:error t})))

(defn- row [r]
  (cond (and (coll? r) (not (map? r))) (mapv row r)
        (map? r) (into {} (map (fn [[k v]] [k (row v)])) r)
        (instance? java.util.Collection r) (mapv row r)
        (instance? Float r) (double r)
        :else r))

(defn- canon [r]
  (cond
    (instance? java.util.Set r) [:set (set (map row r))]
    (vector? r) [:vec (frequencies (map row r))]
    (map? r) [:map (row r)]
    (seq? r) [:seq (frequencies (map row r))]
    :else [:val (row r)]))

(defn- error-kind [^Throwable t]
  (or (:db/error (ex-data t)) (some-> (.getCause t) ex-data :db/error)))

(defn- compare-one [q args dbs-d dbs-m]
  (let [d (run-with d/q q args dbs-d)
        mm (run-with m/q q args dbs-m)]
    (cond
      (and (contains? d :ok) (contains? mm :ok))
      (let [a (canon (:ok d)) b (canon (:ok mm))]
        (or (= a b) {:q q :args (vec args) :datomic a :mnemonic b}))

      (and (contains? d :error) (contains? mm :error))
      (let [kd (error-kind (:error d)) km (error-kind (:error mm))]
        (or (nil? kd) (= kd km)
            {:q q :error-kinds [kd km] :datomic (ex-message (:error d)) :mnemonic (ex-message (:error mm))}))

      :else {:q q
             :datomic (if (contains? d :ok) (canon (:ok d)) (ex-message (:error d)))
             :mnemonic (if (contains? mm :ok) (canon (:ok mm)) (ex-message (:error mm)))})))

(defn- load-both [cd cm]
  (doseq [tx [schema idents people]]
    (let [{:keys [datomic mnemonic]} (o/transact-both [cd cm] tx)]
      (assert (not (:error datomic)) (pr-str datomic))
      (assert (not (:error mnemonic)) (pr-str mnemonic)))))

(deftest queries-equal-datomic
  (o/with-both [cd cm]
    (load-both cd cm)
    (let [db-d (d/db cd) db-m (m/db cm)
          dbs-d {:DB db-d :DB2 db-d :HIST (d/history db-d) :ASOF (d/as-of db-d (d/basis-t db-d))}
          dbs-m {:DB db-m :DB2 db-m :HIST (m/history db-m) :ASOF (m/as-of db-m (m/basis-t db-m))}]
      (is (>= (count queries) 40))
      (doseq [[q & args] queries
              :let [r (compare-one q args dbs-d dbs-m)]]
        (is (true? r) (pr-str r))))))

(deftest random-aggregates-have-datomic-shape
  (o/with-both [cd cm]
    (load-both cd cm)
    (let [db-d (d/db cd) db-m (m/db cm)
          ages #{10 20 30}]
      (doseq [[q check] [['[:find (sample 2 ?a) :where [?e :p/age ?a]]
                          (fn [r] (let [s (ffirst r)] (and (vector? s) (= 2 (count (set s))) (every? ages s))))]
                         ['[:find (sample 10 ?a) :where [?e :p/age ?a]]
                          (fn [r] (= ages (set (ffirst r))))]
                         ['[:find (rand 4 ?a) :with ?e :where [?e :p/age ?a]]
                          (fn [r] (let [s (ffirst r)] (and (seq? s) (= 4 (count s)) (every? ages s))))]
                         ['[:find (rand ?a) :where [?e :p/age ?a]]
                          (fn [r] (contains? ages (ffirst r)))]]
              :let [rd (d/q q db-d) rm (m/q q db-m)]]
        (is (check rd) (str "datomic " (pr-str q) (pr-str rd)))
        (is (check rm) (str "mnemonic " (pr-str q) (pr-str rm)))
        (is (= (class rd) (class rm)))
        (is (= (class (ffirst rd)) (class (ffirst rm))))))))

(deftest double-variance-close-to-datomic
  ;; the summation order over a set of doubles is Datomic's own, so compare approximately
  (o/with-both [cd cm]
    (load-both cd cm)
    (let [q '[:find (variance ?s) (stddev ?s) :where [?e :p/score ?s]]
          [vd sd] (first (d/q q (d/db cd)))
          [vm sm] (first (m/q q (m/db cm)))]
      (is (< (Math/abs (- (double vd) (double vm))) 1e-12))
      (is (< (Math/abs (- (double sd) (double sm))) 1e-12)))))

(deftest qseq-equal-datomic
  (o/with-both [cd cm]
    (load-both cd cm)
    (let [db-d (d/db cd) db-m (m/db cm)]
      (doseq [[q & args] [['[:find ?n ?a :in $ :where [?e :p/name ?n] [?e :p/age ?a]] :DB]
                          ['[:find (pull ?e [:p/name]) :in $ :where [?e :p/age ?a]] :DB]
                          ['[:find (pull ?e [:p/name]) (count ?t) :in $ :where [?e :p/tags ?t]] :DB]
                          ['[:find [?n ...] :in $ :where [?e :p/name ?n]] :DB]
                          ['[:find [?n ?a] :in $ :where [?e :p/name "Al"] [?e :p/name ?n] [?e :p/age ?a]] :DB]
                          ['[:find ?n :keys n :in $ :where [?e :p/name ?n]] :DB]
                          ['[:find ?e :in $ :where [?e :p/age 999]] :DB]
                          ['[:find ?n :in $ % :where (anc ?x ?y) [?x :p/name ?n]] :DB anc]
                          ['[:find ?e . :in $ :where [?e :p/age 20]] :DB]
                          ['[:find (count ?e) . :in $ :where [?e :p/age 20]] :DB]
                          ['[:find (pull ?e [:p/name]) . :in $ :where [?e :p/age 10]] :DB]
                          ['[:find ?e . :in $ :where [?e :p/age 999]] :DB]]
              :let [ad (map #(if (= % :DB) db-d %) args)
                    am (map #(if (= % :DB) db-m %) args)
                    rd (try {:ok (canon (frequencies (d/qseq {:query q :args (vec ad)})))}
                            (catch Throwable t {:error (error-kind t)}))
                    rm (try {:ok (canon (frequencies (m/qseq {:query q :args (vec am)})))}
                            (catch Throwable t {:error (error-kind t)}))]]
        (if (contains? rd :error)
          (is (contains? rm :error) (pr-str q))
          (is (= rd rm) (pr-str q)))))))
