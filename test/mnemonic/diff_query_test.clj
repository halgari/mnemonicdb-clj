(ns mnemonic.diff-query-test
  "Differential tests: the same queries run against Datomic (oracle) and MnemonicDB must
  return equal results, or both fail."
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
    [:p/f :db.type/float one]
    [:p/big :db.type/bigint one]
    [:p/score :db.type/double one :db/index true]
    [:p/born :db.type/instant one :db/index true]
    [:p/active :db.type/boolean one]
    [:p/key :db.type/keyword one :db/index true]
    [:p/uuid :db.type/uuid one :db/unique :db.unique/value]]))

(def ^:private idents [{:db/ident :kind/a} {:db/ident :kind/b} {:db/ident :kind/c}])

(def ^:private people
  [{:db/id "al" :p/name "Al" :p/age 10 :p/city "X" :p/tags ["x" "y"] :p/kind :kind/a
    :p/f (float 1.5) :p/big 5N :p/score 2.5 :p/born #inst "1990-01-01" :p/active true
    :p/key :a/b :p/uuid #uuid "123e4567-e89b-12d3-a456-426614174000"}
   {:db/id "bo" :p/name "Bo" :p/age 20 :p/city "X" :p/boss "al" :p/kind :kind/b :p/tags ["y" "z"]
    :p/score 3.5 :p/born #inst "2000-01-01" :p/active false :p/friends ["al"]}
   {:db/id "cy" :p/name "Cy" :p/age 30 :p/city "Y" :p/boss "bo" :p/friends ["al" "bo"] :p/key :c/d}
   {:db/id "di" :p/name "Di" :p/age 20 :p/city "Z" :p/boss "bo" :p/tags ["x"] :p/kind :kind/a}
   {:db/id "ed" :p/name "Ed"}])

(def ^:private strict-queries
  "Queries whose results must be equal. Inputs :DB / :DB2 stand for the db under test."
  [;; patterns and joins
   ['[:find ?n :where [?e :p/name ?n]] :DB]
   ['[:find ?e :where [?e :p/age 20]] :DB]
   ['[:find ?n :where [?e :p/age 20] [?e :p/name ?n]] :DB]
   ['[:find ?a ?b :where [?e :p/name ?a] [?x :p/boss ?e] [?x :p/name ?b]] :DB]
   ['[:find ?a ?c :where [?e :p/boss ?m] [?m :p/boss ?o] [?e :p/name ?a] [?o :p/name ?c]] :DB]
   ['[:find ?n :where [?e :p/city "X"] [?e :p/name ?n]] :DB]
   ['[:find ?n ?c :where [?e :p/city ?c] [?e :p/name ?n]] :DB]
   ['[:find ?n :where [?e :p/tags "y"] [?e :p/name ?n]] :DB]
   ['[:find ?t :where [?e :p/tags ?t]] :DB]
   ['[:find ?n ?t :where [?e :p/name ?n] [?e :p/tags ?t]] :DB]
   ['[:find ?n :where [?e :p/kind :kind/a] [?e :p/name ?n]] :DB]
   ['[:find ?i :where [:kind/a :db/ident ?i]] :DB]
   ['[:find ?v :where [:p/age :db/valueType ?v]] :DB]
   ['[:find ?i :where [:p/age :db/valueType ?v] [?v :db/ident ?i]] :DB]
   ['[:find ?e :where [?e :db/valueType :db.type/string]] :DB]
   ['[:find ?n :where [?e :p/boss [:p/name "Al"]] [?e :p/name ?n]] :DB]
   ['[:find ?a :in $ ?e :where [?e :p/age ?a]] :DB [:p/name "Al"]]
   ['[:find ?a :in $ ?e :where [?e :p/age ?a]] :DB :kind/a]
   ['[:find ?n :in $ [?l ...] :where [?e :p/boss ?l] [?e :p/name ?n]] :DB [[:p/name "Al"] [:p/name "Bo"]]]
   ['[:find ?a ?v :in $ ?e :where [?e ?a ?v]] :DB [:p/name "Al"]]
   ['[:find ?a ?v :in $ ?e :where [?e ?a ?v]] :DB :kind/a]
   ['[:find ?a :in $ ?e :where [?e ?a "Al"]] :DB [:p/name "Al"]]
   ['[:find ?e ?a :in $ ?v :where [?e ?a ?v]] :DB 17592186045418]
   ['[:find ?e ?a :in $ ?v :where [?e ?a ?v]] :DB "Al"]
   ['[:find ?e ?a :in $ ?v :where [?e ?a ?v]] :DB 10]
   ['[:find ?e ?tx :where [?e :p/age 10 ?tx]] :DB]
   ['[:find ?e ?tx ?ad :where [?e :p/age 10 ?tx ?ad]] :DB]
   ['[:find ?e :where [?e :p/age 10 _ true]] :DB]
   ['[:find ?e :where [?e :p/age 10 _ false]] :DB]
   ['[:find ?e :where [?e :p/age ?e]] :DB]
   ['[:find ?e ?e2 :where [?e :p/age ?x] [?e2 :p/age ?x]] :DB]
   ['[:find ?e ?e2 :where [?e :p/boss ?x] [?e2 :p/boss ?x] [(< ?e ?e2)]] :DB]
   ['[:find ?x ?y :where [?e :p/name ?x] [?e2 :p/name ?y] [(not= ?x ?y)] [(< ?x ?y)]] :DB]
   ['[:find ?e :where [?e :p/age ?a] [?e :p/boss _]] :DB]
   ['[:find ?n :where [_ :p/boss ?e] [?e :p/name ?n]] :DB]
   ['[:find ?e :where [?e :p/age]] :DB]
   ;; value types
   ['[:find ?e :where [?e :p/f 1.5]] :DB]
   ['[:find ?e :in $ ?v :where [?e :p/f ?v]] :DB (float 1.5)]
   ['[:find ?e :in $ ?v :where [?e :p/f ?v]] :DB 1.5]
   ['[:find ?e :in $ ?v :where [?e :p/big ?v]] :DB 5]
   ['[:find ?e :in $ ?v :where [?e :p/big ?v]] :DB 5N]
   ['[:find ?e :where [?e :p/age 10.0]] :DB]
   ['[:find ?e :in $ ?v :where [?e :p/age ?v]] :DB (int 20)]
   ['[:find ?e :in $ ?v :where [?e :p/score ?v]] :DB 2.5]
   ['[:find ?e :where [?e :p/score 3.5]] :DB]
   ['[:find ?e :where [?e :p/active true]] :DB]
   ['[:find ?e :where [?e :p/key :a/b]] :DB]
   ['[:find ?e :in $ ?v :where [?e :p/uuid ?v]] :DB #uuid "123e4567-e89b-12d3-a456-426614174000"]
   ['[:find ?e :in $ ?d :where [?e :p/born ?b] [(= ?b ?d)]] :DB #inst "2000-01-01"]
   ['[:find ?e :where [?e :p/born ?b] [(compare ?b #inst "1995-01-01") ?c] [(pos? ?c)]] :DB]
   ['[:find ?n :where [?e :p/age ?a] [(> ?a 15)] [?e :p/name ?n]] :DB]
   ['[:find ?n :where [(> ?a 15)] [?e :p/age ?a] [?e :p/name ?n]] :DB]
   ['[:find ?n :where [?e :p/age ?a] [(>= ?a 20)] [(<= ?a 20)] [?e :p/name ?n]] :DB]
   ['[:find ?n :where [?e :p/name ?n] [(< ?n "C")]] :DB]
   ['[:find ?n :where [?e :p/name ?n] [(re-find #"^[AB]" ?n)]] :DB]
   ['[:find ?n :where [?e :p/name ?n] [(clojure.string/starts-with? ?n "A")]] :DB]
   ['[:find ?n :where [?e :p/name ?n] [(.startsWith ?n "A")]] :DB]
   ['[:find ?n :where [?e :p/name ?n] [(!= ?n "Al")]] :DB]
   ['[:find ?a :where [?e :p/age ?a] [(> 3 5)]] :DB]
   ['[:find ?a :where [?e :p/age ?a] [(identity ?a)]] :DB]
   ;; function expressions
   ['[:find ?b :where [?e :p/age ?a] [(inc ?a) ?b]] :DB]
   ['[:find ?e ?s :where [?e :p/name ?n] [(str ?n "!") ?s]] :DB]
   ['[:find ?e ?s :where [?e :p/name ?n] [(subs ?n 0 1) ?s]] :DB]
   ['[:find ?s :where [?e :p/name ?n] [(clojure.string/upper-case ?n) ?s]] :DB]
   ['[:find ?r :where [(ground 4) ?x] [(* ?x ?x) ?r]]]
   ['[:find ?b :where [(ground [1 2 3]) [?b ...]]]]
   ['[:find ?b :where [(ground [1 2 3]) [?b ...]] [(even? ?b)]]]
   ['[:find ?b ?c :where [(ground [[1 2] [3 4]]) [[?b ?c]]]]]
   ['[:find ?b ?c :where [(ground [1 2]) [?b ?c]]]]
   ['[:find ?b :where [(ground 1) _] [(ground 5) ?b]]]
   ['[:find ?r :where [(ground [1 2]) [?x ...]] [(ground [3 4]) [?y ...]] [(+ ?x ?y) ?r]]]
   ['[:find ?z :where [(identity nil) ?z]]]
   ['[:find ?z :where [(identity [1 nil]) [?z ...]]]]
   ['[:find ?n :where [?e :p/name ?n] [(ground "Al") ?n]] :DB]
   ['[:find ?n :where [?e :p/name ?n] [(ground ["Al" "Zz"]) [?n ...]]] :DB]
   ['[:find ?n :where [(ground ["Al" "Zz"]) [?n ...]] [?e :p/name ?n]] :DB]
   ['[:find ?n :where [(ground "Al") ?n] [?e :p/name ?n]] :DB]
   ['[:find ?e :where [(ground [:p/name "Al"]) ?l] [?l :p/age ?e]] :DB]
   ['[:find ?e :where [(ground [:p/name "Al"]) ?l] [?e :p/boss ?l]] :DB]
   ['[:find ?e :where [(ground :kind/a) ?k] [?e :p/kind ?k]] :DB]
   ['[:find ?t :where [(ground 10) ?a] [(tuple ?a 1) ?t]]]
   ['[:find ?x ?y :where [(ground [10 1]) ?t] [(untuple ?t) [?x ?y]]]]
   ['[:find ?e ?d :where [?e :p/age _] [(get-else $ ?e :p/boss :none) ?d]] :DB]
   ['[:find ?e ?d :where [?e :p/age _] [(get-else $ ?e :p/kind 0) ?d]] :DB]
   ['[:find ?e ?d :where [?e :p/age _] [(get-some $ ?e :p/boss :p/kind) [?k ?d]]] :DB]
   ['[:find ?e :where [?e :p/age _] [(missing? $ ?e :p/boss)]] :DB]
   ['[:find ?e :where [?e :p/age _] [(missing? $ ?e :p/tags)]] :DB]
   ['[:find ?e :where [?e :p/age _] [(missing? $ ?e :nope/x)]] :DB]
   ['[:find ?e ?x :where [?e :p/age ?a] [(identity ?e) ?x] [?x :p/age ?a]] :DB]
   ['[:find ?x ?y :where [(ground 3) ?x] [(+ ?x 2) ?y]]]
   ['[:find ?r :where [(java.lang.Math/abs -3) ?r]]]
   ['[:find ?r :where [(max 1 5 3) ?r]]]
   ['[:find ?r :where [(vector 1 2) ?r]]]
   ['[:find ?r :where [(hash-map :a 1) ?r]]]
   ['[:find ?r :where [(count "abc") ?r]]]
   ['[:find ?r :where [(keyword "a" "b") ?r]]]
   ['[:find ?r :where [(name :a/b) ?r] ]]
   ;; :in forms
   ['[:find ?x :in [?x ...]] [1 2 3]]
   ['[:find ?x :in [?x ...]] []]
   ['[:find ?x ?y :in [[?x ?y]]] [[1 2] [3 4]]]
   ['[:find ?x ?y :in [?x ?y]] [1 2]]
   ['[:find ?x ?z :in [?x _ ?z]] [1 2 3]]
   ['[:find ?x :in [[?x _]]] [[1 2] [3 4]]]
   ['[:find ?x ?y :in ?x [?y ...]] 10 [20 30]]
   ['[:find ?x :in ?x] 5]
   ['[:find ?x ?y :in [?x ?y]] [1 2 3]]
   ['[:find ?x ?y :in [[?x ?y]]] [[1 2 3]]]
   ['[:find ?x :in [?x ...]] #{1 2}]
   ['[:find ?x :in [?x ...]] 5]
   ['[:find ?x ?y :in [?x ?y]] [1]]
   ['[:find ?x :in ?x] nil]
   ['[:find ?x ?n :in $ [?x ...] :where [?e :p/age ?x] [?e :p/name ?n]] :DB [10 20 30 40]]
   ['[:find ?x ?n :in $ [[?x _]] :where [?e :p/age ?x] [?e :p/name ?n]] :DB [[10 2] [20 4]]]
   ['[:find ?n :in $ ?x :where [?e :p/age ?x] [?e :p/name ?n]] :DB 20]
   ['[:find ?n :in $ ?x :where [?e :p/age ?x] [?e :p/name ?n]] :DB nil]
   ['[:find ?n :in $ ?x :where [?e :p/name ?n]] :DB]
   ['[:find ?n :in $ :where [?e :p/name ?n]] :DB :DB 5]
   ['[:find ?x :in $ ?x :where [?e :p/age ?a]] :DB 1]
   ['[:find ?n :in $ [?e ...] :where [?e :p/name ?n]] :DB :RANGE-EIDS]
   ['[:find ?x ?n :in $ [?x ...] :where [?e :p/age ?x] [?e :p/name ?n]] :DB :RANGE-3000]
   ;; sources
   ['[:find ?x ?y :in $c :where [$c ?x ?y]] [[1 2] [3 4]]]
   ['[:find ?x :in $c :where [$c ?x]] [[1 2] [3 4]]]
   ['[:find ?x :in $c :where [$c ?x 2]] [[1 2] [3 4]]]
   ['[:find ?x ?z :in $c :where [$c ?x _ ?z]] [[1 2 3]]]
   ['[:find ?x ?z :in $c :where [$c ?x ?y ?z]] [[1 2]]]
   ['[:find ?x ?y :in $c :where [$c ?x ?y]] #{[1 2]}]
   ['[:find ?x ?y :in $c :where [$c ?x ?y]] [[1 2] [1 2 3]]]
   ['[:find ?x ?n :in $ $c :where [$c ?x ?y] [?e :p/age ?x] [?e :p/name ?n]] :DB [[10 2] [3 4]]]
   ['[:find ?a ?n ?m :in $ $2 :where [$ ?e :p/age ?a] [$ ?e :p/name ?n] [$2 ?e :p/name ?m]] :DB :DB2]
   ['[:find ?n :in $ :where [?e :p/name ?n]] [[1 2]]]
   ['[:find ?n :in $ :where [?e :p/name ?n]] nil]
   ['[:find ?n :in $ :where [$2 ?e :p/name ?n]] :DB]
   ;; find specs
   ['[:find [?n ...] :where [?e :p/age 10] [?e :p/name ?n]] :DB]
   ['[:find [?n ...] :where [?e :p/name ?n]] :DB]
   ['[:find ?n . :where [?e :p/age 10] [?e :p/name ?n]] :DB]
   ['[:find ?n . :where [?e :p/age 99] [?e :p/name ?n]] :DB]
   ['[:find [?n ?a] :where [?e :p/age 10] [?e :p/name ?n] [?e :p/age ?a]] :DB]
   ['[:find [?n ?a] :where [?e :p/age 99] [?e :p/name ?n] [?e :p/age ?a]] :DB]
   ['[:find [?n ...] :where [?e :p/age 99] [?e :p/name ?n]] :DB]
   ['[:find ?n ?a :keys n a :where [?e :p/name ?n] [?e :p/age ?a]] :DB]
   ['[:find ?n ?a :strs n a :where [?e :p/name ?n] [?e :p/age ?a]] :DB]
   ['[:find ?n ?a :syms n a :where [?e :p/name ?n] [?e :p/age ?a]] :DB]
   ['[:find ?n ?a :keys n a :where [?e :p/age 99] [?e :p/name ?n] [?e :p/age ?a]] :DB]
   ['[:find ?a :with ?e :where [?e :p/age ?a]] :DB]
   ['[:find ?c :with ?e :where [?e :p/city ?c]] :DB]
   ['[:find ?a ?b :where [?x :p/name ?a] [?y :p/name ?b]] :DB]
   ;; errors
   ['[:find ?e :where [?e :nope/x ?v]] :DB]
   ['[:find ?e :where [?e :p/boss :nope/x]] :DB]
   ['[:find ?e :where [?e :p/boss [:p/name "Zz"]]] :DB]
   ['[:find ?v :in $ ?e :where [?e :p/age ?v]] :DB [:p/name "Zz"]]
   ['[:find ?v :in $ ?e :where [?e :p/age ?v]] :DB [:p/age 10]]
   ['[:find ?v :in $ ?e :where [?e :p/age ?v]] :DB :nope/x]
   ['[:find ?e :where [?e ?a ?v]] :DB]
   ['[:find ?e :where [?e]] :DB]
   ['[:find ?e :where [?e :p/age ?a] [(> ?z 1)]] :DB]
   ['[:find ?e :where [?e :p/age ?a] [(frob ?a)]] :DB]
   ['[:find ?e :where [?e :p/age ?a] [(no.such/f ?a)]] :DB]
   ['[:find ?e :where [?e :p/age ?a] [(get-else $ ?e :nope/x 1) ?d]] :DB]
   ['[:find ?e :where [?e :p/age ?a] [(get-else $ ?e :p/tags "d") ?d]] :DB]
   ['[:find ?x :in $ ?x :where [?e :p/age ?x]] :DB]
   ['[:find ?e :where [?e :p/age 10]]]])

(defn- subst [x db db2]
  (case x
    :DB db
    :DB2 db2
    :RANGE-3000 (vec (range 0 3000))
    x))

(defn- run-with [q-fn q args db]
  (try {:ok (apply q-fn q (map #(subst % db db) args))}
       (catch Throwable t {:error t})))

(defn- row [r]
  (cond (and (coll? r) (not (map? r))) (mapv row r)
        (instance? java.util.Collection r) (mapv row r)
        (instance? Float r) (double r)
        :else r))

(defn- canon [r]
  (cond
    (instance? java.util.Set r) [:set (set (map row r))]
    (vector? r) [:vec (frequencies (map row r))]
    (map? r) [:map (row r)]
    :else [:val (row r)]))

(defn- error-kind [^Throwable t]
  (or (:db/error (ex-data t)) (some-> (.getCause t) ex-data :db/error)))

(defn- compare-one [q args db-d db-m]
  (let [d (run-with d/q q args db-d)
        mm (run-with m/q q args db-m)]
    (cond
      (and (contains? d :ok) (contains? mm :ok))
      (let [a (canon (:ok d)) b (canon (:ok mm))]
        (or (= a b) {:q q :args (vec args) :datomic a :mnemonic b}))

      (and (contains? d :error) (contains? mm :error))
      (let [kd (error-kind (:error d)) km (error-kind (:error mm))]
        (or (nil? kd) (= kd km)
            {:q q :error-kinds [kd km] :datomic (ex-message (:error d)) :mnemonic (ex-message (:error mm))}))

      :else {:q q :datomic (if (contains? d :ok) (canon (:ok d)) (ex-message (:error d)))
             :mnemonic (if (contains? mm :ok) (canon (:ok mm)) (ex-message (:error mm)))})))

(defn- load-both [cd cm]
  (doseq [tx [schema idents people]]
    (let [{:keys [datomic mnemonic]} (o/transact-both [cd cm] tx)]
      (assert (not (:error datomic)) (pr-str datomic))
      (assert (not (:error mnemonic)) (pr-str mnemonic)))))

(deftest queries-equal-datomic
  (o/with-both [cd cm]
    (load-both cd cm)
    (let [db-d (d/db cd)
          db-m (m/db cm)
          ;; ids are identical on both sides; build the entity-id input from Datomic
          al (d/entid db-d [:p/name "Al"])
          fix (fn [[q & args]]
                (into [q] (map (fn [a]
                                 (cond (= a 17592186045418) al
                                       (= a :RANGE-EIDS) (vec (concat (repeat 1500 al) (repeat 1500 [:p/name "Bo"])))
                                       :else a))) args))]
      (is (>= (count strict-queries) 50))
      (doseq [qa strict-queries
              :let [[q & args] (fix qa)
                    r (compare-one q args db-d db-m)]]
        (is (true? r) (pr-str r))))))

(deftest query-map-forms-equal-datomic
  (o/with-both [cd cm]
    (load-both cd cm)
    (let [db-d (d/db cd) db-m (m/db cm)
          q '[:find ?n ?a :in $ :where [?e :p/name ?n] [?e :p/age ?a]]]
      (is (= (canon (d/query {:query q :args [db-d]})) (canon (m/query {:query q :args [db-m]}))))
      (is (= (canon (d/query {:query q :args [db-d] :timeout 5000}))
             (canon (m/query {:query q :args [db-m] :timeout 5000}))))
      (is (= (canon (d/q "[:find ?n :in $ :where [?e :p/name ?n]]" db-d))
             (canon (m/q "[:find ?n :in $ :where [?e :p/name ?n]]" db-m))))
      (is (= (canon (d/q '{:find [?n] :in [$] :where [[?e :p/name ?n]]} db-d))
             (canon (m/q '{:find [?n] :in [$] :where [[?e :p/name ?n]]} db-m))))
      (is (= (set (d/qseq {:query q :args [db-d]})) (set (m/qseq {:query q :args [db-m]})))))))

(deftest as-of-and-history-equal-datomic
  (o/with-both [cd cm]
    (load-both cd cm)
    @(d/transact cd [[:db/add [:p/name "Al"] :p/age 11] [:db/retract [:p/name "Bo"] :p/age 20]])
    @(m/transact cm [[:db/add [:p/name "Al"] :p/age 11] [:db/retract [:p/name "Bo"] :p/age 20]])
    (let [t (dec (d/basis-t (d/db cd)))
          asof-m (m/as-of (m/db cm) t) asof-d (d/as-of (d/db cd) t)
          since-m (m/since (m/db cm) t) since-d (d/since (d/db cd) t)
          hist-m (m/history (m/db cm)) hist-d (d/history (d/db cd))]
      (doseq [[q ds ms] [['[:find ?n ?a :where [?e :p/name ?n] [?e :p/age ?a]] asof-d asof-m]
                         ['[:find ?n ?a :where [?e :p/name ?n] [?e :p/age ?a]] since-d since-m]
                         ['[:find ?n ?a :where [?e :p/name ?n] [?e :p/age ?a]] hist-d hist-m]
                         ['[:find ?a :where [?e :p/name "Al"] [?e :p/age ?a]] hist-d hist-m]
                         ['[:find ?a ?ad :where [?e :p/name "Al"] [?e :p/age ?a _ ?ad]] hist-d hist-m]
                         ['[:find ?a ?tx ?ad :where [?e :p/age ?a ?tx ?ad]] hist-d hist-m]
                         ['[:find ?a ?tx ?ad :where [?e :p/age ?a ?tx ?ad]] since-d since-m]
                         ['[:find ?a :where [?e :p/age ?a _ false]] hist-d hist-m]
                         ['[:find ?e ?tx :where [?e :p/age 20 ?tx false]] hist-d hist-m]
                         ['[:find ?e :where [?e :p/age 20]] hist-d hist-m]
                         ['[:find ?e :where [?e :p/age 20]] asof-d asof-m]
                         ['[:find ?v ?ad :where [?e :p/tags ?v _ ?ad]] hist-d hist-m]
                         ['[:find ?a ?v :where [?e :p/name "Al"] [?e ?a ?v _ false]] hist-d hist-m]
                         ['[:find ?e ?a :where [?e ?a]] hist-d hist-m] ;; full scan error on both sides
                         ['[:find ?e ?a :where [?e :p/age 20]] hist-d hist-m]]] ;; unbound ?a: both error
        (let [a (try {:ok (canon (d/q q ds))} (catch Throwable e {:err (error-kind e)}))
              b (try {:ok (canon (m/q q ms))} (catch Throwable e {:err (error-kind e)}))]
          (if (contains? a :err)
            (do (is (contains? b :err) (pr-str q))
                (when (:err a) (is (= (:err a) (:err b)) (pr-str q))))
            (is (= a b) (pr-str q))))))))
