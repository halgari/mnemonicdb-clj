(ns mnemonic.query.aggregates-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [mnemonic.api :as m]
            [mnemonic.conn :as conn]
            [mnemonic.pull :as pull]))

(use-fixtures :each (fn [f] (try (f) (finally (conn/reset-registry!)))))

(def ^:private schema
  [{:db/ident :p/name :db/valueType :db.type/string :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :p/age :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :p/boss :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/ident :p/tags :db/valueType :db.type/string :db/cardinality :db.cardinality/many}])

;; ages 1 2 2 4
(def ^:private people
  [{:db/id "a" :p/name "A" :p/age 1}
   {:db/id "b" :p/name "B" :p/age 2 :p/boss "a"}
   {:db/id "c" :p/name "C" :p/age 2 :p/boss "b"}
   {:db/id "d" :p/name "D" :p/age 4 :p/boss "c" :p/tags ["x"]}
   {:db/id "e" :p/name "E"}])

(defn- fresh-db []
  (let [u (str "mnemonic:mem://" (java.util.UUID/randomUUID))]
    (m/create-database u)
    (let [c (m/connect u)]
      @(m/transact c schema)
      @(m/transact c people)
      (m/db c))))

(defn- err [f]
  (try (f) nil (catch Throwable t (or (:db/error (ex-data t)) (:db/error (ex-data (.getCause t))) t))))

(defn- agg
  "Value of aggregate expression `a` over the values `vs` (as an :in collection)."
  [a vs]
  (m/q [:find a :in '[?x ...]] vs))

(deftest basic-aggregates-over-ages
  (let [db (fresh-db)
        q (fn [a & {:keys [with]}]
            (m/q (cond-> [:find a] with (into [:with with]) true (into '[:where [?e :p/age ?a]])) db))]
    (testing "distinct values without :with"
      (is (= [[3]] (q '(count ?a))))
      (is (= [[3]] (q '(count-distinct ?a))))
      (is (= [[7]] (q '(sum ?a))))
      (is (= [[1]] (q '(min ?a))))
      (is (= [[4]] (q '(max ?a)))))
    (testing ":with ?e keeps duplicates"
      (is (= [[4]] (q '(count ?a) :with '?e)))
      (is (= [[3]] (q '(count-distinct ?a) :with '?e)))
      (is (= [[9]] (q '(sum ?a) :with '?e)))
      (is (= [[2.25]] (q '(avg ?a) :with '?e)))
      (is (= [[2]] (q '(median ?a) :with '?e)))
      (is (= [[1.1875 (Math/sqrt 1.1875)]]
             (m/q '[:find (variance ?a) (stddev ?a) :with ?e :where [?e :p/age ?a]] db)))
      (is (= [[#{1 2 4}]] (q '(distinct ?a) :with '?e))))
    (testing "result types"
      (is (instance? Integer (ffirst (q '(count ?a)))))
      (is (instance? Integer (ffirst (q '(count-distinct ?a)))))
      (is (instance? Long (ffirst (q '(sum ?a)))))
      (is (instance? Double (ffirst (q '(avg ?a)))))
      (is (vector? (q '(count ?a)))))
    (testing "(min n) (max n) return vectors"
      (is (= [[[1 2 2]]] (q '(min 3 ?a) :with '?e)))
      (is (= [[[4 2 2]]] (q '(max 3 ?a) :with '?e)))
      (is (= [[[1 2]]] (q '(min 2 ?a))))
      (is (= [[[4 2]]] (q '(max 2 ?a))))
      (is (= [[[1 2 4]]] (q '(min 5 ?a))))
      (is (= [[[]]] (q '(min 0 ?a))))
      (is (vector? (ffirst (q '(max 2 ?a))))))
    (testing "sample and rand"
      (let [s (ffirst (q '(sample 2 ?a)))]
        (is (vector? s))
        (is (= 2 (count s)))
        (is (every? #{1 2 4} s))
        (is (= 2 (count (set s)))))
      (is (= #{1 2 4} (set (ffirst (q '(sample 10 ?a))))))
      (let [r (ffirst (q '(rand 5 ?a)))]
        (is (seq? r))
        (is (= 5 (count r)))
        (is (every? #{1 2 4} r)))
      (is (contains? #{1 2 4} (ffirst (q '(rand ?a))))))))

(deftest median-variance-edge-cases
  (testing "median: even count is the integer-quotient of the middle pair"
    (is (= [[2]] (agg '(median ?x) [1 2 3 4])))
    (is (= [[3]] (agg '(median ?x) [1 2 3 4 5 6])))
    (is (= [[2]] (agg '(median ?x) [4 1 3 2])))
    (is (= [[1]] (agg '(median ?x) [1 2])))
    (is (= [[50]] (agg '(median ?x) [1 100])))
    (is (= [[1.0]] (agg '(median ?x) [1.0 2.0])))
    (is (= [[5]] (agg '(median ?x) [5])))
    (is (= [[3]] (agg '(median ?x) [5 1 3]))))
  (testing "variance and stddev are population statistics"
    (is (= [[0.25]] (agg '(variance ?x) [1 2])))
    (is (= [[0.0]] (agg '(variance ?x) [5])))
    (is (= [[0.25]] (agg '(variance ?x) [1.5 2.5])))
    (is (= [[1.118033988749895]] (agg '(stddev ?x) [1 2 3 4]))))
  (testing "sum and avg numeric types"
    (is (= [[3.5]] (agg '(sum ?x) [1 2.5])))
    (is (= [[3N]] (agg '(sum ?x) [1N 2N])))
    (is (= [[3M]] (agg '(sum ?x) [1M 2M])))
    (is (= [[1.5]] (agg '(avg ?x) [1 2])))
    (is (= [[1.5]] (agg '(avg ?x) [1M 2M]))))
  (testing "min / max on non-numbers"
    (is (= [["a"]] (agg '(min ?x) ["b" "a"])))
    (is (= [["b"]] (agg '(max ?x) ["b" "a"])))
    (is (= [[:a]] (agg '(min ?x) [:b :a])))
    (is (= [[1]] (agg '(min ?x) [1 2.5])))
    (is (= [[2.5]] (agg '(max ?x) [1 2.5])))
    (is (= [[["c" "b"]]] (agg '(max 2 ?x) ["a" "c" "b"]))))
  (testing "count-distinct distinguishes 1 and 1.0 like Datomic"
    (is (= [[2]] (agg '(count-distinct ?x) [1 1.0]))))
  (testing "bad arities and values"
    (is (some? (err #(agg '(sum 3 ?x) [1 2]))))
    (is (some? (err #(agg '(sample ?x) [1 2]))))
    (is (some? (err #(agg '(sum ?x) ["a" "b"]))))))

(deftest grouping
  (let [db (fresh-db)]
    (testing "group by the non-aggregate find vars"
      (is (= #{[1 1] [2 2] [4 1]}
             (set (m/q '[:find ?a (count ?e) :where [?e :p/age ?a]] db))))
      (is (= #{[1 1] [2 2] [4 1]}
             (set (m/q '[:find ?a (count ?e) :with ?n :where [?e :p/age ?a] [?e :p/name ?n]] db))))
      (is (= [[1 1 "A"] [2 2 "B"] [2 2 "C"] [4 4 "D"]]
             (sort-by (juxt first last)
                      (m/q '[:find (sum ?a) (max ?a) ?n :where [?e :p/age ?a] [?e :p/name ?n]] db)))))
    (testing "no rows"
      (is (= [] (m/q '[:find (count ?e) :where [?e :p/age 99]] db)))
      (is (= [] (m/q '[:find ?a (count ?e) :where [?e :p/age ?a] [?e :p/age 99]] db)))
      (is (nil? (m/q '[:find (count ?e) . :where [?e :p/age 99]] db)))
      (is (= [] (m/q '[:find [(count ?e) ...] :where [?e :p/age 99]] db)))
      (is (nil? (m/q '[:find [(count ?e) ?a] :where [?e :p/age 99] [?e :p/age ?a]] db))))
    (testing "find specs with aggregates"
      (is (= 4 (m/q '[:find (count ?e) . :where [?e :p/age ?a]] db)))
      (is (= [4] (m/q '[:find [(count ?e) ...] :where [?e :p/age ?a]] db)))
      (is (= [4 9] (m/q '[:find [(count ?a) (sum ?a)] :with ?e :where [?e :p/age ?a]] db)))
      (is (= [{:c 4}] (m/q '[:find (count ?e) :keys c :where [?e :p/age ?a]] db)))
      (is (= {:c 4 :s 9} (m/q '[:find [(count ?e) (sum ?a)] :keys c s :where [?e :p/age ?a]] db)))
      (is (= [[4 4]] (m/q '[:find (count ?e) (count ?e) :where [?e :p/age ?a]] db))))
    (testing "aggregate over a different variable than the grouping one"
      (is (= [["A" "D"]] (m/q '[:find (min ?n) (max ?n) :where [?e :p/name ?n] [?e :p/age _]] db))))
    (testing "different aggregate args dedupe on all of them"
      (is (= [[7 3]] (m/q '[:find (sum ?a) (count ?a) :where [?e :p/age ?a]] db))))
    (testing "aggregates over ors"
      (is (= [[3]] (m/q '[:find (count ?e) :where (or [?e :p/age 2] [?e :p/boss _])] db))))
    (testing "tuples with nil are counted"
      (is (= [[2]] (agg '(count ?x) [3 nil]))))
    (testing "unbound aggregate argument"
      (is (= :db.error/unbound-query-variables
             (err #(m/q '[:find (count ?z) :where [?e :p/age ?a]] db)))))))

(defn my-agg [xs] (reduce + 100 xs))
(defn my-agg-n [n xs] [n (count xs)])

(deftest custom-aggregates
  (let [db (fresh-db)]
    (testing "qualified function is called with the collection"
      (is (= [[107]] (m/q '[:find (mnemonic.query.aggregates-test/my-agg ?a) :where [?e :p/age ?a]] db)))
      (is (= [[109]] (m/q '[:find (mnemonic.query.aggregates-test/my-agg ?a) :with ?e :where [?e :p/age ?a]] db))))
    (testing "constant arguments come first"
      (is (= [[[3 3]]] (m/q '[:find (mnemonic.query.aggregates-test/my-agg-n 3 ?a) :where [?e :p/age ?a]] db))))
    (testing "grouped"
      (is (= #{["A" 101] ["B" 102] ["C" 102] ["D" 104]}
             (set (m/q '[:find ?n (mnemonic.query.aggregates-test/my-agg ?a)
                         :where [?e :p/age ?a] [?e :p/name ?n]] db)))))
    (testing "clojure.core functions work too"
      (is (= [[3]] (m/q '[:find (clojure.core/count ?a) :where [?e :p/age ?a]] db))))
    (testing "unknown aggregates"
      (is (= :db.error/invalid-aggregate (err #(m/q '[:find (nope ?a) :where [?e :p/age ?a]] db))))
      (is (some? (err #(m/q '[:find (no.such/f ?a) :where [?e :p/age ?a]] db)))))))

(deftest pull-in-find
  (let [db (fresh-db)
        eid #(m/entid db [:p/name %])]
    (testing "pull elements"
      (is (= [[{:p/name "B"}] [{:p/name "C"}]]
             (sort-by (comp :p/name first) (m/q '[:find (pull ?e [:p/name]) :where [?e :p/age 2]] db))))
      (is (= {:p/name "D"} (m/q '[:find (pull ?e [:p/name]) . :where [?e :p/age 4]] db)))
      (is (= [{:p/name "B"} {:p/name "C"}]
             (sort-by :p/name (m/q '[:find [(pull ?e [:p/name]) ...] :where [?e :p/age 2]] db))))
      (is (= [{:p/name "D"} 4] (m/q '[:find [(pull ?e [:p/name]) ?a] :where [?e :p/age 4] [?e :p/age ?a]] db)))
      (is (= [] (m/q '[:find (pull ?e [:p/name]) :where [?e :p/age 99]] db)))
      (is (= {:db/id (eid "D") :p/name "D" :p/age 4 :p/boss {:db/id (eid "C")} :p/tags ["x"]}
             (m/q '[:find (pull ?e [*]) . :where [?e :p/age 4]] db))))
    (testing "result is a vector; duplicates of equal pulls stay (deduped by entity)"
      (let [r (m/q '[:find (pull ?e [:p/age]) :where [?e :p/age 2]] db)]
        (is (vector? r))
        (is (= [[{:p/age 2}] [{:p/age 2}]] r))))
    (testing "mixed with variables and aggregates"
      (is (= #{[{:p/name "A"} 1] [{:p/name "B"} 2] [{:p/name "C"} 2] [{:p/name "D"} 4]}
             (set (m/q '[:find (pull ?e [:p/name]) ?a :where [?e :p/age ?a]] db))))
      (is (= #{[{:p/name "B"} 1] [{:p/name "C"} 1] [{:p/name "D"} 1]}
             (set (m/q '[:find (pull ?e [:p/name]) (count ?x) :where [?e :p/boss ?x]] db))))
      (is (= [{:n {:p/name "A"} :x 1}]
             (m/q '[:find (pull ?e [:p/name]) ?x :keys n x :where [?e :p/age ?x] [?e :p/age 1]] db))))
    (testing "pattern as input, explicit source, nested"
      (is (= {:p/name "D" :p/boss {:p/name "C"}}
             (m/q '[:find (pull ?e ?pat) . :in $ ?pat :where [?e :p/age 4]]
                  db [:p/name {:p/boss [:p/name]}])))
      (is (= {:p/name "D"} (m/q '[:find (pull $ ?e [:p/name]) . :where [?e :p/age 4]] db)))
      (is (= {:p/name "D"} (m/q '[:find (pull $2 ?e [:p/name]) . :in $ $2 :where [$2 ?e :p/age 4]] db db)))
      (is (= {:p/name "A"} (m/q '[:find (pull ?e [:p/name]) . :in $ ?e :where] db [:p/name "A"]))))
    (testing "errors"
      (is (some? (err #(m/q '[:find (pull ?e [:p/name]) :where [?e :p/age 4]] (m/history db)))))
      (is (some? (err #(m/q '[:find (pull ?e [:p/name]) . :in $c :where [$c ?e]] [[1]]))))
      (is (= {:p/name "D"} (m/q '[:find (pull ?e [:p/name]) . :where [?e :p/age 4]] (m/as-of db (m/basis-t db))))))))

(deftest qseq-is-lazy
  (let [db (fresh-db)
        calls (atom 0)
        q '[:find (pull ?e [:p/name]) :in $ :where [?e :p/name _]]]
    (testing "same elements as q"
      (is (= (set (map first (m/q q db))) (set (map first (m/qseq {:query q :args [db]})))))
      (is (= 5 (count (m/qseq q db))))
      (is (seq? (m/qseq q db))))
    (testing "pull is realized as the seq is consumed"
      (let [orig pull/pull-from-ast]
        (with-redefs [pull/pull-from-ast (fn [& args] (swap! calls inc) (apply orig args))]
          (let [s (m/qseq q db)]
            (is (zero? @calls))
            (first s)
            (is (< @calls 5))
            (doall s)
            (is (= 5 @calls))))))
    (testing "find specs"
      (is (nil? (m/qseq '[:find ?e :where [?e :p/age 99]] db)))
      (is (nil? (m/qseq '[:find ?e . :where [?e :p/age 99]] db)))
      (is (= 2 (count (m/qseq '[:find [?e ?a] :where [?e :p/age 4] [?e :p/age ?a]] db))))
      (is (= 4 (count (m/qseq '[:find [?e ...] :where [?e :p/age ?a]] db))))
      (is (= [{:n "A"}] (m/qseq '[:find ?n :keys n :where [?e :p/age 1] [?e :p/name ?n]] db)))
      (is (= [[1]] (m/qseq '[:find (count ?e) :where [?e :p/age 4] [?e :p/age ?a]] db))))))

(deftest timeout
  (let [db (fresh-db)
        q '[:find ?a ?b ?c ?d :in $ :where [?a :p/name] [?b :p/name] [?c :p/name] [?d :p/name]]]
    (is (= :db.error/query-timeout (err #(m/query {:query q :args [db] :timeout -1}))))
    (is (= :db.error/query-timeout
           (err #(m/query {:query '[:find (count ?a) :in $ :where [?a :p/name] [?b :p/name] [?c :p/name]]
                           :args [db] :timeout -1}))))
    (is (= 625 (count (m/query {:query q :args [db] :timeout 60000}))))
    (is (instance? java.util.concurrent.TimeoutException
                   (try (m/query {:query q :args [db] :timeout -1})
                        (catch Throwable t (.getCause t)))))))
