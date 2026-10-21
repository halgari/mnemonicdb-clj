(ns mnemonic.query.engine-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [mnemonic.api :as m]
            [mnemonic.conn :as conn]))

(use-fixtures :each (fn [f] (try (f) (finally (conn/reset-registry!)))))

(def ^:private schema
  [{:db/id "n" :db/ident :p/name :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/id "a" :db/ident :p/age :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/id "c" :db/ident :p/city :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one :db/index true}
   {:db/id "t" :db/ident :p/tags :db/valueType :db.type/string :db/cardinality :db.cardinality/many}
   {:db/id "b" :db/ident :p/boss :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/id "k" :db/ident :p/kind :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/id "f" :db/ident :p/f :db/valueType :db.type/float :db/cardinality :db.cardinality/one}
   {:db/id "g" :db/ident :p/big :db/valueType :db.type/bigint :db/cardinality :db.cardinality/one}
   {:db/ident :kind/a} {:db/ident :kind/b}])

(def ^:private people
  [{:db/id "al" :p/name "Al" :p/age 10 :p/city "X" :p/tags ["x" "y"] :p/kind :kind/a
    :p/f (float 1.5) :p/big 5N}
   {:db/id "bo" :p/name "Bo" :p/age 20 :p/city "X" :p/boss "al" :p/kind :kind/b}
   {:db/id "cy" :p/name "Cy" :p/age 30 :p/city "Y" :p/boss "bo"}])

(defn- fresh-db []
  (let [u (str "mnemonic:mem://" (java.util.UUID/randomUUID))]
    (m/create-database u)
    (let [c (m/connect u)]
      @(m/transact c schema)
      @(m/transact c people)
      (m/db c))))

(defmacro ^:private with-db [& body]
  `(let [~'db (fresh-db)
         ~'eid (fn [n#] (m/entid ~'db [:p/name n#]))]
     ~@body))

(defn- err [f]
  (try (f) nil (catch Throwable t (or (:db/error (ex-data t)) (:db/error (ex-data (.getCause t))) t))))

(deftest data-patterns
  (with-db
    (testing "result type and basic scan"
      (let [r (m/q '[:find ?n :where [?e :p/name ?n]] db)]
        (is (instance? java.util.HashSet r))
        (is (= #{["Al"] ["Bo"] ["Cy"]} (set r)))))
    (testing "constants, joins"
      (is (= #{["Bo"]} (m/q '[:find ?n :where [?e :p/age 20] [?e :p/name ?n]] db)))
      (is (= #{["Al" "Bo"] ["Bo" "Cy"]}
             (set (m/q '[:find ?a ?b :where [?e :p/name ?a] [?x :p/boss ?e] [?x :p/name ?b]] db))))
      (is (= #{["Al" "Bo"]}
             (set (m/q '[:find ?a ?b :where [?e :p/boss ?o] [?o :p/name ?a] [?e :p/name ?b]
                         [?o :p/age 10]] db)))))
    (testing "indexed value (AVET) and card-many"
      (is (= #{["Al"] ["Bo"]} (set (m/q '[:find ?n :where [?e :p/city "X"] [?e :p/name ?n]] db))))
      (is (= #{["Al"]} (set (m/q '[:find ?n :where [?e :p/tags "y"] [?e :p/name ?n]] db))))
      (is (= #{["x"] ["y"]} (set (m/q '[:find ?t :where [?e :p/tags ?t]] db)))))
    (testing "ident constants in ref value position and e position"
      (is (= #{["Al"]} (set (m/q '[:find ?n :where [?e :p/kind :kind/a] [?e :p/name ?n]] db))))
      (is (= #{[:kind/a]} (set (m/q '[:find ?i :where [:kind/a :db/ident ?i]] db))))
      (is (= #{[:p/age]} (set (m/q '[:find ?i :where [?a :db/ident :p/age] [?a :db/ident ?i]] db)))))
    (testing "lookup refs"
      (is (= #{["Bo"]} (set (m/q '[:find ?n :where [?e :p/boss [:p/name "Al"]] [?e :p/name ?n]] db))))
      (is (= #{[10]} (set (m/q '[:find ?a :in $ ?e :where [?e :p/age ?a]] db [:p/name "Al"]))))
      (is (= #{["Bo"]} (set (m/q '[:find ?n :in $ ?l :where [?e :p/boss ?l] [?e :p/name ?n]]
                                 db [:p/name "Al"]))))
      (is (= #{["Bo"] ["Cy"]}
             (set (m/q '[:find ?n :in $ [?l ...] :where [?e :p/boss ?l] [?e :p/name ?n]]
                       db [[:p/name "Al"] [:p/name "Bo"]]))))
      (is (= :db.error/not-an-entity (err #(m/q '[:find ?n :in $ ?l :where [?l :p/age ?n]] db [:p/name "Zz"]))))
      (is (= :db.error/lookup-ref-attr-not-unique
             (err #(m/q '[:find ?n :in $ ?l :where [?l :p/age ?n]] db [:p/age 10])))))
    (testing "unknown attribute and ident"
      (is (= :db.error/not-an-entity (err #(m/q '[:find ?e :where [?e :nope/x ?v]] db))))
      (is (= :db.error/not-an-entity (err #(m/q '[:find ?e :where [?e :p/boss :nope/x]] db))))
      (is (= #{} (m/q '[:find ?e :where [?e :p/boss 99999]] db))))
    (testing "variable attribute"
      (let [r (m/q '[:find ?a ?v :in $ ?e :where [?e ?a ?v]] db (eid "Al"))]
        (is (= 8 (count r)))
        (is (contains? r [(m/entid db :p/name) "Al"]) "attributes come back as entity ids")
        (is (contains? r [(m/entid db :p/tags) "y"])))
      (is (= #{[(m/entid db :p/name)]}
             (m/q '[:find ?a :in $ ?e :where [?e ?a "Al"]] db (eid "Al")))))
    (testing "variable attribute with value only uses refs"
      (is (= #{[(eid "Bo") (m/entid db :p/boss)]}
             (m/q '[:find ?e ?a :in $ ?v :where [?e ?a ?v]] db (eid "Al"))))
      (is (= #{} (m/q '[:find ?e ?a :in $ ?v :where [?e ?a ?v]] db "Al"))))
    (testing "full scan is an error"
      (is (= :db.error/insufficient-binding (err #(m/q '[:find ?e :where [?e ?a ?v]] db))))
      (is (= :db.error/insufficient-binding (err #(m/q '[:find ?e :where [?e]] db)))))
    (testing "tx and added positions"
      (let [r (m/q '[:find ?e ?tx ?ad :where [?e :p/age 10 ?tx ?ad]] db)]
        (is (= 1 (count r)))
        (is (true? (nth (first r) 2))))
      (is (= #{} (m/q '[:find ?e :where [?e :p/age 10 _ false]] db)))
      (is (= #{[(eid "Al")]} (m/q '[:find ?e :where [?e :p/age 10 _ true]] db))))
    (testing "same variable twice"
      (is (= #{} (m/q '[:find ?e :where [?e :p/age ?e]] db))))
    (testing "value coercions"
      (is (= #{} (m/q '[:find ?e :where [?e :p/age 10.0]] db)))
      (is (= #{[(eid "Al")]} (m/q '[:find ?e :in $ ?v :where [?e :p/age ?v]] db (int 10))))
      (is (= #{[(eid "Al")]} (m/q '[:find ?e :in $ ?v :where [?e :p/big ?v]] db 5)))
      (is (= #{[(eid "Al")]} (m/q '[:find ?e :in $ ?v :where [?e :p/f ?v]] db (float 1.5))))
      (is (= #{} (m/q '[:find ?e :in $ ?v :where [?e :p/f ?v]] db 1.5))))
    (testing "cartesian product of disjoint patterns"
      (is (= 9 (count (m/q '[:find ?a ?b :where [?x :p/name ?a] [?y :p/name ?b]] db)))))))

(deftest large-relation-hash-join
  (with-db
    (let [r (m/q '[:find ?x ?n :in $ [?x ...] :where [?e :p/age ?x] [?e :p/name ?n]]
                 db (range 0 3000))]
      (is (= #{[10 "Al"] [20 "Bo"] [30 "Cy"]} (set r))))
    (let [r (m/q '[:find ?n :in $ [?e ...] :where [?e :p/name ?n]]
                 db (concat (repeat 1500 (eid "Al")) (repeat 1500 [:p/name "Bo"])))]
      (is (= #{["Al"] ["Bo"]} (set r))))))

(deftest predicates
  (with-db
    (is (= #{["Bo"] ["Cy"]} (set (m/q '[:find ?n :where [?e :p/age ?a] [(> ?a 15)] [?e :p/name ?n]] db))))
    (is (= #{["Bo"]} (set (m/q '[:find ?n :where [?e :p/age ?a] [(> ?a 15)] [(< ?a 25)] [?e :p/name ?n]
                                 [(not= ?n "Al")]] db))))))

(deftest predicate-deferral
  (with-db
    (is (= #{["Bo"] ["Cy"]}
           (set (m/q '[:find ?n :where [(> ?a 15)] [?e :p/age ?a] [?e :p/name ?n]] db))))
    (is (= :db.error/insufficient-binding
           (err #(m/q '[:find ?n :where [?e :p/age ?a] [(> ?z 1)]] db))))))

(deftest predicate-constants-do-not-filter
  (with-db
    (is (= 3 (count (m/q '[:find ?e :where [?e :p/age ?a] [(> 3 5)]] db))))))

(deftest function-expressions
  (with-db
    (is (= #{[11] [21] [31]} (set (m/q '[:find ?b :where [?e :p/age ?a] [(inc ?a) ?b]] db))))
    (is (= #{[4]} (set (m/q '[:find ?r :where [(ground 4) ?x] [(* ?x 1) ?r]]))))
    (is (= #{[1] [2] [3]} (set (m/q '[:find ?b :where [(ground [1 2 3]) [?b ...]]]))))
    (is (= #{[2]} (set (m/q '[:find ?b :where [(ground [1 2 3]) [?b ...]] [(even? ?b)]]))))
    (is (= #{[1 2] [3 4]} (set (m/q '[:find ?b ?c :where [(ground [[1 2] [3 4]]) [[?b ?c]]]]))))
    (is (= #{[1 2]} (set (m/q '[:find ?b ?c :where [(ground [1 2 3]) [?b ?c]]]))))
    (is (= #{[3 5]} (set (m/q '[:find ?x ?y :where [(ground 3) ?x] [(+ ?x 2) ?y]]))))
    (is (= #{[11 1] [11 2]}
           (set (m/q '[:find ?a ?f :where [?e :p/name "Al"] [?e :p/age ?b] [(inc ?b) ?a]
                       [(ground [1 2]) [?f ...]]] db))))
    (testing "nil results drop the row"
      (is (= #{} (m/q '[:find ?z :where [(identity nil) ?z]])))
      (is (= #{} (m/q '[:find ?z :where [(identity nil) [?z ...]]])))
      (is (= #{[1] [nil]} (set (m/q '[:find ?z :where [(identity [1 nil]) [?z ...]]])))))
    (testing "already-bound result variable filters"
      (is (= #{["Al"]} (set (m/q '[:find ?n :where [?e :p/name ?n] [(ground "Al") ?n]] db))))
      (is (= #{["Al"]} (set (m/q '[:find ?n :where [?e :p/name ?n] [(ground ["Al" "Zz"]) [?n ...]]] db)))))
    (testing "tuple/untuple, str, re"
      (is (= #{[[10 1]]} (set (m/q '[:find ?t :where [(ground 10) ?a] [(tuple ?a 1) ?t]]))))
      (is (= #{[10 1]} (set (m/q '[:find ?x ?y :where [(ground [10 1]) ?t] [(untuple ?t) [?x ?y]]]))))
      (is (= #{["Al!"]} (set (m/q '[:find ?s :where [?e :p/name ?n] [(re-find #"^A" ?n)] [(str ?n "!") ?s]] db))))
      (is (= #{["Al"]} (set (m/q '[:find ?n :in $ ?re :where [?e :p/name ?n] [(re-find ?re ?n)]] db #"^A")))))
    (testing "resolution"
      (is (= #{["AL"]} (set (m/q '[:find ?s :where [?e :p/name "Al"] [?e :p/name ?n]
                                   [(clojure.string/upper-case ?n) ?s]] db))))
      (is (= #{["Al"]} (set (m/q '[:find ?n :where [?e :p/name ?n] [(.startsWith ?n "A")]] db))))
      (is (= #{[3]} (set (m/q '[:find ?n :where [(java.lang.Math/abs -3) ?n]]))))
      (is (= #{[true]} (set (m/q '[:find ?n :where [(odd? 3) ?n]]))))
      (is (thrown? Exception (m/q '[:find ?n :where [?e :p/name ?x] [(frob ?x) ?n]] db)))
      (is (thrown? Exception (m/q '[:find ?n :where [?e :p/name ?x] [(no.such.ns/f ?x) ?n]] db))))))

(deftest db-built-ins
  (with-db
    (is (= #{["Al" :none] ["Bo" (eid "Al")] ["Cy" (eid "Bo")]}
           (set (m/q '[:find ?n ?b :where [?e :p/name ?n] [(get-else $ ?e :p/boss :none) ?b]] db))))
    (is (= #{["Al"]} (set (m/q '[:find ?n :where [?e :p/name ?n] [(missing? $ ?e :p/boss)]] db))))
    (is (= #{["Al"]} (set (m/q '[:find ?n :where [?e :p/name ?n] [(missing? $ ?e :nope/x)] [(missing? $ ?e :p/boss)]] db))))
    (is (= #{["Al" (m/entid db :p/f) (float 1.5)]}
           (set (m/q '[:find ?n ?a ?v :where [?e :p/name ?n] [(get-some $ ?e :p/boss :p/f) [?a ?v]]
                       [(= ?n "Al")]] db)))
        "get-some returns [attr-id value] (first attr present)")
    (is (= :db.error/not-an-entity (err #(m/q '[:find ?b :where [?e :p/age ?a] [(get-else $ ?e :nope/x 1) ?b]] db))))
    (is (some? (err #(m/q '[:find ?b :where [?e :p/age ?a] [(get-else $ ?e :p/tags "d") ?b]] db))))
    (is (some? (err #(m/q '[:find ?b :where [?e :p/age ?a] [(get-else $ ?e :p/boss nil) ?b]] db))))))

(deftest in-forms
  (testing "scalar, coll, tuple, rel, blank"
    (is (= #{[1 2]} (m/q '[:find ?x ?y :in ?x ?y] 1 2)))
    (is (= #{[1] [2] [3]} (m/q '[:find ?x :in [?x ...]] [1 2 3])))
    (is (= #{[1] [2]} (m/q '[:find ?x :in [?x ...]] #{1 2})))
    (is (= #{[1] [2]} (m/q '[:find ?x :in [?x ...]] (list 1 2))))
    (is (= #{} (m/q '[:find ?x :in [?x ...]] [])))
    (is (= #{[1 2]} (m/q '[:find ?x ?y :in [?x ?y]] [1 2])))
    (is (= #{[1 2]} (m/q '[:find ?x ?y :in [?x ?y]] [1 2 3])))
    (is (= #{[1 2] [3 4]} (m/q '[:find ?x ?y :in [[?x ?y]]] [[1 2] [3 4]])))
    (is (= #{[1 2]} (m/q '[:find ?x ?y :in [[?x ?y]]] #{[1 2]})))
    (is (= #{[1 2]} (m/q '[:find ?x ?y :in [[?x ?y]]] [[1 2 3]])))
    (is (= #{[1 3]} (m/q '[:find ?x ?z :in [?x _ ?z]] [1 2 3])))
    (is (= #{[1] [3]} (m/q '[:find ?x :in [[?x _]]] [[1 2] [3 4]])))
    (is (= #{[1]} (m/q '[:find ?x :in _ ?x] :ignored 1)))
    (is (= #{[10 20]} (m/q '[:find ?x ?y :in ?x [?y ...]] 10 [20])))
    (is (some? (err #(m/q '[:find ?x :in [?x ...]] 5))))
    (is (some? (err #(m/q '[:find ?x ?y :in [?x ?y]] [1]))))
    (is (some? (err #(m/q '[:find ?x ?y :in [[?x ?y]]] [[1]]))))
    (is (some? (err #(m/q '[:find ?x :in ?x] nil))) "nil scalar inputs are rejected like Datomic"))
  (testing "too few inputs; extra inputs ignored"
    (with-db
      (is (= :db.error/too-few-inputs (err #(m/q '[:find ?x :in $ ?x :where [?e :p/age ?x]] db))))
      (is (= #{["Al"] ["Bo"] ["Cy"]} (m/q '[:find ?n :where [?e :p/name ?n]] db db 5)))))
  (testing "inputs join with where"
    (with-db
      (is (= #{[10 "Al"] [20 "Bo"] [30 "Cy"]}
             (set (m/q '[:find ?x ?n :in $ [?x ...] :where [?e :p/age ?x] [?e :p/name ?n]] db [10 20 30 40]))))
      (is (= #{[10 "Al"]}
             (set (m/q '[:find ?x ?n :in $ [[?x _]] :where [?e :p/age ?x] [?e :p/name ?n]] db [[10 2] [3 4]]))))
      (is (= #{["Al"]} (set (m/q '[:find ?n :in $ ?x :where [?e :p/age ?x] [?e :p/name ?n]] db 10))))
      (is (= #{[1]} (set (m/q '[:find ?x :in $ ?x :where [?e :p/age ?a]] db 1)))))))

(deftest sources
  (testing "collection sources"
    (is (= #{[1 2] [3 4]} (m/q '[:find ?x ?y :in $c :where [$c ?x ?y]] [[1 2] [3 4]])))
    (is (= #{[1] [3]} (m/q '[:find ?x :in $c :where [$c ?x]] [[1 2] [3 4]])))
    (is (= #{[1]} (m/q '[:find ?x :in $c :where [$c ?x 2]] [[1 2] [3 4]])))
    (is (= #{[1 3]} (m/q '[:find ?x ?z :in $c :where [$c ?x _ ?z]] [[1 2 3]])))
    (is (= #{[1 2]} (m/q '[:find ?x ?y :in $c :where [$c ?x ?y]] [[1 2 3]])) "longer tuples fine")
    (is (= #{[1 2]} (m/q '[:find ?x ?y :in $c :where [$c ?x ?y]] #{[1 2]})))
    (is (some? (err #(m/q '[:find ?x ?z :in $c :where [$c ?x ?y ?z]] [[1 2]]))) "short tuples fail")
    (is (= #{[1 :a] [2 :b]} (m/q '[:find ?x ?y :in $c :where [$c ?x ?y]] [[1 :a] [2 :b]])))
    (is (= #{["X" 2]} (m/q '[:find ?c ?n :in $a $b :where [$a ?c ?n] [$b ?c _]] [["X" 2] ["Z" 3]] [["X" 9]]))))
  (testing "db with collection"
    (with-db
      (is (= #{[10 "Al"]}
             (set (m/q '[:find ?x ?n :in $ $c :where [$c ?x ?y] [?e :p/age ?x] [?e :p/name ?n]]
                       db [[10 2] [3 4]]))))
      (is (= #{[10 "Al" "Al"] [20 "Bo" "Bo"] [30 "Cy" "Cy"]}
             (set (m/q '[:find ?a ?n ?m :in $ $2 :where [$ ?e :p/age ?a] [$ ?e :p/name ?n]
                         [$2 ?e :p/name ?m]] db db))))
      (is (= #{} (m/q '[:find ?n :in $ :where [?e :p/name ?n]] [[1 2]])))))
  (testing "undeclared / invalid sources"
    (with-db
      (is (some? (err #(m/q '[:find ?n :in $ :where [$2 ?e :p/name ?n]] db))))
      (is (= :db.error/invalid-data-source
             (err #(m/q '[:find ?n :in $ :where [?e :p/name ?n]] nil))))
      (is (= :db.error/invalid-data-source
             (err #(m/q '[:find ?n :in $ :where [?e :p/name ?n]] 5)))))))

(deftest find-specs
  (with-db
    (testing "collection"
      (is (= ["Al"] (m/q '[:find [?n ...] :where [?e :p/age 10] [?e :p/name ?n]] db)))
      (is (vector? (m/q '[:find [?n ...] :where [?e :p/name ?n]] db)))
      (is (= #{"Al" "Bo" "Cy"} (set (m/q '[:find [?n ...] :where [?e :p/name ?n]] db))))
      (is (= [] (m/q '[:find [?n ...] :where [?e :p/age 99] [?e :p/name ?n]] db))))
    (testing "scalar"
      (is (= "Al" (m/q '[:find ?n . :where [?e :p/age 10] [?e :p/name ?n]] db)))
      (is (nil? (m/q '[:find ?n . :where [?e :p/age 99] [?e :p/name ?n]] db))))
    (testing "tuple"
      (is (= ["Bo" 20] (m/q '[:find [?n ?a] :where [?e :p/age 20] [?e :p/name ?n] [?e :p/age ?a]] db)))
      (is (nil? (m/q '[:find [?n ?a] :where [?e :p/age 99] [?e :p/name ?n] [?e :p/age ?a]] db))))
    (testing ":keys :strs :syms"
      (is (= #{{:n "Al" :a 10} {:n "Bo" :a 20} {:n "Cy" :a 30}}
             (set (m/q '[:find ?n ?a :keys n a :where [?e :p/name ?n] [?e :p/age ?a]] db))))
      (is (= #{{"n" "Al" "a" 10} {"n" "Bo" "a" 20} {"n" "Cy" "a" 30}}
             (set (m/q '[:find ?n ?a :strs n a :where [?e :p/name ?n] [?e :p/age ?a]] db))))
      (is (= #{{'n "Al" 'a 10} {'n "Bo" 'a 20} {'n "Cy" 'a 30}}
             (set (m/q '[:find ?n ?a :syms n a :where [?e :p/name ?n] [?e :p/age ?a]] db))))
      (is (vector? (m/q '[:find ?n ?a :keys n a :where [?e :p/name ?n] [?e :p/age ?a]] db)))
      (is (= [] (m/q '[:find ?n ?a :keys n a :where [?e :p/age 99] [?e :p/name ?n] [?e :p/age ?a]] db)))
      (is (= {:n "Bo" :a 20}
             (m/q '[:find [?n ?a] :keys n a :where [?e :p/age 20] [?e :p/name ?n] [?e :p/age ?a]] db))))
    (testing ":with keeps duplicates"
      (is (= #{[20] [10] [30]} (set (m/q '[:find ?a :with ?e :where [?e :p/age ?a]] db))))
      (let [r (m/q '[:find ?c :with ?e :where [?e :p/city ?c]] db)]
        (is (vector? r))
        (is (= {["X"] 2 ["Y"] 1} (frequencies r)))))
    (testing "aggregates and pull are deferred"
      (is (= :db.error/unsupported (err #(m/q '[:find (count ?e) :where [?e :p/name _]] db))))
      (is (= :db.error/unsupported (err #(m/q '[:find (pull ?e [*]) :where [?e :p/name _]] db)))))))

(deftest query-forms
  (with-db
    (testing "map, string, query-map, qseq"
      (is (= #{["Al"] ["Bo"] ["Cy"]} (set (m/q '{:find [?n] :in [$] :where [[?e :p/name ?n]]} db))))
      (is (= #{["Al"] ["Bo"] ["Cy"]} (set (m/q "[:find ?n :in $ :where [?e :p/name ?n]]" db))))
      (is (= #{["Al"] ["Bo"] ["Cy"]}
             (set (m/query {:query '[:find ?n :in $ :where [?e :p/name ?n]] :args [db]}))))
      (is (= #{["Al"] ["Bo"] ["Cy"]}
             (set (m/query {:query '[:find ?n :in $ :where [?e :p/name ?n]] :args [db] :timeout 5000}))))
      (is (= 3 (count (m/qseq {:query '[:find ?n :in $ :where [?e :p/name ?n]] :args [db]}))))
      (is (seq? (m/qseq {:query '[:find ?n :in $ :where [?e :p/name ?n]] :args [db]})))
      (is (= 3 (count (m/qseq '[:find ?n :in $ :where [?e :p/name ?n]] db))))
      (is (= :db.error/too-few-inputs
             (err #(m/query {:query '[:find ?n :in $ ?x :where [?e :p/name ?n]] :args [db]})))))))

(deftest timeout
  (with-db
    (is (= :db.error/query-timeout
           (err #(m/query {:query '[:find ?n :in $ :where [?e :p/name ?n]] :args [db] :timeout -1}))))))

(deftest history-flag-and-layers
  (testing "as-of / history dbs when the layers task is present"
    (with-db
      (let [asof (try (m/as-of db (m/basis-t db)) (catch Throwable _ nil))]
        (when asof
          (is (= #{["Al"] ["Bo"] ["Cy"]} (set (m/q '[:find ?n :where [?e :p/name ?n]] asof)))))))))

(deftest cursors-closed
  (with-db
    (let [before (.get mnemonic.index/open-cursors)]
      (m/q '[:find ?n :where [?e :p/name ?n] [?e :p/age ?a] [(> ?a 15)]] db)
      (m/q '[:find ?n :in $ [?x ...] :where [?e :p/age ?x] [?e :p/name ?n]] db (range 2000))
      (is (= before (.get mnemonic.index/open-cursors))))))
