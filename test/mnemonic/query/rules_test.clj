(ns mnemonic.query.rules-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [mnemonic.api :as m]
            [mnemonic.conn :as conn]))

(use-fixtures :each (fn [f] (try (f) (finally (conn/reset-registry!)))))

(def ^:private schema
  [{:db/ident :p/name :db/valueType :db.type/string :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :p/age :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :p/boss :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/ident :p/tags :db/valueType :db.type/string :db/cardinality :db.cardinality/many}])

;; chain A <- B <- C <- D (boss), E alone
(def ^:private people
  [{:db/id "a" :p/name "A" :p/age 1}
   {:db/id "b" :p/name "B" :p/age 2 :p/boss "a"}
   {:db/id "c" :p/name "C" :p/age 2 :p/boss "b"}
   {:db/id "d" :p/name "D" :p/age 4 :p/boss "c" :p/tags ["x"]}
   {:db/id "e" :p/name "E"}])

(defn- fresh-db
  ([] (fresh-db people))
  ([data]
   (let [u (str "mnemonic:mem://" (java.util.UUID/randomUUID))]
     (m/create-database u)
     (let [c (m/connect u)]
       @(m/transact c schema)
       @(m/transact c data)
       (m/db c)))))

(defn- err [f]
  (try (f) nil (catch Throwable t (or (:db/error (ex-data t)) (:db/error (ex-data (.getCause t))) t))))

(defmacro ^:private thrown
  "Error kind of the exception thrown by `body` (a macro: `%` inside quoted queries
  would otherwise be read as an anonymous-fn parameter)."
  [& body]
  `(err (fn [] ~@body)))

(def ^:private anc
  '[[(anc ?x ?y) [?x :p/boss ?y]]
    [(anc ?x ?y) [?x :p/boss ?z] (anc ?z ?y)]])

(deftest recursive-ancestor
  (let [db (fresh-db)]
    (testing "all pairs"
      (is (= #{["C" "B"] ["D" "C"] ["B" "A"] ["D" "A"] ["D" "B"] ["C" "A"]}
             (m/q '[:find ?xn ?yn :in $ % :where (anc ?x ?y) [?x :p/name ?xn] [?y :p/name ?yn]]
                  db anc))))
    (testing "first argument bound by an earlier clause / lookup ref"
      (is (= #{["A"] ["B"] ["C"]}
             (m/q '[:find ?yn :in $ % :where [?x :p/name "D"] (anc ?x ?y) [?y :p/name ?yn]] db anc)))
      (is (= #{["A"] ["B"] ["C"]}
             (m/q '[:find ?yn :in $ % :where (anc [:p/name "D"] ?y) [?y :p/name ?yn]] db anc))))
    (testing "second argument bound"
      (is (= #{["B"] ["C"] ["D"]}
             (m/q '[:find ?xn :in $ % :where [?y :p/name "A"] (anc ?x ?y) [?x :p/name ?xn]] db anc)))
      (is (= #{["B"] ["C"] ["D"]}
             (m/q '[:find ?xn :in $ % :where (anc ?x [:p/name "A"]) [?x :p/name ?xn]] db anc))))
    (testing "blanks, repeated var, no match"
      (is (= 3 (count (m/q '[:find ?x :in $ % :where (anc ?x _)] db anc))))
      (is (= 3 (count (m/q '[:find ?x :in $ % :where (anc _ ?x)] db anc))))
      (is (= #{} (m/q '[:find ?x :in $ % :where (anc ?x ?x)] db anc)))
      (is (= #{} (m/q '[:find ?x :in $ % :where (anc ?x ?y) (anc ?y ?x)] db anc)))
      (is (= #{} (m/q '[:find ?x :in $ % :where (anc 1 ?x)] db anc))))
    (testing "a single non-recursive definition"
      (is (= 3 (count (m/q '[:find ?x :in $ % :where (anc ?x ?y)] db (take 1 anc))))))))

(deftest cyclic-data-terminates
  (let [cyc '[[(path ?a ?b) [?a ?b]] [(path ?a ?b) [?a ?c] (path ?c ?b)]]
        edges [[1 2] [2 3] [3 1]]]
    (is (= #{[1] [2] [3]} (m/q '[:find ?b :in $ % :where (path 1 ?b)] edges cyc)))
    (is (= #{[1 1] [1 2] [1 3] [2 1] [2 2] [2 3] [3 1] [3 2] [3 3]}
           (m/q '[:find ?a ?b :in $ % :where (path ?a ?b)] edges cyc)))))

(deftest deep-recursion
  (let [n 300
        edges (mapv (fn [i] [i (inc i)]) (range n))
        cyc '[[(path ?a ?b) [?a ?b]] [(path ?a ?b) [?a ?c] (path ?c ?b)]]]
    (is (= (set (map (comp vector inc) (range n)))
           (m/q '[:find ?b :in $ % :where (path 0 ?b)] edges cyc)))))

(deftest mutual-recursion
  (let [db (fresh-db)
        rs '[[(ev ?x) [?x :p/age 1]]
             [(ev ?x) [?x :p/boss ?y] (od ?y)]
             [(od ?x) [?x :p/boss ?y] (ev ?y)]]]
    (is (= #{["A"] ["C"]} (m/q '[:find ?n :in $ % :where (ev ?x) [?x :p/name ?n]] db rs)))
    (is (= #{["B"] ["D"]} (m/q '[:find ?n :in $ % :where (od ?x) [?x :p/name ?n]] db rs)))))

(deftest required-vars
  (let [db (fresh-db)
        rs '[[(older [?a] ?b) [?a :p/age ?aa] [?b :p/age ?ba] [(< ?aa ?ba)]]
             [(rq [?a ?b] ?c) [?a :p/age ?c] [?b :p/age ?c]]]]
    (is (= #{["D"]} (m/q '[:find ?bn :in $ % :where [?a :p/name "B"] (older ?a ?b) [?b :p/name ?bn]] db rs)))
    (testing "deferred until bound by a later clause"
      (is (= #{["D"]} (m/q '[:find ?bn :in $ % :where (older ?a ?b) [?a :p/name "B"] [?b :p/name ?bn]] db rs))))
    (testing "constants, lookup refs and inputs satisfy required vars"
      (is (= #{["D"]} (m/q '[:find ?bn :in $ % :where (older [:p/name "B"] ?b) [?b :p/name ?bn]] db rs)))
      (is (= #{} (m/q '[:find ?bn :in $ % :where (older 5 ?b) [?b :p/name ?bn]] db rs)))
      (is (= #{["D"]} (m/q '[:find ?bn :in $ ?a % :where (older ?a ?b) [?b :p/name ?bn]]
                           db [:p/name "B"] rs))))
    (testing "never bound"
      (is (= :db.error/insufficient-binding
             (thrown (m/q '[:find ?bn :in $ % :where (older ?a ?b) [?b :p/name ?bn]] db rs))))
      (is (= :db.error/insufficient-binding
             (thrown (m/q '[:find ?c :in $ % :where [?a :p/name "B"] (rq ?a ?b ?c)] db rs)))))
    (is (= #{[2]} (m/q '[:find ?c :in $ % :where [?a :p/name "B"] [?b :p/name "C"] (rq ?a ?b ?c)] db rs)))))

(deftest rule-bodies
  (let [db (fresh-db)
        rs '[[(nm ?e ?n) [?e :p/name ?n]]
             [(adult ?e) [?e :p/age ?a] [(> ?a 1)]]
             [(root ?e) [?e :p/name _] (not [?e :p/boss _])]
             [(kid-or-old ?e) (or [?e :p/age 4] [?e :p/boss _])]
             [(tagged ?e) [?e :p/tags _]]
             [(tagged ?e) [?e :p/age 1]]]
        eid #(m/entid db [:p/name %])
        ids (fn [& ns] (set (map (comp vector eid) ns)))]
    (is (= #{[1]} (m/q '[:find ?n :in $ % :where (nm ?e "A") [?e :p/age ?n]] db rs)))
    (is (= #{} (m/q '[:find ?n :in $ % :where (nm 1 ?n)] db rs)))
    (is (= (ids "B" "C" "D") (m/q '[:find ?e :in $ % :where (adult ?e)] db rs)))
    (is (= (ids "A" "E") (m/q '[:find ?e :in $ % :where (root ?e)] db rs)))
    (is (= (ids "B" "C" "D") (m/q '[:find ?e :in $ % :where (kid-or-old ?e)] db rs)))
    (is (= (ids "A" "D") (m/q '[:find ?e :in $ % :where (tagged ?e)] db rs)))
    (testing "rules inside not, deferral of not"
      (is (= (ids "A" "E") (m/q '[:find ?e :in $ % :where [?e :p/name _] (not (adult ?e))] db rs)))
      (is (= (ids "A" "E") (m/q '[:find ?e :in $ % :where (not (adult ?e)) [?e :p/name _]] db rs)))
      (is (= (ids "A" "B" "C" "E")
             (m/q '[:find ?e :in $ % :where [?e :p/name _] (not (adult ?e) (tagged ?e))] db rs))))
    (testing "function expression feeding a rule"
      (is (= #{["B"] ["C"] ["D"]}
             (m/q '[:find ?n :in $ % :where [?e :p/name ?n] [(identity ?e) ?z] (adult ?z)] db rs))))
    (testing "same rule twice"
      (is (= #{[2] [4]} (m/q '[:find ?a :in $ % :where [?x :p/age ?a] (adult ?x) (adult ?x)] db rs))))
    (testing "unknown rule, wrong arity, missing rules input"
      (is (= :db.error/invalid-query (thrown (m/q '[:find ?x :in $ % :where (nope ?x)] db rs))))
      (is (= :db.error/invalid-query (thrown (m/q '[:find ?x :in $ % :where (adult ?x ?y)] db rs))))
      (is (= :db.error/invalid-query (thrown (m/q '[:find ?x :in $ :where (adult ?x)] db)))))))

(deftest rule-sources
  (let [db (fresh-db)
        db2 (let [u2 (str "mnemonic:mem://" (java.util.UUID/randomUUID))
                  _ (m/create-database u2)
                  c2 (m/connect u2)]
              @(m/transact c2 (take 1 schema))
              @(m/transact c2 [{:p/name "Z"}])
              (m/db c2))
        rs '[[(nm ?e ?n) [?e :p/name ?n]]]]
    (is (= #{["Z"]} (m/q '[:find ?n :in $ $2 % :where ($2 nm ?e ?n)] db db2 rs)))
    (is (= 5 (count (m/q '[:find ?n :in $ $2 % :where (nm ?e ?n)] db db2 rs))))
    (is (= :db.error/invalid-query (thrown (m/q '[:find ?n :in $ % :where ($3 nm ?e ?n)] db rs))))))

(deftest rule-source-and-arity-checks
  (let [db (fresh-db)]
    (testing "undeclared source in a called rule body"
      (is (= :db.error/invalid-query
             (thrown (m/q '[:find ?e :in $ % :where (r ?e)] db '[[(r ?e) [$2 ?e :p/age 1]]]))))
      (is (= #{} (m/q '[:find ?e :in $ % :where [?e :p/age 99]] db '[[(r ?e) [$2 ?e :p/age 1]]]))))
    (testing "arity differing between definitions is rejected with the rules"
      (is (= :db.error/invalid-query
             (thrown (m/q '[:find ?e :in $ % :where (r ?e)] db
                          '[[(r ?e) [?e :p/age 1]] [(r ?e ?f) [?e :p/age ?f]]])))))))

(deftest rule-timeout
  (let [db (fresh-db)]
    (is (= :db.error/query-timeout
           (thrown (m/query {:query '[:find ?x ?y :in $ % :where (anc ?x ?y)] :args [db anc]
                           :timeout -1}))))))

(deftest not-and-or
  (let [db (fresh-db)
        eid #(m/entid db [:p/name %])
        ids (fn [& ns] (set (map (comp vector eid) ns)))
        names (fn [q & args] (apply m/q q db args))]
    (testing "not"
      (is (= #{["A"] ["D"] ["E"]} (names '[:find ?n :where [?e :p/name ?n] (not [?e :p/age 2])])))
      (is (= #{["A"] ["D"] ["E"]} (names '[:find ?n :where (not [?e :p/age 2]) [?e :p/name ?n]])))
      (is (= #{["A"] ["D"] ["E"]} (names '[:find ?n :where [?e :p/name ?n] (not [?e :p/age 2] [?e :p/boss _])])))
      (is (= #{["B"] ["C"]} (names '[:find ?n :where [?e :p/name ?n] (not (not [?e :p/age 2]))])))
      (is (= #{["A"] ["E"]} (names '[:find ?n :where [?e :p/name ?n] (not (or [?e :p/age 2] [?e :p/age 4]))])))
      (is (= #{["A"] ["B"] ["C"] ["D"] ["E"]} (names '[:find ?n :where [?e :p/name ?n] (not [?e :p/age 99])])))
      (is (= #{} (names '[:find ?n :where [?e :p/name ?n] (not [?e :p/name])])))
      (is (= #{["B"] ["C"] ["D"] ["E"]}
             (names '[:find ?n :where [?e :p/name ?n] (not [(= ?n "A")])]))))
    (testing "not-join local variables"
      (is (= #{["A"] ["E"]}
             (names '[:find ?n :where [?e :p/name ?n] (not-join [?e] [?e :p/age ?a] [(> ?a 1)])])))
      (is (= #{["A"] ["B"] ["E"]}
             (names '[:find ?n :where [?e :p/name ?n] (not-join [?e] [?e :p/boss ?b] [?b :p/age 2])])))
      (is (= #{["A"] ["C"] ["D"] ["E"]}
             (names '[:find ?n :where [?e :p/name ?n] [?b :p/age 1] (not [?e :p/boss ?b])]))))
    (testing "not with a source"
      (let [db2 (fresh-db (take 1 people))]
        (is (= #{["B"] ["C"] ["D"] ["E"]}
               (m/q '[:find ?n :in $ $2 :where [?e :p/name ?n] ($2 not [?e :p/name "A"])] db db2)))))
    (testing "not errors"
      (is (= :db.error/insufficient-binding
             (thrown (names '[:find ?n :where [?e :p/name ?n] (not [?x :p/age 99])]))))
      (is (= :db.error/insufficient-binding
             (thrown (names '[:find ?n :where [?e :p/name ?n] (not-join [?q] [?q :p/age 99])])))))
    (testing "or"
      (is (= #{["A"] ["D"]} (names '[:find ?n :where [?e :p/name ?n] (or [?e :p/age 1] [?e :p/age 4])])))
      (is (= #{["A"] ["D"]} (names '[:find ?n :where (or [?e :p/age 1] [?e :p/age 4]) [?e :p/name ?n]])))
      (is (= #{["A"] ["C"] ["D"]}
             (names '[:find ?n :where (or [?e :p/age 1] [?e :p/age 4] [?e :p/name "C"]) [?e :p/name ?n]])))
      (is (= (ids "A" "B" "C") (names '[:find ?e :where (or [?e :p/age 1] [?e :p/age 2])])))
      (is (= #{[(eid "B") (eid "A")] [(eid "C") (eid "B")] [(eid "D") (eid "C")]
               [(eid "C") 2] [(eid "A") 1] [(eid "B") 2] [(eid "D") 4]}
             (names '[:find ?e ?a :where (or [?e :p/age ?a] [?e :p/boss ?a])])))
      (is (= #{["A"] ["B"]} (names '[:find ?n :where [?e :p/name ?n] (or [(= ?n "A")] [(= ?n "B")])]))))
    (testing "and inside or; branch-local variables need or-join"
      (is (= (ids "B" "C" "A")
             (names '[:find ?e :where (or (and [?e :p/age 2] [?e :p/boss _]) [?e :p/age 1])])))
      (is (= :db.error/invalid-query
             (thrown (names '[:find ?e :where (or (and [?e :p/age 2] [?e :p/boss ?a]) [?e :p/age 1])])))))
    (testing "or-join"
      (is (= (ids "A" "B" "C")
             (names '[:find ?e :where (or-join [?e] (and [?e :p/age 2] [?e :p/boss ?a]) [?e :p/age 1])])))
      (is (= (ids "A" "D") (names '[:find ?e :where (or-join [?e] [?e :p/age 1] [?e :p/age 4]) [?e :p/name _]])))
      (is (= (ids "A" "D") (names '[:find ?e :where [?e :p/name _] (or-join [[?e]] [?e :p/age 1] [?e :p/age 4])])))
      (is (= (ids "A" "D") (names '[:find ?e :where (or-join [[?e]] [?e :p/age 1] [?e :p/age 4]) [?e :p/name _]])))
      (is (= #{} (names '[:find ?e :where [?e :p/name _] (or-join [?e] [?e :p/age 99])])))
      (is (= :db.error/insufficient-binding
             (thrown (names '[:find ?e :where (or-join [[?e]] [?e :p/age 1] [?e :p/age 4])])))))
    (testing "or keeps outer duplicates under :with"
      (is (= [["B"] ["C"]]
             (sort (names '[:find ?n :with ?e :where [?e :p/name ?n] (or [?e :p/age 2] [?e :p/age 2])])))))
    (testing "or with a source"
      (let [db2 (let [u2 (str "mnemonic:mem://" (java.util.UUID/randomUUID))
                      _ (m/create-database u2)
                      c2 (m/connect u2)]
                  @(m/transact c2 (take 1 schema))
                  @(m/transact c2 [{:p/name "A"}])
                  (m/db c2))]
        (is (= #{["A"]}
               (m/q '[:find ?n :in $ $2 :where [?e :p/name ?n] ($2 or [?x :p/name ?n] [?x :p/name ?n])]
                    db db2)))))))
