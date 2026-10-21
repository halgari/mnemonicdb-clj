(ns mnemonic.tx-error-order-test
  "Regressions found by the generative differential test (expected kinds probed from Datomic)."
  (:require [clojure.test :refer [deftest is]]
            [mnemonic.api :as m]))

(defn- kind-of [conn tx-data]
  (try @(m/transact conn tx-data) :ok
       (catch Throwable t
         (let [c (or (.getCause t) t)] (:db/error (ex-data c))))))

(defn- with-conn [f]
  (let [uri (str "mnemonic:mem://" (random-uuid))]
    (m/create-database uri)
    (let [conn (m/connect uri)]
      (try
        @(m/transact conn [{:db/id "a" :db/ident :g/a0 :db/valueType :db.type/string
                            :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                           {:db/id "b" :db/ident :g/a1 :db/valueType :db.type/string
                            :db/cardinality :db.cardinality/many}])
        (f conn)
        (finally (m/release conn) (m/delete-database uri))))))

(deftest lookup-ref-failure-precedes-later-op-errors
  (with-conn
    (fn [conn]
      (is (= :db.error/not-an-entity
             (kind-of conn [[:db/add [:g/a0 "nope"] :g/a0 "x"] [:db/cas "t0" :g/a0 nil "y"]])))
      (is (= :db.error/not-an-entity
             (kind-of conn [[:db/add [:g/a0 "nope"] :g/a0 "x"] [:db/cas "t0" :g/a1 nil "y"]])))
      (is (= :db.error/not-a-keyword
             (kind-of conn [[:db/cas "t0" :g/a0 nil "y"] [:db/add [:g/a0 "nope"] :g/a0 "x"]]))))))

(deftest value-less-retract
  (with-conn
    (fn [conn]
      (is (= :db.error/not-a-keyword (kind-of conn [[:db/retract "t0" :g/a0]])))
      (is (= :db.error/not-an-entity (kind-of conn [[:db/retract "t0" :g/nope]])))
      (is (= :ok (kind-of conn [[:db/retract 17592186045999 :g/a1]])) "unallocated id is not range-checked")
      (is (= :db.error/invalid-entity-id (kind-of conn [[:db/retract 17592186045999 :g/a1 "p"]])))
      (is (= :db.error/nil-value (kind-of conn [[:db/retract 17592186045999 :g/a1 nil]]))
          "nil value is reported before the entity id check"))))

(deftest cas-on-unresolvable-entity
  (with-conn
    (fn [conn]
      (is (= :db.error/cas-failed (kind-of conn [[:db/cas [:g/a0 "nope"] :g/a0 "q" "y"]])))
      (is (= :db.error/cas-failed (kind-of conn [[:db/cas :nokk :g/a0 "q" "y"]])))
      (is (= :db.error/not-an-entity (kind-of conn [[:db/cas [:g/a0 "nope"] :g/a0 nil "y"]]))))))

(deftest retract-with-unique-identity-upserts-tempid
  (with-conn
    (fn [conn]
      (let [e (get-in @(m/transact conn [{:db/id "n" :g/a0 "x" :g/a1 ["p"]}]) [:tempids "n"])
            r @(m/transact conn [[:db/retract "t0" :g/a0 "x"] [:db/add "t0" :g/a1 "z"]])]
        (is (= e (get-in r [:tempids "t0"])))
        (is (= 2 (count (filter #(= e (:e %)) (:tx-data r)))))))))

(deftest upsert-order-of-tempid-groups
  (let [uri (str "mnemonic:mem://" (random-uuid))]
    (m/create-database uri)
    (let [conn (m/connect uri)]
      (try
        @(m/transact conn [{:db/id "a" :db/ident :g/u1 :db/valueType :db.type/string
                            :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                           {:db/id "b" :db/ident :g/u2 :db/valueType :db.type/string
                            :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}])
        (let [e (get-in @(m/transact conn [{:db/id "n" :g/u1 "x" :g/u2 "y"}]) [:tempids "n"])]
          ;; an unbound tempid first, then one that upserts to an existing entity AND shares the
          ;; new value: Datomic reports a conflict
          (is (= :db.error/datoms-conflict
                 (kind-of conn [[:db/add "t1" :g/u1 "new"] [:db/add "t0" :g/u1 "new"] [:db/add "t0" :g/u2 "y"]])))
          ;; the upserting tempid first: the later tempid joins the existing entity
          (let [r @(m/transact conn [[:db/add "t0" :g/u2 "y"] [:db/add "t0" :g/u1 "new"] [:db/add "t1" :g/u1 "new"]])]
            (is (= e (get-in r [:tempids "t0"]) (get-in r [:tempids "t1"])))))
        (finally (m/release conn) (m/delete-database uri))))))

(deftest identity-values-held-by-two-entities
  (let [uri (str "mnemonic:mem://" (random-uuid))]
    (m/create-database uri)
    (let [conn (m/connect uri)]
      (try
        @(m/transact conn [{:db/id "a" :db/ident :g/m :db/valueType :db.type/string
                            :db/cardinality :db.cardinality/many :db/unique :db.unique/identity}])
        @(m/transact conn [{:g/m "p"} {:g/m "q"}])
        ;; "p" and "q" belong to different entities: the tempid cannot be both
        (is (= :db.error/unique-conflict (kind-of conn [{:g/m ["p" "q"]}])))
        (finally (m/release conn) (m/delete-database uri))))))

(deftest asserted-identity-wins-over-retracted-identity
  (let [uri (str "mnemonic:mem://" (random-uuid))]
    (m/create-database uri)
    (let [conn (m/connect uri)]
      (try
        @(m/transact conn [{:db/id "a" :db/ident :g/m :db/valueType :db.type/string
                            :db/cardinality :db.cardinality/many :db/unique :db.unique/identity}])
        (let [r @(m/transact conn [[:db/add "x" :g/m "p"] [:db/add "y" :g/m "q"]])
              ey (get-in r [:tempids "y"])
              r2 @(m/transact conn [[:db/retract "t" :g/m "p"] [:db/add "t" :g/m "q"]])]
          ;; Datomic binds the tempid to the holder of the asserted value
          (is (= ey (get-in r2 [:tempids "t"]))))
        (finally (m/release conn) (m/delete-database uri))))))

(deftest assert-retract-clash-between-tempids-dropped-when-one-binds
  ;; Datomic probe: a retract of a new unique value for one tempid vs an assert of it for
  ;; another is a datoms-conflict only if neither tempid is (or later becomes) bound to an
  ;; existing entity; a clash seen once one side is already bound is immediate.
  (with-conn
    (fn [conn]
      @(m/transact conn [{:db/ident :g/a2 :db/valueType :db.type/string
                          :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}])
      @(m/transact conn [{:g/a0 "c" :g/a2 "k"}])
      (let [r [:db/retract "t0" :g/a0 "a"]
            mp {:g/a0 "a" :g/a2 "n"}
            bind [:db/add "t0" :g/a2 "k"]]
        (is (= :ok (kind-of conn [r mp bind])) "retract, map, then t0 binds")
        (is (= :ok (kind-of conn [r {:g/a0 "a"} bind])))
        (is (= :ok (kind-of conn [{:g/a0 "a"} r bind])))
        (is (= :db.error/datoms-conflict (kind-of conn [r {:g/a0 "a"}])) "both unbound")
        (is (= :db.error/datoms-conflict (kind-of conn [{:g/a0 "a"} bind r])) "t0 already bound")
        (is (= :db.error/datoms-conflict (kind-of conn [bind r {:g/a0 "a"}])))
        (is (= :db.error/datoms-conflict (kind-of conn [r bind r {:g/a0 "a"}])))
        (is (= :ok (kind-of conn [r [:db/add "t1" :g/a0 "a"] [:db/add "t1" :g/a2 "k"]]))
            "the asserting tempid binds later")))))
