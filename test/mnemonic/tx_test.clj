(ns mnemonic.tx-test
  (:require [clojure.test :refer [deftest is testing]]
            [mnemonic.bootstrap :as bootstrap]
            [mnemonic.codec :as codec]
            [mnemonic.db :as db]
            [mnemonic.index :as ix]
            [mnemonic.rocks :as rocks]
            [mnemonic.tx :as tx]))

(def ^:private counter (atom 0))

(defn- with-store [f]
  (let [store (rocks/open-store {:path (str "tx-test-" (swap! counter inc)) :mem? true})]
    (try
      (bootstrap/bootstrap! store)
      (f store)
      (finally (rocks/close-store store)))))

(defn- run!*
  "prepare + write against the current db; returns the prepare result."
  [store tx-data]
  (let [d (db/base-db store)
        r (tx/prepare d (bootstrap/read-meta store) tx-data)]
    (rocks/write! store (:ops r) {})
    r))

(defn- kind-of [store tx-data]
  (try (run!* store tx-data) nil
       (catch clojure.lang.ExceptionInfo e (:db/error (ex-data e)))))

(defn- datoms
  ([store idx] (vec (ix/datoms-iterable (db/base-db store) idx (codec/encode-prefix idx []))))
  ([store idx comps] (vec (ix/datoms-iterable (db/base-db store) idx (codec/encode-prefix idx comps)))))

(defn- dv [d] [(:e d) (:a d) (:v d) (:tx d) (:added d)])

(defn- strip-inst [r] (remove #(= 50 (:a %)) (:tx-data r)))

(def ^:private user-part (bit-shift-left 4 42))

(def ^:private schema-tx
  [{:db/id "n" :db/ident :person/name :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/id "a" :db/ident :person/age :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/id "t" :db/ident :person/tags :db/valueType :db.type/string :db/cardinality :db.cardinality/many}
   {:db/id "f" :db/ident :person/friend :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many}
   {:db/id "b" :db/ident :person/boss :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/id "ad" :db/ident :person/addr :db/valueType :db.type/ref :db/cardinality :db.cardinality/one
    :db/isComponent true}
   {:db/id "c" :db/ident :addr/city :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/id "e" :db/ident :person/email :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one :db/unique :db.unique/value}])

(deftest pinned-ids-test
  (with-store
    (fn [store]
      (let [r1 (run!* store [{:db/id "n" :db/ident :person/name :db/valueType :db.type/string
                              :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                             {:db/id "a" :db/ident :person/age :db/valueType :db.type/long
                              :db/cardinality :db.cardinality/one}])]
        (is (= (codec/t->tx 1000) (:tx r1)))
        (is (= {"n" 73 "a" 74} (select-keys (:tempids r1) ["n" "a"])))
        (testing "tx-data has txInstant, attribute datoms and auto install"
          (is (= #{[73 10 :person/name] [73 40 23] [73 41 35] [73 42 38]
                   [74 10 :person/age] [74 40 22] [74 41 35]
                   [0 13 73] [0 13 74]}
                 (set (map (comp vec (partial take 3) dv) (strip-inst r1)))))
          (is (= 1 (count (filter #(= 50 (:a %)) (:tx-data r1)))))
          (is (every? #(= (:tx r1) (:tx %)) (:tx-data r1))))
        (is (= {:format 2 :next-t 1001 :next-db-n 75} (select-keys (:meta r1) [:format :next-t :next-db-n])))
        (is (= 1000 (ix/-basis-t (db/base-db store))))
        (let [r2 (run!* store [{:db/id "x" :person/name "Al"} {:db/id "y" :person/name "Bo"}])]
          (is (= (codec/t->tx 1001) (:tx r2)))
          (is (= {"x" (+ user-part 1002) "y" (+ user-part 1003)}
                 (select-keys (:tempids r2) ["x" "y"])))
          (is (= 1004 (:next-t (:meta r2))))
          (is (= 75 (:next-db-n (:meta r2)))))))))

(deftest cardinality-test
  (with-store
    (fn [store]
      (run!* store schema-tx)
      (let [r1 (run!* store [{:db/id "x" :person/name "Al" :person/age 3 :person/tags ["a" "b"]}])
            x (get (:tempids r1) "x")]
        (testing "card-one replacement emits retract + assert"
          (let [r2 (run!* store [[:db/add x :person/age 4]])
                ds (strip-inst r2)]
            (is (= #{[x 74 4 true] [x 74 3 false]}
                   (set (map (juxt :e :a :v :added) ds))))
            (is (= [4] (map :v (datoms store :eavt [x 74]))))))
        (testing "card-many accumulates"
          (let [r3 (run!* store [[:db/add x :person/tags "c"]])]
            (is (= 1 (count (strip-inst r3))))
            (is (= #{"a" "b" "c"} (set (map :v (datoms store :eavt [x 75])))))))
        (testing "redundant assert is elided"
          (let [r4 (run!* store [[:db/add x :person/age 4] [:db/add x :person/tags "a"]])]
            (is (empty? (strip-inst r4)))
            (is (some? (:tx r4)))))
        (testing "retract of absent datom is a no-op"
          (let [r5 (run!* store [[:db/retract x :person/age 99] [:db/retract x :person/tags "zzz"]])]
            (is (empty? (strip-inst r5)))))
        (testing "retract present"
          (let [r6 (run!* store [[:db/retract x :person/tags "a"]])]
            (is (= [[x 75 "a" false]] (map (juxt :e :a :v :added) (strip-inst r6))))
            (is (= #{"b" "c"} (set (map :v (datoms store :eavt [x 75]))))))
          (testing "history has the retraction"
            (let [d (db/base-db store)
                  h (vec (ix/datoms-iterable (reify ix/IDb (-view [_ i p] (ix/-seek-history d i p)))
                                             :eavt (codec/encode-prefix :eavt [x 75 "a"])))]
              (is (= 1 (count h))))))
        (testing "retract all values of an attribute"
          (run!* store [[:db/retract x :person/tags]])
          (is (empty? (datoms store :eavt [x 75]))))))))

(deftest conflicts-test
  (with-store
    (fn [store]
      (run!* store schema-tx)
      (let [r (run!* store [{:db/id "x" :person/name "Al" :person/email "al@x"}
                            {:db/id "y" :person/name "Bo"}])
            x (get (:tempids r) "x")
            y (get (:tempids r) "y")
            before (count (datoms store :eavt))]
        (is (= :db.error/unique-conflict (kind-of store [[:db/add y :person/name "Al"]])))
        (is (= :db.error/unique-conflict (kind-of store [[:db/add y :person/email "al@x"]])))
        (is (= :db.error/unique-conflict (kind-of store [{:db/id "z" :person/email "al@x"}])))
        (is (= :db.error/datoms-conflict (kind-of store [[:db/add y :person/age 1] [:db/add y :person/age 2]])))
        (is (= :db.error/datoms-conflict (kind-of store [[:db/add y :person/age 1] [:db/retract y :person/age 1]])))
        (is (= :db.error/datoms-conflict
               (kind-of store [{:db/id "p" :person/email "n@x"} {:db/id "q" :person/email "n@x"}])))
        (is (= :db.error/not-an-entity (kind-of store [[:db/add y :nope/attr 1]])))
        (is (= :db.error/not-an-entity (kind-of store [[:db/add :nobody :person/age 1]])))
        (is (= :db.error/wrong-type-for-attribute (kind-of store [[:db/add y :person/age "str"]])))
        (is (= :db.error/nil-value (kind-of store [[:db/add y :person/age nil]])))
        (is (= :db.error/tempid-not-an-entity (kind-of store [[:db/add y :person/boss "ghost"]])))
        (is (= :db.error/invalid-entity-id (kind-of store [[:db/add (+ user-part 99999) :person/age 1]])))
        (is (= :db.error/not-a-data-function (kind-of store [[:foo/bar 1 2 3]])))
        (is (= :db.error/past-tx-instant
               (kind-of store [[:db/add "datomic.tx" :db/txInstant #inst "2000-01-01"]])))
        (is (= :db.error/future-tx-instant
               (kind-of store [[:db/add "datomic.tx" :db/txInstant #inst "2999-01-01"]])))
        (is (= :db.error/invalid-install-attribute
               (kind-of store [{:db/id "q" :db/ident :bad/attr :db/valueType :db.type/string
                                :db.install/_attribute :db.part/db}])))
        (testing "nothing was written by failed transactions"
          (is (= before (count (datoms store :eavt))))
          (is (= {:next-t 1004} (select-keys (bootstrap/read-meta store) [:next-t]))))
        (is (some? x))))))

(deftest refs-and-maps-test
  (with-store
    (fn [store]
      (run!* store schema-tx)
      (let [r (run!* store [{:db/id "al" :person/name "Al"}])
            al (get (:tempids r) "al")]
        (testing "lookup refs in e and v position"
          (let [r2 (run!* store [{:db/id "bo" :person/name "Bo" :person/boss [:person/name "Al"]}
                                 [:db/add [:person/name "Al"] :person/age 9]])
                bo (get (:tempids r2) "bo")]
            (is (= [al] (map :v (datoms store :eavt [bo 77]))))
            (is (= [9] (map :v (datoms store :eavt [al 74]))))
            (is (= :db.error/not-an-entity (kind-of store [[:db/add [:person/name "Nobody"] :person/age 1]])))
            (is (= :db.error/lookup-ref-attr-not-unique (kind-of store [[:db/add [:person/age 9] :person/age 1]])))))
        (testing "ident value for ref"
          (run!* store [[:db/add al :person/boss al]])
          (let [r2 (run!* store [[:db/add al :person/boss :person/name]])]
            (is (= [73] (map :v (datoms store :eavt [al 77]))))
            (is (= 2 (count (strip-inst r2))) "replaced boss: retract + assert")))
        (testing "reverse ref in map form"
          (let [r2 (run!* store [{:db/id "cy" :person/name "Cy" :person/_friend al}])
                cy (get (:tempids r2) "cy")]
            (is (= [cy] (map :v (datoms store :eavt [al 76]))))))
        (testing "nested component maps"
          (let [r2 (run!* store [{:db/id "dee" :person/name "Dee" :person/addr {:addr/city "Denver"}}])
                dee (get (:tempids r2) "dee")
                addr (:v (first (datoms store :eavt [dee 78])))]
            (is (= "Denver" (:v (first (datoms store :eavt [addr 79])))))
            (is (= 3 (count (strip-inst r2))))
            (is (= (inc dee) addr) "parent first, then nested")))
        (testing "nested collection of maps on card-many ref"
          (let [r2 (run!* store [{:db/id "ed" :person/name "Ed"
                                  :person/friend [{:person/name "F1"} {:person/name "F2"}]}])
                ed (get (:tempids r2) "ed")]
            (is (= 2 (count (datoms store :eavt [ed 76]))))))))))

(deftest upsert-test
  (with-store
    (fn [store]
      (run!* store schema-tx)
      (let [r (run!* store [{:db/id "al" :person/name "Al"}])
            al (get (:tempids r) "al")]
        (testing "tempid with identity value upserts to the existing entity"
          (let [r2 (run!* store [{:db/id "t1" :person/name "Al" :person/age 7}])]
            (is (= al (get (:tempids r2) "t1")))
            (is (= [[al 74 7 true]] (map (juxt :e :a :v :added) (strip-inst r2))))
            (is (= (:next-t (:meta r2)) (:next-t (bootstrap/read-meta store)))
                "upsert consumes no ids")))
        (testing "map without :db/id upserts and reports an anonymous tempid"
          (let [r2 (run!* store [{:person/name "Al" :person/age 8}])]
            (is (= [al] (vals (:tempids r2))))))
        (testing "two tempids with the same new identity value unify"
          (let [before (:next-t (bootstrap/read-meta store))
                r2 (run!* store [{:db/id "a1" :person/name "New"}
                                 {:db/id "a2" :person/name "New" :person/age 3}])
                e (get (:tempids r2) "a1")]
            (is (= e (get (:tempids r2) "a2")))
            (is (= (+ user-part (inc before)) e))
            (is (= (+ before 2) (:next-t (:meta r2))) "one tx id + one entity id")
            (is (= #{[e 73 "New"] [e 74 3]}
                   (set (map (comp vec (partial take 3) dv) (strip-inst r2)))))))
        (testing "chained upsert through a ref-valued value"
          (let [r2 (run!* store [{:db/id "u1" :person/name "Al" :person/boss "u2"}
                                 {:db/id "u2" :person/name "Bo2"}])]
            (is (= al (get (:tempids r2) "u1")))
            (is (not= al (get (:tempids r2) "u2")))))
        (testing "tempid upserting to two entities conflicts"
          (run!* store [{:db/id "bo" :person/name "Bo"}])
          (is (= :db.error/datoms-conflict
                 (kind-of store [{:db/id "m" :person/name "Al"} {:db/id "m" :person/name "Bo"}]))))
        (testing "explicit id with a held identity value is a unique-conflict"
          (let [bo (:e (first (datoms store :avet [73 "Bo"])))]
            (is (= :db.error/unique-conflict (kind-of store [[:db/add bo :person/name "Al"]])))))))))

(deftest tempid-forms-test
  (with-store
    (fn [store]
      (run!* store schema-tx)
      (let [t1 (tx/->TempId :db.part/user -1)
            t2 (tx/->TempId :db.part/user -2)
            r (run!* store [[:db/add t1 :person/name "T1"] [:db/add t2 :person/name "T2"]
                            [:db/add t1 :person/boss t2]
                            [:db/add "datomic.tx" :person/name "TxNamed"]])
            e1 (get (:tempids r) (+ Long/MIN_VALUE (bit-shift-left 5 42) -1))]
        (is (some? e1))
        (is (= (:tx r) (get (:tempids r) "datomic.tx")))
        (is (= #{"T1" "T2" "TxNamed"} (set (map :v (filter #(= 73 (:a %)) (:tx-data r))))))
        (testing "tempid unresolved partition"
          (is (= :db.error/not-a-db-id (kind-of store [[:db/add (tx/->TempId :no/part -1) :person/age 1]]))))
        (testing "entity with no ops allocates nothing"
          (let [n (:next-t (bootstrap/read-meta store))
                r2 (run!* store [{:db/id "lonely"}])]
            (is (empty? (:tempids r2)))
            (is (= (inc n) (:next-t (:meta r2))))))))))

(deftest value-coercion-test
  (with-store
    (fn [store]
      (run!* store [{:db/id "l" :db/ident :t/long :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
                    {:db/id "f" :db/ident :t/float :db/valueType :db.type/float :db/cardinality :db.cardinality/one}
                    {:db/id "k" :db/ident :t/kw :db/valueType :db.type/keyword :db/cardinality :db.cardinality/one}])
      (let [r (run!* store [{:db/id "x" :t/long (int 5) :t/float 1.5 :t/kw 'foo/bar}])
            vals* (into {} (map (juxt :a :v)) (strip-inst r))]
        (is (= 5 (get vals* 73)))
        (is (instance? Long (get vals* 73)))
        (is (instance? Float (get vals* 74)))
        (is (= :foo/bar (get vals* 75)))
        (is (= :db.error/not-a-keyword (kind-of store [{:db/id "y" :t/kw "plain"}])))
        (is (= :db.error/wrong-type-for-attribute (kind-of store [{:db/id "y" :t/long 1.5}])))))))

(deftest no-history-test
  (with-store
    (fn [store]
      (run!* store [{:db/id "v" :db/ident :v/val :db/valueType :db.type/long
                     :db/cardinality :db.cardinality/one :db/noHistory true}
                    {:db/id "w" :db/ident :w/val :db/valueType :db.type/long
                     :db/cardinality :db.cardinality/one}])
      (let [r (run!* store [{:db/id "x" :v/val 1 :w/val 1}])
            x (get (:tempids r) "x")]
        (run!* store [[:db/add x :v/val 2] [:db/add x :w/val 2]])
        (let [d (db/base-db store)
              hist (fn [a] (vec (ix/datoms-iterable (reify ix/IDb (-view [_ i p] (ix/-seek-history d i p)))
                                                    :eavt (codec/encode-prefix :eavt [x a]))))]
          (is (= 0 (count (hist 73))) "noHistory attribute keeps no history")
          (is (= 1 (count (hist 74)))))))))
