(ns mnemonic.diff-rename-test
  "Differential test: a renamed or retracted ident keeps resolving (as an alias) until it
  is assigned to another entity, exactly as in Datomic."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datomic.api :as d]
            [mnemonic.api :as m]
            [mnemonic.conn :as conn]
            [mnemonic.oracle :as o]))

(use-fixtures :each (fn [f] (try (f) (finally (conn/reset-registry!)))))

(defn- api [c]
  (if (instance? mnemonic.conn.Connection c)
    {:transact m/transact :db m/db :entid m/entid :ident m/ident :q m/q :attribute m/attribute
     :entity m/entity :pull m/pull :datoms m/datoms :as-of m/as-of :with m/with}
    {:transact d/transact :db d/db :entid d/entid :ident d/ident :q d/q :attribute d/attribute
     :entity d/entity :pull d/pull :datoms d/datoms :as-of d/as-of :with d/with}))

(defn- try* [f]
  (try (f) (catch Throwable t [:error (or (:db/error (ex-data t)) (:db/error (ex-data (.getCause t))) (class t))])))

(defn- scenario [c]
  (let [{:keys [transact db entid ident q attribute entity pull datoms as-of with]} (api c)
        tx! #(deref (transact c %))
        out (atom [])
        note! (fn [k f] (swap! out conj [k (try* f)]))]
    (tx! [{:db/ident :p/name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
          {:db/ident :p/age :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
          {:db/ident :color/red}])
    (tx! [{:p/name "Al" :p/age 1}])
    (tx! [{:db/id :p/name :db/ident :p/full-name}])
    (let [db1 (db c)
          al (q '[:find ?e . :where [?e :p/age 1]] db1)]
      (note! :entid-old #(entid db1 :p/name))
      (note! :entid-new #(entid db1 :p/full-name))
      (note! :ident #(ident db1 (entid db1 :p/name)))
      (note! :attribute-old #(:ident (attribute db1 :p/name)))
      (note! :tx-old #(count (:tx-data (tx! [{:p/name "Bo"}]))))
      (let [db2 (db c)]
        (note! :q-old #(q '[:find ?v :where [_ :p/name ?v]] db2))
        (note! :pull-old #(pull db2 [:p/name :p/full-name] al))
        (note! :pull-star #(pull db2 '[*] al))
        (note! :entity-old #(:p/name (entity db2 al)))
        (note! :datoms-old #(count (seq (datoms db2 :aevt :p/name))))
        (note! :as-of-old #(entid (as-of db2 1001) :p/name))
        (note! :with-old #(entid (:db-after (with db2 [{:p/age 9}])) :p/name))
        (note! :with-rename #(let [w (:db-after (with db2 [{:db/id :p/age :db/ident :p/years}]))]
                               [(entid w :p/age) (entid w :p/years) (ident w (entid w :p/age))]))))
    (tx! [[:db/retract :color/red :db/ident :color/red]])
    (note! :retracted-ident #(entid (db c) :color/red))
    (note! :retracted-ident-ident #(ident (db c) (entid (db c) :color/red)))
    (tx! [{:db/ident :p/name :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])
    (note! :reassigned #(vector (entid (db c) :p/name) (entid (db c) :p/full-name)))
    (tx! [{:db/id :p/age :db/ident :p/age2}])
    (tx! [{:db/id :p/age2 :db/ident :p/age3}])
    (note! :chain #(let [x (db c)] [(entid x :p/age) (entid x :p/age2) (entid x :p/age3)]))
    (tx! [{:db/ident :s/a :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
          {:db/ident :s/b :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])
    (let [x (db c) sa (entid x :s/a) sb (entid x :s/b)]
      (tx! [{:db/id sa :db/ident :s/tmp}])
      (tx! [{:db/id sb :db/ident :s/a}])
      (note! :swap #(let [x (db c)] [sa sb (entid x :s/a) (entid x :s/b) (entid x :s/tmp)])))
    (note! :old-ident-in-avet #(count (seq (datoms (db c) :avet :db/ident :p/age))))
    (note! :q-on-old-ident-value #(q '[:find ?e :where [?e :db/ident :p/age]] (db c)))
    (note! :tx-old-alias-attr #(do (tx! [{:p/age3 7 :p/age 8}]) :ok))
    @out))

(deftest renamed-idents-match-datomic
  (o/with-both [cd cm]
    (let [rd (scenario cd) rm (scenario cm)]
      (doseq [[[k vd] [_ vm]] (map vector rd rm)]
        (testing (str k)
          (is (= (o/norm-value vd) (o/norm-value vm))))))))
