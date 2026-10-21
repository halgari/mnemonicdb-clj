(ns mnemonic.schema-test
  (:require [clojure.test :refer [deftest is testing]]
            [mnemonic.bootstrap :as bootstrap]
            [mnemonic.schema :as schema]))

(def s (schema/schema-from-datoms (bootstrap/bootstrap-datoms)))

(deftest bootstrap-schema-test
  (testing ":db/ident attribute"
    (let [a (schema/attr s :db/ident)]
      (is (= 10 (:id a)))
      (is (= :db/ident (:ident a)))
      (is (= :db.type/keyword (:value-type a)))
      (is (= :db.cardinality/one (:cardinality a)))
      (is (= :db.unique/identity (:unique a)))
      (is (schema/indexed? a))))
  (testing "66 idents (Datomic probe: 66, brief said 64)"
    (is (= 66 (count (:idents s)))))
  (testing "lookup by id, entid"
    (is (= (schema/attr s :db/valueType) (schema/attr s 40)))
    (is (= 40 (schema/entid s :db/valueType)))
    (is (= 40 (schema/entid s 40)))
    (is (nil? (schema/entid s :nope)))
    (is (nil? (schema/attr s :nope)))
    (is (= :db.type/ref (:value-type (schema/attr s :db/valueType))))
    (is (= :db.cardinality/many (:cardinality (schema/attr s :db.install/attribute))))
    (is (= :db/ident (get (:ids s) 10)))
    (is (nil? (schema/attr s :db.part/db)) "non-attribute idents are not attrs"))
  (testing "tuples and indexing"
    (is (= :db.type/tuple (:value-type (schema/attr s :db/tupleTypes))))
    (is (not (schema/indexed? (schema/attr s :db/doc))))))
