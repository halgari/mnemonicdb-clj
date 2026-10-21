(ns mnemonic.entity-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [mnemonic.api :as m]
            [mnemonic.conn :as conn]))

(use-fixtures :each (fn [f] (try (f) (finally (conn/reset-registry!)))))

(def ^:private schema
  [{:db/ident :p/name :db/valueType :db.type/string :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :p/age :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :p/tags :db/valueType :db.type/string :db/cardinality :db.cardinality/many}
   {:db/ident :p/friends :db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   {:db/ident :p/boss :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/ident :p/addr :db/valueType :db.type/ref :db/cardinality :db.cardinality/one
    :db/isComponent true}
   {:db/ident :p/items :db/valueType :db.type/ref :db/cardinality :db.cardinality/many
    :db/isComponent true}
   {:db/ident :p/kind :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :a/city :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :kind/admin :db/doc "an admin"}])

(defn- fixture []
  (let [u (str "mnemonic:mem://" (java.util.UUID/randomUUID))
        _ (m/create-database u)
        c (m/connect u)
        _ @(m/transact c schema)
        r @(m/transact c [{:db/id "a" :p/name "Al" :p/age 30 :p/tags ["x" "y"] :p/kind :kind/admin
                           :p/addr {:a/city "NYC"}
                           :p/items [{:a/city "i1"} {:a/city "i2"}]}
                          {:db/id "b" :p/name "Bo" :p/boss "a" :p/friends ["a" "c"]}
                          {:db/id "c" :p/name "Cy" :p/boss "b" :p/friends ["a" "b"]}])]
    {:db (m/db c) :c c :ids (:tempids r)}))

(deftest lookup-test
  (let [{:keys [db ids]} (fixture)
        a (m/entity db (ids "a"))]
    (is (= "Al" (:p/name a)))
    (is (= "Al" (get a :p/name)))
    (is (= "Al" (a :p/name)) "IFn")
    (is (= :nf (get a :p/nothing :nf)))
    (is (nil? (:p/nothing a)))
    (is (= (ids "a") (:db/id a)))
    (is (= #{"x" "y"} (:p/tags a)) "cardinality-many is a set")
    (is (= :kind/admin (:p/kind a)) "enum ref is the ident keyword")
    (is (= (ids "a") (:db/id (:p/boss (m/entity db (ids "b"))))) "ref is an entity")
    (is (= #{(ids "a") (ids "c")} (set (map :db/id (:p/friends (m/entity db (ids "b")))))))
    (is (= "Bo" (:p/name (:p/boss (m/entity db (ids "c"))))))
    (is (contains? a :p/name))
    (is (not (contains? a :p/boss)))
    (is (= [:p/name "Al"] (.entryAt ^clojure.lang.Associative a :p/name)))
    (is (nil? (.entryAt ^clojure.lang.Associative a :p/boss)))
    (is (= "Al" (:p/name (m/entity db [:p/name "Al"]))) "lookup ref")
    (is (nil? (m/entity db [:p/name "Zz"])))
    (is (= :kind/admin (:db/ident (m/entity db :kind/admin))))
    (is (= "an admin" (:db/doc (m/entity db :kind/admin))))
    (is (nil? (m/entity db :nope)))
    (is (nil? (m/entity db nil)))
    (is (= 99999999 (:db/id (m/entity db 99999999))) "any long yields an entity")
    (is (nil? (seq (m/entity db 99999999))))
    (is (= :db.error/not-a-keyword
           (try (m/entity db "x") (catch clojure.lang.ExceptionInfo e (:db/error (ex-data e))))))
    (is (= "Al" (get a ":p/name")) "colon strings resolve like keywords")
    (is (= :db.error/not-a-keyword
           (try (get a "p/name") (catch clojure.lang.ExceptionInfo e (:db/error (ex-data e))))))))

(deftest reverse-test
  (let [{:keys [db ids]} (fixture)
        a (m/entity db (ids "a"))]
    (is (= #{(ids "b")} (set (map :db/id (:p/_boss a)))))
    (is (= #{(ids "b") (ids "c")} (set (map :db/id (:p/_friends a)))))
    (is (set? (:p/_boss a)))
    (is (nil? (:p/_zzz a)))
    (is (nil? (:p/_name a)))
    (let [addr (:p/addr a)]
      (is (= (ids "a") (:db/id (:p/_addr addr))) "component reverse is a single entity")
      (is (= (ids "a") (:db/id (:p/_items (first (:p/items a)))))))
    (is (= #{(:db/id a)} (set (map :db/id (:p/_kind (m/entity db :kind/admin))))))))

(deftest seq-and-touch-test
  (let [{:keys [db ids]} (fixture)
        a (m/entity db (ids "a"))]
    (testing "keys/seq load everything but :db/id"
      (is (= #{:p/name :p/age :p/tags :p/addr :p/items :p/kind} (set (keys a))))
      (is (= 6 (count a)))
      (is (= #{"Al" 30} (set (filter #(or (string? %) (number? %)) (vals a)))))
      (is (= "Al" (second (first (filter #(= :p/name (key %)) (seq a)))))))
    (testing "untouched prints as {:db/id n}"
      (is (= (pr-str {:db/id (ids "b")}) (pr-str (m/entity db (ids "b"))))))
    (testing "touch loads attributes and touches components"
      (let [t (m/touch (m/entity db (ids "a")))]
        (let [printed (read-string (pr-str t))]
          (is (= "Al" (:p/name printed)))
          (is (= "NYC" (:a/city (:p/addr printed))) "component expanded")
          (is (= #{"i1" "i2"} (set (map :a/city (:p/items printed)))))
          (is (= :kind/admin (:p/kind printed))))))
    (testing "touch does not expand plain refs"
      (let [printed (read-string (pr-str (m/touch (m/entity db (ids "b")))))]
        (is (= #{{:db/id (ids "a")} {:db/id (ids "c")}} (set (:p/friends printed))))
        (is (= {:db/id (ids "a")} (:p/boss printed)))))
    (testing "touch returns the same entity"
      (let [e (m/entity db (ids "a"))]
        (is (identical? e (m/touch e)))))
    (testing "touching a missing entity"
      (is (= {:db/id 99999999} (read-string (pr-str (m/touch (m/entity db 99999999)))))))))

(deftest equality-test
  (let [{:keys [db c ids]} (fixture)
        a (m/entity db (ids "a"))]
    (is (= a (m/entity db (ids "a"))))
    (is (= (hash a) (hash (m/entity db (ids "a")))))
    (is (= a (m/entity (m/db c) (ids "a"))) "same basis, different db value")
    (is (not= a (m/entity db (ids "b"))))
    (is (not= a {:db/id (ids "a")}))
    (is (identical? db (m/entity-db a)))
    (is (not= a (do @(m/transact c [{:db/id (ids "a") :p/age 31}])
                    (m/entity (m/db c) (ids "a"))))
        "different basis")
    (is (= 30 (:p/age a)) "old db value unchanged")))

(deftest read-only-test
  (let [{:keys [db ids]} (fixture)
        a (m/entity db (ids "a"))]
    (is (thrown? UnsupportedOperationException (assoc a :x 1)))
    (is (thrown? UnsupportedOperationException (conj a [:x 1])))))
