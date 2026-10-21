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

(deftest key-set-test
  (let [{:keys [db ids]} (fixture)
        a (m/entity db (ids "a"))]
    (is (= #{:p/name :p/age :p/tags :p/addr :p/items :p/kind} (.keySet ^mnemonic.entity.IEntityKeys a)))))

(deftest component-cycle-test
  (let [u (str "mnemonic:mem://" (java.util.UUID/randomUUID))
        _ (m/create-database u)
        c (m/connect u)
        _ @(m/transact c [{:db/id "r" :db/ident :c/ref :db/valueType :db.type/ref
                           :db/cardinality :db.cardinality/one :db/isComponent true}
                          {:db/id "n" :db/ident :c/name :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}])
        tids (:tempids @(m/transact c [{:db/id "x" :c/name "X" :c/ref "y"}
                                       {:db/id "y" :c/name "Y" :c/ref "x"}]))
        db (m/db c)
        x (tids "x") y (tids "y")]
    (testing "touch terminates and prints finitely"
      (is (string? (pr-str (m/touch (m/entity db x))))))
    (testing "pull falls back to {:db/id} on a component cycle (Datomic overflows)"
      (is (= {:db/id x :c/name "X" :c/ref {:db/id y :c/name "Y" :c/ref {:db/id x}}}
             (m/pull db '[*] x)))
      (is (= {:c/ref {:db/id y :c/name "Y" :c/ref {:db/id x}}} (m/pull db '[:c/ref] x)))
      (is (= {:c/name "X" :c/ref {:c/ref {:db/id x :c/name "X" :c/ref {:db/id y}}}}
             (m/pull db '[:c/name {:c/ref [:c/ref]}] x))))))

(deftest layered-db-test
  (let [{:keys [c ids]} (fixture)
        t0 (m/basis-t (m/db c))
        _ @(m/transact c [{:db/id (ids "a") :p/age 31 :p/tags ["z"]}])
        db (m/db c)
        old (m/as-of db t0)
        a (ids "a")]
    (is (= 31 (:p/age (m/entity db a))))
    (is (= 30 (:p/age (m/entity old a))) "as-of entity")
    (is (= {:p/age 30} (m/pull old [:p/age] a)))
    (is (= {:p/age 31} (m/pull db [:p/age] a)))
    (let [age-id (:id (m/attribute db :p/age))
          f (m/filter db (fn [_ d] (not= (:a d) age-id)))]
      (is (nil? (:p/age (m/entity f a))))
      (is (= {:p/name "Al"} (m/pull f [:p/name :p/age] a))))
    (let [h (m/history db)
          msg (fn [f] (try (f) nil (catch IllegalStateException e (ex-message e))))]
      (is (= "Can't create entity from history" (msg #(m/entity h a))))
      (is (= "Can't pull from history" (msg #(m/pull h [:p/age] a))))
      (is (= "Can't pull from history" (msg #(m/pull-many h [:p/age] [a])))))
    (is (not= (m/entity old a) (m/entity db a)))))

(deftest shared-entity-concurrent-reads-test
  (testing "one entity read by many threads (lazy cache) always yields the same values"
    (let [{:keys [db ids]} (fixture)
          a (m/entity db (ids "a"))
          expected {:p/name "Al" :p/age 30 :p/tags #{"x" "y"} :p/kind :kind/admin}
          absent #{:p/nothing :p/boss :p/friends}
          ks [:p/name :p/age :p/tags :p/kind :p/nothing :p/boss :p/friends]
          run (fn []
                (dotimes [_ 200]
                  (doseq [k (shuffle ks)]
                    (let [v (get a k)]
                      (when (and (contains? expected k) (not= (expected k) v))
                        (throw (ex-info "torn read" {:k k :v v})))
                      (when (and (absent k) (some? v))
                        (throw (ex-info "phantom value" {:k k :v v})))))
                  (when-not (= 6 (count (seq a)))
                    (throw (ex-info "torn entity seq" {:n (count (seq a))})))))
          fs (doall (repeatedly 8 #(future (run))))]
      (doseq [f fs] (is (nil? @f)))))
  (testing "absent attributes are cached as absent, not as a value"
    (let [{:keys [db ids]} (fixture)
          b (m/entity db (ids "b"))]
      (is (nil? (:p/age b)))
      (is (nil? (:p/age b)))
      (is (= :nf (get b :p/age :nf)))
      (is (not (contains? b :p/age))))))

(deftest attribute-resolution-test
  ;; absent, unknown and renamed attributes (reads served from cached [e] segments)
  (let [{:keys [db c ids]} (fixture)
        a (m/entity db (ids "a"))]
    (is (= 30 (:p/age a)))
    (is (nil? (:p/boss a)) "absent attribute")
    (is (nil? (:no/such a)) "unknown attribute")
    (is (= #{"x" "y"} (:p/tags a)))
    (is (= :kind/admin (:p/kind a)))
    (is (= "Al" (:p/name a)))
    (testing "renamed attribute: the old ident still resolves"
      @(m/transact c [{:db/id :p/age :db/ident :p/years}])
      (let [a2 (m/entity (m/db c) (ids "a"))]
        (is (= 30 (:p/age a2)))
        (is (= 30 (:p/years a2)))))))
