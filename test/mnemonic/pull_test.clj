(ns mnemonic.pull-test
  "Pull grammar; expected values were probed from Datomic 1.0.7745."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.walk :as walk]
            [mnemonic.api :as m]
            [mnemonic.conn :as conn]
            [mnemonic.index :as index]
            [mnemonic.pull :as pull]))

(use-fixtures :each (fn [f] (try (f) (finally (conn/reset-registry!)))))

(defn- attr [id vt card & {:as opts}]
  (merge {:db/ident id :db/valueType vt :db/cardinality card} opts))

(def ^:private one :db.cardinality/one)
(def ^:private many :db.cardinality/many)

(def ^:private schema
  [(attr :p/name :db.type/string one :db/unique :db.unique/identity)
   (attr :p/age :db.type/long one)
   (attr :p/tags :db.type/string many)
   (attr :p/nums :db.type/long many)
   (attr :p/friends :db.type/ref many)
   (attr :p/boss :db.type/ref one)
   (attr :p/addr :db.type/ref one :db/isComponent true)
   (attr :p/items :db.type/ref many :db/isComponent true)
   (attr :p/kind :db.type/ref one)
   (attr :a/city :db.type/string one)
   (attr :a/zip :db.type/string one)
   (attr :a/note :db.type/ref one :db/isComponent true)
   (attr :n/text :db.type/string one)
   {:db/ident :kind/admin}])

(defn- fixture []
  (let [u (str "mnemonic:mem://" (java.util.UUID/randomUUID))
        _ (m/create-database u)
        c (m/connect u)
        _ @(m/transact c schema)
        r @(m/transact
            c (concat
               [{:db/id "a" :p/name "Al" :p/age 30 :p/tags ["x" "y"] :p/kind :kind/admin
                 :p/addr {:a/city "NYC" :a/zip "1" :a/note {:n/text "hi"}}
                 :p/items [{:a/city "i1"} {:a/city "i2"}]}
                {:db/id "b" :p/name "Bo" :p/boss "a" :p/friends ["a" "c"]}
                {:db/id "c" :p/name "Cy" :p/boss "b" :p/friends ["a" "b"]}
                {:db/id "d" :p/name "Di"}
                {:db/id "t1" :p/name "T1" :p/friends ["t2"] :p/boss "t2"}
                {:db/id "t2" :p/name "T2" :p/friends ["t3"] :p/boss "t3" :p/age 2}
                {:db/id "t3" :p/name "T3" :p/friends ["t1"] :p/boss "t1" :p/age 3}
                {:db/id "x" :p/name "X" :p/friends ["y"]}
                {:db/id "y" :p/name "Y" :p/friends ["x"]}
                {:db/id "e" :p/name "Ed" :p/nums (vec (range 1100))
                 :p/friends (mapv #(str "g" %) (range 1100))}]
               (for [i (range 1100)] {:db/id (str "g" i) :p/age i})))]
    (let [db (m/db c)
          a (get (:tempids r) "a")
          v (fn [e at] (:v (first (m/datoms db :eavt e at))))
          addr (v a :p/addr)
          items (mapv :v (m/datoms db :eavt a :p/items))]
      {:db db
       :ids (assoc (into {} (filter (comp string? key)) (:tempids r))
                   "kind" (m/entid db :kind/admin)
                   "a/addr" addr
                   "a/note" (v addr :a/note)
                   "a/i1" (first items)
                   "a/i2" (second items))})))

(defn- named
  "Replaces {:db/id n} values by the tempid name of n so expectations are readable."
  [ids x]
  (let [rev (into {} (map (fn [[k v]] [v k])) ids)]
    (walk/postwalk
     (fn [v] (if (and (map? v) (contains? v :db/id))
               (update v :db/id #(get rev % %))
               v))
     x)))

(defmacro ^:private pulls
  "Asserts each [pattern entity-name expected] triple."
  [& cases]
  `(let [{:keys [~'db ~'ids]} (fixture)]
     ~@(for [[pattern e expected] (partition 3 cases)]
         `(is (= ~expected (named ~'ids (m/pull ~'db '~pattern (~'ids ~e))))
              (pr-str '~pattern ~e)))))

(defn- err [f]
  (try (f) :no-error
       (catch clojure.lang.ExceptionInfo e (:db/error (ex-data e)))))

(deftest basic-test
  (pulls
   [:p/name :p/age] "a" {:p/name "Al" :p/age 30}
   [:db/id :p/name] "a" {:db/id "a" :p/name "Al"}
   [:p/name] "d" {:p/name "Di"}
   [:p/age] "d" nil
   [:p/boss] "b" {:p/boss {:db/id "a"}}
   [:p/kind] "a" {:p/kind {:db/id "kind"}}
   [:p/tags] "a" {:p/tags ["x" "y"]}
   [:p/friends :p/tags] "b" {:p/friends [{:db/id "a"} {:db/id "c"}]}
   [] "a" nil
   [{}] "a" nil
   [:zz/foo] "a" nil
   [:p/name :bogus] "a" {:p/name "Al"}))

(deftest nested-test
  (pulls
   [{:p/boss [:p/name]}] "b" {:p/boss {:p/name "Al"}}
   [{:p/friends [:p/name]}] "b" {:p/friends [{:p/name "Al"} {:p/name "Cy"}]}
   [{:p/boss [:p/name] :p/friends [:p/name]}] "b"
   {:p/boss {:p/name "Al"} :p/friends [{:p/name "Al"} {:p/name "Cy"}]}
   [{:p/kind [:db/ident]}] "a" {:p/kind {:db/ident :kind/admin}}
   [{:p/boss [:p/age]}] "d" nil
   [{:zz/foo [:a]}] "a" nil))

(deftest reverse-test
  (pulls
   [:p/_boss] "a" {:p/_boss [{:db/id "b"}]}
   [{:p/_boss [:p/name]}] "a" {:p/_boss [{:p/name "Bo"}]}
   [:p/_friends] "a" {:p/_friends [{:db/id "b"} {:db/id "c"}]}
   [:p/_boss] "d" nil
   [:p/_name] "a" nil
   [(:p/_boss :as :bosses)] "a" {:bosses [{:db/id "b"}]}
   [(:p/_friends :limit 1)] "a" {:p/_friends [{:db/id "b"}]}))

(deftest component-test
  (pulls
   [:p/addr] "a" {:p/addr {:db/id "a/addr" :a/city "NYC" :a/zip "1"
                           :a/note {:db/id "a/note" :n/text "hi"}}}))

(deftest options-test
  (pulls
   [(:p/name :as :n)] "a" {:n "Al"}
   [(:p/name :as "n")] "a" {"n" "Al"}
   [(:p/missing :default "x")] "a" {:p/missing "x"}
   [(:p/age :default 5)] "d" {:p/age 5}
   [(:p/boss :default 5)] "a" {:p/boss 5}
   [(:p/tags :default [])] "d" {:p/tags []}
   [(:p/tags :limit 1)] "a" {:p/tags ["x"]}
   [(:p/tags :limit nil)] "a" {:p/tags ["x" "y"]}
   [(:p/tags :limit 0)] "a" nil
   [(:p/tags :limit 0 :default 3)] "a" {:p/tags 3}
   [{(:p/friends :limit 1 :as :fr) [:p/name]}] "b" {:fr [{:p/name "Al"}]}
   [(:p/age :xform str :as :s)] "a" {:s "30"}
   [(:p/name :xform clojure.edn/read-string)] "a" {:p/name 'Al}
   [(:p/name :bogus 1)] "a" {:p/name "Al"}
   [(:p/name :as :a) (:p/age :as :a)] "a" {:a "Al"}
   [{(:p/boss :default {}) [:p/name]}] "a" {:p/boss {}}
   [{(:p/friends :default 9) [:p/age]}] "x" {:p/friends 9}
   [(limit :p/tags 1)] "a" {:p/tags ["x"]}
   [(limit :p/tags nil)] "a" {:p/tags ["x" "y"]}
   [(default :p/zzz "d")] "a" {:p/zzz "d"}
   [{(limit :p/friends 1) [:p/name]}] "b" {:p/friends [{:p/name "Al"}]}
   [(:p/name) :p/age] "a" {:p/name "Al" :p/age 30}))

(deftest xform-test
  (let [{:keys [db ids]} (fixture)]
    (is (= {:p/name "Al!"} (m/pull db [(list :p/name :xform (fn [v] (str v "!")))] (ids "a")))
        "functions are accepted")
    (is (= {:p/tags 2} (m/pull db [(list :p/tags :xform count)] (ids "a")))
        "xform sees the whole card-many vector")
    (is (= {:p/missing ""} (m/pull db '[(:p/missing :xform str)] (ids "a")))
        "xform runs on absent values (Datomic parity)")))

(deftest limit-test
  (let [{:keys [db ids]} (fixture)
        e (ids "e")]
    (is (= 1000 (count (:p/nums (m/pull db [:p/nums] e)))) "default limit")
    (is (= 1000 (count (:p/friends (m/pull db [:p/friends] e)))))
    (is (= 1000 (count (:p/nums (m/pull db '[*] e)))))
    (is (= 1100 (count (:p/nums (m/pull db '[(:p/nums :limit nil)] e)))))
    (is (= 1100 (count (:p/friends (m/pull db '[{(:p/friends :limit nil) [:p/age]}] e)))))
    (is (= [0 1 2] (:p/nums (m/pull db '[(:p/nums :limit 3)] e))))))

(deftest wildcard-test
  (pulls
   [*] "b" {:db/id "b" :p/name "Bo" :p/friends [{:db/id "a"} {:db/id "c"}]
            :p/boss {:db/id "a"}}
   [*] "a" {:db/id "a" :p/name "Al" :p/age 30 :p/tags ["x" "y"] :p/kind {:db/id "kind"}
            :p/addr {:db/id "a/addr" :a/city "NYC" :a/zip "1"
                     :a/note {:db/id "a/note" :n/text "hi"}}
            :p/items [{:db/id "a/i1" :a/city "i1"} {:db/id "a/i2" :a/city "i2"}]}
   [* (:p/name :as :n)] "b" {:db/id "b" :n "Bo" :p/friends [{:db/id "a"} {:db/id "c"}]
                            :p/boss {:db/id "a"}}
   [* {:p/boss [:p/name]}] "b" {:db/id "b" :p/name "Bo" :p/boss {:p/name "Al"}
                               :p/friends [{:db/id "a"} {:db/id "c"}]}
   [* :p/_boss] "b" {:db/id "b" :p/name "Bo" :p/friends [{:db/id "a"} {:db/id "c"}]
                    :p/boss {:db/id "a"} :p/_boss [{:db/id "c"}]}
   [:db/id *] "d" {:db/id "d" :p/name "Di"}
   [* *] "d" {:db/id "d" :p/name "Di"}))

(deftest missing-entity-test
  (let [{:keys [db]} (fixture)]
    (is (nil? (m/pull db [:p/name] 99999999)))
    (is (= {:db/id 99999999} (m/pull db '[*] 99999999)))
    (is (= {:db/id 99999999} (m/pull db [:db/id] 99999999)))
    (is (= {:p/age 1} (m/pull db '[(:p/age :default 1)] 99999999)))
    (is (nil? (m/pull db [:p/name] [:p/name "Nope"])))
    (is (= {:p/name "Al"} (m/pull db [:p/name] [:p/name "Al"])))
    (is (nil? (m/pull db [:p/name] :kind/admin)))
    (is (= :kind/admin (:db/ident (m/pull db '[*] :kind/admin))))
    (is (nil? (m/pull db [:p/name] :nope/x)))
    (is (nil? (m/pull db nil 99999999)))
    (is (= :db.error/lookup-ref-attr-not-unique (err #(m/pull db [:p/name] [:p/age 1]))))))

(deftest recursion-test
  (pulls
   [:p/name {:p/boss ...}] "c" {:p/name "Cy" :p/boss {:p/name "Bo" :p/boss {:p/name "Al"}}}
   [{:p/boss ...}] "c" nil
   [:p/name {:p/boss 1}] "c" {:p/name "Cy" :p/boss {:p/name "Bo"}}
   [:p/name {:p/boss 2}] "c" {:p/name "Cy" :p/boss {:p/name "Bo" :p/boss {:p/name "Al"}}}
   [:p/name {:p/friends ...}] "b"
   {:p/name "Bo" :p/friends [{:p/name "Al"}
                             {:p/name "Cy" :p/friends [{:p/name "Al"} {:p/name "Bo"}]}]}
   [:db/id {:p/friends ...}] "x"
   {:db/id "x" :p/friends [{:db/id "y" :p/friends [{:db/id "x" :p/friends [{:db/id "y"}]}]}]}
   [:p/name {:p/friends ...}] "x"
   {:p/name "X" :p/friends [{:p/name "Y" :p/friends [{:p/name "X"}]}]}
   [:p/name {:p/boss ...}] "t1"
   {:p/name "T1" :p/boss {:p/name "T2" :p/boss {:p/name "T3" :p/boss {:p/name "T1"}}}}
   [:db/id :p/age {:p/boss ...}] "t1"
   {:db/id "t1" :p/boss {:db/id "t2" :p/age 2
                         :p/boss {:db/id "t3" :p/age 3
                                  :p/boss {:db/id "t1" :p/boss {:db/id "t2"}}}}}
   [:db/id :p/name {:p/boss 1}] "t1"
   {:db/id "t1" :p/name "T1" :p/boss {:db/id "t2" :p/name "T2" :p/boss {:db/id "t3"}}}
   [:p/name {:p/boss 1} {:p/friends 1}] "t1"
   {:p/name "T1" :p/boss {:p/name "T2" :p/friends [{:p/name "T3"}]}
    :p/friends [{:p/name "T2" :p/boss {:p/name "T3"}}]}
   [:p/name {(:p/boss :as :b) ...}] "t1"
   {:p/name "T1" :b {:p/name "T2" :b {:p/name "T3" :b {:p/name "T1"}}}}
   [:p/name {(:p/boss :default 1) 1}] "t1" {:p/name "T1" :p/boss {:p/name "T2" :p/boss 1}}
   [:p/name {:p/_boss ...}] "a" {:p/name "Al" :p/_boss [{:p/name "Bo" :p/_boss [{:p/name "Cy"}]}]}
   [:p/name {:p/friends [:p/name {:p/friends ...}]}] "t1"
   {:p/name "T1" :p/friends [{:p/name "T2" :p/friends
                              [{:p/name "T3" :p/friends
                                [{:p/name "T1" :p/friends [{:p/name "T2"}]}]}]}]}
   [* {:p/friends ...}] "x"
   {:db/id "x" :p/name "X" :p/friends
    [{:db/id "y" :p/name "Y" :p/friends [{:db/id "x" :p/name "X" :p/friends [{:db/id "y"}]}]}]}))

(deftest errors-test
  (let [{:keys [db ids]} (fixture)
        pl (fn [p] (err #(m/pull db p (ids "a"))))]
    (is (= :pull/duplicate-attribute (pl '[:p/name :p/name])))
    (is (= :pull/duplicate-attribute (pl '[{:p/boss [:p/name]} :p/boss])))
    (is (= :pull/duplicate-attribute (pl '[:p/_boss :p/_boss])))
    (is (= :no-error (pl '[:p/name :p/_boss])))
    (is (= :no-error (pl '[:db/id :db/id])))
    (is (= :db.error/invalid-attr-spec (pl '["p/name"])))
    (is (= :db.error/invalid-attr-spec (pl '[5])))
    (is (= :db.error/invalid-attr-spec (pl '[(:p/name :limit)])))
    (is (= :db.error/invalid-attr-spec (pl '[(foo :p/friends 1)])))
    (is (= :db.error/invalid-recur-limit (pl '[{:p/boss 0}])))
    (is (= :db.error/invalid-recur-limit (pl '[{:p/boss -1}])))
    (is (= :db.error/invalid-recur-limit (pl '[{:p/boss "x"}])))
    (is (= :db.error/not-a-keyword (pl '[{:p/name [:x]}])))))

(deftest pull-many-test
  (let [{:keys [db ids]} (fixture)]
    (is (= [{:p/name "Al"} {:p/name "Bo"} nil {:p/name "Cy"}]
           (m/pull-many db [:p/name] [(ids "a") (ids "b") 99999999 (ids "c")])))
    (is (= [] (m/pull-many db [:p/name] [])))
    (is (= [] (m/pull-many db [:p/name] nil)))
    (is (= [{:p/name "Al"} nil] (m/pull-many db [:p/name] [[:p/name "Al"] :kind/admin])))
    (is (= [nil] (m/pull-many db [:p/name] [[:p/name "Zz"]])))
    (is (vector? (m/pull-many db [:p/name] [(ids "a")])))))

(deftest ast-test
  (let [{:keys [db ids]} (fixture)
        ast (pull/parse-pattern '[:p/name {:p/boss [:p/name]}])]
    (is (= {:p/name "Bo" :p/boss {:p/name "Al"}}
           (pull/pull-from-ast db ast (ids "b"))))
    (is (identical? ast (pull/parse-pattern ast)) "an AST parses to itself")
    (is (= (pull/pull-from-ast db ast (ids "b")) (m/pull db '[:p/name {:p/boss [:p/name]}] (ids "b"))))))

(deftest no-cursor-leak-test
  (let [{:keys [db ids]} (fixture)
        before (.get ^java.util.concurrent.atomic.AtomicLong index/open-cursors)]
    (dotimes [_ 5]
      (m/pull db '[* {:p/friends ...} :p/_boss (:p/tags :limit 1)] (ids "b"))
      (m/pull db '[:p/name (:p/friends :limit 1)] (ids "e"))
      (m/touch (m/entity db (ids "a")))
      (:p/_friends (m/entity db (ids "a"))))
    (is (= before (.get ^java.util.concurrent.atomic.AtomicLong index/open-cursors))
        "pull and entity close every cursor eagerly")))

(defn shout [v] (str v "!"))

(deftest xform-allowlist-test
  (let [{:keys [db ids]} (fixture)
        forbidden? (fn [pattern]
                     (try (m/pull db pattern (ids "a")) false
                          (catch clojure.lang.ExceptionInfo e
                            (and (= :db.error/xform-not-allowed (:db/error (ex-data e)))
                                 (= :cognitect.anomalies/forbidden (:cognitect.anomalies/category (ex-data e)))))))]
    (testing "symbols naming arbitrary loaded vars are refused (as in Datomic)"
      (doseq [x '[clojure.core/load-string load-string read-string clojure.core/eval
                  clojure.core/slurp count clojure.string/upper-case not.loaded.ns/f]]
        (is (forbidden? [(list :p/name :xform x)]) (str x)))
      (is (forbidden? '[{(:p/boss :xform clojure.core/load-string) [:p/name]}]) "nested")
      (is (nil? (find-ns 'not.loaded.ns)) "namespaces are never loaded for unlisted symbols"))
    (testing "Datomic's built-in xforms"
      (is (= {:p/name "Al"} (m/pull db '[(:p/name :xform str)] (ids "a"))))
      (is (= {:p/name "Al"} (m/pull db '[(:p/name :xform name)] (ids "a"))))
      (is (= {:p/name :Al} (m/pull db '[(:p/name :xform keyword)] (ids "a"))))
      (is (= {:p/name 'Al} (m/pull db '[(:p/name :xform symbol)] (ids "a"))))
      (is (= {:p/name 'Al} (m/pull db '[(:p/name :xform clojure.edn/read-string)] (ids "a")))))
    (testing "mnemonic/extensions.edn on the classpath lists more"
      (is (= {:p/name "Al!"} (m/pull db '[(:p/name :xform mnemonic.pull-test/shout)] (ids "a")))))
    (testing "*xforms* adds symbols for a dynamic extent; the parse cache respects it"
      (is (forbidden? '[(:p/name :xform clojure.string/upper-case)]))
      (binding [pull/*xforms* '#{clojure.string/upper-case}]
        (is (= {:p/name "AL"} (m/pull db '[(:p/name :xform clojure.string/upper-case)] (ids "a")))))
      (is (forbidden? '[(:p/name :xform clojure.string/upper-case)])))
    (testing "function and var objects from code are trusted"
      (is (= {:p/name "AL"} (m/pull db [(list :p/name :xform clojure.string/upper-case)] (ids "a"))))
      (is (= {:p/name "AL"} (m/pull db [(list :p/name :xform #'clojure.string/upper-case)] (ids "a")))))
    (testing "in queries"
      (is (thrown? Exception (m/q '[:find (pull ?e [(:p/name :xform clojure.core/load-string)]) :in $ ?e]
                                  db (ids "a")))))))
