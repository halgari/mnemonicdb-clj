(ns mnemonic.tx-advanced-test
  "Advanced transaction features that do not need an oracle: API shape of function/invoke,
  stored representation, documented deviations and a few direct checks. The behavior matrix
  against Datomic lives in diff-tx-advanced-test."
  (:require [clojure.edn :as edn]
            [clojure.test :refer [deftest is testing]]
            [mnemonic.api :as m]
            [mnemonic.test-fns :as tf]))

(defmacro ^:private with-conn [[c] & body]
  `(let [uri# (str "mnemonic:mem://" (random-uuid))]
     (m/create-database uri#)
     (let [~c (m/connect uri#)]
       (try ~@body
            (finally (m/release ~c) (m/delete-database uri#))))))

(defn- tx! [c data] @(m/transact c data))

(defn- kind [c data]
  (try (tx! c data) nil
       (catch Exception e (let [x (or (ex-cause e) e)] (or (:db/error (ex-data x)) [:unclassified (class x)])))))

(defn- attrs [& specs]
  (for [[id vt card & {:as opts}] specs]
    (merge {:db/id (str "attr-" id) :db/ident id :db/valueType vt :db/cardinality card} opts)))

(def ^:private one :db.cardinality/one)
(def ^:private many :db.cardinality/many)

(def ^:private base
  (attrs [:p/name :db.type/string one :db/unique :db.unique/identity]
         [:p/age :db.type/long one]
         [:p/kids :db.type/ref many :db/isComponent true]
         [:t/a :db.type/string one]
         [:t/b :db.type/long one]
         [:t/ab :db.type/tuple one :db/tupleAttrs [:t/a :t/b]]))

(defn- vals-of [c e a]
  (mapv :v (m/datoms (m/db c) :eavt e a)))

(deftest cas-with-numeric-entity-id
  (with-conn [c]
    (tx! c base)
    (let [e (get (:tempids (tx! c [{:db/id "x" :p/name "x" :p/age 1}])) "x")]
      (tx! c [[:db/cas e :p/age 1 2]])
      (is (= [2] (vals-of c e :p/age)))
      (is (= :db.error/cas-failed (kind c [[:db/cas e :p/age 1 3]])))
      (is (= :db.error/cas-failed (kind c [[:db.fn/cas e :p/age nil 3]]))))))

(deftest cas-failed-error-data
  (with-conn [c]
    (tx! c base)
    (tx! c [{:db/id "x" :p/name "x" :p/age 1}])
    (let [e (try (tx! c [[:db/cas [:p/name "x"] :p/age 5 6]])
                 (catch Exception e (ex-cause e)))
          data (ex-data e)]
      (is (= :db.error/cas-failed (:db/error data)))
      (is (= :cognitect.anomalies/conflict (:cognitect.anomalies/category data)))
      (is (= [:p/name "x"] (:e data)))
      (is (= :p/age (:a data)))
      (is (= 5 (:v-old data)))
      (is (= 1 (:v data))))))

(deftest retract-entity-cascades
  (with-conn [c]
    (tx! c base)
    (let [r (tx! c [{:db/id "a" :p/name "A" :p/kids [{:p/name "K1" :p/kids [{:p/name "K2"}]}]}
                    {:db/id "b" :p/name "B" :p/kids ["a"]}])
          a (get (:tempids r) "a")]
      (tx! c [[:db/retractEntity a]])
      (is (empty? (m/datoms (m/db c) :eavt a)))
      (is (empty? (m/datoms (m/db c) :vaet a)))
      (is (empty? (m/datoms (m/db c) :avet :p/name "K1")))
      (is (empty? (m/datoms (m/db c) :avet :p/name "K2")))
      (is (seq (m/datoms (m/db c) :avet :p/name "B"))))))

(deftest function-objects
  (testing "function builds a callable fn object with the documented shape"
    (let [f (m/function '{:lang "clojure" :params [db e n] :code [[:db/add e :p/age (inc n)]]})]
      (is (= :clojure (:lang f)))
      (is (= '[db e n] (:params f)))
      (is (= [] (:requires f)))
      (is (= [] (:imports f)))
      (is (string? (:code f)))
      (is (= [[:db/add 1 :p/age 3]] (f nil 1 2)))
      (is (= [[:db/add 1 :p/age 3]] (apply f nil [1 2])))))
  (testing "requires and imports"
    (let [f (m/function '{:lang :clojure :params [db x] :requires [[clojure.string :as str]]
                          :imports [[java.util Date]] :code [(str/upper-case x) (.getTime (Date. 5))]})]
      (is (= ["AB" 5] (f nil "ab")))))
  (testing "code as a string"
    (is (= [[:db/add 1 :p/age 2]]
           ((m/function {:lang "clojure" :params '[db] :code "[[:db/add 1 :p/age 2]]"}) nil))))
  (testing "missing parts are rejected"
    (is (thrown? Throwable (m/function '{:params [db] :code []})))))

(deftest stored-functions-and-invoke
  (with-conn [c]
    (tx! c base)
    (tx! c [{:db/ident :my/set-age
             :db/fn (m/function '{:lang "clojure" :params [db e n] :code [[:db/add e :p/age n]]})}])
    (tx! c [{:db/id "x" :p/name "x"}])
    (tx! c [[:my/set-age [:p/name "x"] 41]])
    (is (= [41] (vals-of c [:p/name "x"] :p/age)))
    (testing "invoke by ident and by entity id, db is passed explicitly"
      (let [db (m/db c)]
        (is (= [[:db/add [:p/name "x"] :p/age 5]] (m/invoke db :my/set-age db [:p/name "x"] 5)))
        (is (= [[:db/add [:p/name "x"] :p/age 5]]
               (m/invoke db (m/entid db :my/set-age) db [:p/name "x"] 5)))))
    (testing "invoke of the built-in functions"
      (let [db (m/db c)]
        (is (= [[:db/add [:p/name "x"] :p/age 42]] (m/invoke db :db.fn/cas db [:p/name "x"] :p/age 41 42)))
        (is (= :db.error/cas-failed
               (try (m/invoke db :db/cas db [:p/name "x"] :p/age 1 2) nil
                    (catch Exception e (:db/error (ex-data e))))))
        (let [ops (m/invoke db :db.fn/retractEntity db [:p/name "x"])]
          (is (every? #(= :db/retract (first %)) ops))
          (is (= #{:p/name :p/age} (set (map #(nth % 2) ops)))))))
    (testing "invoke errors"
      (let [db (m/db c)]
        (is (= :db.error/not-an-entity
               (try (m/invoke db :no/such db) nil (catch Exception e (:db/error (ex-data e))))))
        (is (= :db.error/not-a-data-function
               (try (m/invoke db :p/name db) nil (catch Exception e (:db/error (ex-data e))))))))))

(deftest stored-function-value-representation
  (with-conn [c]
    (tx! c base)
    (tx! c [{:db/ident :my/f :db/fn (m/function '{:lang "clojure" :params [db] :code []})}])
    (let [d (first (m/datoms (m/db c) :eavt :my/f :db/fn))]
      (is (string? (:v d)) "stored as the EDN text of the function map (documented deviation)")
      (is (= :clojure (:lang (edn/read-string (:v d))))))))

(deftest tx-function-exceptions-propagate
  (with-conn [c]
    (tx! c base)
    (let [e (try (tx! c [['mnemonic.test-fns/bad]]) (catch Exception e (ex-cause e)))]
      (is (= {:x 1} (ex-data e)))
      (is (= "boom" (ex-message e))))))

(deftest excision-attributes-are-unsupported
  (with-conn [c]
    (tx! c base)
    (doseq [data [[[:db/add "x" :db/excise :p/name]]
                  [{:db/id "x" :db.excise/attrs [:p/name]}]
                  [{:db/id "x" :db.excise/beforeT 1000}]
                  [{:db/id "x" :db.excise/before #inst "2020"}]]]
      (is (= :db.error/unsupported (kind c data)) (pr-str data)))))

(deftest fulltext-is-accepted-in-schema
  (with-conn [c]
    (tx! c (attrs [:doc/body :db.type/string one :db/fulltext true]))
    (tx! c [{:db/id "x" :doc/body "hello world"}])
    (is (= ["hello world"] (mapv :v (m/datoms (m/db c) :aevt :doc/body))))))

(deftest composite-direct-ops-are-ignored
  (with-conn [c]
    (tx! c base)
    (let [e (get (:tempids (tx! c [{:db/id "x" :t/a "s" :t/b 1}])) "x")]
      (is (= [["s" 1]] (vals-of c e :t/ab)))
      (tx! c [[:db/add e :t/ab ["q" 9]]])
      (is (= [["s" 1]] (vals-of c e :t/ab)))
      (tx! c [[:db/retract e :t/ab ["s" 1]]])
      (is (= [["s" 1]] (vals-of c e :t/ab)))
      (tx! c [[:db/retract e :t/b 1]])
      (is (= [["s" nil]] (vals-of c e :t/ab)))
      (tx! c [[:db/retract e :t/a "s"]])
      (is (empty? (vals-of c e :t/ab))))))

(deftest repeated-alteration-keeps-one-alter-datom
  (with-conn [c]
    (tx! c base)
    (tx! c [{:db/id :p/age :db/noHistory true}])
    (tx! c [{:db/id :p/age :db/noHistory false}])
    (is (= 1 (count (seq (m/datoms (m/db c) :eavt 0 :db.alter/attribute)))))))

(deftest attr-pred-and-ensure-resolution-failures
  (with-conn [c]
    (tx! c base)
    (tx! c (attrs [:v/bad :db.type/long one :db.attr/preds ['nonexist.ns/pred]]))
    (is (= [:unclassified java.io.FileNotFoundException] (kind c [{:db/id "x" :v/bad 1}])))
    (tx! c [{:db/ident :ent/bad :db.entity/preds ['nonexist.ns/pred]}])
    (is (= [:unclassified java.io.FileNotFoundException]
           (kind c [{:db/id "x" :p/name "n" :db/ensure :ent/bad}])))))

(deftest entity-pred-sees-db-after
  (with-conn [c]
    (tx! c base)
    (tx! c [{:db/ident :ent/is99 :db.entity/preds ['mnemonic.test-fns/age-is-99?]}])
    (is (nil? (kind c [{:db/id "x" :p/name "n" :p/age 99 :db/ensure :ent/is99}])))
    (is (= :db.error/entity-pred (kind c [{:db/id "y" :p/name "m" :p/age 98 :db/ensure :ent/is99}])))
    (is (= :db.error/entity-pred (kind c [[:db/add [:p/name "n"] :p/age 5]
                                          [:db/add [:p/name "n"] :db/ensure :ent/is99]])))
    (is (tf/datoms (m/db c) :avet :p/name "n"))))

(deftest namespaces-load-in-either-order
  (doseq [first-ns ['mnemonic.tx-fns 'mnemonic.tx]]
    (let [java (str (java.io.File. (java.io.File. (System/getProperty "java.home") "bin") "java"))
          form (pr-str `(do (require '~first-ns) (require 'mnemonic.api) (shutdown-agents)))
          pb (doto (ProcessBuilder. ^java.util.List
                                    [java "--enable-native-access=ALL-UNNAMED"
                                     "-cp" (System/getProperty "java.class.path")
                                     "clojure.main" "-e" form])
               (.redirectErrorStream true))
          proc (.start pb)
          out (slurp (.getInputStream proc))]
      (is (zero? (.waitFor proc)) (str first-ns " first: " out)))))

(deftest compiled-function-cache-is-reused
  (let [f (m/function '{:lang "clojure" :params [x] :code (inc x)})]
    (is (= 2 (f 1)))
    (is (= 3 (f 2)))
    (is (thrown? Exception ((m/function '{:lang "clojure" :params [x] :code (nope-undefined x)}) 1)))))
