(ns mnemonic.diff-tx-advanced-test
  "Differential tests for the advanced transaction features: the same transactions run
  against Datomic (oracle) and MnemonicDB must produce equal tx-data, tempids, error kinds
  and index contents.

  A step is tx-data, a fn of a tempid fn, or `(ok data)` / `(err kind data)` which also pin
  the expected outcome (guards against both systems failing for the same wrong reason).
  `(fnspec m)` stands for `(d/function m)` / `(m/function m)` on the respective side; the
  values of `:db/fn` datoms are compared as `:fn` because the stored representation differs."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.walk :as walk]
            [datomic.api :as d]
            [mnemonic.api :as m]
            [mnemonic.oracle :as o]
            [mnemonic.test-fns]))

(defrecord FnSpec [m])
(defn fnspec [m] (->FnSpec m))

(defn ok [data] {::data data ::expect :ok})
(defn err [kind data] {::data data ::expect kind})

(def one :db.cardinality/one)
(def many :db.cardinality/many)

(defn- attr-tx [specs]
  (for [[id vt card & {:as opts}] specs]
    (merge {:db/id (str "attr-" id) :db/ident id :db/valueType vt :db/cardinality card} opts)))

(def schema
  (attr-tx
   [[:p/name :db.type/string one :db/unique :db.unique/identity]
    [:p/age :db.type/long one]
    [:p/tags :db.type/string many]
    [:p/friend :db.type/ref many]
    [:p/boss :db.type/ref one]
    [:p/addr :db.type/ref one :db/isComponent true]
    [:p/kids :db.type/ref many :db/isComponent true]
    [:a/city :db.type/string one]]))

(def tuple-schema
  (attr-tx
   [[:t/a :db.type/string one]
    [:t/b :db.type/long one]
    [:t/c :db.type/ref one]
    [:t/d :db.type/string many]
    [:t/ab :db.type/tuple one :db/tupleAttrs [:t/a :t/b]]
    [:t/abu :db.type/tuple one :db/tupleAttrs [:t/b :t/a] :db/unique :db.unique/identity]
    [:t/abc :db.type/tuple one :db/tupleAttrs [:t/a :t/b :p/age]]
    [:t/hetero :db.type/tuple one :db/tupleTypes [:db.type/string :db.type/long]]
    [:t/homo :db.type/tuple one :db/tupleType :db.type/long]]))

(def ^:private who {:db/id "a" :p/name "al" :p/age 1 :p/tags ["x" "y"]})

(defn- install [& specs] (attr-tx specs))

;; ------------------------------------------------------------------ scenarios

(def cas-scenario
  ["errors: cas"
   [schema
    (ok [who])
    (ok [[:db/cas [:p/name "al"] :p/age 1 2]])
    (err :db.error/cas-failed [[:db/cas [:p/name "al"] :p/age 1 3]])
    (ok [[:db.fn/cas [:p/name "al"] :p/age 2 3]])
    (ok [[:db/cas [:p/name "al"] :a/city nil "c"]])
    (err :db.error/cas-failed [[:db/cas [:p/name "al"] :a/city nil "d"]])
    (ok [[:db/cas [:p/name "al"] :p/age 3 3]])
    (err :db.error/invalid-cas-many [[:db/cas [:p/name "al"] :p/tags "x" "z"]])
    (err :db.error/invalid-cas-many [[:db/cas [:p/name "al"] :p/tags "q" "z"]])
    (err :db.error/invalid-cas [[:db/cas [:p/name "al"] :p/age 3 nil]])
    (err :db.error/invalid-cas [[:db/cas nil :p/age 3 4]])
    (err :db.error/not-an-entity [[:db/cas [:p/name "zz"] :p/age nil 3]])
    (err :db.error/not-a-keyword [[:db/cas "t" :p/age nil 3]])
    (err :db.error/not-an-entity [[:db/cas [:p/name "al"] :nope 3 4]])
    (err :db.error/cas-failed [[:db/cas [:p/name "al"] :p/age "x" 4]])
    (err :db.error/wrong-type-for-attribute [[:db/cas [:p/name "al"] :p/age 3 "s"]])
    (err :db.error/cas-failed [[:db/add [:p/name "al"] :p/age 50] [:db/cas [:p/name "al"] :p/age 50 51]])
    (err :db.error/datoms-conflict [[:db/cas [:p/name "al"] :p/age 3 4] [:db/add [:p/name "al"] :p/age 5]])
    (ok [[:db/cas [:p/name "al"] :p/boss nil [:p/name "al"]]])
    (ok [[:db/cas [:p/name "al"] :p/age 3 8] [:db/cas [:p/name "al"] :p/age 3 8]])
    (err :db.error/datoms-conflict [[:db/cas [:p/name "al"] :p/age 8 9] [:db/cas [:p/name "al"] :p/age 8 10]])
    (ok [{:db/id "b" :p/name "bo"}])
    (err :db.error/unique-conflict [[:db/cas [:p/name "al"] :p/name "al" "bo"]])
    (ok [[:db/cas [:p/name "al"] :p/age (int 8) 9]])
    (err :db.error/not-a-keyword [[:db/cas "datomic.tx" :a/city nil "x"]])]])

(def retract-entity-scenario
  ["retractEntity"
   [schema
    (ok [{:db/id "a" :p/name "A" :p/kids [{:p/name "C1" :p/kids [{:p/name "C2"}]} {:p/name "C3"}]}
         {:db/id "b" :p/name "B" :p/boss "a" :p/friend ["a" "b"]}
         {:db/id "c" :p/name "C" :p/boss "e2"}
         {:db/id "e2" :p/name "E2"}
         {:db/id "d" :p/name "D" :p/kids "e2"}
         {:db/id "f" :p/name "F" :p/boss "f"}
         {:db/id "z" :p/name "Z" :db/ident :my/ent :p/age 5}])
    (ok [[:db/retractEntity [:p/name "A"]]])
    (ok [[:db/retractEntity [:p/name "E2"]]])
    (ok [[:db.fn/retractEntity [:p/name "F"]]])
    (ok [[:db/retractEntity :my/ent]])
    (ok [[:db/retractEntity 12345]])
    (ok [[:db/retractEntity [:p/name "nobody"]]])
    (ok [[:db/retractEntity :no/such]])
    (ok [[:db/retractEntity nil]])
    (ok [{:db/id "n" :p/name "N" :p/age 1}])
    (ok [[:db/retractEntity [:p/name "N"]] [:db/retractEntity [:p/name "N"]]])
    (ok [{:db/id "n" :p/name "N2" :p/age 1}])
    (ok [[:db/retractEntity [:p/name "N2"]] [:db/retract [:p/name "N2"] :p/age 1]])
    (ok [{:db/id "n" :p/name "N3" :p/age 1}])
    (err :db.error/datoms-conflict [[:db/retractEntity [:p/name "N3"]] [:db/add [:p/name "N3"] :p/age 1]])
    (ok [[:db/retractEntity [:p/name "N3"]] [:db/add [:p/name "N3"] :a/city "kept"]])
    (ok [[:db/retractEntity [:p/name "N3"]] {:db/id "new" :p/name "N3"}])
    (err :db.error/not-a-keyword [[:db/retractEntity "t"]])
    (err :db.error/not-a-keyword [[:db/retractEntity "datomic.tx"]])
    (err :db.error/reset-tx-instant [[:db/retractEntity (d/t->tx 1000)]])
    (err :db.error/datom-cannot-be-altered [[:db/retractEntity :db.part/user]])
    (err :db.error/datom-cannot-be-altered [[:db/retractEntity 1]])
    (err :db.error/invalid-alter-attribute [[:db/retractEntity :a/city]])]])

(def tx-fn-scenario
  ["errors: classpath tx fns"
   [schema
    (ok [who])
    (ok [['mnemonic.test-fns/add-age [:p/name "al"] 5]])
    (ok [['mnemonic.test-fns/nested [:p/name "al"]]])
    (ok [['mnemonic.test-fns/ret-nil]])
    (ok [['mnemonic.test-fns/ret-lazy]])
    (ok [['mnemonic.test-fns/ret-set]])
    (ok [['mnemonic.test-fns/db-basis]])
    (ok [['mnemonic.test-fns/tempid-fn] [:db/add "tt" :p/tags "extra"]])
    (ok [['mnemonic.test-fns/sees-before? [:p/name "al"]]])
    (ok [['mnemonic.test-fns/add-age [:p/name "al"] 6] ['mnemonic.test-fns/add-age [:p/name "al"] 6]])
    (err :db.error/datoms-conflict
         [['mnemonic.test-fns/add-age [:p/name "al"] 7] ['mnemonic.test-fns/add-age [:p/name "al"] 8]])
    (err :db.error/not-a-data-function [['mnemonic.test-fns/ret-map]])
    (err :db.error/not-a-data-function [['foo]])
    (err :db.error/not-a-data-function [[:no/such 1]])
    (err :db.error/not-a-data-function [[:p/name 1]])
    (err :db.error/not-a-data-function [[17 1]])
    (err :db.error/not-a-keyword [["foo" 1]])
    (ok [[:db/add "x" :p/name "after"]])
    (err :unclassified [['mnemonic.test-fns/bad]])]])

(def stored-fn-scenario
  ["errors: stored tx fns"
   [schema
    (ok [who])
    (ok [{:db/ident :my/inc-age
          :db/fn (fnspec '{:lang "clojure" :params [db e] :requires [[mnemonic.test-fns :as tf]]
                           :code (let [cur (:v (first (tf/datoms db :eavt e :p/age)))]
                                   [[:db/add e :p/age (inc cur)]])})}])
    (ok [[:my/inc-age [:p/name "al"]]])
    (ok [{:db/ident :my/millis
          :db/fn (fnspec '{:lang :clojure :params [db e] :imports [[java.util Date]]
                           :code [[:db/add e :p/age (.getTime (Date. 5))]]})}])
    (ok [[:my/millis [:p/name "al"]]])
    (ok [{:db/ident :my/chain
          :db/fn (fnspec '{:lang "clojure" :params [db e]
                           :code [[:my/inc-age e] [:db/add e :a/city "from-chain"]
                                  {:db/id "made" :p/name "made-by-chain" :p/boss e}]})}])
    (ok [[:my/chain [:p/name "al"]]])
    (ok [{:db/id "f" :db/fn (fnspec '{:lang "clojure" :params [db] :code []})}])
    (err :db.error/not-a-data-function [[:my/no-such-fn 1]])
    (err :db.error/not-a-data-function [{:db/ident :my/later
                                         :db/fn (fnspec '{:lang "clojure" :params [db] :code []})}
                                        [:my/later]])
    (err :db.error/wrong-type-for-attribute [{:db/ident :my/bad :db/fn "notafn"}])
    (ok [{:db/ident :my/throws
          :db/fn (fnspec '{:lang "clojure" :params [db] :code (throw (ex-info "stored boom" {:a 1}))})}])
    (err :unclassified [[:my/throws]])
    (ok [[:db/retractEntity :my/inc-age]])
    (err :db.error/not-a-data-function [[:my/inc-age [:p/name "al"]]])]])

(def tuple-scenario
  ["errors: tuples"
   [schema
    tuple-schema
    (ok [{:db/id "e1" :t/a "x" :t/b 1}])
    (ok [{:db/id "e2" :t/a "y"}])
    (ok [[:db/add [:t/abu [1 "x"]] :t/b 2]])
    (ok [[:db/retract [:t/abu [2 "x"]] :t/a "x"]])
    (ok [{:t/abu [2 nil] :t/a "z"}])
    (ok [{:db/id "e5" :t/a "w" :t/b 5}])
    (ok [[:db/retractEntity [:t/abu [5 "w"]]]])
    (ok [{:db/id "e6" :t/ab ["a" 1]}])
    (ok [{:db/id "e7" :t/ab ["a" 1] :t/a "a" :t/b 1}])
    (ok [{:db/id "e8" :t/ab ["a" 1] :t/a "a" :t/b 2}])
    (ok [{:db/id "e9" :t/a "k" :t/b 5 :p/age 7}])
    (ok [{:db/id "e10" :t/ab [1 2]}])
    (err :db.error/invalid-tuple-value [{:db/id "bad" :t/ab "notvec"}])
    (err :db.error/invalid-tuple-value [{:db/id "bad" :t/ab [1]}])
    (err :db.error/invalid-tuple-value [{:db/id "bad" :t/abc [1 2]}])
    (err :db.error/invalid-tuple-value [{:db/id "bad" :t/ab [1 2 3]}])
    (err :db.error/invalid-tuple-value [{:db/id "bad" :t/ab []}])
    (ok [{:db/id "h1" :t/hetero ["s" 1]}])
    (ok [{:db/id "h1b" :t/hetero ["s" nil]}])
    (err :db.error/invalid-tuple-value [{:db/id "h2" :t/hetero [1 "s"]}])
    (err :db.error/invalid-tuple-value [{:db/id "h2" :t/hetero ["s"]}])
    (err :db.error/invalid-tuple-value [{:db/id "h2" :t/hetero ["s" 1 2]}])
    (err :db.error/invalid-tuple-value [{:db/id "h2" :t/hetero '("s" 1)}])
    (err :db.error/invalid-tuple-value [{:db/id "h2" :t/hetero "s"}])
    (ok [{:db/id "h4" :t/homo [1 2 3]}])
    (ok [{:db/id "h4b" :t/homo [1 nil 3]}])
    (ok [{:db/id "h7" :t/homo [1 2 3 4 5 6 7 8]}])
    (err :db.error/invalid-tuple-value [{:db/id "h5" :t/homo [1 "x"]}])
    (err :db.error/invalid-tuple-value [{:db/id "h5" :t/homo []}])
    (err :db.error/invalid-tuple-value [{:db/id "h5" :t/homo [1 2 3 4 5 6 7 8 9]}])
    (err :db.error/datoms-conflict [{:db/id "u1" :t/b 7} {:db/id "u2" :t/b 7}])
    (ok [{:db/id "q1" :t/b 9}])
    (ok [{:db/id "q2" :t/a "m"}])
    (ok [[:db/add [:t/abu [9 nil]] :t/a "filled"]])
    (ok [[:db/cas [:t/abu [9 "filled"]] :t/a "filled" "swapped"]])
    (ok [[:db/retract [:t/abu [9 "swapped"]] :t/a]])
    (ok [[:db/retract [:t/abu [9 nil]] :t/b 9]])]])

(def tuple-install-scenario
  ["errors: tuple schema validation"
   [schema
    tuple-schema
    (err :db.error/invalid-install-attribute
         (install [:s/x1 :db.type/string one :db/tupleAttrs [:t/a :t/b]]))
    (err :db.error/invalid-install-attribute (install [:s/x2 :db.type/tuple one]))
    (err :db.error/invalid-tuple-value (install [:s/x3 :db.type/tuple one :db/tupleAttrs [:t/a]]))
    (err :db.error/invalid-tuple-value
         (install [:s/x4 :db.type/tuple one :db/tupleAttrs [:t/a :t/b :t/c :t/d :t/a :t/b :t/c :t/a :t/b]]))
    (err :db.error/invalid-install-attribute (install [:s/x5 :db.type/tuple one :db/tupleAttrs [:t/a :t/d]]))
    (err :db.error/invalid-install-attribute (install [:s/x6 :db.type/tuple many :db/tupleAttrs [:t/a :t/b]]))
    (err :db.error/invalid-install-attribute (install [:s/x7 :db.type/tuple one :db/tupleAttrs [:t/a :t/ab]]))
    (ok (install [:s/x8 :db.type/tuple one :db/tupleAttrs [:t/a :t/c]]))
    (err :db.error/invalid-install-attribute (install [:s/x9 :db.type/tuple one :db/tupleAttrs [:t/a :t/nope]]))
    (ok (install [:s/x10 :db.type/tuple one :db/tupleAttrs [:t/a :t/a]]))
    (ok (install [:s/n1 :db.type/string one] [:s/n2 :db.type/long one]
                 [:s/n12 :db.type/tuple one :db/tupleAttrs [:s/n1 :s/n2]]))
    (err :db.error/invalid-install-attribute
         (install [:s/x11 :db.type/tuple one :db/tupleAttrs [:t/a :t/b] :db/tupleType :db.type/string]))
    (err :db.error/invalid-install-attribute
         (install [:s/x12 :db.type/tuple one :db/tupleAttrs [:t/a :t/b] :db/tupleTypes [:db.type/string :db.type/long]]))
    (err :db.error/invalid-install-attribute
         (install [:s/x13 :db.type/tuple one :db/tupleType :db.type/string
                   :db/tupleTypes [:db.type/string :db.type/long]]))
    (err :db.error/invalid-install-attribute (install [:s/x14 :db.type/tuple one :db/tupleType :db.type/tuple]))
    (err :db.error/invalid-tuple-value (install [:s/x15 :db.type/tuple one :db/tupleTypes [:db.type/string]]))
    (err :db.error/invalid-tuple-value
         (install [:s/x16 :db.type/tuple one :db/tupleTypes (vec (repeat 9 :db.type/long))]))
    (err :db.error/invalid-install-attribute
         (install [:s/x17 :db.type/tuple one :db/tupleTypes [:db.type/string :db.type/tuple]]))
    (err :db.error/invalid-install-attribute
         (install [:s/x18 :db.type/tuple one :db/tupleTypes [:db.type/string :db.type/nope]]))
    (err :db.error/invalid-install-attribute (install [:s/x19 :db.type/string one :db/tupleType :db.type/long]))
    (err :db.error/invalid-install-attribute
         (install [:s/x20 :db.type/string one :db/tupleTypes [:db.type/long :db.type/long]]))
    (ok (install [:s/x21 :db.type/tuple one :db/tupleType :db.type/ref]))
    (ok (install [:s/x22 :db.type/tuple many :db/tupleType :db.type/long]))
    (ok (install [:s/x23 :db.type/tuple one :db/tupleTypes [:db.type/ref :db.type/long]]))]])

(def attr-pred-scenario
  ["errors: attribute predicates"
   [schema
    (ok (install [:v/pos :db.type/long one :db.attr/preds ['mnemonic.test-fns/pos-val?]]
                 [:v/two :db.type/string one
                  :db.attr/preds ['mnemonic.test-fns/short-name? 'mnemonic.test-fns/short-name?]]
                 [:v/nilp :db.type/long one :db.attr/preds ['mnemonic.test-fns/nil-pred]]
                 [:v/strp :db.type/long one :db.attr/preds ['mnemonic.test-fns/str-pred]]
                 [:v/th :db.type/string one :db.attr/preds ['mnemonic.test-fns/throws-pred]]
                 [:v/many :db.type/long many :db.attr/preds ['mnemonic.test-fns/pos-val?]]
                 [:v/ref :db.type/ref one :db.attr/preds ['mnemonic.test-fns/pos-val?]]
                 [:v/uq :db.type/long one :db.attr/preds ['pos?]]
                 [:v/a :db.type/string one]
                 [:v/b :db.type/long one]
                 [:v/ab :db.type/tuple one :db/tupleAttrs [:v/a :v/b]
                  :db.attr/preds ['mnemonic.test-fns/pair-sum-pos?]]))
    (ok [{:db/id "v" :v/pos 5}])
    (err :db.error/attr-pred [{:db/id "v" :v/pos -5}])
    (err :db.error/attr-pred [[:db/add "v" :v/pos -5]])
    (ok [{:db/id "v" :v/two "ab"}])
    (err :db.error/attr-pred [{:db/id "v" :v/two "abcdef"}])
    (err :db.error/attr-pred [{:db/id "v" :v/nilp 1}])
    (err :db.error/attr-pred [{:db/id "v" :v/strp 1}])
    (err :unclassified [{:db/id "v" :v/th "z"}])
    (ok [{:db/id "v" :v/many [1 2 3]}])
    (err :db.error/attr-pred [{:db/id "v" :v/many [1 -2 3]}])
    (err :db.error/attr-pred [{:db/id "v" :v/ref 0}])
    (ok [{:db/id "v" :v/uq 5}])
    (err :db.error/attr-pred [{:db/id "v" :v/uq -5}])
    (ok [{:db/id "w" :p/name "w" :p/age -3}])
    (ok [{:db/id :p/age :db.attr/preds ['mnemonic.test-fns/pos-val?]}])
    (err :db.error/attr-pred [{:db/id "v" :p/age -3}])
    (ok [{:db/id "v" :p/age 3}])
    (ok [[:db/retract [:p/name "w"] :p/age -3]])
    (ok [{:db/id "v" :v/a "q" :v/b 3}])
    (err :db.error/attr-pred [{:db/id "v" :v/a "q" :v/b -3}])
    (err :db.error/not-an-entity [(first (install [:v/same :db.type/long one :db.attr/preds ['mnemonic.test-fns/pos-val?]]))
                                  {:db/id "x" :v/same -1}])
    (err :db.error/wrong-type-for-attribute
         (install [:v/ns :db.type/long one :db.attr/preds ["mnemonic.test-fns/pos-val?"]]))]])

(def ensure-scenario
  ["errors: ensure"
   [schema
    (ok [{:db/ident :ent/person :db.entity/attrs [:p/name :p/age]}
         {:db/ident :ent/pred :db.entity/preds ['mnemonic.test-fns/person-ok?]}
         {:db/ident :ent/both :db.entity/attrs [:p/name] :db.entity/preds ['mnemonic.test-fns/person-ok?]}
         {:db/ident :ent/throws :db.entity/preds ['mnemonic.test-fns/throws-ent?]}
         {:db/ident :ent/nil :db.entity/preds ['mnemonic.test-fns/no-ret-ok?]}
         {:db/ident :ent/is99 :db.entity/preds ['mnemonic.test-fns/age-is-99?]}
         {:db/ident :ent/none}])
    (ok [{:db/id "e" :p/name "a" :p/age 1 :db/ensure :ent/person}])
    (err :db.error/entity-attr [{:db/id "e" :p/name "a2" :db/ensure :ent/person}])
    (ok [[:db/add "e2" :p/name "b"] [:db/add "e2" :p/age 5] [:db/add "e2" :db/ensure :ent/person]])
    (ok [{:db/id "e" :p/name "p1" :p/age 1 :db/ensure :ent/pred}])
    (err :db.error/entity-pred [{:db/id "e" :p/name "p2" :db/ensure :ent/pred}])
    (err :unclassified [{:db/id "e" :p/name "p3" :db/ensure :ent/throws}])
    (err :db.error/entity-pred [{:db/id "e" :p/name "p3" :db/ensure :ent/nil}])
    (err :db.error/entity-attr [{:db/id "e" :p/age 1 :db/ensure :ent/both}])
    (ok [{:db/id "e" :p/name "bb" :p/age 1 :db/ensure :ent/both}])
    (ok [{:db/id "e" :p/name "nn" :db/ensure :ent/none}])
    (err :db.error/not-an-entity [{:db/id "e" :p/name "unk" :db/ensure :ent/unknown}])
    (ok [{:db/id "e" :p/name "ns" :db/ensure :p/name}])
    (ok [{:db/id "e" :p/name "mm" :p/age 1 :db/ensure [:ent/person :ent/pred]}])
    (ok [{:db/id "e" :p/name "keep" :p/age 1 :db/ensure :ent/person}])
    (err :db.error/entity-attr [[:db/retract [:p/name "keep"] :p/age 1]
                                [:db/add [:p/name "keep"] :db/ensure :ent/person]])
    (ok [[:db/add [:p/name "a"] :db/ensure :ent/person]])
    (ok [{:db/id "e" :p/name "pp" :p/age 99 :db/ensure :ent/is99}])
    (err :db.error/entity-pred [{:db/id "e" :p/name "qq" :p/age 98 :db/ensure :ent/is99}])
    (ok [[:db/retract [:p/name "a"] :db/ensure :ent/person]])
    (err :db.error/entity-attr [{:db/id "datomic.tx" :db/ensure :ent/person}])
    (ok [{:db/id "datomic.tx" :db/ensure :ent/none}])]])

;; Alterations: each case runs in its own pair of fresh dbs.
(def alter-schema
  (attr-tx
   [[:s/one :db.type/string one]
    [:s/many :db.type/string many]
    [:s/idx :db.type/string one :db/index true]
    [:s/uniq :db.type/string one :db/unique :db.unique/value]
    [:s/ident :db.type/string one :db/unique :db.unique/identity]
    [:s/ref :db.type/ref one]
    [:s/comp :db.type/ref one :db/isComponent true]
    [:s/nh :db.type/string one :db/noHistory true]
    [:s/doc :db.type/string one :db/doc "hello"]
    [:s/long :db.type/long one]]))

(def alter-data
  [{:db/id "a" :s/one "x" :s/many ["p" "q"] :s/idx "i1" :s/uniq "u1"}
   {:db/id "b" :s/one "x" :s/many ["p"] :s/idx "i2"}])

(def alter-cases
  [["one->many" :ok [{:db/id :s/one :db/cardinality many}]]
   ["many->one with multiple values" :db.error/invalid-alter-attribute [{:db/id :s/many :db/cardinality one}]]
   ["one->many on refs" :ok [{:db/id :s/comp :db/cardinality many} {:db/id :s/ref :db/cardinality many}]]
   ["index true->false" :ok [{:db/id :s/idx :db/index false}]]
   ["index add" :ok [{:db/id :s/one :db/index true}]]
   ["index retract" :ok [[:db/retract :s/idx :db/index true]]]
   ["redundant index" :ok [{:db/id :s/idx :db/index true}]]
   ["unique without index (has data)" :db.error/invalid-alter-attribute [{:db/id :s/one :db/unique :db.unique/value}]]
   ["unique add on empty attr" :ok [{:db/id :s/long :db/unique :db.unique/identity}]]
   ["unique add on index, distinct" :ok [{:db/id :s/idx :db/unique :db.unique/value}]]
   ["unique identity add on index, distinct" :ok [{:db/id :s/idx :db/unique :db.unique/identity}]]
   ["index+unique, duplicate values" :db.error/invalid-alter-attribute
    [{:db/id :s/one :db/index true :db/unique :db.unique/value}]]
   ["unique remove" :ok [[:db/retract :s/uniq :db/unique :db.unique/value]]]
   ["unique value->identity" :ok [{:db/id :s/uniq :db/unique :db.unique/identity}]]
   ["unique identity->value" :ok [{:db/id :s/ident :db/unique :db.unique/value}]]
   ["unique remove + index false" :ok [[:db/retract :s/uniq :db/unique :db.unique/value]
                                       {:db/id :s/uniq :db/index false}]]
   ["isComponent true" :ok [{:db/id :s/ref :db/isComponent true}]]
   ["isComponent on non-ref" :ok [{:db/id :s/one :db/isComponent true}]]
   ["isComponent false" :ok [{:db/id :s/comp :db/isComponent false}]]
   ["isComponent retract" :ok [[:db/retract :s/comp :db/isComponent true]]]
   ["isComponent retract absent" :ok [[:db/retract :s/one :db/isComponent true]]]
   ["noHistory true" :ok [{:db/id :s/one :db/noHistory true}]]
   ["noHistory false" :ok [{:db/id :s/nh :db/noHistory false}]]
   ["noHistory + retract" :ok [{:db/id :s/one :db/noHistory true} [:db/retract [:s/uniq "u1"] :s/uniq "u1"]]]
   ["doc" :ok [{:db/id :s/one :db/doc "new doc"}]]
   ["doc retract" :ok [[:db/retract :s/doc :db/doc "hello"]]]
   ["doc unchanged" :ok [{:db/id :s/doc :db/doc "hello"}]]
   ["rename ident" :ok [{:db/id :s/one :db/ident :s/one2}]]
   ["rename ident, old name usable in same tx" :ok [{:db/id :s/one :db/ident :s/one2} {:db/id "n" :s/one "zz"}]]
   ["rename ident, new name not usable in same tx" :db.error/not-an-entity
    [{:db/id :s/one :db/ident :s/one2} {:db/id "n" :s/one2 "zz"}]]
   ["rename ident conflict" :db.error/unique-conflict [{:db/id :s/one :db/ident :s/many}]]
   ["retract ident" :ok [[:db/retract :s/one :db/ident :s/one]]]
   ["valueType change" :db.error/invalid-alter-attribute [{:db/id :s/one :db/valueType :db.type/long}]]
   ["valueType change on empty attr" :db.error/invalid-alter-attribute [{:db/id :s/long :db/valueType :db.type/string}]]
   ["valueType same" :ok [{:db/id :s/one :db/valueType :db.type/string}]]
   ["valueType retract" :db.error/invalid-alter-attribute [[:db/retract :s/one :db/valueType :db.type/string]]]
   ["cardinality retract" :db.error/invalid-alter-attribute [[:db/retract :s/one :db/cardinality one]]]
   ["fulltext on existing attr" :db.error/invalid-alter-attribute [{:db/id :s/one :db/fulltext true}]]
   ["fulltext off on existing attr" :ok [{:db/id :s/one :db/fulltext false}]]
   ["fulltext on new attr" :ok [{:db/ident :s/ft :db/valueType :db.type/string :db/cardinality one :db/fulltext true}]]
   ["tupleAttrs on existing attr" :db.error/invalid-alter-attribute [{:db/id :s/one :db/tupleAttrs [:s/one :s/idx]}]]
   ["attr preds on existing attr" :ok [{:db/id :s/one :db.attr/preds ['mnemonic.test-fns/pos-val?]}]]
   ["attr preds retract absent" :ok [[:db/retract :s/one :db.attr/preds 'mnemonic.test-fns/pos-val?]]]
   ["explicit alter datom, nothing altered" :ok [[:db/add 0 :db.alter/attribute :s/one]]]
   ["explicit alter datom, doc change" :ok [[:db/add 0 :db.alter/attribute :s/one] {:db/id :s/one :db/doc "z"}]]
   ["explicit install of installed attr" :ok [[:db/add 0 :db.install/attribute :s/one]]]
   ["alter and use in the same tx" :db.error/wrong-type-for-attribute
    [{:db/id :s/one :db/cardinality many} {:db/id "n" :s/one ["a" "b"]}]]
   ["boot attribute: replace doc" :db.error/datom-cannot-be-altered [{:db/id :db/doc :db/doc "x"}]]
   ["boot attribute: rename ident" :db.error/datom-cannot-be-altered [{:db/id :db/doc :db/ident :db/docc}]]
   ["boot attribute: retract ident" :db.error/datom-cannot-be-altered [[:db/retract :db/doc :db/ident :db/doc]]]
   ["boot attribute: add index" :ok [{:db/id :db/ident :db/index true}]]
   ["boot partition doc" :db.error/datom-cannot-be-altered [{:db/id :db.part/user :db/doc "p"}]]
   ["boot fn doc" :db.error/datom-cannot-be-altered [{:db/id :db/cas :db/doc "x"}]]
   ["doc on a non attribute" :ok [{:db/ident :s/just :db/doc "just"}]]])

(def alter-scenarios
  (vec (for [[nm expect tx] alter-cases]
         [(str "errors: alter " nm) [alter-schema (ok alter-data) (if (= expect :ok) (ok tx) (err expect tx))]])))

(def alter-sequence-scenario
  ["alterations in sequence"
   [alter-schema
    (ok alter-data)
    (ok [{:db/id :s/one :db/cardinality many}])
    (ok [{:db/id "n" :s/one ["a" "b"]}])
    (ok [{:db/id :s/idx :db/unique :db.unique/identity}])
    (ok [{:db/id "i" :s/idx "i1" :s/one "upserted"}])
    (ok [{:db/id :s/idx :db/ident :s/index-renamed}])
    (ok [{:db/id "j" :s/index-renamed "i2" :s/doc "by new name"}])
    (ok [{:db/id :s/comp :db/isComponent false}])
    (ok [{:db/id :s/nh :db/noHistory false}])]])

(def no-id-schema-scenario
  ["schema maps without :db/id"
   [(ok [{:db/ident :n/name :db/valueType :db.type/string :db/cardinality one}
         {:db/ident :n/tags :db/valueType :db.type/string :db/cardinality many :db/index true}])
    (ok [{:n/name "first"}
         {:db/ident :n/age :db/valueType :db.type/long :db/cardinality one}
         {:n/tags ["a" "b"]}
         {:db/ident :n/boss :db/valueType :db.type/ref :db/cardinality one :db/isComponent true}
         {:db/id "named" :n/name "named"}
         {:db/ident :n/enum}])
    (ok [{:n/age 3 :n/name "x"}
         {:db/ident :n/uniq :db/valueType :db.type/string :db/cardinality one :db/unique :db.unique/identity}])
    (ok [{:n/uniq "u" :n/age 1}])]])

(def scenarios
  (into [no-id-schema-scenario cas-scenario retract-entity-scenario tx-fn-scenario stored-fn-scenario tuple-scenario
         tuple-install-scenario attr-pred-scenario ensure-scenario alter-sequence-scenario]
        alter-scenarios))

;; ------------------------------------------------------------------ runner

(defn- subst [side data]
  (letfn [(go [x]
            (cond
              (instance? FnSpec x) (if (= side :datomic) (d/function (:m x)) (m/function (:m x)))
              (map? x) (reduce-kv (fn [acc k v] (assoc acc (go k) (go v))) (empty x) x)
              (vector? x) (mapv go x)
              (seq? x) (apply list (map go x))
              (set? x) (set (map go x))
              :else x))]
    (go data)))

(defn- norm-fn-vals [r]
  (if (contains? r :error)
    r
    (update r :tx-data (fn [s] (set (map #(if (= 52 (nth % 1)) (assoc % 2 :fn) %) s))))))

(defn- tx-both [[cd cm] data]
  (let [explicit (atom #{})
        run (fn [transact conn tempid side]
              (norm-fn-vals (#'o/run-tx transact conn tempid explicit (if (fn? data) data (subst side data)))))]
    {:datomic (run d/transact cd d/tempid :datomic)
     :mnemonic (run m/transact cm m/tempid :mnemonic)}))

(defn- index-snapshot [cd cm]
  (let [dd (d/db cd) dm (m/db cm)
        fix (fn [ds] (mapv #(if (= 52 (nth % 1)) (assoc % 2 :fn) %) ds))]
    (into {:basis-t [(d/basis-t dd) (m/basis-t dm)]
           :next-t [(d/next-t dd) (m/next-t dm)]}
          (for [idx o/indexes]
            (let [{:keys [datomic mnemonic]} (o/index-dump dd dm idx)]
              [idx {:datomic (fix datomic) :mnemonic (fix mnemonic)}])))))

(def ^:private avet-exempt
  "Datomic keeps listing an attribute in AVET after its :db/index or :db/unique is removed
  (until reindexing); MnemonicDB stops listing it (see docs/deviations.md)."
  #{"errors: alter index true->false" "errors: alter index retract" "errors: alter unique remove"
    "errors: alter unique remove + index false"})

(defn- run-scenario [[nm steps]]
  (o/with-both [cd cm]
    (doseq [[i step] (map-indexed vector steps)]
      (let [pinned? (and (map? step) (contains? step ::expect))
            data (if pinned? (::data step) step)
            {:keys [datomic mnemonic]} (tx-both [cd cm] data)
            kind (:error mnemonic)]
        (when pinned?
          (let [actual (cond (not (contains? mnemonic :error)) :ok
                             (vector? kind) :unclassified
                             :else kind)]
            (is (= (::expect step) actual) (str nm " / step " i " expected " (::expect step) " got " actual " " (pr-str (:error mnemonic))))))
        (is (= datomic mnemonic) (str nm " / step " i))))
    (let [snap (index-snapshot cd cm)]
      (is (apply = (:basis-t snap)) (str nm " basis-t " (:basis-t snap)))
      (is (apply = (:next-t snap)) (str nm " next-t " (:next-t snap)))
      (doseq [idx o/indexes :when (not (and (= idx :avet) (contains? avet-exempt nm)))]
        (let [{:keys [datomic mnemonic]} (get snap idx)]
          (is (= (count datomic) (count mnemonic)) (str nm " " idx " count"))
          (is (= datomic mnemonic) (str nm " " idx " datoms")))))))

(deftest scenarios-test
  (is (>= (count scenarios) 20))
  (doseq [sc scenarios]
    (testing (first sc)
      (run-scenario sc))))
