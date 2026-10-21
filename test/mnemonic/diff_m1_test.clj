(ns mnemonic.diff-m1-test
  "Differential tests: the same transactions run against Datomic (oracle) and MnemonicDB
  must produce equal tx-data, tempids, error kinds and index contents."
  (:require [clojure.test :refer [deftest is testing]]
            [datomic.api :as d]
            [mnemonic.api :as m]
            [mnemonic.oracle :as o]))

(defn- attr-tx
  "Schema maps with string tempids (promoted to :db.part/db because of :db/valueType)."
  [specs]
  (for [[id vt card & {:as opts}] specs]
    (merge {:db/id (str "attr-" id) :db/ident id :db/valueType vt :db/cardinality card} opts)))

(def one :db.cardinality/one)
(def many :db.cardinality/many)

(def base-schema
  (attr-tx
   [[:p/name :db.type/string one :db/unique :db.unique/identity]
    [:p/email :db.type/string one :db/unique :db.unique/value]
    [:p/age :db.type/long one :db/index true]
    [:p/tags :db.type/string many]
    [:p/friends :db.type/ref many]
    [:p/boss :db.type/ref one]
    [:p/addr :db.type/ref one :db/isComponent true]
    [:p/kind :db.type/ref one]
    [:p/nick :db.type/string one :db/noHistory true]
    [:p/org :db.type/ref one :db/unique :db.unique/identity]
    [:a/city :db.type/string one :db/index true]
    [:org/name :db.type/string one :db/unique :db.unique/identity]]))

(def type-schema
  (attr-tx
   [[:t/string :db.type/string one :db/index true]
    [:t/long :db.type/long one :db/index true]
    [:t/double :db.type/double one :db/index true]
    [:t/float :db.type/float one :db/index true]
    [:t/boolean :db.type/boolean one :db/index true]
    [:t/bigint :db.type/bigint one :db/index true]
    [:t/bigdec :db.type/bigdec one :db/index true]
    [:t/instant :db.type/instant one :db/index true]
    [:t/uuid :db.type/uuid one :db/index true]
    [:t/uri :db.type/uri one :db/index true]
    [:t/bytes :db.type/bytes one]
    [:t/keyword :db.type/keyword one :db/index true]
    [:t/symbol :db.type/symbol one :db/index true]
    [:t/ref :db.type/ref one :db/index true]]))

(defn- people [& names]
  (vec (map-indexed (fn [i n] {:db/id (str "p" i) :p/name n :p/age (* 10 (inc i))}) names)))

(def scenarios
  "name -> steps; a step is tx-data or (fn [tempid] tx-data)."
  [["schema only" [base-schema]]
   ["maps with tempids"
    [base-schema
     (people "Al" "Bo" "Cy")
     [{:db/id "x" :p/name "Dee" :p/tags ["a" "b" "c"] :p/email "dee@x"}]]]
   ["card-one replacement"
    [base-schema
     (people "Al")
     [[:db/add [:p/name "Al"] :p/age 99]]
     [[:db/add [:p/name "Al"] :p/age 100] [:db/add [:p/name "Al"] :p/nick "A"]]
     [[:db/add [:p/name "Al"] :p/nick "AA"]]]]
   ["card-many accumulate and retract"
    [base-schema
     (people "Al")
     [[:db/add [:p/name "Al"] :p/tags "a"] [:db/add [:p/name "Al"] :p/tags "b"]]
     [{:db/id [:p/name "Al"] :p/tags ["b" "c" #_"d"]}]
     [[:db/retract [:p/name "Al"] :p/tags "a"]]
     [[:db/add [:p/name "Al"] :p/tags "a"]]
     [[:db/retract [:p/name "Al"] :p/tags]]]]
   ["redundant assert and absent retract"
    [base-schema
     (people "Al")
     [[:db/add [:p/name "Al"] :p/age 10] [:db/add [:p/name "Al"] :p/name "Al"]]
     [[:db/retract [:p/name "Al"] :p/age 12345] [:db/retract [:p/name "Al"] :p/tags "nope"]]
     []]]
   ["upsert and anonymous maps"
    [base-schema
     (people "Al" "Bo")
     [{:db/id "t" :p/name "Al" :p/age 7}]
     [{:p/name "Bo" :p/age 8}]
     [{:p/age 9}]
     [{:db/id "w" :p/name "Al"} [:db/retract "w" :p/age 7]]]]
   ["tempid unification"
    [base-schema
     [{:db/id "a1" :p/name "New"} {:db/id "a2" :p/name "New" :p/age 3}]
     [{:db/id "b1" :p/name "Al2" :p/boss "b2"} {:db/id "b2" :p/name "Al2"} {:db/id "b3" :p/name "Zed"}]
     [{:db/id "c1" :p/name "New" :p/tags "x"} {:db/id "c2" :p/name "Zed" :p/boss "c1"}]]]
   ["lookup refs"
    [base-schema
     (people "Al" "Bo")
     [[:db/add [:p/name "Al"] :p/boss [:p/name "Bo"]]
      {:db/id [:p/name "Bo"] :p/friends [[:p/name "Al"] [:p/name "Bo"]]}]
     [[:db/retract [:p/name "Al"] :p/boss [:p/name "Bo"]]]
     [{:db/id "n" :p/name "Cy" :p/boss [:p/name "Bo"]}]]]
   ["ref values: idents, tempids, cycles"
    [base-schema
     [{:db/id "k1" :db/ident :kind/a} {:db/id "k2" :db/ident :kind/b}]
     [{:db/id "x" :p/name "X" :p/kind :kind/a :p/boss "y"} {:db/id "y" :p/name "Y" :p/boss "x"}]
     [[:db/add [:p/name "X"] :p/kind :kind/b]]
     [{:db/id [:p/name "X"] :p/friends [:kind/a :kind/b]}]]]
   ["reverse refs"
    [base-schema
     (people "Al" "Bo")
     [{:db/id "n" :p/name "New" :p/_friends [:p/name "Al"]}]
     [{:db/id "z" :p/name "Zed" :p/_boss [:p/name "Al"]}]
     [{:db/id "q" :p/name "Q" :p/_friends "r"} {:db/id "r" :p/name "R"}]]]
   ["nested maps and components"
    [base-schema
     [{:db/id "d" :p/name "Dee" :p/addr {:a/city "Denver"} :p/boss {:p/name "Boss" :p/age 50}}
      {:db/id "e" :p/name "Ed" :p/friends [{:p/name "F1"} {:p/name "F2" :a/city "Q"}]}]
     [{:p/name "Dee" :p/addr {:a/city "Boulder"}}]
     [{:db/id "f" :p/name "Fy" :p/addr {:db/id "ad" :a/city "Aspen"}}]]]
   ["allocation order"
    [base-schema
     [[:db/add "p" :p/boss "q"] [:db/add "r" :p/name "R"] [:db/add "p" :p/name "P"] [:db/add "q" :p/name "Q"]]
     [{:db/id "a" :p/name "A" :p/friends [{:p/name "Y1" :p/friends {:p/name "Z"}} {:p/name "Y2" :p/boss "w"}]}
      {:db/id "v" :p/name "V"} {:db/id "w" :p/name "W"}]
     [{:p/name "K1" :p/friends ["kk"] :p/boss {:p/name "K2"} :p/age 4 :p/tags "x" :p/addr {:a/city "q"}
       :a/city "t" :p/email "e1"}
      {:db/id "kk" :p/name "KK"}]]]
   ["all value types"
    [type-schema
     [{:db/id "a" :t/string "hello" :t/long 42 :t/double 3.5 :t/float (float 1.5) :t/boolean true
       :t/bigint (bigint 12345678901234567890N) :t/bigdec 1.25M
       :t/instant #inst "2020-02-03T04:05:06.007-00:00"
       :t/uuid #uuid "123e4567-e89b-12d3-a456-426614174000"
       :t/uri (java.net.URI. "http://example.com/a") :t/bytes (byte-array [1 2 3])
       :t/keyword :foo/bar :t/symbol 'foo/bar}
      {:db/id "b" :t/string "" :t/long -1 :t/double -0.0 :t/float (float -2.5) :t/boolean false
       :t/bigint (biginteger -5) :t/bigdec -0.5M :t/instant #inst "1969-12-31T23:59:59.999-00:00"
       :t/uuid #uuid "00000000-0000-0000-0000-000000000000" :t/uri (java.net.URI. "a:b")
       :t/bytes (byte-array 0) :t/keyword :plain :t/symbol 'plain :t/ref "a"}]]]
   ["value ordering"
    [type-schema
     (vec (concat
           (map-indexed (fn [i s] {:db/id (str "s" i) :t/string s})
                        ["b" "a" "" "ab" "\u0000" "￿" "" "😀" "B" "aa" "z\u0000a"])
           (map-indexed (fn [i n] {:db/id (str "l" i) :t/long n})
                        [0 -1 1 Long/MAX_VALUE Long/MIN_VALUE 255 256 -256])
           (map-indexed (fn [i x] {:db/id (str "d" i) :t/double x})
                        [0.0 1.5 -1.5 Double/MAX_VALUE Double/MIN_VALUE Double/POSITIVE_INFINITY
                         Double/NEGATIVE_INFINITY Double/NaN])
           (map-indexed (fn [i x] {:db/id (str "k" i) :t/keyword x})
                        [:b :a :a/b :a/a :aa :b/a])))]]
   ["numeric coercions"
    [type-schema
     [{:db/id "a" :t/long (int 5) :t/float 2.5}
      {:db/id "b" :t/long (short 7) :t/keyword 'sym/bol :t/ref 73}
      {:db/id "c" :t/keyword ":str/kw" :t/ref :db.type/string}]]]
   ["errors: types and values"
    [type-schema
     [[:db/add "a" :t/long "str"]]
     [[:db/add "a" :t/string 5]]
     [[:db/add "a" :t/double 1]]
     [[:db/add "a" :t/long 1.5]]
     [[:db/add "a" :t/float 1]]
     [[:db/add "a" :t/boolean 1]]
     [[:db/add "a" :t/instant 5]]
     [[:db/add "a" :t/long nil]]
     [[:db/add "a" :t/keyword "nocolon"]]
     [[:db/add "a" :t/ref :no/such-ident]]
     [[:db/add "a" :t/ref 1.5]]
     [[:db/add "a" :t/ref "only-as-value"]]
     [[:db/add "a" :t/long [1 2]]]]]
   ["errors: conflicts and ids"
    [base-schema
     (people "Al" "Bo")
     [[:db/add [:p/name "Al"] :p/email "al@x"]]
     [[:db/add [:p/name "Bo"] :p/email "al@x"]]
     [{:db/id "t" :p/email "al@x"}]
     [{:db/id "t" :p/name "Al" :p/email "x@x"} {:db/id "u" :p/name "Bo" :p/email "x@x"}]
     [[:db/add [:p/name "Bo"] :p/name "Al"]]
     [[:db/add [:p/name "Al"] :p/age 1] [:db/add [:p/name "Al"] :p/age 2]]
     [[:db/add [:p/name "Al"] :p/age 1] [:db/retract [:p/name "Al"] :p/age 1]]
     [[:db/add [:p/name "Al"] :p/tags "z"] [:db/retract [:p/name "Al"] :p/tags "z"]]
     [{:db/id "m" :p/name "Al"} {:db/id "m" :p/name "Bo"}]
     [[:db/add [:p/name "Nobody"] :p/age 1]]
     [[:db/add [:p/age 10] :p/age 1]]
     [[:db/add [:p/nope "x"] :p/age 1]]
     [[:db/add :no/ident :p/age 1]]
     [[:db/add [:p/name "Al"] :p/nope 1]]
     [{:db/id [:p/name "Al"] :p/nope 1}]
     [{:db/id [:p/name "Al"] :p/age nil}]
     [{:db/id [:p/name "Al"] "str" 1}]
     [[:db/add 17592186049999 :p/age 1]]
     [[:foo/bar 1 2 3]]
     [[:db/add [:p/name "Al"] :p/boss [:p/name "Bo"]] [:db/retract [:p/name "Al"] :p/boss [:p/name "Bo"]]]
     ]]
   ["errors: schema"
    [base-schema
     [{:db/id "q" :db/ident :bad/attr :db/valueType :db.type/string :db.install/_attribute :db.part/db}]
     [{:db/id "q" :db/ident :bad/attr2 :db/cardinality one :db.install/_attribute :db.part/db}]
     [{:db/id "q" :db/ident :bad/attr3 :db/valueType :db.type/string :db/cardinality one}
      {:db/id "u" :bad/attr3 "used in same tx"}]
     [[:db/add :db.part/db :db.install/attribute :p/name]]]]
   ["tx instants"
    [base-schema
     (fn [_] [[:db/add "datomic.tx" :db/txInstant (java.util.Date.)]])
     [[:db/add "datomic.tx" :db/txInstant #inst "2001-01-01"]]
     [[:db/add "datomic.tx" :db/txInstant #inst "2999-01-01"]]
     [[:db/add "datomic.tx" :db/txInstant 5]]
     (fn [_] [{:db/id "datomic.tx" :db/doc "named tx"} {:db/id "e" :p/name "E"}])]]
   ["tx entity"
    [base-schema
     [{:db/id "datomic.tx" :p/name "TxName"}]
     [[:db/add "t" :p/name "T"] [:db/add "t" :p/boss "datomic.tx"]]
     (fn [tempid] [[:db/add (tempid :db.part/tx) :p/age 5] [:db/add (tempid :db.part/user -1) :p/name "U"]])]]
   ["no-history attribute"
    [base-schema
     [{:db/id "x" :p/name "NH" :p/nick "one"}]
     [[:db/add [:p/name "NH"] :p/nick "two"]]
     [[:db/retract [:p/name "NH"] :p/nick "two"]]]]
   ["partitions"
    [base-schema
     [{:db/id "part" :db/ident :my/part :db.install/_partition :db.part/db}]
     (fn [tempid] [{:db/id (tempid :my/part) :p/name "InPart"} {:db/id "u" :p/name "InUser"}
                   {:db/id (tempid :db.part/user) :p/name "InUser2"}])
     (fn [tempid] [{:db/id (tempid :nope/part) :p/name "NoPart"}])
     (fn [tempid] [{:db/id (tempid :db.part/db) :db/ident :db/ident-only}])]]
   ["idents and enum upsert"
    [base-schema
     [{:db/id "i" :db/ident :p/age :db/doc "age doc"}]
     [{:db/id :p/name :db/doc "name doc"}]
     [[:db/add :p/age :db/doc "age doc 2"]]
     [{:db/id "x" :db/ident :color/red}]
     [{:db/id "x" :db/ident :color/red :db/doc "red"}]
     [[:db/add [:p/name "nobody"] :db/ident :color/blue]]]]
   ["explicit tempids"
    [base-schema
     (fn [t] [[:db/add (t :db.part/user -1) :p/name "Neg"] [:db/add (t :db.part/user -1) :p/age 3]
              [:db/add (t :db.part/user -2) :p/name "Neg2"] [:db/add (t :db.part/user -2) :p/boss (t :db.part/user -1)]])
     (fn [t] [{:db/id (t :db.part/user -7) :p/name "Neg"} {:db/id (t :db.part/user) :p/name "Anon"}])
     (fn [t] [{:db/id (t :db.part/db -1) :db/ident :x/y :db/valueType :db.type/string
               :db/cardinality one :db.install/_attribute :db.part/db}])
     (fn [t] [[:db/add (t :db.part/user -1) :x/y "uses new attr"]])]]
   ["retract forms"
    [base-schema
     [{:db/id "a" :p/name "A" :p/age 1 :p/tags ["x" "y"] :p/boss {:p/name "B"}}]
     [[:db/retract [:p/name "A"] :p/tags "x"]]
     [[:db/retract [:p/name "A"] :p/boss [:p/name "B"]]]
     [[:db/retract [:p/name "A"] :p/age]]
     [[:db/retract [:p/name "A"] :p/tags]]
     [[:db/retract "zzz" :p/age 5]]
     [[:db/retract [:p/name "A"] :p/age 5] [:db/add [:p/name "A"] :p/age 6]]
     [[:db/retract [:p/name "A"] :p/name "A"]]
     [[:db/add [:p/name "A"] :p/name "A2"]]]]
   ["unique value moves between entities"
    [base-schema
     [{:db/id "a" :p/name "A" :p/email "e@x"} {:db/id "b" :p/name "B"}]
     [[:db/retract [:p/name "A"] :p/email "e@x"] [:db/add [:p/name "B"] :p/email "e@x"]]
     [[:db/add [:p/name "A"] :p/email "e@x"]]
     [[:db/add [:p/name "A"] :p/email "other@x"]]
     [[:db/add [:p/name "A"] :p/email "e@x"]]]]
   ["ref identity upsert"
    [base-schema
     [{:db/id "o" :org/name "Acme"} {:db/id "p" :p/name "P" :p/org "o"}]
     [{:db/id "q" :p/name "Q" :p/org [:org/name "Acme"]}]
     [{:db/id "r" :p/org {:org/name "Acme"} :p/age 2}]
     [{:db/id "s" :p/org {:org/name "Globex"} :p/age 3}]]]
   ["tempid only entities and empty txs"
    [base-schema
     [{:db/id "lonely"}]
     []
     [{:db/id "x" :p/name "after"}]
     [[:db/retract [:p/name "after"] :p/age 1]]
     [{:db/id [:p/name "after"]}]]]
   ["bulk"
    [base-schema
     (vec (for [i (range 60)] {:db/id (str "p" i) :p/name (str "Person" i) :p/age (mod (* i 7) 13)
                               :p/tags [(str "t" (mod i 5)) (str "u" (mod i 3))]
                               :p/boss (str "p" (mod (inc i) 60))}))
     (vec (for [i (range 0 60 3)] [:db/retract [:p/name (str "Person" i)] :p/age]))]]])

(defn- index-snapshot [cd cm]
  (let [dd (d/db cd) dm (m/db cm)]
    (into {:basis-t [(d/basis-t dd) (m/basis-t dm)]
           :next-t [(d/next-t dd) (m/next-t dm)]}
          (for [idx o/indexes]
            [idx (o/index-dump dd dm idx)]))))

(def ^:private expected-failures
  "Scenario name -> indexes of steps that are expected to fail (scenarios named
  \"errors: ...\" are exempt). Guards against both systems failing for the wrong reason."
  {"tx instants" #{2 3 4}
   "partitions" #{3}
   "idents and enum upsert" #{6}
   "retract forms" #{9}
   "unique value moves between entities" #{3 5}
   "ref identity upsert" #{3}})

(defn- run-scenario [[nm steps]]
  (o/with-both [cd cm]
    (doseq [[i step] (map-indexed vector steps)]
      (let [{:keys [datomic mnemonic]} (o/transact-both [cd cm] step)]
        (is (not (vector? (:error mnemonic))) (str nm " / step " i " unclassified error: " (:error mnemonic)))
        (when-not (.startsWith ^String nm "errors:")
          (is (= (contains? (get expected-failures nm) i) (some? (:error mnemonic)))
              (str nm " / step " i " unexpected outcome " (:error mnemonic))))
        (is (= datomic mnemonic) (str nm " / step " i))))
    (let [snap (index-snapshot cd cm)]
      (is (apply = (:basis-t snap)) (str nm " basis-t " (:basis-t snap)))
      (is (apply = (:next-t snap)) (str nm " next-t " (:next-t snap)))
      (doseq [idx o/indexes]
        (let [{:keys [datomic mnemonic]} (get snap idx)]
          (is (= (count datomic) (count mnemonic)) (str nm " " idx " count"))
          (is (= datomic mnemonic) (str nm " " idx " datoms")))))))

(deftest bootstrap-equals-datomic-test
  (o/with-both [cd cm]
    (let [snap (index-snapshot cd cm)]
      (is (apply = (:basis-t snap)))
      (is (apply = (:next-t snap)))
      (doseq [idx o/indexes]
        (let [{:keys [datomic mnemonic]} (get snap idx)]
          (is (pos? (count datomic)))
          (is (= datomic mnemonic) (str idx)))))))

(deftest scenarios-test
  (is (>= (count scenarios) 20))
  (doseq [sc scenarios]
    (testing (first sc)
      (run-scenario sc))))

(deftest index-api-test
  (testing "datoms / seek-datoms / index-range agree on components"
    (o/with-both [cd cm]
      (let [steps [base-schema
                   (people "Al" "Bo" "Cy" "Dee")
                   [{:db/id [:p/name "Al"] :p/friends [[:p/name "Bo"] [:p/name "Cy"]] :p/boss [:p/name "Dee"]
                     :a/city "Denver"}
                    {:db/id [:p/name "Bo"] :a/city "Austin"}]]]
        (doseq [s steps] (o/transact-both [cd cm] s))
        (let [dd (d/db cd) dm (m/db cm)
              pairs {d/datoms m/datoms d/seek-datoms m/seek-datoms d/index-range m/index-range}
              cmp (fn [f & args]
                    (is (= (o/normalize-datoms (seq (apply f dd args)))
                           (o/normalize-datoms (seq (apply (get pairs f) dm args))))
                        (pr-str args)))]
          (cmp d/datoms :eavt [:p/name "Al"])
          (cmp d/datoms :eavt [:p/name "Al"] :p/friends)
          (cmp d/datoms :aevt :p/age)
          (cmp d/datoms :aevt :p/age (:e (first (d/datoms dd :avet :p/name "Bo"))))
          (cmp d/datoms :avet :p/name "Cy")
          (cmp d/datoms :avet :a/city)
          (cmp d/datoms :avet :p/age 20)
          (cmp d/datoms :vaet [:p/name "Dee"])
          (cmp d/datoms :vaet [:p/name "Bo"] :p/friends)
          (cmp d/datoms :eavt :p/name)
          (cmp d/datoms :eavt 73 :nope/attr)
          (cmp d/seek-datoms :eavt [:p/name "Al"] :p/age)
          (cmp d/seek-datoms :avet :a/city "Austin")
          (cmp d/seek-datoms :avet :p/age 15)
          (cmp d/seek-datoms :aevt :p/name)
          (cmp d/index-range :a/city "A" "Z")
          (cmp d/index-range :a/city "Austin" "Austin")
          (cmp d/index-range :a/city nil "B")
          (cmp d/index-range :a/city "Bz" nil)
          (cmp d/index-range :p/age 10 30)
          (cmp d/index-range :p/name "B" "D")
          (doseq [f [(fn [db* api] (api :avet db* :p/boss))
                     (fn [db* api] (api :avet db* :p/tags))]]
            (is (= :db.error/attribute-not-indexed
                   (try (doall (f dd (fn [i db* a] (d/datoms db* i a)))) nil
                        (catch Exception e (:db/error (ex-data e)))))
                "datomic")
            (is (= :db.error/attribute-not-indexed
                   (try (doall (f dm (fn [i db* a] (m/datoms db* i a)))) nil
                        (catch Exception e (:db/error (ex-data e)))))
                "mnemonic")))))))
