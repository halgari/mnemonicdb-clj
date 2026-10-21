(ns mnemonic.diff-pull-test
  "Differential tests: pull and entity results must equal Datomic's over the same
  entity graph (components, cycles, enums, wide cardinality-many)."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datomic.api :as d]
            [mnemonic.api :as m]
            [mnemonic.conn :as conn]
            [mnemonic.oracle :as o]))

(use-fixtures :each (fn [f] (try (f) (finally (conn/reset-registry!)))))

(defn- attr [id vt card & {:as opts}]
  (merge {:db/id (str "attr-" id) :db/ident id :db/valueType vt :db/cardinality card} opts))

(def ^:private one :db.cardinality/one)
(def ^:private many :db.cardinality/many)

(def ^:private schema-tx
  [(attr :p/name :db.type/string one :db/unique :db.unique/identity)
   (attr :p/age :db.type/long one)
   (attr :p/tags :db.type/string many)
   (attr :p/nums :db.type/long many)
   (attr :p/friends :db.type/ref many)
   (attr :p/boss :db.type/ref one)
   (attr :p/best :db.type/ref one)
   (attr :p/addr :db.type/ref one :db/isComponent true)
   (attr :p/items :db.type/ref many :db/isComponent true)
   (attr :p/kind :db.type/ref one)
   (attr :a/city :db.type/string one)
   (attr :a/zip :db.type/string one)
   (attr :a/note :db.type/ref one :db/isComponent true)
   (attr :n/text :db.type/string one)
   {:db/id "k1" :db/ident :kind/admin :db/doc "administrator"}
   {:db/id "k2" :db/ident :kind/user}])

(def ^:private data-tx
  (concat
   [{:db/id "a" :p/name "Al" :p/age 30 :p/tags ["x" "y"] :p/kind :kind/admin
     :p/addr {:a/city "NYC" :a/zip "1" :a/note {:n/text "hi"}}
     :p/items [{:a/city "i1"} {:a/city "i2"}]}
    {:db/id "b" :p/name "Bo" :p/boss "a" :p/friends ["a" "c"]}
    {:db/id "c" :p/name "Cy" :p/boss "b" :p/friends ["a" "b"] :p/kind :kind/user}
    {:db/id "d" :p/name "Di" :p/friends ["f0" "f1" "f2"]}
    {:db/id "e" :p/name "Ed" :p/nums (vec (range 1100))
     :p/friends (mapv #(str "g" %) (range 1100))}
    {:db/id "x" :p/name "X" :p/friends ["y"]}
    {:db/id "y" :p/name "Y" :p/friends ["x"]}
    {:db/id "s" :p/name "S" :p/friends ["s"] :p/boss "s"}
    {:db/id "t1" :p/name "T1" :p/friends ["t2"] :p/boss "t2"}
    {:db/id "t2" :p/name "T2" :p/friends ["t3"] :p/boss "t3" :p/age 2}
    {:db/id "t3" :p/name "T3" :p/friends ["t1"] :p/boss "t1" :p/age 3}
    {:db/id "n1" :p/name "N1" :p/friends ["n2" "n3"]}
    {:db/id "n2" :p/name "N2" :p/age 5}
    {:db/id "n3" :p/name "N3"}
    {:db/id "g1" :p/name "G1" :p/friends ["g2" "g3"]}
    {:db/id "g2" :p/name "G2" :p/friends ["g4"]}
    {:db/id "g3" :p/name "G3" :p/friends ["g4"]}
    {:db/id "g4" :p/name "G4"}
    {:db/id "l1" :p/name "L1" :p/friends ["l2" "l3"]}
    {:db/id "l2" :p/name "L2"}
    {:db/id "l3" :p/name "L3" :p/age 9}]
   (for [i (range 3)] {:db/id (str "f" i) :p/name (str "f" i)})
   (for [i (range 1100)] {:db/id (str "g" i) :p/age i})))

(defn- setup
  "Both dbs loaded with the same data; returns {:d db :m db :ids tempids}."
  [conn-d conn-m]
  @(d/transact conn-d schema-tx)
  @(m/transact conn-m schema-tx)
  (let [rd @(d/transact conn-d data-tx)
        rm @(m/transact conn-m data-tx)]
    (let [named (fn [r] (into {} (filter (comp string? key)) (:tempids r)))]
      (is (= (named rd) (named rm)) "same entity ids on both sides")
      {:d (d/db conn-d) :m (m/db conn-m) :ids (named rm)})))

(defn- run [f]
  (try (f)
       (catch Throwable t
         [:error (or (:db/error (ex-data t)) [:unclassified (class t)])])))

(def ^:private patterns
  "Pattern forms; the eid key is a tempid name or a literal."
  [['[:p/name :p/age] "a"]
   ['[:db/id :p/name] "a"]
   ['[*] "a"]
   ['[*] "b"]
   ['[:p/boss] "b"]
   ['[:p/kind] "a"]
   ['[{:p/kind [:db/ident]}] "a"]
   ['[{:p/kind [*]}] "a"]
   ['[{:p/boss [:p/name]}] "b"]
   ['[{:p/friends [:p/name]}] "b"]
   ['[:p/friends :p/tags] "a"]
   ['[:p/friends :p/tags] "b"]
   ['[:p/_boss] "a"]
   ['[{:p/_boss [:p/name]}] "a"]
   ['[:p/_friends] "a"]
   ['[{:p/_friends [:p/name]}] "a"]
   ['[:p/_kind] :kind/admin]
   ['[{:p/_kind [:p/name]}] :kind/admin]
   ['[(:p/name :as :n)] "a"]
   ['[(:p/missing :default "x")] "a"]
   ['[(:p/age :default 5)] "d"]
   ['[(:p/boss :default 5)] "a"]
   ['[(:p/tags :limit 1)] "a"]
   ['[(:p/tags :limit nil)] "a"]
   ['[(:p/tags :limit 0)] "a"]
   ['[(:p/friends :limit 1)] "b"]
   ['[{(:p/friends :limit 1) [:p/name]}] "b"]
   ['[{(:p/friends :limit 1 :as :fr) [:p/name]}] "b"]
   ['[(:p/age :xform str :as :s)] "a"]
   ['[(limit :p/tags 1)] "a"]
   ['[(limit :p/tags nil)] "a"]
   ['[(default :p/zzz "d")] "a"]
   ['[(default :p/age 99)] "d"]
   ['[{(limit :p/friends 1) [:p/name]}] "b"]
   ['[:p/name {:p/boss ...}] "c"]
   ['[{:p/boss ...}] "c"]
   ['[:p/name {:p/boss 1}] "c"]
   ['[:p/name {:p/boss 2}] "c"]
   ['[:p/name {:p/friends ...}] "b"]
   ['[:p/name {:p/friends 1}] "b"]
   ['[:db/id {:p/friends ...}] "b"]
   ['[:p/name {:p/_boss ...}] "a"]
   ['[:p/name {(:p/friends :limit 1) ...}] "b"]
   ['[:p/name {:p/boss ...} {:p/friends 1}] "c"]
   ['[:p/name {:p/friends ...}] "x"]
   ['[:db/id {:p/friends ...}] "x"]
   ['[:db/id :p/name {:p/friends ...}] "x"]
   ['[:p/name {:p/friends ...}] "s"]
   ['[:db/id {:p/friends ...}] "s"]
   ['[:p/name {:p/boss ...}] "t1"]
   ['[:db/id :p/age {:p/boss ...}] "t1"]
   ['[:p/name {:p/friends 2}] "t1"]
   ['[:p/name {:p/boss 3}] "t1"]
   ['[:p/name {:p/boss ...} {:p/friends ...}] "t1"]
   ['[:p/name {:p/boss 1} {:p/friends 1}] "t1"]
   ['[:p/name {:p/boss 2} {:p/friends 1}] "t1"]
   ['[:p/name {:p/friends [:p/name {:p/friends ...}]}] "t1"]
   ['[{:p/boss ...} {:p/friends [:p/name {:p/boss ...}]}] "t1"]
   ['[:p/name {(:p/boss :as :b) ...}] "t1"]
   ['[:p/name {(:p/boss :default 1) 1}] "t1"]
   ['[:db/id :p/name {:p/boss 1}] "t1"]
   ['[* {:p/friends ...}] "x"]
   ['[* {:p/boss ...}] "t1"]
   ['[:p/name {:p/_friends ...} {:p/friends 1}] "x"]
   ['[:p/name {:p/friends ...}] "g1"]
   ['[:db/id {:p/friends ...}] "g1"]
   ['[{:p/friends [:p/age]}] "n1"]
   ['[{(:p/friends :limit 1) [:p/age]}] "l1"]
   ['[{(:p/friends :limit 2) [:p/age]}] "l1"]
   ['[{(:p/friends :default 9) [:p/age]}] "x"]
   ['[{:p/boss [:p/age]}] "n1"]
   ['[:p/addr] "a"]
   ['[:p/items] "a"]
   ['[{:p/addr [*]}] "a"]
   ['[{:p/addr [:a/city {:a/note [*]}]}] "a"]
   ['[{:p/items [:a/city]}] "a"]
   ['[{:p/items [:a/city :p/_items]}] "a"]
   ['[:p/_addr] ["a" :p/addr]]
   ['[* {:p/boss [:p/name]}] "b"]
   ['[* (:p/name :as :n)] "b"]
   ['[* :p/_boss] "b"]
   ['[* {:p/_boss [:p/name]}] "b"]
   ['[* {:p/addr [:a/city]}] "a"]
   ['[* (:p/addr :as :ad)] "a"]
   ['[* (:p/items :limit 1)] "a"]
   ['[*] ["a" :p/addr]]
   ['[:db/id :db/id] "a"]
   ['[(:db/id :as :id)] "a"]
   ['[:p/name] 99999999]
   ['[*] 99999999]
   ['[:db/id] 99999999]
   ['[(:p/age :default 1)] 99999999]
   ['[:p/name] "d"]
   ['[:p/age] "d"]
   ['[:p/name] [:p/name "Al"]]
   ['[:p/name] [:p/name "Nope"]]
   ['[:p/name] :kind/admin]
   ['[*] :kind/admin]
   ['[:db/ident] :kind/admin]
   ['[:zz/foo] "a"]
   ['[{:zz/foo [:a]}] "a"]
   ['[:p/_name] "a"]
   ['[{:p/name [:x]}] "a"]
   ['[{:p/age [*]}] "a"]
   ['[:p/name :bogus] "a"]
   ['[(:p/name :bogus 1)] "a"]
   ['[(:p/name :as "n")] "a"]
   ['[(:p/name :as :n :as :m)] "a"]
   ['[(:p/name :as :a) (:p/age :as :a)] "a"]
   ['[(:p/age :as :a) (:p/name :as :a)] "a"]
   ['[{:p/boss [:p/name] :p/friends [:p/name]}] "b"]
   ['[{:p/boss [:p/name] :p/friends ...}] "t1"]
   ['[{} ] "a"]
   ['[] "a"]
   ['[:p/name :p/name] "a"]
   ['[{:p/boss [:p/name]} :p/boss] "b"]
   ['[:p/_boss :p/_boss] "a"]
   ['["p/name"] "a"]
   ['[(:p/name :limit)] "a"]
   ['[{:p/boss 0}] "c"]
   ['[{:p/boss -1}] "c"]
   ['[{:p/boss "x"}] "c"]
   ['[(:p/name :xform str)] "a"]
   ['[*] "e"]
   ['[(:p/nums :limit nil)] "e"]
   ['[(:p/nums :limit 3)] "e"]
   ['[:p/nums] "e"]
   ['[:p/friends] "e"]
   ['[{:p/friends [:p/age]}] "e"]
   ['[{(:p/friends :limit nil) [:p/age]}] "e"]
   ['[:p/_friends] "g5"]
   ['[:db/valueType :db/cardinality :db/unique] :p/name]
   ['[{:db/valueType [*]}] :p/name]
   ['[*] :p/name]
   ['[*] 10]])

(defn- eid-for
  "The eid for a pattern entry key, resolved on `db` (works for either side)."
  [ids db lookup x]
  (cond
    (string? x) (get ids x)
    (and (vector? x) (string? (first x)))
    (let [e (get ids (first x))]
      (:db/id (get (lookup db e) (second x))))
    :else x))

(defn- both-pulls [{:keys [d m ids]} pattern x]
  (let [ed (eid-for ids d (fn [db e] (d/pull db '[*] e)) x)
        em (eid-for ids m (fn [db e] (m/pull db '[*] e)) x)]
    (is (= ed em) "same target eid")
    {:datomic (run #(d/pull d pattern ed))
     :mnemonic (run #(m/pull m pattern em))}))

(deftest pull-matches-datomic
  (o/with-both [cd cm]
    (let [env (setup cd cm)]
      (is (>= (count patterns) 30))
      (doseq [[pattern x] patterns
              :let [{:keys [datomic mnemonic]} (both-pulls env pattern x)]]
        (testing (pr-str pattern x)
          (is (= datomic mnemonic)))))))

(deftest pull-many-matches-datomic
  (o/with-both [cd cm]
    (let [{:keys [d m ids]} (setup cd cm)
          eids [(ids "a") (ids "b") 99999999 (ids "c") (ids "a")]]
      (doseq [pattern ['[:p/name] '[*] '[:db/id {:p/boss [:p/name]}] '[(:p/age :default 0)]]]
        (testing (pr-str pattern)
          (is (= (d/pull-many d pattern eids) (m/pull-many m pattern eids)))))
      (is (= (d/pull-many d '[:p/name] [[:p/name "Al"] :kind/admin [:p/name "Zz"]])
             (m/pull-many m '[:p/name] [[:p/name "Al"] :kind/admin [:p/name "Zz"]])))
      (is (= (d/pull-many d '[:p/name] []) (m/pull-many m '[:p/name] [])))
      (is (= (d/pull-many d '[:p/name] nil) (m/pull-many m '[:p/name] nil))))))

(defn- plain
  "Entity -> plain data: touched shape one level deep, refs reduced to :db/id maps or
  idents, so Datomic and MnemonicDB values compare equal."
  [get-db-id entity? v]
  (letfn [(norm [x]
            (cond
              (entity? x) {:db/id (get-db-id x)}
              (set? x) (set (map norm x))
              :else x))]
    (norm v)))

(deftest entity-matches-datomic
  (o/with-both [cd cm]
    (let [{:keys [d m ids]} (setup cd cm)
          is-d? #(instance? datomic.query.EntityMap %)
          is-m? #(instance? mnemonic.entity.Entity %)
          view (fn [e entity? touch]
                 (let [t (touch e)]
                   (into {} (map (fn [[k v]] [k (plain :db/id entity? v)])) t)))]
      (doseq [k ["a" "b" "c" "d" "x" "t1" "n1"]
              :let [ed (d/entity d (ids k))
                    em (m/entity m (ids k))]]
        (testing k
          (is (= (view ed is-d? identity) (view em is-m? identity)))
          (is (= (set (keys ed)) (set (keys em))))
          (is (= (:db/id ed) (:db/id em)))
          (doseq [a [:p/name :p/age :p/tags :p/friends :p/boss :p/kind :p/addr :p/items
                     :p/_boss :p/_friends :p/_addr :p/_items :p/missing :zz/_none]]
            (is (= (plain :db/id is-d? (get ed a)) (plain :db/id is-m? (get em a))) (str k " " a)))))
      (testing "enums, missing, lookup refs"
        (is (= (:p/kind (d/entity d (ids "a"))) (:p/kind (m/entity m (ids "a")))))
        (is (= (into {} (d/touch (d/entity d :kind/admin))) (into {} (m/touch (m/entity m :kind/admin))))
            "enum entity touch")
        (is (= (nil? (d/entity d :nope)) (nil? (m/entity m :nope))))
        (is (= (nil? (d/entity d [:p/name "Zz"])) (nil? (m/entity m [:p/name "Zz"]))))
        (is (= (:db/id (d/entity d [:p/name "Al"])) (:db/id (m/entity m [:p/name "Al"]))))
        (is (= (:db/id (d/entity d 99999999)) (:db/id (m/entity m 99999999))))
        (is (= (seq (d/entity d 99999999)) (seq (m/entity m 99999999))))
        (is (= (pr-str (d/entity d (ids "a"))) (pr-str (m/entity m (ids "a")))))))))
