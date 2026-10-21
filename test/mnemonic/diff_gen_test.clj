(ns mnemonic.diff-gen-test
  "Generative differential test: random schemas and transaction sequences are applied to
  Datomic (the oracle) and MnemonicDB; after every transaction the normalized report,
  error kind and every index must agree. At the end as-of/since/history views, entities,
  pull and a query corpus are compared too.

  Trials: 200 by default, override with the MNEMONIC_DIFF_TRIALS environment variable."
  (:require [clojure.test :refer [deftest is]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.walk :as walk]
            [datomic.api :as d]
            [mnemonic.api :as m]
            [mnemonic.oracle :as o]))

(def ^:private trials
  (or (some-> (System/getenv "MNEMONIC_DIFF_TRIALS") parse-long) 200))

;; ---------------------------------------------------------------------------
;; Schema generation

(def ^:private domains
  "Small value domains per type to force collisions. No -0.0 / NaN (see docs/deviations.md)."
  {:db.type/string ["" "a" "b" "c"]
   :db.type/long [0 1 2 -1 Long/MAX_VALUE]
   :db.type/double [0.0 1.5 -2.5]
   :db.type/float [(float 0.5) (float 1.5) (float -2.5)]
   :db.type/boolean [true false]
   :db.type/bigint [0N 5N -3N]
   :db.type/bigdec [0M 1.5M 2M -0.25M]
   :db.type/instant [#inst "2020-01-01" #inst "1969-12-31T23:59:59.999-00:00"]
   :db.type/uuid [#uuid "123e4567-e89b-12d3-a456-426614174000"
                  #uuid "00000000-0000-0000-0000-000000000001"]
   :db.type/uri [(java.net.URI. "http://x.org/a") (java.net.URI. "b:c")]
   :db.type/keyword [:k/a :k/b :plain]
   :db.type/symbol ['s/a 'plain]})

(def ^:private idents [:g.k/a :g.k/b])

(def ^:private types
  (vec (concat (keys domains) [:db.type/ref :db.type/bytes])))

(def ^:private gen-attr-spec
  (gen/let [t (gen/elements types)
            card (gen/elements [:db.cardinality/one :db.cardinality/one :db.cardinality/many])
            unique (gen/frequency [[6 (gen/return nil)]
                                   [2 (gen/return :db.unique/identity)]
                                   [1 (gen/return :db.unique/value)]])
            index gen/boolean
            component gen/boolean]
    (cond-> {:type t :card card}
      (not= t :db.type/bytes) (assoc :unique unique)
      index (assoc :index true)
      (and (= t :db.type/ref) component) (assoc :component true))))

(defn- restrict-identity
  "At most one :db.unique/identity attribute per schema, and it is cardinality-one (other
  unique attributes become :db.unique/value): how Datomic unifies tempids that carry several
  identity assertions in one tx is only partly matched, see docs/deviations.md."
  [specs]
  (loop [out [] seen? false [s & more :as specs] specs]
    (if (empty? specs)
      out
      (let [ident? (= :db.unique/identity (:unique s))
            demote? (and ident? (or seen? (= :db.cardinality/many (:card s))))]
        (recur (conj out (if demote? (assoc s :unique :db.unique/value) s))
               (or seen? (and ident? (not demote?))) more)))))

(def ^:private gen-schema
  (gen/let [specs (gen/vector gen-attr-spec 2 7)]
    (vec (map-indexed (fn [i s] (assoc s :ident (keyword "g" (str "a" i))))
                      (restrict-identity specs)))))

(defn- schema-tx [schema]
  (vec (concat
        (for [{:keys [ident type card unique index component]} schema]
          (cond-> {:db/id (str "attr-" (name ident)) :db/ident ident
                   :db/valueType type :db/cardinality card}
            unique (assoc :db/unique unique)
            index (assoc :db/index true)
            component (assoc :db/isComponent true)))
        (for [i idents] {:db/ident i}))))

;; ---------------------------------------------------------------------------
;; Operation generation. Entity specs are symbolic and resolved against the entities
;; known so far, so both databases receive identical concrete tx-data.

(defn- gen-value [{:keys [type]}]
  (case type
    :db.type/ref (gen/frequency [[3 (gen/fmap (fn [i] [:t i]) (gen/choose 0 3))]
                                 [3 (gen/fmap (fn [i] [:k i]) (gen/choose 0 20))]
                                 [1 (gen/fmap (fn [i] [:i i]) (gen/choose 0 1))]])
    :db.type/bytes (gen/elements [[:bytes [1 2]] [:bytes []] [:bytes [3]]])
    (gen/elements (domains type))))

(defn- gen-entity [schema]
  (let [lookups (filter #(and (:unique %) (not (#{:db.type/ref :db.type/bytes} (:type %)))) schema)]
    (gen/frequency
     (cond-> [[4 (gen/fmap (fn [i] [:t i]) (gen/choose 0 3))]
              [4 (gen/fmap (fn [i] [:k i]) (gen/choose 0 20))]]
       (seq lookups)
       (conj [3 (gen/let [a (gen/elements lookups)
                          v (gen/elements (domains (:type a)))]
                  [:l (:ident a) v])])))))

(defn- gen-op [schema]
  (gen/let [kind (gen/frequency [[8 (gen/return :add)] [4 (gen/return :retract)]
                                 [1 (gen/return :retract-attr)] [7 (gen/return :map)]
                                 [1 (gen/return :retract-entity)] [2 (gen/return :cas)]])
            e (gen-entity schema)
            a (gen/elements schema)
            v (gen-value a)
            v2 (gen-value a)
            more (gen/vector (gen/bind (gen/elements schema)
                                     (fn [b] (gen/tuple (gen/return b) (gen-value b))))
                             0 2)
            vs (gen/vector (gen-value a) 1 3)
            anon gen/boolean]
    (case kind
      :add [:add e (:ident a) v]
      :retract [:retract e (:ident a) v]
      :retract-attr [:retract-attr e (:ident a)]
      :retract-entity [:retract-entity e]
      :cas [:cas e (:ident a) (when anon v) v2]
      :map [:map (when-not anon e)
            (into {(:ident a) (if (= :db.cardinality/many (:card a)) [:many vs] v)}
                  (for [[b bv] more] [(:ident b) (if (= :db.cardinality/many (:card b)) [:many [bv]] bv)]))])))

(defn- gen-tx [schema]
  (gen/vector (gen-op schema) 0 5))

(def ^:private gen-case
  (gen/bind gen-schema
            (fn [schema]
              (gen/let [txs (gen/vector (gen-tx schema) 1 6)
                        probes (gen/vector (gen/choose 0 1000) 3)]
                {:schema schema :txs txs :probes probes}))))

;; ---------------------------------------------------------------------------
;; Materialization

(defn- ent [spec known]
  (case (first spec)
    :t (str "t" (second spec))
    :k (if (seq known) (nth known (mod (second spec) (count known))) (str "t" (second spec)))
    :i (nth idents (second spec))
    :l [(second spec) (nth spec 2)]))

(def ^:private bytes-counter
  "Byte-array values get distinct content per occurrence: Datomic compares arrays by identity
  (two equal-content arrays on a card-one attribute conflict); see docs/deviations.md."
  (atom 0))

(defn- val* [v known]
  (cond
    (nil? v) nil
    (and (vector? v) (= :bytes (first v))) (byte-array (conj (vec (second v)) (swap! bytes-counter #(mod (inc %) 100))))
    (and (vector? v) (= :many (first v))) (mapv #(val* % known) (second v))
    (vector? v) (ent v known)
    :else v))

(defn- materialize [ops known]
  (vec
   (for [[kind & args] ops]
     (case kind
       :add (let [[e a v] args] [:db/add (ent e known) a (val* v known)])
       :retract (let [[e a v] args] [:db/retract (ent e known) a (val* v known)])
       :retract-attr (let [[e a] args] [:db/retract (ent e known) a])
       :retract-entity [:db/retractEntity (ent (first args) known)]
       :cas (let [[e a old new] args] [:db/cas (ent e known) a (val* old known) (val* new known)])
       :map (let [[e attrs] args
                  attrs (into {} (for [[a v] attrs] [a (val* v known)]))]
              (if e (assoc attrs :db/id (ent e known)) attrs))))))

;; Datomic quirk (docs/deviations.md): asserting the same unique-attribute value twice for one
;; tempid in one tx can raise a spurious datoms-conflict once tempids are unified. The
;; generator never emits such duplicates.
(defn- dedupe-unique [uniq ops]
  (let [seen (volatile! #{})
        fresh! (fn [k] (when-not (@seen k) (vswap! seen conj k) true))]
    (vec
     (keep-indexed
      (fn [i [kind & args :as op]]
        (case kind
          :add (let [[e a v] args]
                 (when (or (not (uniq a)) (fresh! [e a v])) op))
          :map (let [[e attrs] args
                     e* (or e [:anon i])
                     attrs (into {}
                                 (keep (fn [[a v]]
                                         (if (uniq a)
                                           (if (and (vector? v) (= :many (first v)))
                                             (when-let [vs (seq (filter #(fresh! [e* a %]) (distinct (second v))))]
                                               [a [:many (vec vs)]])
                                             (when (fresh! [e* a v]) [a v]))
                                           [a v])))
                                 attrs)]
                 [:map e attrs])
          op))
      ops))))

;; ---------------------------------------------------------------------------
;; Comparison

(defn- norm [x]
  (walk/postwalk (fn [v] (cond (bytes? v) (vec v)
                               (instance? Float v) (double v)
                               (instance? Integer v) (long v)
                               (and (instance? Double v) (Double/isNaN v)) :NaN
                               :else v))
                 x))

(defn- entity-map
  "Touched entity -> plain map; nested entities become {:db/id n}."
  [e]
  (let [entity? #(boolean (re-find #"Entity" (.getName (class %))))
        flat (fn [v] (if (entity? v) {:db/id (:db/id v)} v))]
    (into {} (for [[k v] e]
               [k (if (and (coll? v) (not (entity? v))) (set (map flat v)) (flat v))]))))

(defn- safe [f]
  (try {:ok (let [r (f)] (norm (if (instance? java.util.Set r) (set r) r)))} (catch Throwable t {:error (if (or (instance? StackOverflowError t) (instance? StackOverflowError (.getCause t))) :stack-overflow true)})))

(defn- queries [schema]
  (let [idn (map :ident schema)
        refs (filter #(= :db.type/ref (:type %)) schema)
        nonref (remove #(#{:db.type/ref :db.type/bytes} (:type %)) schema)]
    (vec
     (concat
      [['[:find ?e ?a ?v :where [?e ?a ?v]]]
       ['[:find ?e :where [?e :db/ident ?i]]]]
      (for [a idn] [`[:find ~'?e ~'?v :where [~'?e ~a ~'?v]]])
      (for [a idn] [`[:find ~'?v :where [~'_ ~a ~'?v]]])
      (for [a idn] [`[:find ~'?e ~'?tx :where [~'?e ~a ~'?v ~'?tx]]])
      (for [a nonref] [`[:find ~'?e :in ~'$ ~'?x :where [~'?e ~(:ident a) ~'?x]]
                       (first (domains (:type a)))])
      (for [a nonref]
        [`[:find ~'?e ~'?v :in ~'$ ~'?c :where [~'?e ~(:ident a) ~'?v] [(~'not= ~'?v ~'?c)]]
         (first (domains (:type a)))])
      (for [[a b] (partition 2 1 idn)]
        [`[:find ~'?e ~'?x ~'?y :where [~'?e ~a ~'?x] [~'?e ~b ~'?y]]])
      (for [r refs a idn]
        [`[:find ~'?e ~'?v :where [~'?e ~(:ident r) ~'?x] [~'?x ~a ~'?v]]])))))

(defn- compare-dbs
  "Returns nil when equal, else a description of the first difference."
  [label dd dm schema]
  (or
   (first (for [idx [:eavt :aevt :avet :vaet]
                :let [r (o/index-dump dd dm idx)
                      a (:datomic r) b (:mnemonic r)]
                :when (not= a b)]
            {:label label :index idx
             :only-datomic (take 3 (remove (set b) a))
             :only-mnemonic (take 3 (remove (set a) b))
             :count [(count a) (count b)]}))
   (first (for [[q & args] (queries schema)
                :let [rd (safe #(apply d/q q dd args))
                      rm (safe #(apply m/q q dm args))]
                :when (not= rd rm)]
            {:label label :query q :args args :datomic rd :mnemonic rm}))))

(defn- same-result?
  "Equal reports; two failing txs with several simultaneous uniqueness conflicts may report
  :db.error/unique-conflict or :db.error/datoms-conflict (docs/deviations.md), so those two
  kinds compare equal."
  [{d :datomic m :mnemonic}]
  (let [conflict #{:db.error/unique-conflict :db.error/datoms-conflict}]
    (or (= d m)
        (and (conflict (:error d)) (conflict (:error m))))))

(defn- tx-entities [r]
  (->> (:tx-data (:datomic r))
       (map first) distinct
       (remove #(= 3 (bit-shift-right (long %) 42)))
       (take-last 10)))

(defn- end-checks [cd cm schema known ts probes]
  (let [dd (d/db cd) dm (m/db cm)
        tpool (vec (distinct (conj ts (d/basis-t dd))))]
    (or
     (first
      (for [p probes
            :let [t (nth tpool (mod p (count tpool)))]
            [nm df mf] [[:as-of d/as-of m/as-of] [:since d/since m/since]]
            :let [x (compare-dbs [nm t] (df dd t) (mf dm t) schema)]
            :when x]
        x))
     (let [r (o/index-dump (d/history dd) (m/history dm) :eavt)
           ;; deviation: Datomic's history has two retractions at t0 (docs/deviations.md)
           t0-retract? (fn [[_ _ _ tx added]] (and (= tx (bit-shift-left 3 42)) (false? added)))
           a (remove t0-retract? (:datomic r)) b (remove t0-retract? (:mnemonic r))]
       (when (not= (set a) (set b))
         {:label :history :count [(count a) (count b)]
          :only-datomic (take 3 (remove (set b) a))
          :only-mnemonic (take 3 (remove (set a) b))}))
     (first
      (for [e known
            :let [pd (safe #(d/pull dd '[*] e))
                  pm (safe #(m/pull dm '[*] e))
                  ed (safe #(entity-map (d/touch (d/entity dd e))))
                  em (safe #(entity-map (m/touch (m/entity dm e))))]
            ;; component cycles overflow the stack in Datomic (docs/deviations.md)
            x [(when (and (not= pd pm) (not= :stack-overflow (:error pd))) {:pull e :datomic pd :mnemonic pm})
               (when (and (not= ed em) (not= :stack-overflow (:error ed))) {:entity e :datomic ed :mnemonic em})]
            :when x]
        x)))))

(defn- run-case [{:keys [schema txs probes]}]
  (o/with-both [cd cm]
    (let [uniq (set (map :ident (filter :unique schema)))
          r0 (o/transact-both [cd cm] (schema-tx schema))]
      (or
       (when-not (same-result? r0) {:schema-tx r0})
       (loop [txs txs known [] ts [] i 0]
         (if-let [ops (first txs)]
           (let [data (materialize (dedupe-unique uniq ops) known)
                 r (o/transact-both [cd cm] data)]
             (or (when-not (same-result? r)
                   {:step i :tx data :result r})
                 (compare-dbs [:after i] (d/db cd) (m/db cm) schema)
                 (let [ok? (contains? (:datomic r) :tx-data)]
                   (recur (rest txs)
                          (if ok? (vec (take-last 12 (distinct (concat known (tx-entities r))))) known)
                          (if ok? (conj ts (d/basis-t (d/db cd))) ts)
                          (inc i)))))
           (end-checks cd cm schema known ts probes)))))))

(deftest ^:generative generative-differential
  (let [res (tc/quick-check
             trials
             (prop/for-all [c gen-case]
               (let [r (run-case c)]
                 (when r (println "DIFF:" (pr-str r)))
                 (nil? r)))
             :seed (some-> (System/getenv "MNEMONIC_DIFF_SEED") parse-long))]
    (when-not (:pass? res)
      (println "Failing (shrunk):" (pr-str (-> res :shrunk :smallest)))
      (println "Result seed:" (:seed res)))
    (is (:pass? res))))
