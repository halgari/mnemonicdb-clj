(ns mnemonic.tx
  "Transaction preparation: expands tx-data, resolves ids and tempids, and computes the
  datoms and storage ops of one transaction. `prepare` is pure with respect to storage:
  it reads only through the db it is given."
  (:require [clojure.string :as str]
            [mnemonic.bootstrap :as bootstrap]
            [mnemonic.codec :as codec]
            [mnemonic.datom :as datom]
            [mnemonic.index :as index]
            [mnemonic.schema :as schema])
  (:import (java.net URI)
           (java.nio ByteBuffer)
           (java.util ArrayList Arrays Date LinkedHashMap UUID)
           (java.util.concurrent.atomic AtomicLong)
           (mnemonic.datom Datom)
           (mnemonic.schema Attr)))

(set! *warn-on-reflection* true)

;; ------------------------------------------------------------------ errors

(defn error
  "An ex-info shaped like Datomic's transaction errors."
  ([kind msg] (error kind msg :cognitect.anomalies/incorrect nil))
  ([kind msg data] (error kind msg :cognitect.anomalies/incorrect data))
  ([kind msg category data]
   (ex-info (str kind " " msg)
            (assoc data
                   :db/error kind
                   :cognitect.anomalies/category category
                   :cognitect.anomalies/message msg))))

(defn- fail
  ([kind msg] (throw (error kind msg)))
  ([kind msg data] (throw (error kind msg data)))
  ([kind msg category data] (throw (error kind msg category data))))

;; ------------------------------------------------------------------ tempids

(defrecord TempId [part idx])

(defmethod print-method TempId [^TempId t ^java.io.Writer w]
  (.write w (str "#db/id[" (pr-str (:part t)) " " (:idx t) "]")))

(defmethod print-dup TempId [^TempId t ^java.io.Writer w]
  (.write w (str "#db/id[" (pr-str (:part t)) " " (:idx t) "]")))

(defonce ^:private ^AtomicLong tempid-counter (AtomicLong. -1000000))

(defn tempid
  "A tempid in `part` (an ident or partition id), with a generated or given negative idx."
  ([part] (->TempId part (.decrementAndGet tempid-counter)))
  ([part n] (->TempId part (long n))))

(defn- anon-tempid [] (tempid :db.part/user))

;; Internal wrapper distinguishing tempids from plain values after resolution.
(defrecord Tmp [k])

(defn- tmp? [x] (instance? Tmp x))

(defn tempid-key
  "The key under which `tempid` appears in the :tempids map, given its partition id."
  ^long [^long part-id ^long idx]
  (+ Long/MIN_VALUE (bit-shift-left (inc part-id) 42) idx))

;; ------------------------------------------------------------------ constants

(def ^:private ^:const a-ident 10)
(def ^:private ^:const a-install-partition 11)
(def ^:private ^:const a-install-attribute 13)
(def ^:private ^:const a-value-type 40)
(def ^:private ^:const a-cardinality 41)
(def ^:private ^:const a-tx-instant 50)
(def ^:private ^:const part-db 0)
(def ^:private ^:const part-tx 3)
(def ^:private ^:const part-user 4)
(def ^:private ^:const first-user-attr 73)
;; :db.install/partition, valueType, attribute, function
(def ^:private install-attrs #{11 12 13 14})

;; ------------------------------------------------------------------ db reads

(defn- datoms-of
  "Vector of datoms of `idx` whose key starts with `comps`, read through `db`."
  [db idx comps]
  (into [] (index/datoms-iterable db idx (codec/encode-prefix idx comps))))

(defn- vkey
  "Equality key for a value, consistent with the storage encoding."
  [v]
  (ByteBuffer/wrap (codec/value->bytes v)))

(defn- same-value? [a b]
  (Arrays/equals ^bytes (codec/value->bytes a) ^bytes (codec/value->bytes b)))

(defn- lookup-eid
  "Entity holding value `v` of attribute `aid` in `db` (via AVET), or nil."
  [db aid v]
  (some-> (first (datoms-of db :avet [aid v])) :e))

;; ------------------------------------------------------------------ attribute and value resolution

(defn resolve-attr
  "Attr for an attribute ident or id; throws :db.error/not-an-entity / not-an-attribute."
  ^Attr [schema a]
  (cond
    (keyword? a) (or (schema/attr schema a)
                     (if (contains? (:idents schema) a)
                       (fail :db.error/not-an-attribute (str a " is not an attribute.") {:entity a})
                       (fail :db.error/not-an-entity (str "Unable to resolve entity: " a) {:entity a})))
    (integer? a) (or (get (:attrs schema) (long a))
                     (fail :db.error/not-an-attribute (str a " is not an attribute.") {:entity a}))
    :else (fail :db.error/not-a-keyword (str "Cannot interpret as a keyword: " a ", no leading :")
                {:value a})))

(defn- type-label [vt]
  (keyword (name vt)))

(defn coerce-value
  "Check and normalize `v` for non-ref attribute `attr` (throws wrong-type-for-attribute)."
  [^Attr attr v]
  (let [vt (:value-type attr)
        bad (fn []
              (fail :db.error/wrong-type-for-attribute
                    (str "Value " (if (bytes? v) "<bytes>" v) " is not a valid " (type-label vt)
                         " for attribute " (:ident attr))
                    {:value v :attribute (:ident attr)}))]
    (case vt
      :db.type/string (if (string? v) v (bad))
      :db.type/long (cond (instance? Long v) v
                          (or (instance? Integer v) (instance? Short v) (instance? Byte v)) (long v)
                          :else (bad))
      :db.type/double (if (instance? Double v) v (bad))
      :db.type/float (cond (instance? Float v) v
                           (instance? Double v) (float v)
                           :else (bad))
      :db.type/boolean (if (instance? Boolean v) v (bad))
      :db.type/bigint (if (or (instance? BigInteger v) (instance? clojure.lang.BigInt v)) v (bad))
      :db.type/bigdec (if (instance? BigDecimal v) v (bad))
      :db.type/instant (if (instance? Date v) v (bad))
      :db.type/uuid (if (instance? UUID v) v (bad))
      :db.type/uri (if (instance? URI v) v (bad))
      :db.type/bytes (if (bytes? v) v (bad))
      :db.type/keyword (cond (keyword? v) v
                             (symbol? v) (keyword (namespace v) (name v))
                             (string? v) (if (str/starts-with? v ":")
                                           (keyword (subs v 1))
                                           (fail :db.error/not-a-keyword
                                                 (str "Cannot interpret as a keyword: " v ", no leading :")
                                                 {:value v}))
                             :else (bad))
      :db.type/symbol (if (symbol? v) v (bad))
      :db.type/tuple (if (vector? v) v (bad))
      (fail :db.error/unsupported (str "Unsupported value type " vt) {:attribute (:ident attr)}))))

(declare resolve-ref)

(defn lookup-ref-eid
  "Entity id for lookup ref `x` in (:db ctx), or nil when absent. Throws for malformed
  refs and non-unique attributes."
  [ctx x]
  (let [{:keys [db schema]} ctx]
    (when-not (= 2 (count x))
      (fail :db.error/not-an-entity (str "Unable to resolve entity: " (pr-str x)) {:entity x}))
    (let [^Attr attr (resolve-attr schema (nth x 0))]
      (when-not (:unique attr)
        (fail :db.error/lookup-ref-attr-not-unique
              (str "Attribute values not unique: " (:ident attr)) {:attribute (:ident attr)}))
      (let [v (nth x 1)
            _ (when (nil? v) (fail :db.error/nil-value "Nil is not a legal value"))
            v (if (= :db.type/ref (:value-type attr))
                (let [r (resolve-ref ctx v false)]
                  (if (tmp? r)
                    (fail :db.error/not-an-entity (str "Unable to resolve entity: " (pr-str x)) {:entity x})
                    r))
                (coerce-value attr v))]
        (lookup-eid db (:id attr) v)))))

(defn- resolve-lookup-ref
  [ctx x]
  (or (lookup-ref-eid ctx x)
      (fail :db.error/not-an-entity (str "Unable to resolve entity: " (pr-str x)) {:entity x})))

(defn- check-eid
  [ctx ^long eid]
  (let [part (codec/part-of eid)]
    (when (or (neg? eid)
              (and (not= part part-db) (not= part part-tx)
                   (>= (bit-and eid (dec (bit-shift-left 1 42))) (long (:next-t (:meta ctx))))))
      (fail :db.error/invalid-entity-id (str "Invalid entity id: " eid) {:entity eid}))
    eid))

(defn- resolve-ref
  "Entity reference -> long entity id, or a Tmp for tempids. `validate?` checks that a
  literal id could have been allocated (entity position only)."
  [ctx x validate?]
  (cond
    (integer? x) (if validate? (check-eid ctx (long x)) (long x))
    (keyword? x) (or (get (:idents (:schema ctx)) x)
                     (fail :db.error/not-an-entity (str "Unable to resolve entity: " x) {:entity x}))
    (string? x) (if (str/starts-with? x ":")
                  (resolve-ref ctx (keyword (subs x 1)) validate?)
                  (->Tmp x))
    (instance? TempId x) (->Tmp x)
    (vector? x) (resolve-lookup-ref ctx x)
    :else (fail :db.error/not-an-entity (str "Unable to resolve entity: " (pr-str x)) {:entity x})))

(defn- resolve-value
  [ctx ^Attr attr v]
  (when (nil? v) (fail :db.error/nil-value "Nil is not a legal value"))
  (if (= :db.type/ref (:value-type attr))
    (resolve-ref ctx v false)
    (coerce-value attr v)))

;; ------------------------------------------------------------------ expansion

(defmulti expand-op
  "Expand a list-form op (a sequential whose first element is the keyword `op-kw`) into
  a seq of raw ops `[kind e a v]`, with kind one of :add, :retract, :retract-attr
  (`v` is absent for :retract-attr). `ctx` is {:db :schema :meta :t :tx}. Raw `e`, `a`
  and `v` are as written by the caller; they are resolved afterwards. Use `expand-forms`
  to expand nested tx-data."
  (fn [op-kw _ctx _op] op-kw))

(defmethod expand-op :default [op-kw _ctx op]
  (fail :db.error/not-a-data-function (str "Unable to resolve data function: " (pr-str op-kw))
        {:function op-kw}))

(defn- check-arity [op n]
  (when-not (= n (count op))
    (fail :db.error/invalid-tx-data (str "Wrong number of elements in " (pr-str op)) {:op op})))

(defmethod expand-op :db/add [_ _ctx op]
  (check-arity op 4)
  [[:add (nth op 1) (nth op 2) (nth op 3)]])

(defmethod expand-op :db/retract [_ _ctx op]
  (case (count op)
    4 [[:retract (nth op 1) (nth op 2) (nth op 3)]]
    3 [[:retract-attr (nth op 1) (nth op 2)]]
    (check-arity op 4)))

(declare expand-map!)

(defn- reverse-attr-name?
  [k]
  (and (keyword? k) (str/starts-with? (name k) "_")))

(defn- forward-entry!
  [ctx ^ArrayList out e k v]
  (let [^Attr attr (resolve-attr (:schema ctx) k)
        many? (= :db.cardinality/many (:cardinality attr))
        vs (if (and many? (or (sequential? v) (set? v))) v [v])]
    (doseq [x vs]
      (cond
        (map? x)
        (do (when-not (= :db.type/ref (:value-type attr))
              (fail :db.error/wrong-type-for-attribute
                    (str "Value " x " is not a valid " (type-label (:value-type attr))
                         " for attribute " (:ident attr))
                    {:value x}))
            (let [child (if-some [id (get x :db/id)] id (anon-tempid))]
              (.add out [:add e (:id attr) child])
              (expand-map! ctx out child x)))
        (nil? x) (fail :db.error/nil-value "Nil is not a legal value")
        :else (.add out [:add e (:id attr) x])))))

(defn- reverse-entry!
  [ctx ^ArrayList out e k v]
  (let [fwd (keyword (namespace k) (subs (name k) 1))
        ^Attr attr (resolve-attr (:schema ctx) fwd)]
    (when-not (= :db.type/ref (:value-type attr))
      (fail :db.error/wrong-type-for-attribute
            (str "Reverse attribute " k " is not a ref attribute") {:attribute fwd}))
    (doseq [x (if (or (set? v) (and (sequential? v) (not (vector? v)))) v [v])]
      (when (nil? x) (fail :db.error/nil-value "Nil is not a legal value"))
      (.add out [:add x (:id attr) e]))))

(defn- expand-map!
  [ctx ^ArrayList out e m]
  (doseq [[k v] (seq m)]
    (when-not (= k :db/id)
      (cond
        (not (keyword? k)) (fail :db.error/not-a-keyword
                                 (str "Cannot interpret as a keyword: " k ", no leading :") {:value k})
        (reverse-attr-name? k) (reverse-entry! ctx out e k v)
        :else (forward-entry! ctx out e k v)))))

(defn expand-forms
  "Expand tx-data into a vector of raw ops (see `expand-op`). Maps become :add ops with
  nested maps expanded inline (depth first, parent op before the child's ops)."
  [ctx tx-data]
  (let [out (ArrayList.)]
    (doseq [form tx-data]
      (cond
        (map? form) (expand-map! ctx out (if-some [id (get form :db/id)] id (anon-tempid)) form)
        (sequential? form) (doseq [op (expand-op (first form) ctx form)] (.add out op))
        :else (fail :db.error/invalid-tx-data (str "Unexpected tx-data element: " (pr-str form))
                    {:form form})))
    (vec out)))

;; ------------------------------------------------------------------ resolution of ops

(defn- resolve-op
  "Raw op -> [kind e Attr v] with e a long or Tmp, v a normalized value (long or Tmp for refs)."
  [ctx [kind e a v]]
  (let [attr (resolve-attr (:schema ctx) a)
        e' (resolve-ref ctx e true)]
    (if (= kind :retract-attr)
      [kind e' attr nil]
      [kind e' attr (resolve-value ctx attr v)])))

;; ------------------------------------------------------------------ tempid resolution

(defn- find-root
  "Follow union/binding links: a Long (bound) or the root Tmp."
  [ptr x]
  (loop [x x]
    (if (tmp? x)
      (let [p (get ptr x)]
        (if (nil? p) x (recur p)))
      x)))

(defn- tx-tmp? [^Tmp t]
  (let [k (.-k t)]
    (or (= "datomic.tx" k)
        (and (instance? TempId k)
             (let [p (:part k)] (or (= p :db.part/tx) (= p part-tx)))))))

(defn- partition-id
  "Partition entity id for a tempid's `part`, validating that it is an installed partition."
  [ctx part]
  (let [pid (cond (integer? part) (long part)
                  (keyword? part) (get (:idents (:schema ctx)) part)
                  :else nil)]
    (if (and pid (seq (datoms-of (:db ctx) :eavt [part-db a-install-partition pid])))
      pid
      (fail :db.error/not-a-db-id (str "Invalid db/id: " (pr-str part)) {:part part}))))

(defn- resolve-tempids
  "Binds every tempid: upserts via :db.unique/identity to a fixpoint, unifies tempids with
  equal new identity values, then allocates ids in order of first appearance in entity
  position. Returns {:ops resolved-ops :tempids map :n-shared :n-db :db-eids}."
  [ctx ops]
  (let [{:keys [db tx t meta]} ctx
        ptr (volatile! {})
        tmps (let [seen (LinkedHashMap.)]
               (doseq [[_ e _ v] ops, x [e v] :when (tmp? x)]
                 (.putIfAbsent seen x true))
               (vec (keys seen)))
        ;; tempids reported to the caller: those used in entity position
        e-tmps (let [seen (LinkedHashMap.)]
                 (doseq [[_ e] ops :when (tmp? e)]
                   (.putIfAbsent seen e true))
                 (vec (keys seen)))
        _ (doseq [x tmps :when (tx-tmp? x)] (vswap! ptr assoc x tx))
        lookup-cache (java.util.HashMap.)
        cached-lookup (fn [aid v]
                        (let [k [aid (vkey v)]]
                          (if (.containsKey lookup-cache k)
                            (.get lookup-cache k)
                            (let [r (lookup-eid db aid v)] (.put lookup-cache k r) r))))
        identity-ops (filterv (fn [[kind e ^Attr attr]]
                                (and (= kind :add) (tmp? e) (= :db.unique/identity (:unique attr))))
                              ops)
        conflict! (fn [e1 attr v e2]
                    (fail :db.error/datoms-conflict
                          (str "Two datoms in the same transaction conflict\n"
                               (pr-str {:d1 [e1 (:ident attr) v tx true] :d2 [e2 (:ident attr) v tx true]}))
                          {:attribute (:ident attr)}))]
    ;; upsert fixpoint
    (loop []
      (let [changed? (volatile! false)
            pending (java.util.HashMap.)]
        (doseq [[_ e ^Attr attr v] identity-ops]
          (let [re (find-root @ptr e)
                rv v]
            (when-not (tmp? rv)
              (let [existing (cached-lookup (:id attr) rv)]
                (cond
                  existing (cond (tmp? re) (do (vswap! ptr assoc re existing) (vreset! changed? true))
                                 (not= re existing) (conflict! re attr rv existing))
                  (tmp? re) (let [k [(:id attr) (vkey rv)]
                                  other (some->> (.get pending k) (find-root @ptr))]
                              (cond
                                (nil? other) (.put pending k re)
                                (not= other re) (do (vswap! ptr assoc re other) (vreset! changed? true))))))))
          nil)
        (when @changed? (recur))))
    ;; allocation
    (let [members (let [m (LinkedHashMap.)]
                    (doseq [x tmps :let [r (find-root @ptr x)] :when (tmp? r)]
                      (.put m r (conj (or (.get m r) []) x)))
                    m)
          schema-roots (into #{}
                             (keep (fn [[kind e ^Attr attr v]]
                                     (when (= kind :add)
                                       (let [x (cond (= a-value-type (:id attr)) e
                                                     (contains? install-attrs (:id attr)) v)
                                             r (when x (find-root @ptr x))]
                                         (when (tmp? r) r)))))
                             ops)
          n-shared (volatile! (inc (long t)))
          n-db (volatile! (long (:next-db-n meta)))
          db-eids (volatile! #{})
          allocate! (fn [root]
                      (let [ms (get members root [root])
                            explicit (some (fn [^Tmp m] (when (instance? TempId (.-k m)) (:part (.-k m)))) ms)
                            pid (cond explicit (partition-id ctx explicit)
                                      (contains? schema-roots root) part-db
                                      :else part-user)
                            n (if (== pid part-db)
                                (let [n @n-db] (vreset! n-db (inc n)) n)
                                (let [n @n-shared] (vreset! n-shared (inc n)) n))
                            eid (codec/eid pid n)]
                        (when (== pid part-db) (vswap! db-eids conj eid))
                        (vswap! ptr assoc root eid)))]
      (doseq [[_ e] ops :when (tmp? e)]
        (let [r (find-root @ptr e)]
          (when (tmp? r) (allocate! r))))
      (let [resolved (mapv (fn [[kind e attr v]]
                             [kind (find-root @ptr e) attr (if (tmp? v) (find-root @ptr v) v)])
                           ops)]
        (doseq [[_ _ _ v] resolved :when (tmp? v)]
          (let [k (.-k ^Tmp v)]
            (fail :db.error/tempid-not-an-entity
                  (str "tempid '" (if (instance? TempId k) (pr-str k) k) "' used only as value in transaction")
                  {:tempid k})))
        {:ops resolved
         :tempids (persistent!
                   (reduce (fn [m ^Tmp x]
                             (let [k (.-k x)
                                   key (if (instance? TempId k)
                                         (tempid-key (partition-id ctx (:part k)) (:idx k))
                                         k)]
                               (assoc! m key (find-root @ptr x))))
                           (transient {}) e-tmps))
         :n-shared @n-shared
         :n-db @n-db
         :db-eids @db-eids}))))

;; ------------------------------------------------------------------ datoms

(defn- datom-vec [schema ^Datom d]
  [(.-e d) (or (get (:ids schema) (.-a d)) (.-a d)) (.-v d) (.-tx d) (.-added d)])

(defn- instant-ms ^long [^Date d] (.getTime d))

(defn- last-instant
  "Millis of the latest tx instant in `db` (0 when none)."
  ^long [db]
  (let [basis (index/-basis-t db)
        d (first (datoms-of db :eavt [(codec/t->tx basis) a-tx-instant]))]
    (if d (instant-ms ^Date (:v d)) 0)))

(defn- ref-attr? [schema aid]
  (= :db.type/ref (:value-type (get (:attrs schema) aid))))

(defn- put-ops [schema ^Datom d]
  (let [k (fn [idx] (codec/encode-key idx (.-e d) (.-a d) (.-v d) (.-tx d)))
        empty-val (byte-array 0)]
    (cond-> [[:put :eavt (k :eavt) empty-val]
             [:put :aevt (k :aevt) empty-val]
             [:put :avet (k :avet) empty-val]]
      (ref-attr? schema (.-a d)) (conj [:put :vaet (k :vaet) empty-val]))))

(def ^:private history-cf {:eavt :eavt-h :aevt :aevt-h :avet :avet-h :vaet :vaet-h})

(defn- retract-ops
  "Move the current datom `old` to history (unless noHistory), recording `retract-tx`."
  [schema ^Datom old retract-tx no-history?]
  (let [k (fn [idx] (codec/encode-key idx (.-e old) (.-a old) (.-v old) (.-tx old)))
        idxs (cond-> [:eavt :aevt :avet] (ref-attr? schema (.-a old)) (conj :vaet))
        rtx (codec/long->bytes retract-tx)]
    (into (mapv (fn [idx] [:delete idx (k idx)]) idxs)
          (when-not no-history?
            (map (fn [idx] [:put (history-cf idx) (k idx) rtx]) idxs)))))

(defn- compute-datoms
  "Resolved ops -> {:asserts [Datom] :retracts [[Datom retracted-old-datom]]}."
  [ctx ops db-eids]
  (let [{:keys [db schema tx]} ctx
        adds (LinkedHashMap.)
        rets (LinkedHashMap.)
        ->d (fn [e ^Attr attr v added] (datom/datom e (:id attr) v tx added))
        conflict! (fn [d1 d2]
                    (fail :db.error/datoms-conflict
                          (str "Two datoms in the same transaction conflict\n"
                               (pr-str {:d1 (datom-vec schema d1) :d2 (datom-vec schema d2)}))
                          {:d1 (datom-vec schema d1) :d2 (datom-vec schema d2)}))
        existing-of (fn [e aid] (datoms-of db :eavt [e aid]))
        ;; expand :retract-attr against db-before
        ops (into []
                  (mapcat (fn [[kind e ^Attr attr v :as op]]
                            (if (= kind :retract-attr)
                              (map (fn [^Datom d] [:retract e attr (.-v d)]) (existing-of e (:id attr)))
                              [op])))
                  ops)]
    ;; txInstant placement rules
    (doseq [[_ e ^Attr attr] ops
            :when (and (== (:id attr) a-tx-instant) (== (codec/part-of e) part-tx) (not= e tx))]
      (fail :db.error/reset-tx-instant "You can set :db/txInstant only on the current transaction."
            {:entity e}))
    (doseq [[kind e ^Attr attr v] ops]
      (let [k [e (:id attr) (vkey v)]]
        (case kind
          :add (when-not (.containsKey adds k) (.put adds k (->d e attr v true)))
          :retract (when-not (.containsKey rets k) (.put rets k (->d e attr v false))))))
    (doseq [k (keys adds)]
      (when (.containsKey rets k)
        (conflict! (.get adds k) (.get rets k))))
    ;; card-one: two different asserted values
    (let [seen (java.util.HashMap.)]
      (doseq [^Datom d (vals adds)]
        (let [^Attr attr (get (:attrs schema) (.-a d))]
          (when (= :db.cardinality/one (:cardinality attr))
            (let [k [(.-e d) (.-a d)]
                  prev (.get seen k)]
              (when (and prev (not (same-value? (.-v ^Datom prev) (.-v d))))
                (conflict! prev d))
              (.put seen k d))))))
    ;; compute the effective changes against db-before
    (let [retracted (LinkedHashMap.)   ; key -> [retract-datom old-datom]
          asserted (ArrayList.)
          retract! (fn [^Datom old]
                     (let [k [(.-e old) (.-a old) (vkey (.-v old))]]
                       (when-not (.containsKey retracted k)
                         (.put retracted k [(datom/datom (.-e old) (.-a old) (.-v old) tx false) old]))))]
      (doseq [^Datom d (vals adds)]
        (let [^Attr attr (get (:attrs schema) (.-a d))
              cur (existing-of (.-e d) (.-a d))]
          (when-not (some (fn [^Datom c] (same-value? (.-v c) (.-v d))) cur)
            (when (= :db.cardinality/one (:cardinality attr))
              (doseq [c cur] (retract! c)))
            (.add asserted d))))
      (doseq [^Datom r (vals rets)]
        (let [cur (existing-of (.-e r) (.-a r))]
          (when-let [old (first (filter (fn [^Datom c] (same-value? (.-v c) (.-v r))) cur))]
            (retract! old))))
      {:asserts (vec asserted)
       :retracts (vec (vals retracted))
       :retracted-keys (set (keys retracted))})))

(defn- check-uniqueness!
  "Unique attributes: no other entity may hold an asserted value, in db-before or in this tx."
  [ctx asserts retracted-keys]
  (let [{:keys [db schema]} ctx
        in-tx (java.util.HashMap.)]
    (doseq [^Datom d asserts
            :let [^Attr attr (get (:attrs schema) (.-a d))]
            :when (:unique attr)]
      (let [k [(.-a d) (vkey (.-v d))]
            prev ^Datom (.get in-tx k)]
        (when (and prev (not= (.-e prev) (.-e d)))
          (fail :db.error/datoms-conflict
                (str "Two datoms in the same transaction conflict\n"
                     (pr-str {:d1 (datom-vec schema prev) :d2 (datom-vec schema d)}))
                {:d1 (datom-vec schema prev) :d2 (datom-vec schema d)}))
        (.put in-tx k d)
        (doseq [^Datom h (datoms-of db :avet [(.-a d) (.-v d)])]
          (when (and (not= (.-e h) (.-e d))
                     (not (contains? retracted-keys [(.-e h) (.-a h) (vkey (.-v h))])))
            (fail :db.error/unique-conflict
                  (str "Unique conflict: " (:ident attr) ", value: " (.-v d) " already held by: "
                       (.-e h) " asserted for: " (.-e d))
                  :cognitect.anomalies/conflict
                  {:attribute (:ident attr) :value (.-v d) :held-by (.-e h) :asserted-for (.-e d)})))))))

(defn- schema-installs
  "Auto-install datoms for new attribute entities, plus validation of all installs."
  [ctx asserts]
  (let [{:keys [db tx]} ctx
        explicit-installs (into #{} (comp (filter #(== a-install-attribute (.-a ^Datom %)))
                                          (map #(.-v ^Datom %)))
                                asserts)
        new-attr-es (distinct (keep (fn [^Datom d] (when (== (.-a d) a-value-type) (.-e d))) asserts))
        auto (vec (for [e new-attr-es
                        :when (and (not (contains? explicit-installs e))
                                   (empty? (datoms-of db :eavt [part-db a-install-attribute e])))]
                    (datom/datom part-db a-install-attribute e tx true)))
        installs (into explicit-installs (map #(.-v ^Datom %)) auto)
        has-attr? (fn [e aid]
                    (or (some (fn [^Datom d] (and (== (.-e d) e) (== (.-a d) aid))) asserts)
                        (seq (datoms-of db :eavt [e aid]))))]
    (doseq [e installs]
      (when-not (and (has-attr? e a-value-type) (has-attr? e a-cardinality))
        (fail :db.error/invalid-install-attribute
              "First error: :db.error/schema-attribute-missing" {:entity e})))
    auto))

(defn- check-db-partition!
  "New entities in :db.part/db must be schema components."
  [asserts db-eids]
  (doseq [e db-eids]
    (when-not (some (fn [^Datom d] (and (== (.-e d) e) (< (.-a d) first-user-attr))) asserts)
      (fail :db.error/schema-without-install
            (str "Only schema components can be installed in partition :db.part/db\n"
                 (pr-str {:datoms (filterv #(== (.-e ^Datom %) e) asserts)}))
            {:entity e}))))

(defn- check-no-valuetype-change!
  [ctx retracts]
  (doseq [[_ ^Datom old] retracts]
    (when (== (.-a old) a-value-type)
      (fail :db.error/unsupported "Changing :db/valueType of an attribute is not supported"
            {:entity (.-e old)}))))

;; ------------------------------------------------------------------ prepare

(defn prepare
  "Prepare a transaction against `db-before` with store `meta` ({:next-t :next-db-n ...}).
  Returns {:tx-data [Datom] :tempids map :ops [rocks-ops] :meta meta' :tx tx-id}.
  Throws ex-info (with :db/error) on invalid tx-data; never writes."
  [db-before meta tx-data]
  (let [schema (index/-schema db-before)
        t (long (:next-t meta))
        tx (codec/t->tx t)
        ctx {:db db-before :schema schema :meta meta :t t :tx tx}
        raw (expand-forms ctx tx-data)
        ops (mapv #(resolve-op ctx %) raw)
        {:keys [ops tempids n-shared n-db db-eids]} (resolve-tempids ctx ops)
        {:keys [asserts retracts retracted-keys]} (compute-datoms ctx ops db-eids)
        _ (check-no-valuetype-change! ctx retracts)
        _ (check-uniqueness! ctx asserts retracted-keys)
        _ (check-db-partition! asserts db-eids)
        auto (schema-installs ctx asserts)
        ;; the tx's own instant
        now (System/currentTimeMillis)
        last-ms (last-instant db-before)
        supplied (first (filter (fn [^Datom d] (and (== (.-a d) a-tx-instant) (== (.-e d) tx))) asserts))
        _ (when supplied
            (let [ms (instant-ms ^Date (.-v ^Datom supplied))]
              (when (< ms last-ms)
                (fail :db.error/past-tx-instant
                      (str "Time conflict: " (.-v ^Datom supplied) " is older than database basis")
                      {:instant (.-v ^Datom supplied)}))
              (when (> ms now)
                (fail :db.error/future-tx-instant
                      (str "Time conflict: " (.-v ^Datom supplied) " is in the future")
                      {:instant (.-v ^Datom supplied)}))))
        instant-datom (when-not supplied
                        (datom/datom tx a-tx-instant (Date. (max now last-ms)) tx true))
        asserted (-> []
                     (cond-> instant-datom (conj instant-datom))
                     (into asserts)
                     (into auto))
        retract-datoms (mapv first retracts)
        tx-data (-> [] (into (when instant-datom [instant-datom]))
                    (into (remove #(identical? % instant-datom) asserted))
                    (into retract-datoms))
        no-history? (fn [aid] (boolean (:no-history? (get (:attrs schema) aid))))
        ops (-> []
                (into (mapcat #(put-ops schema %)) asserted)
                (into (mapcat (fn [[_ ^Datom old]] (retract-ops schema old tx (no-history? (.-a old))))) retracts)
                (conj (bootstrap/log-op tx tx-data))
                (conj (bootstrap/meta-op (assoc meta :next-t n-shared :next-db-n n-db))))]
    {:tx-data tx-data
     :tempids tempids
     :ops ops
     :meta (assoc meta :next-t n-shared :next-db-n n-db)
     :tx tx}))
