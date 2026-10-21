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

(defn- anon-tempid
  "Tempid of an entity map without :db/id; its nil partition lets schema maps land in :db.part/db."
  [] (->TempId nil (.decrementAndGet tempid-counter)))

;; Internal wrapper distinguishing tempids from plain values after resolution. Equality
;; is by `k`; the hash is computed once (tempids are hashed many times during resolution).
(deftype Tmp [k ^int h]
  Object
  (hashCode [_] h)
  (equals [_ o] (and (instance? Tmp o) (= k (.-k ^Tmp o)))))

(defn- ->Tmp [k] (Tmp. k (unchecked-int (hash k))))

(defn- tmp? [x] (instance? Tmp x))

(defn tempid-key
  "The key under which `tempid` appears in the :tempids map, given its partition id."
  ^long [^long part-id ^long idx]
  (+ Long/MIN_VALUE (bit-shift-left (inc part-id) 42) idx))

;; ------------------------------------------------------------------ constants

(def ^:private ^:const a-ident 10)
(def ^:private ^:const a-alter-attribute 19)
(def ^:private ^:const a-ensure 68)
(def ^:private ^:const a-attr-preds 71)
(def ^:private ^:const a-tuple-type 65)
(def ^:private ^:const a-tuple-types 66)
(def ^:private ^:const a-tuple-attrs 67)
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

(defn datoms-of
  "Vector of datoms of `idx` whose key starts with `comps`, read through `db`."
  [db idx comps]
  (into [] (index/datoms-iterable db idx (codec/encode-prefix idx comps))))

(defn vkey
  "Equality key for a value, consistent with the storage encoding."
  [v]
  (ByteBuffer/wrap (codec/value->bytes v)))

(defn same-value? [a b]
  (Arrays/equals ^bytes (codec/value->bytes a) ^bytes (codec/value->bytes b)))

(defn- lookup-eid
  "Entity holding value `v` of attribute `aid` in `db` (via AVET), or nil."
  [db aid v]
  (some-> (first (datoms-of db :avet [aid v])) :e))

;; ------------------------------------------------------------------ encoded-value keys

(def ^:private ^bytes no-bytes (byte-array 0))

;; Hash key of (x, y, encoded value bytes): [e a v] datom identity, [a v] unique-value
;; identity (y = 0). Values are compared by their storage encoding, as in the indexes.
(deftype K3 [^long x ^long y ^bytes b ^int h]
  Object
  (hashCode [_] h)
  (equals [_ o]
    (and (instance? K3 o)
         (let [^K3 o o]
           (and (== x (.-x o)) (== y (.-y o)) (Arrays/equals b ^bytes (.-b o)))))))

(defn- k3 [^long x ^long y ^bytes b]
  (K3. x y b (unchecked-int (+ (* 31 (+ (* 31 (Long/hashCode x)) (Long/hashCode y)))
                               (Arrays/hashCode b)))))

(defn- ekey
  "Key of datom `d` in :eavt, :aevt or :avet given the value's encoding `vb`."
  ^bytes [idx ^Datom d ^bytes vb]
  (codec/encode-key-vb idx (.-e d) (.-a d) vb (.-tx d)))

(defn- vaet-key
  "VAET key of ref datom `d` (the value is an entity id, stored as a plain long)."
  ^bytes [^Datom d]
  (codec/encode-vaet-key (long (.-v d)) (.-a d) (.-e d) (.-tx d)))

(defn- prefix-ea+v
  "EAVT key prefix [e a v]; `vb` is the value encoding."
  ^bytes [^long e ^long a ^bytes vb]
  (codec/encode-prefix-eav-vb e a vb))

(defn- prefix-av
  "AVET key prefix [a v]."
  ^bytes [^long a ^bytes vb]
  (codec/encode-prefix-av-vb a vb))

(defn- datoms-of-prefix [db idx ^bytes prefix]
  (into [] (index/datoms-iterable db idx prefix)))

(defn- avet-holders
  "Current datoms holding value `vb` (encoded) of attribute `aid` in the tx's db-before, cached
  for the transaction (shared by upsert resolution and the uniqueness check)."
  [ctx ^long aid ^bytes vb]
  (let [^java.util.HashMap cache (:lookup-cache ctx)
        k (k3 aid 0 vb)]
    (or (.get cache k)
        (let [r (datoms-of-prefix (:db ctx) :avet (prefix-av aid vb))]
          (.put cache k r)
          r))))

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

(defn resolve-callable
  "The fn var named by `sym`: qualified symbols load their namespace on demand, unqualified
  symbols resolve in clojure.core. Nil when the var does not exist; a var that is not
  a function is :db.error/not-a-data-function."
  [sym]
  (when-some [v (if (qualified-symbol? sym)
                  (requiring-resolve sym)
                  (ns-resolve (the-ns 'clojure.core) sym))]
    (if (fn? @v)
      v
      (fail :db.error/not-a-data-function (str "Not a function: " sym) {:function sym}))))

(def ^:private scalar-types
  #{:db.type/ref :db.type/keyword :db.type/long :db.type/string :db.type/boolean
    :db.type/instant :db.type/double :db.type/float :db.type/bigint :db.type/bigdec
    :db.type/uuid :db.type/uri :db.type/bytes :db.type/symbol})

(declare coerce-value)

(defn- coerce-tuple
  "Validates a tuple value. Composite (:db/tupleAttrs) tuples are maintained by the
  transactor, so only their shape is checked; the others are checked element by element
  (nil elements are allowed)."
  [^Attr attr v]
  (let [invalid (fn [] (fail :db.error/invalid-tuple-value "Invalid tuple value"
                             {:value v :attribute (:ident attr)}))
        n (when (vector? v) (count v))
        elem (fn [vt x]
               (cond
                 (nil? x) nil
                 (= vt :db.type/ref) (if (integer? x) (long x) (invalid))
                 :else (try (coerce-value (assoc attr :value-type vt :tuple-attrs nil :tuple-type nil
                                                 :tuple-types nil)
                                          x)
                            (catch clojure.lang.ExceptionInfo _ (invalid)))))]
    (when-not n (invalid))
    (cond
      (:tuple-attrs attr) (if (== n (count (:tuple-attrs attr))) v (invalid))
      (:tuple-types attr) (if (== n (count (:tuple-types attr)))
                            (mapv elem (:tuple-types attr) v)
                            (invalid))
      (:tuple-type attr) (if (<= (if (contains? #{66 67} (:id attr)) 2 1) n 8)
                           (mapv #(elem (:tuple-type attr) %) v)
                           (invalid))
      :else (invalid))))

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
      :db.type/tuple (coerce-tuple attr v)
      :db.type/fn (or ((requiring-resolve 'mnemonic.tx-fns/->stored) v) (bad))
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
  "Rejects ids Datomic rejects: negative ids, user-partition ids not yet allocated, and
  :db.part/db / :db.part/tx ids beyond Datomic's allocation window (probed: tx ids above
  max(999, current tx n); db ids above last allocated + 927)."
  [ctx ^long eid]
  (let [part (codec/part-of eid)
        n (bit-and eid (dec (bit-shift-left 1 42)))
        meta (:meta ctx)]
    (when (or (neg? eid)
              (cond (== part part-db) (>= n (+ (long (:next-db-n meta)) 927))
                    (== part part-tx) (and (> n (long (:next-t meta))) (>= n 1000))
                    :else (>= n (long (:next-t meta)))))
      (fail :db.error/invalid-entity-id (str "Invalid entity id: " eid) {:entity eid}))
    eid))

(defn resolve-ref
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

;; :db/cas, :db/retractEntity and transaction function calls are implemented in
;; mnemonic.tx-fns, which requires this namespace; they are reached through a var resolved
;; at call time so neither namespace needs the other at load time.
(defn- expand-via-fns [head ctx op]
  ((requiring-resolve 'mnemonic.tx-fns/expand-call) head ctx op))

(defmethod expand-op :default [head ctx op] (expand-via-fns head ctx op))
(defmethod expand-op :db/cas [head ctx op] (expand-via-fns head ctx op))
(defmethod expand-op :db.fn/cas [head ctx op] (expand-via-fns head ctx op))
(defmethod expand-op :db/retractEntity [head ctx op] (expand-via-fns head ctx op))
(defmethod expand-op :db.fn/retractEntity [head ctx op] (expand-via-fns head ctx op))

(defn- check-arity [op n]
  (when-not (= n (count op))
    (fail :db.error/invalid-tx-data (str "Wrong number of elements in " (pr-str op)) {:op op})))

(defmethod expand-op :db/add [_ _ctx op]
  (check-arity op 4)
  [[:add (nth op 1) (nth op 2) (nth op 3)]])

(defmethod expand-op :db/retract [_ ctx op]
  (case (count op)
    4 [[:retract (nth op 1) (nth op 2) (nth op 3)]]
    3 (let [e (nth op 1)
            _ (resolve-attr (:schema ctx) (nth op 2))]
        ;; Datomic resolves the entity with entid here, so a tempid string is not a keyword
        (when (and (string? e) (not (str/starts-with? e ":")))
          (fail :db.error/not-a-keyword (str "Cannot interpret as a keyword: " e ", no leading :")
                {:value e}))
        [[:retract-attr e (nth op 2)]])
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
        (and (map? x) (not (record? x)))
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

(declare resolve-op)

(defn- expand-forms*
  "As `expand-forms`; when `resolved` (an ArrayList) is given, the resolved form of each op
  is appended to it, so the caller need not resolve them again."
  [ctx tx-data ^ArrayList resolved]
  (let [out (ArrayList.)]
    (doseq [form tx-data]
      (let [from (.size out)]
        (cond
          (map? form) (expand-map! ctx out (if-some [id (get form :db/id)] id (anon-tempid)) form)
          (sequential? form) (doseq [op (expand-op (first form) ctx form)] (.add out op))
          :else (fail :db.error/invalid-tx-data (str "Unexpected tx-data element: " (pr-str form))
                      {:form form}))
        ;; Datomic validates each op (attribute, value type, lookup refs) at the point it is
        ;; expanded, so the first failing op wins over errors raised by later ops.
        (dotimes [i (- (.size out) from)]
          (let [r (resolve-op ctx (.get out (+ from i)))]
            (when resolved (.add resolved r))))))
    (vec out)))

(defn expand-forms
  "Expand tx-data into a vector of raw ops (see `expand-op`). Maps become :add ops with
  nested maps expanded inline (depth first, parent op before the child's ops)."
  [ctx tx-data]
  (expand-forms* ctx tx-data nil))

;; ------------------------------------------------------------------ resolution of ops

(defn- resolve-op
  "Raw op -> [kind e Attr v] with e a long or Tmp, v a normalized value (long or Tmp for refs)."
  [ctx [kind e a v]]
  (let [attr (resolve-attr (:schema ctx) a)
        _ (when (and (not= kind :retract-attr) (nil? v))
            (fail :db.error/nil-value "Nil is not a legal value"))
        ;; Datomic does not range-check the entity id of a value-less retract
        e' (resolve-ref ctx e (not= kind :retract-attr))]
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
  (let [part (if (nil? part) :db.part/user part)
        cache (:part-cache ctx)]
    (or (some-> cache deref (get part))
        (let [pid (cond (integer? part) (long part)
                        (keyword? part) (get (:idents (:schema ctx)) part)
                        :else nil)]
          (if (and pid (seq (datoms-of (:db ctx) :eavt [part-db a-install-partition pid])))
            (do (when cache (swap! cache assoc part pid))
                pid)
            (fail :db.error/not-a-db-id (str "Invalid db/id: " (pr-str part)) {:part part}))))))

(defn- resolve-tempids
  "Binds every tempid: upserts via :db.unique/identity to a fixpoint, unifies tempids with
  equal new identity values, then allocates ids in order of first appearance in entity
  position. Returns {:ops resolved-ops :tempids map :n-shared :n-db :db-eids}."
  [ctx ops]
  (let [{:keys [db tx t meta]} ctx
        ptr (volatile! {})
        ;; all tempids, and (e-tmps) those used in entity position, which are the ones
        ;; reported to the caller; both in order of first appearance
        seen-all (LinkedHashMap.)
        seen-e (LinkedHashMap.)
        _ (doseq [op ops]
            (let [e (nth op 1) v (nth op 3)]
              (when (tmp? e)
                (.putIfAbsent seen-all e true)
                (.putIfAbsent seen-e e true))
              (when (tmp? v) (.putIfAbsent seen-all v true))))
        tmps (vec (keys seen-all))
        e-tmps (vec (keys seen-e))
        _ (doseq [x tmps :when (tx-tmp? x)] (vswap! ptr assoc x tx))
        ;; a tempid that never appears in entity position can never be bound
        _ (let [e-set (set e-tmps)]
            (doseq [^Tmp x tmps :when (not (contains? e-set x))
                    :let [k (.-k x)]
                    :when (not (tx-tmp? x))]
              (fail :db.error/tempid-not-an-entity
                    (str "tempid '" (if (instance? TempId k) (pr-str k) k) "' used only as value in transaction")
                    {:tempid k})))
        id-vb (java.util.IdentityHashMap.)   ; identity op -> encoded value, reused downstream
        identity-ops (filterv (fn [[kind e ^Attr attr]]
                                (and (or (= kind :add) (= kind :retract)) (tmp? e) (= :db.unique/identity (:unique attr))))
                              ops)
        conflict! (fn [e1 attr v e2]
                    (fail :db.error/datoms-conflict
                          (str "Two datoms in the same transaction conflict\n"
                               (pr-str {:d1 [e1 (:ident attr) v tx true] :d2 [e2 (:ident attr) v tx true]}))
                          {:attribute (:ident attr)}))]
    ;; Upsert, op by op in transaction order (this mirrors Datomic's observable behavior):
    ;; a tempid asserting an identity value held by an existing entity binds to it; a tempid
    ;; using a new value another tempid used earlier joins that tempid (asserts with asserts,
    ;; retractions with retractions; an assert and a retraction of one new value for
    ;; different tempids conflict). A tempid that already absorbed others cannot be bound to
    ;; an existing entity or joined to another group (datoms-conflict). A tempid whose
    ;; values have different holders is bound to the last one; the uniqueness check then
    ;; reports :db.error/unique-conflict.
    (let [sizes (java.util.HashMap.)   ; root tempid -> number of tempids joined into it
          group-size (fn [r] (long (get sizes r 1)))
          join! (fn [r p]              ; tempid root `r` joins `p` (a tempid root or an entity id)
                  (vswap! ptr assoc r p)
                  (when (tmp? p) (.put sizes p (+ (group-size p) (group-size r)))))
          pending (java.util.HashMap.)
          ;; assert/retract clashes between two still-unbound tempids; Datomic only reports
          ;; them if neither tempid is bound to an existing entity by the end
          deferred (java.util.ArrayList.)]
      (doseq [op identity-ops]
        (let [[kind e ^Attr attr v] op
              r (find-root @ptr e)
              vb (when-not (tmp? v) (codec/value->bytes v))
              _ (when vb (.put id-vb op vb))
              ;; a tempid used as the (ref) identity value matches only the same tempid
              ex (when vb (some-> ^Datom (first (avet-holders ctx (:id attr) vb)) .-e))
              k (if vb (k3 (:id attr) 0 vb) [(:id attr) [::tmp (find-root @ptr v)]])]
          (cond
            ex (let [t (loop [t e] (let [n (get @ptr t)] (if (tmp? n) (recur n) t)))]
                 (if (tmp? r)
                   (if (> (group-size r) 1)
                     (conflict! r attr v ex)
                     (vswap! ptr assoc r ex))
                   ;; bound to another holder earlier: the latest holder wins and the
                   ;; uniqueness check reports the clash (:db.error/unique-conflict)
                   (vswap! ptr assoc t ex)))
            :else
            (let [[p pkind] (when-let [[p pk] (.get pending k)] [(find-root @ptr p) pk])]
              (cond
                (nil? p) (.put pending k [r kind])
                (and (= p r) (= pkind kind)) nil
                (not= pkind kind) (if (and (tmp? r) (tmp? p))
                                    (.add deferred [r attr v p])
                                    (conflict! r attr v p))
                (and (tmp? r) (== 1 (group-size r))) (join! r p)
                (and (tmp? p) (not (tmp? r)) (== 1 (group-size p))) (join! p r)
                :else (conflict! r attr v p))))))
      (doseq [[r attr v p] deferred
              :when (and (tmp? (find-root @ptr r)) (tmp? (find-root @ptr p)))]
        (conflict! r attr v p))
      nil)
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
                            explicit (some (fn [^Tmp m] (when (instance? TempId (.-k m)) (:part (.-k m)))) ms) ; nil part: anonymous
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
      (let [resolved (mapv (fn [op]
                             (let [[kind e attr v] op]
                               (if (tmp? v)
                                 [kind (find-root @ptr e) attr (find-root @ptr v)]
                                 [kind (find-root @ptr e) attr v (.get id-vb op)])))
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

;; Per-datom data computed once and shared by every later step: the value's storage encoding
;; and its attribute. Kept in an identity map beside the Datoms (see `compute-datoms`).
(deftype DInfo [^bytes vb ^Attr attr])

(defn- dinfo-vb
  "Encoded value of `d`, from the transaction's DInfo map or (for datoms made later) encoded now."
  ^bytes [^java.util.IdentityHashMap dinfo ^Datom d]
  (if-some [^DInfo i (.get dinfo d)]
    (.-vb i)
    (codec/value->bytes (.-v d))))

(def ^:private ^bytes empty-val (byte-array 0))

(defn- put-ops
  "Put ops for the current-index entries of asserted datom `d` (VAET only for refs), appended to `out`."
  [^ArrayList out schema ^java.util.IdentityHashMap dinfo ^Datom d]
  (let [vb (dinfo-vb dinfo d)]
    (.add out [:put :eavt (ekey :eavt d vb) empty-val])
    (.add out [:put :aevt (ekey :aevt d vb) empty-val])
    (.add out [:put :avet (ekey :avet d vb) empty-val])
    (when (ref-attr? schema (.-a d))
      (.add out [:put :vaet (vaet-key d) empty-val]))))

(defn- retract-ops
  "Move the current datom `old` to history (unless noHistory), recording `retract-tx`;
  appended to `out`. Each key is built once for both the delete and the history put."
  [^ArrayList out schema ^java.util.IdentityHashMap dinfo ^Datom old retract-tx no-history?]
  (let [vb (dinfo-vb dinfo old)
        ref? (ref-attr? schema (.-a old))
        ke (ekey :eavt old vb)
        ka (ekey :aevt old vb)
        kv (ekey :avet old vb)
        kr (when ref? (vaet-key old))
        rtx (codec/long->bytes retract-tx)]
    (.add out [:delete :eavt ke])
    (.add out [:delete :aevt ka])
    (.add out [:delete :avet kv])
    (when ref? (.add out [:delete :vaet kr]))
    (when-not no-history?
      (.add out [:put :eavt-h ke rtx])
      (.add out [:put :aevt-h ka rtx])
      (.add out [:put :avet-h kv rtx])
      (when ref? (.add out [:put :vaet-h kr rtx])))))

(defn- log-op
  "The :log put for `tx`; byte-identical to `bootstrap/log-op`, reusing the encoded values."
  [tx datoms ^java.util.IdentityHashMap dinfo]
  [:put :log (codec/long->bytes tx)
   (codec/encode-log-entry-vb datoms (mapv (fn [^Datom d] (dinfo-vb dinfo d)) datoms))])

(defn- fresh-entity?
  "True when `e` was allocated by this very tx (so db-before holds no datoms for it): the tx
  entity itself, a :db.part/db id in `db-eids`, or a user-partition id at or beyond the tx's
  t (check-eid rejects literal user ids that high). Explicit ids in :db.part/db or
  :db.part/tx are never assumed fresh."
  [ctx db-eids ^long e]
  (let [part (codec/part-of e)]
    (cond (== part part-tx) (== e (long (:tx ctx)))
          (== part part-db) (contains? db-eids e)
          :else (>= (bit-and e (dec (bit-shift-left 1 42))) (long (:t ctx))))))

(defn- compute-datoms
  "Resolved ops -> {:asserts [Datom] :adds [Datom] :retracts [[Datom retracted-old-datom]]
  :retracted-keys set :dinfo IdentityHashMap}."
  [ctx ops db-eids]
  (let [{:keys [db schema tx]} ctx
        attrs (:attrs schema)
        adds (LinkedHashMap.)
        rets (LinkedHashMap.)
        dinfo (java.util.IdentityHashMap.)
        ->d (fn [e ^Attr attr v added ^bytes vb]
              (let [d (datom/datom e (:id attr) v tx added)]
                (.put dinfo d (DInfo. vb attr))
                d))
        conflict! (fn [d1 d2]
                    (fail :db.error/datoms-conflict
                          (str "Two datoms in the same transaction conflict\n"
                               (pr-str {:d1 (datom-vec schema d1) :d2 (datom-vec schema d2)}))
                          {:d1 (datom-vec schema d1) :d2 (datom-vec schema d2)}))
        ;; entities allocated by this very tx have no datoms in db-before: skip the reads
        existing-of (fn [^long e aid] (if (fresh-entity? ctx db-eids e) [] (datoms-of db :eavt [e aid])))
        ;; the current datom [e a v] (exact key lookup; never scans the other values)
        existing-value (fn [^long e ^long aid ^bytes vb]
                         (when-not (fresh-entity? ctx db-eids e)
                           (first (datoms-of-prefix db :eavt (prefix-ea+v e aid vb)))))
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
    (doseq [op ops]
      (let [[kind e ^Attr attr v] op
            vb (or (nth op 4 nil) (codec/value->bytes v))
            k (k3 e (:id attr) vb)]
        (case kind
          :add (when-not (.containsKey adds k) (.put adds k (->d e attr v true vb)))
          :retract (when-not (.containsKey rets k) (.put rets k (->d e attr v false vb))))))
    (doseq [k (keys adds)]
      (when (.containsKey rets k)
        (conflict! (.get adds k) (.get rets k))))
    ;; card-one: two different asserted values, or two different retracted values
    (doseq [^LinkedHashMap m [adds rets]]
      (let [seen (java.util.HashMap.)]
        (doseq [^Datom d (vals m)]
          (let [^DInfo i (.get dinfo d)]
            (when (= :db.cardinality/one (:cardinality (.-attr i)))
              (let [k (k3 (.-e d) (.-a d) no-bytes)
                    ^Datom prev (.get seen k)]
                (when (and prev (not (Arrays/equals (dinfo-vb dinfo prev) ^bytes (.-vb i))))
                  (conflict! prev d))
                (.put seen k d)))))))
    ;; compute the effective changes against db-before
    (let [retracted (LinkedHashMap.)   ; key -> [retract-datom old-datom]
          asserted (ArrayList.)
          retract! (fn [^Datom old ^bytes vb]
                     (let [k (k3 (.-e old) (.-a old) vb)]
                       (when-not (.containsKey retracted k)
                         (let [r (datom/datom (.-e old) (.-a old) (.-v old) tx false)
                               attr (get attrs (.-a old))]
                           (.put dinfo old (DInfo. vb attr))
                           (.put dinfo r (DInfo. vb attr))
                           (.put retracted k [r old])))))]
      (doseq [^Datom d (vals adds)]
        (let [^DInfo i (.get dinfo d)
              ^Attr attr (.-attr i)
              ^bytes vb (.-vb i)]
          (if (= :db.cardinality/one (:cardinality attr))
            ;; card-one: [e a] holds at most one current value, which a new value replaces
            (let [cur (existing-of (.-e d) (.-a d))
                  curb (mapv (fn [^Datom c] (codec/value->bytes (.-v c))) cur)]
              (when-not (some (fn [^bytes b] (Arrays/equals b vb)) curb)
                (dotimes [j (count cur)] (retract! (nth cur j) (nth curb j)))
                (.add asserted d)))
            (when-not (existing-value (.-e d) (.-a d) vb)
              (.add asserted d)))))
      (doseq [^Datom r (vals rets)]
        (let [vb (dinfo-vb dinfo r)]
          (when-let [old (existing-value (.-e r) (.-a r) vb)]
            (retract! old vb))))
      {:asserts (vec asserted)
       :adds (vec (vals adds))
       :retracts (vec (vals retracted))
       :retracted-keys (.keySet retracted)
       :dinfo dinfo})))

(defn- check-uniqueness!
  "Unique attributes. First, no two entities may add the same value within the tx (counting
  redundant adds: datoms-conflict); then no other entity may already hold an asserted
  value in db-before (unique-conflict)."
  [ctx adds asserts retracted-keys ^java.util.IdentityHashMap dinfo]
  (let [schema (:schema ctx)
        in-tx (java.util.HashMap.)]
    (doseq [^Datom d adds
            :let [^DInfo i (.get dinfo d)]
            :when (:unique (.-attr i))]
      (let [k (k3 (.-a d) 0 (.-vb i))
            ^Datom prev (.get in-tx k)]
        (when (and prev (not= (.-e prev) (.-e d)))
          (fail :db.error/datoms-conflict
                (str "Two datoms in the same transaction conflict\n"
                     (pr-str {:d1 (datom-vec schema prev) :d2 (datom-vec schema d)}))
                {:d1 (datom-vec schema prev) :d2 (datom-vec schema d)}))
        (.put in-tx k d)))
    (doseq [^Datom d asserts
            :let [^DInfo i (.get dinfo d)
                  ^Attr attr (when i (.-attr i))]
            :when (:unique attr)]
      (let [^bytes vb (.-vb i)]
        (doseq [^Datom h (avet-holders ctx (.-a d) vb)]
          (when (and (not= (.-e h) (.-e d))
                     ;; a holder found by this value's prefix has this value's encoding
                     (not (contains? retracted-keys (k3 (.-e h) (.-a h) vb))))
            (fail :db.error/unique-conflict
                  (str "Unique conflict: " (:ident attr) ", value: " (.-v d) " already held by: "
                       (.-e h) " asserted for: " (.-e d))
                  :cognitect.anomalies/conflict
                  {:attribute (:ident attr) :value (.-v d) :held-by (.-e h) :asserted-for (.-e d)})))))))

(defn- check-tuple-install!
  "Validates the tuple specification (:db/tupleAttrs, :db/tupleType, :db/tupleTypes) of
  the attributes `es` installed by this transaction."
  [ctx asserts es]
  (when (seq es)
   (let [schema (:schema ctx)
        ids (:ids schema)
        by-e (group-by (fn [^Datom d] (.-e d)) asserts)
        prop (fn [e aid] (some (fn [^Datom d] (when (== (.-a d) aid) (.-v d))) (get by-e e)))
        tx-attrs (into {} (keep (fn [e]
                                  (when-some [ident (prop e a-ident)]
                                    [ident {:vt (get ids (prop e a-value-type))
                                            :card (get ids (prop e a-cardinality))}])))
                       (keys by-e))
        attr-info (fn [kw] (if-let [^Attr a (schema/attr schema kw)]
                             {:vt (:value-type a) :card (:cardinality a)}
                             (get tx-attrs kw)))]
    (doseq [e es]
      (let [vt (get ids (prop e a-value-type))
            ta (prop e a-tuple-attrs)
            tt (prop e a-tuple-type)
            tts (prop e a-tuple-types)
            bad! (fn [kind]
                   (fail :db.error/invalid-install-attribute (str "First error: " kind)
                         {:db/errors [{:db/error kind :entity e}]}))]
        (cond
          (not= vt :db.type/tuple) (when (or ta tt tts) (bad! :db.error/type-on-non-tuple))
          :else
          (do
            (when-not (== 1 (count (filter some? [ta tt tts]))) (bad! :db.error/invalid-tuple-kind))
            (when (and tt (not (contains? scalar-types tt))) (bad! :db.error/invalid-tuple-type))
            (when (and tts (not (every? #(contains? scalar-types %) tts))) (bad! :db.error/invalid-tuple-types))
            (when ta
              (when (= :db.cardinality/many (get ids (prop e a-cardinality)))
                (bad! :db.error/tuple-of-attrs-must-be-card-one))
              (doseq [kw ta]
                (let [info (attr-info kw)]
                  (when (or (nil? info) (= :db.cardinality/many (:card info)) (= :db.type/tuple (:vt info)))
                    (bad! :db.error/invalid-tuple-attrs))))))))))))

(defn- schema-installs
  "Auto-install datoms for new attribute entities (and `:db.alter/attribute` datoms for the
  altered attributes `alter-eids`), plus validation of all installs."
  [ctx asserts alter-eids]
  (let [{:keys [db tx]} ctx
        explicit-installs (into #{} (comp (filter #(== a-install-attribute (.-a ^Datom %)))
                                          (map #(.-v ^Datom %)))
                                asserts)
        new-attr-es (distinct (keep (fn [^Datom d] (when (== (.-a d) a-value-type) (.-e d))) asserts))
        _ (check-tuple-install! ctx asserts new-attr-es)
        auto (vec (for [e new-attr-es
                        :when (and (not (contains? explicit-installs e))
                                   (empty? (datoms-of db :eavt [part-db a-install-attribute e])))]
                    (datom/datom part-db a-install-attribute e tx true)))
        installs (into explicit-installs (map #(.-v ^Datom %)) auto)
        has-attr? (fn [e aid]
                    (or (some (fn [^Datom d] (and (== (.-e d) e) (== (.-a d) aid))) asserts)
                        (seq (datoms-of db :eavt [e aid]))))
        alters (vec (for [e alter-eids
                          :when (empty? (datoms-of db :eavt [part-db a-alter-attribute e]))]
                      (datom/datom part-db a-alter-attribute e tx true)))]
    (doseq [e installs]
      (when-not (and (has-attr? e a-value-type) (has-attr? e a-cardinality))
        (fail :db.error/invalid-install-attribute
              "First error: :db.error/schema-attribute-missing" {:entity e})))
    (into auto alters)))

(defn- check-db-partition!
  "New entities in :db.part/db must be schema components."
  [asserts db-eids]
  (doseq [e db-eids]
    (when-not (some (fn [^Datom d] (and (== (.-e d) e) (< (.-a d) first-user-attr))) asserts)
      (fail :db.error/schema-without-install
            (str "Only schema components can be installed in partition :db.part/db\n"
                 (pr-str {:datoms (filterv #(== (.-e ^Datom %) e) asserts)}))
            {:entity e}))))

;; ------------------------------------------------------------------ excision, ensure, alter ops

(defn- reject-excision!
  [ops]
  (doseq [[_ _ ^Attr attr] ops]
    (when (<= 15 (long (:id attr)) 18)
      (fail :db.error/unsupported (str "Excision is not supported: " (:ident attr))
            :cognitect.anomalies/unsupported {:attribute (:ident attr)}))))

(defn- split-ops
  "Removes the ops that are not datoms of their own: :db/ensure (collected as [e spec]),
  explicit :db.alter/attribute (derived by the transactor) and composite tuples
  (maintained by the transactor)."
  [ops]
  (reduce (fn [acc [kind e ^Attr attr v :as op]]
            (cond
              (== (:id attr) a-ensure) (cond-> acc (= kind :add) (update :ensures conj [e v]))
              (== (:id attr) a-alter-attribute) acc
              (:tuple-attrs attr) (update acc :dropped conj e)
              :else (update acc :ops conj op)))
          {:ops [] :ensures [] :dropped #{}} ops))

;; ------------------------------------------------------------------ composite tuples

(defonce ^:private composite-cache
  ;; A few recent [schema index] pairs, matched by identity (hashing a schema is costly),
  ;; so several connections do not evict each other's entry.
  (atom []))

(defn- build-composite-index [schema]
  (reduce (fn [m ^Attr a]
            (reduce (fn [m kw]
                      (if-some [cid (get (:idents schema) kw)]
                        (update m cid (fnil conj []) a)
                        m))
                    m (distinct (:tuple-attrs a))))
          {} (filter :tuple-attrs (vals (:attrs schema)))))

(defn- composite-index
  "Component attribute id -> composite Attrs built from it."
  [schema]
  (if-some [[_ idx] (some #(when (identical? (first %) schema) %) @composite-cache)]
    idx
    (let [idx (build-composite-index schema)]
      (swap! composite-cache #(conj (if (>= (count %) 8) (subvec % 1) %) [schema idx]))
      idx)))

(defn- composite-ops
  "Ops maintaining composite tuples after `res` (the effective changes of `ops`). Returns
  {:ops [changes] :checks [[:add e attr tuple] every non-empty computed tuple]}."
  [ctx ops res]
  (let [{:keys [db schema]} ctx
        cidx (composite-index schema)]
    (if (empty? cidx)
      {:ops [] :checks []}
      (let [touched (LinkedHashMap.)
            asserted (into {} (map (fn [^Datom d] [[(.-e d) (.-a d)] d])) (:asserts res))
            retracted-keys (:retracted-keys res)
            final-datom (fn [e aid]
                          (or (get asserted [e aid])
                              (first (remove (fn [^Datom c] (contains? retracted-keys (k3 e aid (codec/value->bytes (.-v c)))))
                                             (datoms-of db :eavt [e aid])))))]
        (doseq [[_ e ^Attr attr] ops, ^Attr c (get cidx (:id attr))]
          (.putIfAbsent touched [e (:id c)] c))
        (reduce (fn [acc [[e caid] ^Attr c]]
                  (let [vals (mapv (fn [kw] (when-some [^Datom d (final-datom e (get (:idents schema) kw))]
                                              (.-v d)))
                                   (:tuple-attrs c))
                        new (when (some some? vals) vals)
                        ^Datom old (first (datoms-of db :eavt [e caid]))]
                    (cond-> acc
                      new (update :checks conj [:add e c new])
                      (and old (or (nil? new) (not (same-value? (.-v old) new))))
                      (update :ops conj [:retract e c (.-v old)])
                      (and new (or (nil? old) (not (same-value? (.-v old) new))))
                      (update :ops conj [:add e c new]))))
                {:ops [] :checks []}
                (map (fn [[k v]] [k v]) touched))))))

;; ------------------------------------------------------------------ attribute predicates

(defn- check-attr-preds!
  [ctx ops]
  (when (some #(= :add (first %)) ops)
    (let [preds (reduce (fn [m ^Datom d] (update m (.-e d) (fnil conj []) (.-v d)))
                        {} (datoms-of (:db ctx) :aevt [a-attr-preds]))]
      (when (seq preds)
        (doseq [[kind e ^Attr attr v] ops
                :when (= kind :add)
                sym (get preds (:id attr))]
          (let [f (or (resolve-callable sym)
                      (fail :db.error/not-a-data-function (str "Unable to resolve data function: " sym)
                            {:function sym}))
                r (f v)]
            (when-not (true? r)
              (fail :db.error/attr-pred
                    (str "Entity " e " attribute " (:ident attr) " value " v " failed pred " sym)
                    {:db.error/pred-return r}))))))))

;; ------------------------------------------------------------------ schema alteration

(defn- check-boot-datoms!
  [schema retracts]
  (doseq [[_ ^Datom old] retracts]
    (when (< (codec/tx->t (.-tx old)) 1000)
      (let [dv (datom-vec schema (datom/datom (.-e old) (.-a old) (.-v old) (.-tx old) false))]
        (fail :db.error/datom-cannot-be-altered (str "Boot datoms cannot be altered: " (pr-str dv))
              {:datom dv})))))

(def ^:private alter-watched #{40 41 42 43 44 45 51 65 66 67 71})
(def ^:private alter-triggers #{41 42 43 44 45 51 71})

(declare check-alterations!*)

(defn- alterations-possible?
  "Cheap pre-check for `check-alterations!`: true when some assert or retract touches a watched
  attribute of an existing attribute entity, or an explicit retraction targets one. False for
  every data-only transaction."
  [attrs ops res]
  (or (boolean (some (fn [^Datom d] (and (contains? alter-watched (.-a d)) (contains? attrs (.-e d))))
                     (:asserts res)))
      (boolean (some (fn [[_ ^Datom old]] (and (contains? alter-watched (.-a old)) (contains? attrs (.-e old))))
                     (:retracts res)))
      (boolean (some (fn [[kind e ^Attr attr]]
                       (and (or (identical? kind :retract) (identical? kind :retract-attr))
                            (integer? e) (contains? attrs e) (contains? alter-triggers (:id attr))))
                     ops))))

(defn- check-alterations!
  "Applies Datomic's schema alteration rules to changes of existing attributes. Returns the
  ids of the attributes that need a :db.alter/attribute datom."
  [ctx ops res]
  (if-not (alterations-possible? (:attrs (:schema ctx)) ops res)
    []
    (check-alterations!* ctx ops res)))

(defn- check-alterations!*
  [ctx ops res]
  (let [{:keys [db schema]} ctx
        attrs (:attrs schema)
        ids (:ids schema)
        watched? (fn [e aid] (and (contains? attrs e) (contains? alter-watched aid)))
        adds (into {} (keep (fn [^Datom d] (when (watched? (.-e d) (.-a d)) [[(.-e d) (.-a d)] d])))
                   (:asserts res))
        rets (reduce (fn [m [_ ^Datom old]]
                       (if (watched? (.-e old) (.-a old))
                         (update m [(.-e old) (.-a old)] (fnil conj []) old)
                         m))
                     {} (:retracts res))
        explicit (into #{} (keep (fn [[kind e ^Attr attr]]
                                   (when (and (#{:retract :retract-attr} kind) (integer? e)
                                              (contains? attrs e) (contains? alter-triggers (:id attr)))
                                     e)))
                       ops)
        es (distinct (concat (map first (keys adds)) (map first (keys rets)) explicit))
        errors (volatile! [])]
    (doseq [e es]
      (let [^Attr attr (get attrs e)
            ident (:ident attr)
            added (fn [aid] (get adds [e aid]))
            retd (fn [aid] (get rets [e aid]))
            ->id (fn [^Datom d] (when d (let [v (.-v d)] (get ids v v))))
            err! (fn [m] (vswap! errors conj (merge {:db/error :db.error/unsupported-alter-schema :entity ident} m)))
            data (fn [] (datoms-of db :aevt [e]))]
        (when (or (added 40) (retd 40))
          (err! {:attribute :db/valueType :from (:value-type attr) :to (or (->id (added 40)) :disabled)}))
        (when (or (added 41) (retd 41))
          (let [new (->id (added 41))]
            (cond
              (nil? new) (err! {:attribute :db/cardinality :from (:cardinality attr) :to :disabled})
              (and (= new :db.cardinality/one) (= (:cardinality attr) :db.cardinality/many))
              (let [by-e (group-by (fn [^Datom d] (.-e d)) (data))]
                (when-some [ds (first (filter #(> (count %) 1) (vals by-e)))]
                  (vswap! errors conj {:db/error :db.error/cardinality-violation :datoms (vec ds)}))))))
        (doseq [[aid kw old] [[a-tuple-type :db/tupleType (:tuple-type attr)]
                              [a-tuple-types :db/tupleTypes (:tuple-types attr)]
                              [a-tuple-attrs :db/tupleAttrs (:tuple-attrs attr)]]]
          (when (or (added aid) (retd aid))
            (err! {:attribute kw :from (or old :disabled)
                   :to (if-some [^Datom d (added aid)] (.-v d) :disabled)})))
        (when-some [^Datom d (added 51)]
          (when (and (true? (.-v d)) (not (true? (:fulltext? attr))))
            (err! {:attribute :db/fulltext :from (if (nil? (:fulltext? attr)) :disabled (:fulltext? attr)) :to true})))
        (when-some [^Datom d (added 42)]
          (when (nil? (:unique attr))
            (let [ds (data)
                  index? (if-some [^Datom i (added 44)]
                           (.-v i)
                           (if (retd 44) false (:index? attr)))]
              (if (and (seq ds) (not index?))
                (vswap! errors conj {:db/error :db.error/unique-without-index :attribute ident})
                (let [seen (java.util.HashMap.)]
                  (doseq [^Datom x ds]
                    (let [k (vkey (.-v x))
                          prev (.put seen k x)]
                      (when prev
                        (vswap! errors conj {:db/error :db.error/unique-violation :datoms [prev x]})))))))))))
    (when (seq @errors)
      (fail :db.error/invalid-alter-attribute (str "Error: " (pr-str (first @errors)))
            {:db/errors (seq @errors)}))
    (filterv (fn [e] (or (contains? explicit e)
                         (some (fn [aid] (or (contains? adds [e aid]) (contains? rets [e aid])))
                               alter-triggers)))
             es)))

;; ------------------------------------------------------------------ db-after (entity specs)

(defn- bytes-start-with? [^bytes k ^bytes prefix]
  (let [n (alength prefix)]
    (and (>= (alength k) n) (Arrays/equals k 0 n prefix 0 n))))

(defn- lower-bound ^long [^objects ks ^bytes start]
  (loop [lo 0 hi (alength ks)]
    (if (< lo hi)
      (let [mid (quot (+ lo hi) 2)]
        (if (neg? (long (codec/compare-bytes (aget ks mid) start)))
          (recur (inc mid) hi)
          (recur lo mid)))
      lo)))

(deftype OverlayCursor [^objects ks ^objects ds ^bytes prefix ^:unsynchronized-mutable ^long i]
  mnemonic.index.ICursor
  (valid [_] (boolean (and (< i (alength ks))
                           (or (nil? prefix) (bytes-start-with? (aget ks i) prefix)))))
  (advance [this] (when (.valid this) (set! i (inc i))) nil)
  (key [this] (when (.valid this) (aget ks i)))
  (datom [this] (when (.valid this) (aget ds i)))
  (retractTx [_] -1)
  (close [_] nil))

(defn- dkey [idx ^Datom d]
  (ByteBuffer/wrap (codec/encode-key idx (.-e d) (.-a d) (.-v d) (.-tx d))))

(deftype TxDb [base ^long basis schema overlays retracted]
  index/IDb
  (-seek-current [this idx prefix] (index/-view this idx prefix))
  (-seek-history [_ _ _]
    (throw (error :db.error/unsupported "History is not available on a transaction's db-after"
                  :cognitect.anomalies/unsupported nil)))
  (-view [_ idx prefix]
    (let [[ks ds] (get overlays idx)
          oc (->OverlayCursor ks ds prefix (lower-bound ks prefix))
          bc (index/-view base idx prefix)
          rs (get retracted idx)
          bc (if (seq rs) (index/filter-cursor bc (fn [d] (not (contains? rs (dkey idx d))))) bc)]
      (index/merge-cursors [oc bc])))
  (-seek-view [_ idx start]
    (let [[ks ds] (get overlays idx)
          oc (->OverlayCursor ks ds nil (lower-bound ks start))
          bc (index/-seek-view base idx start)
          rs (get retracted idx)
          bc (if (seq rs) (index/filter-cursor bc (fn [d] (not (contains? rs (dkey idx d))))) bc)]
      (index/merge-cursors [oc bc])))
  (-basis-t [_] basis)
  (-schema [_] schema)
  (-as-of-t [_] nil)
  (-since-t [_] nil)
  (-history? [_] false)
  (-filter-pred [_] nil)
  (-raw [this] this))

(defn- make-tx-db
  "A db value of `db` with the effects of one transaction applied."
  [db schema t asserted retracts]
  (let [overlays (into {}
                       (for [idx [:eavt :aevt :avet :vaet]]
                         (let [items (->> asserted
                                          (filter (fn [^Datom d] (or (not= idx :vaet) (ref-attr? schema (.-a d)))))
                                          (map (fn [^Datom d] [(codec/encode-key idx (.-e d) (.-a d) (.-v d) (.-tx d)) d]))
                                          (sort-by first (fn [a b] (codec/compare-bytes a b))))]
                           [idx [(object-array (map first items)) (object-array (map second items))]])))
        retracted (into {}
                        (for [idx [:eavt :aevt :avet :vaet]]
                          [idx (into #{} (comp (map second)
                                               (filter (fn [^Datom d] (or (not= idx :vaet) (ref-attr? schema (.-a d)))))
                                               (map #(dkey idx %)))
                                     retracts)]))]
    (->TxDb db t schema overlays retracted)))

(defn- check-ensures!
  "Checks every `:db/ensure` entity spec against the transaction's resulting db."
  [ctx ensures asserted retracts]
  (when (seq ensures)
    (let [{:keys [db schema t]} ctx
          retracted-keys (into #{} (map (fn [[_ ^Datom o]] [(.-e o) (.-a o) (vkey (.-v o))])) retracts)
          has-attr? (fn [e aid]
                      (or (some (fn [^Datom d] (and (== (.-e d) e) (== (.-a d) aid))) asserted)
                          (some (fn [^Datom c] (not (contains? retracted-keys [e aid (vkey (.-v c))])))
                                (datoms-of db :eavt [e aid]))))
          after (delay (make-tx-db db schema t asserted retracts))
          vals-of (fn [spec aid] (mapv (fn [^Datom d] (.-v d)) (datoms-of db :eavt [spec aid])))]
      (doseq [[e spec] (distinct ensures)]
        (let [label (get (:ids schema) spec spec)
              missing (remove (fn [kw] (when-some [aid (get (:idents schema) kw)] (has-attr? e aid)))
                              (vals-of spec 69))]
          (when (seq missing)
            (fail :db.error/entity-attr
                  (str "Entity " e " missing attributes " (pr-str (vec missing)) " of spec " label)
                  {:entity e :spec label}))
          (doseq [sym (vals-of spec 70)]
            (let [f (or (resolve-callable sym)
                        (fail :db.error/not-a-data-function (str "Unable to resolve data function: " sym)
                              {:function sym}))
                  r (f @after e)]
              (when-not (true? r)
                (fail :db.error/entity-pred
                      (str "Entity " e " failed pred " sym " of spec " label)
                      {:db.error/pred-return r})))))))))

;; ------------------------------------------------------------------ prepare

(defn prepare
  "Prepare a transaction against `db-before` with store `meta` ({:next-t :next-db-n ...}).
  Returns {:tx-data [Datom] :tempids map :ops [rocks-ops] :meta meta' :tx tx-id}.
  Throws ex-info (with :db/error) on invalid tx-data; never writes."
  [db-before meta tx-data]
  (let [schema (index/-schema db-before)
        t (long (:next-t meta))
        tx (codec/t->tx t)
        ctx {:db db-before :schema schema :meta meta :t t :tx tx :part-cache (atom {})
             :lookup-cache (java.util.HashMap.)}
        resolved (ArrayList.)
        _ (expand-forms* ctx tx-data resolved)
        ops (vec resolved)
        _ (reject-excision! ops)
        {:keys [ops tempids n-shared n-db db-eids]} (resolve-tempids ctx ops)
        {:keys [ops ensures dropped]} (split-ops ops)
        res1 (compute-datoms ctx ops db-eids)
        {syn :ops checks :checks} (composite-ops ctx ops res1)
        res (if (seq syn) (compute-datoms ctx (into ops syn) db-eids) res1)
        {:keys [asserts adds retracts retracted-keys dinfo]} res
        all-ops (into ops syn)
        _ (check-boot-datoms! schema retracts)
        alter-eids (check-alterations! ctx all-ops res)
        _ (check-uniqueness! ctx adds asserts retracted-keys dinfo)
        _ (check-attr-preds! ctx (into ops checks))
        _ (check-db-partition! asserts db-eids)
        auto (schema-installs ctx asserts alter-eids)
        ;; ids allocated to entities that ended up without datoms (for example only a
        ;; dropped composite tuple) are not consumed
        n-shared (if (empty? dropped)
                   n-shared
                   (let [used (into #{} (mapcat (fn [^Datom d]
                                                (cond-> [(.-e d)] (ref-attr? schema (.-a d)) (conj (.-v d)))))
                                  asserts)
                       mask (dec (bit-shift-left 1 42))
                       top (reduce (fn [m e]
                                     (let [e (long e) n (bit-and e mask) part (codec/part-of e)]
                                       (if (and (not= part part-db) (not= part part-tx) (> n t) (< n (long n-shared))
                                                (or (contains? used e) (not (contains? dropped e))))
                                         (max m n)
                                         m)))
                                   t (vals tempids))]
                   (inc (long top))))
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
        _ (check-ensures! ctx ensures asserted retracts)
        tx-data (-> [] (into (when instant-datom [instant-datom]))
                    (into (remove #(identical? % instant-datom) asserted))
                    (into retract-datoms))
        no-history? (fn [aid] (boolean (:no-history? (get (:attrs schema) aid))))
        out (ArrayList.)
        _ (doseq [^Datom d asserted] (put-ops out schema dinfo d))
        _ (doseq [[_ ^Datom old] retracts]
            (retract-ops out schema dinfo old tx (no-history? (.-a old))))
        _ (.add out (log-op tx tx-data dinfo))
        meta' (assoc meta :next-t n-shared :next-db-n n-db)
        _ (.add out (bootstrap/meta-op meta'))
        ops (vec out)]
    {:tx-data tx-data
     :tempids tempids
     :ops ops
     :meta meta'
     :tx tx}))
