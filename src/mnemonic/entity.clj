(ns mnemonic.entity
  "Lazy entity maps over EAVT/VAET, plus the entity id resolution shared with pull."
  (:require [clojure.string :as str]
            [mnemonic.codec :as codec]
            [mnemonic.index :as index]
            [mnemonic.schema :as schema]
            [mnemonic.tx :as tx])
  (:import (clojure.lang Associative IFn IHashEq ILookup IPersistentCollection MapEntry
                         Seqable Util)
           (java.util ArrayList)
           (mnemonic.codec IKeyBuf)
           (mnemonic.datom IDatomView)
           (mnemonic.db Base)
           (mnemonic.schema Attr)))

(set! *warn-on-reflection* true)

;; ------------------------------------------------------------------ shared helpers

(defn ea-prefix
  "EAVT/VAET key prefix of two long components (entity+attribute, or ref value+attribute)."
  ^bytes [^long x ^long y]
  (let [b (codec/new-key-buf 16)]
    (.putLong ^IKeyBuf b x)
    (.putLong ^IKeyBuf b y)
    (.toBytes ^IKeyBuf b)))

(defn e-prefix
  "EAVT key prefix of the entity `e`."
  ^bytes [^long e]
  (let [b (codec/new-key-buf 8)]
    (.putLong ^IKeyBuf b e)
    (.toBytes ^IKeyBuf b)))

(defn first-value
  "The value of the first asserted datom of `index` under `prefix`, decoded in place from
  the cursor (no Datom is built); `::no-datom` when there is none."
  [db index ^bytes prefix]
  (index/reduce-views (index/datoms-iterable db index prefix)
                      (fn [_ ^IDatomView d] (if (.isAdded d) (reduced (.value d)) ::no-datom))
                      ::no-datom))

(defn values-by-prefix
  "ArrayList of the values (with `e?`, the entity ids) of the asserted datoms of `index`
  under the encoded `prefix`, decoded in place from the cursor (no Datom is built); nil
  when there are none. At most `limit` (a number or nil) are read. The cursor is closed
  on return, so a caller that reads more for each value (a nested pull) holds no cursor
  open while it does."
  [db index ^bytes prefix limit e?]
  (let [limit (if limit (long limit) Long/MAX_VALUE)]
    (when (pos? limit)
      (let [out (ArrayList.)]
        (index/reduce-views (index/datoms-iterable db index prefix)
                            (fn [_ ^IDatomView d]
                              (when (.isAdded d)
                                (.add out (if e? (.eid d) (.value d)))
                                (when (>= (.size out) limit) (reduced nil))))
                            nil)
        (when-not (.isEmpty out) out)))))

(defn attr-values
  "[attrs values]: two ArrayLists, the attribute id and the value of every asserted datom
  of entity `eid` in EAVT order (so grouped by attribute), decoded in place. The cursor
  is closed on return."
  [db ^long eid]
  (let [as (ArrayList.)
        vs (ArrayList.)]
    (index/reduce-views (index/datoms-iterable db :eavt (e-prefix eid))
                        (fn [_ ^IDatomView d]
                          (when (.isAdded d)
                            (.add as (.attr d))
                            (.add vs (.value d)))
                          nil)
                        nil)
    [as vs]))

(defn not-a-keyword
  "The error Datomic raises for a non-keyword where a keyword is required."
  [x]
  (tx/error :db.error/not-a-keyword
            (str "Cannot interpret as a keyword: " x
                 (if (string? x) ", no leading :" (str " of class: " (class x))))
            {:value x}))

(defn resolve-eid
  "Entity id for a long, ident keyword, `\":kw\"` string or lookup ref in `db`; nil when
  it does not resolve. Strings without a leading colon raise :db.error/not-a-keyword."
  [db x]
  (let [sch (index/-schema db)]
    (cond
      (nil? x) nil
      (integer? x) (long x)
      (keyword? x) (get (:idents sch) x)
      (vector? x) (tx/lookup-ref-eid {:db db :schema sch} x)
      (string? x) (if (str/starts-with? x ":")
                    (get (:idents sch) (keyword (subs x 1)))
                    (throw (not-a-keyword x)))
      :else nil)))

(defn reverse-attr-name
  "For a reverse attribute keyword (:ns/_name) the forward keyword (:ns/name), else nil."
  [k]
  (let [n (name k)]
    (when (and (> (count n) 1) (= \_ (.charAt n 0)))
      (keyword (namespace k) (subs n 1)))))

;; ------------------------------------------------------------------ entity

(def ^:private none ::none)

(def ^:private miss ::miss)

(defn- none? [x] (identical? x none))

(definterface IEntityCache
  (getAttr [k nf])
  (putAttr [k v])
  (^boolean allLoaded [])
  (order [])
  (setAll [entries])
  (^boolean touched [])
  (setTouched []))

;; Per-entity cache. Entities may be shared across threads; every field is volatile and
;; holds an immutable value, so readers always see a consistent map/vector. Writers do
;; unsynchronized read-modify-write: a racing put can be lost, which only costs a
;; reload (the loads are idempotent). :order is published after :attrs holds its entries.
(deftype EntityCache [^:volatile-mutable attrs
                      ^:volatile-mutable order*
                      ^:volatile-mutable ^boolean touched?]
  IEntityCache
  (getAttr [_ k nf] (get attrs k nf))
  (putAttr [_ k v] (set! attrs (assoc attrs k v)) nil)
  (allLoaded [_] (some? order*))
  (order [_] order*)
  (setAll [_ entries]
    (set! attrs (into attrs entries))
    (set! order* entries)
    nil)
  (touched [_] touched?)
  (setTouched [_] (set! touched? (boolean true)) nil))

(declare ->Entity)

(defn- new-entity [db eid]
  (->Entity db eid (EntityCache. {} nil false)))

(defn- ref-value
  "A ref target as seen through an entity: the ident keyword for enum-like entities,
  otherwise an entity."
  [db sch eid]
  (or (get (:ids sch) eid)
      (new-entity db eid)))

(defn- view-value
  "Value of the (flyweight) datom view `d` of `attr`, as seen through an entity."
  [db sch ^Attr attr ^IDatomView d]
  (if (= :db.type/ref (:value-type attr))
    (ref-value db sch (.valueLong d))
    (.value d)))

(defn- vreduce
  "Reduce the datoms of `index` under `prefix` with flyweight views (valid during `f`
  only; nothing here retains one)."
  [db index prefix f init]
  (index/reduce-views (index/datoms-iterable db index prefix) f init))

(defn- load-attr
  "Value of forward attribute `k` of `eid`, or ::none."
  [db eid k]
  (let [sch (index/-schema db)]
    (if-let [^Attr attr (schema/attr sch k)]
      (let [prefix (ea-prefix eid (long (:id attr)))]
        (if (= :db.cardinality/many (:cardinality attr))
          (let [out (vreduce db :eavt prefix
                             (fn [acc ^IDatomView d]
                               (if (.isAdded d) (conj! acc (view-value db sch attr d)) acc))
                             (transient #{}))]
            (if (zero? (count out)) none (persistent! out)))
          (vreduce db :eavt prefix
                   (fn [_ ^IDatomView d]
                     (if (.isAdded d) (reduced (view-value db sch attr d)) none))
                   none)))
      none)))

(defn- load-reverse
  "Value of reverse attribute `k` (:ns/_name) of `eid`, or ::none."
  [db eid k fk]
  (let [sch (index/-schema db)
        ^Attr attr (schema/attr sch fk)]
    (if (and attr (= :db.type/ref (:value-type attr)))
      (let [prefix (ea-prefix eid (long (:id attr)))]
        (if (:component? attr)
          (vreduce db :vaet prefix
                   (fn [_ ^IDatomView d]
                     (if (.isAdded d) (reduced (new-entity db (.eid d))) none))
                   none)
          (let [out (vreduce db :vaet prefix
                             (fn [acc ^IDatomView d]
                               (if (.isAdded d) (conj! acc (new-entity db (.eid d))) acc))
                             (transient #{}))]
            (if (zero? (count out)) none (persistent! out)))))
      none)))

(defn- load-all
  "Ordered [[kw value] ...] of every forward attribute of `eid` (attribute id order)."
  [db eid]
  (let [sch (index/-schema db)
        ;; one pass over the entity's datoms: [attr-id value] for every known attribute
        ps (persistent!
            (vreduce db :eavt (e-prefix eid)
                     (fn [acc ^IDatomView d]
                       (if (.isAdded d)
                         (let [a (.attr d)]
                           (if-let [^Attr attr (get (:attrs sch) a)]
                             (conj! acc (MapEntry/create a (view-value db sch attr d)))
                             acc))
                         acc))
                     (transient [])))
        n (count ps)]
    (loop [i 0 out (transient [])]
      (if (< i n)
        (let [a (long (key (nth ps i)))
              j (loop [j (inc i)]
                  (if (and (< j n) (== a (long (key (nth ps j))))) (recur (inc j)) j))
              ^Attr attr (get (:attrs sch) a)
              r [(:ident attr)
                 (if (= :db.cardinality/many (:cardinality attr))
                   (loop [k (long i) acc (transient #{})]
                     (if (< k (long j))
                       (recur (inc k) (conj! acc (val (nth ps k))))
                       (persistent! acc)))
                   (val (nth ps i)))]]
          (recur (long j) (conj! out r)))
        (persistent! out)))))

(defn- same-db? [a b]
  (or (identical? a b)
      (and (= (index/-basis-t a) (index/-basis-t b))
           (= (index/-as-of-t a) (index/-as-of-t b))
           (= (index/-since-t a) (index/-since-t b))
           (= (index/-history? a) (index/-history? b))
           (identical? (index/-filter-pred a) (index/-filter-pred b))
           (identical? (.-store ^Base (index/-raw a)) (.-store ^Base (index/-raw b))))))

(defn- unsupported-write []
  (throw (UnsupportedOperationException. "Entities are read-only")))

(declare entity-seq ensure-all!)

(definterface IEntityKeys
  (keySet []))

(deftype Entity [db ^long eid cache]
  ILookup
  (valAt [this k] (.valAt this k nil))
  (valAt [_ k nf]
    (let [^EntityCache cache cache
          k (if (and (string? k) (str/starts-with? ^String k ":")) (keyword (subs ^String k 1)) k)]
      (cond
        (keyword? k)
        (if (identical? :db/id k)
          eid
          (let [cached (.getAttr cache k miss)
                v (if (identical? cached miss)
                    (let [fk (reverse-attr-name k)
                          v (if fk (load-reverse db eid k fk) (load-attr db eid k))]
                      (.putAttr cache k v)
                      v)
                    cached)]
            (if (none? v) nf v)))
        (nil? k) nf
        (or (string? k) (number? k)) (throw (not-a-keyword k))
        :else nf)))

  IFn
  (invoke [this k] (.valAt this k nil))
  (invoke [this k nf] (.valAt this k nf))
  (applyTo [this args]
    (case (count args)
      1 (.valAt this (first args) nil)
      2 (.valAt this (first args) (second args))
      (throw (clojure.lang.ArityException. (count args) "Entity"))))

  Associative
  (containsKey [this k] (not (identical? none (.valAt this k none))))
  (entryAt [this k]
    (let [v (.valAt this k none)]
      (when-not (identical? none v) (MapEntry/create k v))))
  (assoc [_ _ _] (unsupported-write))

  IPersistentCollection
  (count [this] (count (ensure-all! this)))
  (cons [_ _] (unsupported-write))
  (empty [_] (unsupported-write))
  (equiv [this o] (.equals this o))

  Seqable
  (seq [this] (seq (entity-seq this)))

  IEntityKeys
  (keySet [this] (into #{} (map first) (ensure-all! this)))

  IHashEq
  (hasheq [this] (.hashCode this))

  Object
  (equals [this o]
    (and (instance? Entity o)
         (let [^Entity o o]
           (and (== eid (.-eid o)) (same-db? db (.-db o))))))
  ;; equal entities have equal ids and basis-t (same-db?); no vector per hash
  (hashCode [_] (unchecked-int (+ (* 31 (Long/hashCode eid)) (Long/hashCode (long (index/-basis-t db))))))
  (toString [this] (with-out-str (print-method this *out*))))

(defn- ensure-all!
  "Loads every forward attribute into `e`'s cache; returns the ordered entries."
  [^Entity e]
  (let [^EntityCache cache (.-cache e)]
    (if (.allLoaded cache)
      (.order cache)
      (let [entries (load-all (.-db e) (.-eid e))]
        (.setAll cache entries)
        entries))))

(defn- entity-seq [^Entity e]
  (map (fn [[k _]] (MapEntry/create k (get e k))) (ensure-all! e)))

(defn entity
  "The entity for an id, ident, or lookup ref; nil when it does not resolve."
  [db x]
  (when (index/-history? db)
    (throw (IllegalStateException. "Can't create entity from history")))
  (when-let [eid (resolve-eid db x)]
    (new-entity db eid)))

(defn entity-db
  "The db value `e` was created from."
  [^Entity e]
  (.-db e))

(defn- touch* [^Entity e path]
  (let [^EntityCache cache (.-cache e)
        path (conj path (.-eid e))]
    (when-not (.touched cache)
      (let [sch (index/-schema (.-db e))]
        (doseq [[k _] (ensure-all! e)
                :let [^Attr attr (schema/attr sch k)]
                :when (and attr (:component? attr))
                :let [v (get e k)]]
          (doseq [child (if (set? v) v [v])]
            ;; a component cycle stays untouched (Datomic overflows the stack there)
            (when (and (instance? Entity child) (not (contains? path (.-eid ^Entity child))))
              (touch* child path)))))
      (.setTouched cache)))
  e)

(defn touch
  "Loads all attributes of `e` and touches component entities recursively."
  [^Entity e]
  (touch* e #{}))

(defmethod print-method Entity [^Entity e ^java.io.Writer w]
  (let [^EntityCache cache (.-cache e)]
    (print-method
     (if (.touched cache)
       (apply array-map :db/id (.-eid e)
              (mapcat (fn [[k _]] [k (get e k)]) (.order cache)))
       {:db/id (.-eid e)})
     w)))
