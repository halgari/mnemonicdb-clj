(ns mnemonic.entity
  "Lazy entity maps over EAVT/VAET, plus the entity id resolution shared with pull."
  (:require [clojure.string :as str]
            [mnemonic.codec :as codec]
            [mnemonic.index :as index]
            [mnemonic.schema :as schema]
            [mnemonic.tx :as tx])
  (:import (clojure.lang Associative IFn IHashEq ILookup IPersistentCollection MapEntry
                         Seqable Util)
           (mnemonic.datom Datom)
           (mnemonic.db Base)
           (mnemonic.schema Attr)))

(set! *warn-on-reflection* true)

;; ------------------------------------------------------------------ shared helpers

(defn datoms-seq
  "Vector of the asserted datoms of `index` whose leading components are `components`,
  read through `db` (so as-of, since, history and filtered dbs all work); nil when
  there are none. At most `limit` datoms are read when `limit` is given. The cursor is
  always closed."
  ([db index components] (datoms-seq db index components nil))
  ([db index components limit]
   (let [it (index/datoms-iterable db index (codec/encode-prefix index components))
         limit (when limit (long limit))]
     (when-not (and limit (<= limit 0))
       (not-empty
        (reduce (fn [acc ^Datom d]
                  (if (.-added d)
                    (let [acc (conj acc d)]
                      (if (and limit (>= (count acc) limit)) (reduced acc) acc))
                    acc))
                [] it))))))

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

(defn- none? [x] (identical? x none))

(declare ->Entity)

(defn- ref-value
  "A ref target as seen through an entity: the ident keyword for enum-like entities,
  otherwise an entity."
  [db sch eid]
  (or (get (:ids sch) eid)
      (->Entity db eid (atom {:attrs {} :order nil :all? false :touched? false}))))

(defn- datom-value [db sch ^Attr attr ^Datom d]
  (if (= :db.type/ref (:value-type attr))
    (ref-value db sch (long (.-v d)))
    (.-v d)))

(defn- load-attr
  "Value of forward attribute `k` of `eid`, or ::none."
  [db eid k]
  (let [sch (index/-schema db)]
    (if-let [^Attr attr (schema/attr sch k)]
      (let [many? (= :db.cardinality/many (:cardinality attr))]
        (if-let [ds (datoms-seq db :eavt [eid (:id attr)] (when-not many? 1))]
          (let [vals (map #(datom-value db sch attr %) ds)]
            (if many? (set vals) (first vals)))
          none))
      none)))

(defn- load-reverse
  "Value of reverse attribute `k` (:ns/_name) of `eid`, or ::none."
  [db eid k fk]
  (let [sch (index/-schema db)
        ^Attr attr (schema/attr sch fk)]
    (if (and attr (= :db.type/ref (:value-type attr)))
      (if-let [ds (datoms-seq db :vaet [eid (:id attr)] (when (:component? attr) 1))]
        (let [es (map (fn [^Datom d] (->Entity db (.-e d) (atom {:attrs {} :order nil
                                                                  :all? false :touched? false})))
                      ds)]
          (if (:component? attr) (first es) (set es)))
        none)
      none)))

(defn- load-all
  "Ordered [[kw value] ...] of every forward attribute of `eid` (attribute id order)."
  [db eid]
  (let [sch (index/-schema db)]
    (->> (datoms-seq db :eavt [eid])
         (partition-by (fn [^Datom d] (.-a d)))
         (keep (fn [ds]
                 (let [^Attr attr (get (:attrs sch) (.-a ^Datom (first ds)))]
                   (when attr
                     (let [vals (map #(datom-value db sch attr %) ds)]
                       [(:ident attr)
                        (if (= :db.cardinality/many (:cardinality attr))
                          (set vals)
                          (first vals))]))))))))

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

(declare entity-seq)

(deftype Entity [db ^long eid cache]
  ILookup
  (valAt [this k] (.valAt this k nil))
  (valAt [_ k nf]
    (let [k (if (and (string? k) (str/starts-with? ^String k ":")) (keyword (subs ^String k 1)) k)]
      (cond
        (= :db/id k) eid
        (keyword? k)
        (let [cached (get-in @cache [:attrs k] none)]
          (if-not (none? cached)
            cached
            (let [fk (reverse-attr-name k)
                  v (if fk (load-reverse db eid k fk) (load-attr db eid k))]
              (swap! cache assoc-in [:attrs k] v)
              (if (none? v) nf v))))
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
  (count [this] (count (entity-seq this)))
  (cons [_ _] (unsupported-write))
  (empty [_] (unsupported-write))
  (equiv [this o] (.equals this o))

  Seqable
  (seq [this] (seq (entity-seq this)))

  IHashEq
  (hasheq [this] (.hashCode this))

  Object
  (equals [this o]
    (and (instance? Entity o)
         (let [^Entity o o]
           (and (== eid (.-eid o)) (same-db? db (.-db o))))))
  (hashCode [_] (hash [eid (index/-basis-t db)]))
  (toString [this] (with-out-str (print-method this *out*))))

(defn- ensure-all!
  "Loads every forward attribute into `e`'s cache; returns the ordered entries."
  [^Entity e]
  (let [cache (.-cache e)
        st @cache]
    (if (:all? st)
      (:order st)
      (let [entries (vec (load-all (.-db e) (.-eid e)))]
        (swap! cache (fn [s]
                       (-> s
                           (update :attrs into entries)
                           (assoc :order entries :all? true))))
        entries))))

(defn- entity-seq [^Entity e]
  (map (fn [[k _]] (MapEntry/create k (get e k))) (ensure-all! e)))

(defn entity
  "The entity for an id, ident, or lookup ref; nil when it does not resolve."
  [db x]
  (when-let [eid (resolve-eid db x)]
    (->Entity db eid (atom {:attrs {} :order nil :all? false :touched? false}))))

(defn entity-db
  "The db value `e` was created from."
  [^Entity e]
  (.-db e))

(defn touch
  "Loads all attributes of `e` and touches component entities recursively."
  [^Entity e]
  (when-not (:touched? @(.-cache e))
    (swap! (.-cache e) assoc :touched? true)
    (let [sch (index/-schema (.-db e))]
      (doseq [[k _] (ensure-all! e)
              :let [^Attr attr (schema/attr sch k)]
              :when (and attr (:component? attr))
              :let [v (get e k)]]
        (doseq [child (if (set? v) v [v])]
          (when (instance? Entity child) (touch child))))))
  e)

(defmethod print-method Entity [^Entity e ^java.io.Writer w]
  (let [st @(.-cache e)]
    (print-method
     (if (:touched? st)
       (apply array-map :db/id (.-eid e)
              (mapcat (fn [[k _]] [k (get e k)]) (:order st)))
       {:db/id (.-eid e)})
     w)))
