(ns mnemonic.schema
  "Schema derived from datoms: idents and attribute definitions."
  (:refer-clojure :exclude [indexed?]))

(set! *warn-on-reflection* true)

(defrecord Attr [id ident value-type cardinality unique index? component? no-history?
                 fulltext? tuple-attrs tuple-type tuple-types doc])

(defrecord Schema [attrs idents ids])

(def empty-schema (->Schema {} {} {}))

(def ident-attr-id 10)

(def ^:private attr-props
  "System attribute id -> [Attr field, how to interpret the value]."
  {40 [:value-type :ref]
   41 [:cardinality :ref]
   42 [:unique :ref]
   43 [:component? :raw]
   44 [:index? :raw]
   45 [:no-history? :raw]
   51 [:fulltext? :raw]
   62 [:doc :raw]
   65 [:tuple-type :ref]
   66 [:tuple-types :refs]
   67 [:tuple-attrs :refs]})

(def schema-attr-ids
  "Ids of every system attribute that contributes to the schema (incl. :db/ident)."
  (conj (set (keys attr-props)) ident-attr-id))

(defn- resolve-ref [ids x]
  (if (integer? x) (get ids x x) x))

(defn schema-from-datoms
  "Build a Schema from datoms (Datoms or [e a v ...] vectors; retractions ignored).
  All `:db/ident` entities go into idents; entities with `:db/valueType` become Attrs."
  [datoms]
  (let [asserted (filter #(nth % 4 true) datoms)
        idents (reduce (fn [m d]
                         (if (== (long (nth d 1)) ident-attr-id)
                           (assoc m (nth d 2) (long (nth d 0)))
                           m))
                       {} asserted)
        ids (reduce-kv (fn [m k v] (assoc m v k)) {} idents)
        props (reduce (fn [m d]
                        (let [e (long (nth d 0))
                              [field kind] (get attr-props (long (nth d 1)))]
                          (if field
                            (assoc-in m [e field]
                                      (let [v (nth d 2)]
                                        (case kind
                                          :raw v
                                          :ref (resolve-ref ids v)
                                          :refs (mapv #(resolve-ref ids %) v))))
                            m)))
                      {} asserted)
        attrs (reduce-kv (fn [m e p]
                           (if (contains? p :value-type)
                             (assoc m e (map->Attr (assoc p :id e :ident (get ids e))))
                             m))
                         {} props)]
    (->Schema attrs idents ids)))

(defn with-ident-aliases
  "Adds retracted idents as aliases, as Datomic does: after a rename (or a retraction of
  `:db/ident`) the old keyword keeps resolving to its entity until it is asserted for
  another entity. `history` is a seq of [eid keyword retract-tx] for retracted `:db/ident`
  datoms. When a keyword was retracted from several entities the latest retraction wins;
  current idents always win. An entity without a current ident reports its latest
  retracted one from `ids`."
  [^Schema schema history]
  (if (empty? history)
    schema
    (let [ordered (sort-by #(nth % 2) history)
          idents (:idents schema)
          ids (:ids schema)
          alias-idents (reduce (fn [m [e kw]] (assoc m kw (long e))) {} ordered)
          alias-ids (reduce (fn [m [e kw]] (assoc m (long e) kw)) {} ordered)]
      (assoc schema
             :idents (merge alias-idents idents)
             :ids (merge alias-ids ids)))))

(defn entid
  "Entity id for a long or ident keyword; nil when unknown."
  [^Schema schema x]
  (cond
    (integer? x) (long x)
    (keyword? x) (get (:idents schema) x)
    :else nil))

(defn attr
  "Attr for an attribute id or ident, or nil."
  [^Schema schema id-or-ident]
  (when-let [id (entid schema id-or-ident)]
    (get (:attrs schema) id)))

(defn indexed? [^Attr a]
  (boolean (or (:index? a) (:unique a))))
