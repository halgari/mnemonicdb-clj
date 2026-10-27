(ns mnemonic.pull
  "Pull: pattern parsing into an AST and evaluation over EAVT/VAET.

  Semantics follow Datomic (probed against 1.0.7745):
  - results with no content are nil; empty nested results are dropped
  - cardinality-many values are vectors, limited to 1000 unless :limit says otherwise
  - component refs without a sub-pattern expand like a wildcard pull
  - recursion: expanding a recursive attribute of node n adds n to the path's `seen`
    set (shared by all recursive attributes and inherited by nested sub-patterns);
    an entity pulled while in `seen` is in revisit mode, where its recursive children
    are not expanded; a child that is not expanded (revisit mode, or past the depth
    of its recursive attribute) is rendered as {:db/id n} when the pattern pulls
    :db/id and dropped otherwise"
  (:require [clojure.edn :as edn]
            [mnemonic.entity :as entity]
            [mnemonic.index :as index]
            [mnemonic.schema :as schema]
            [mnemonic.tx :as tx])
  (:import (java.lang.ref Reference)
           (java.util ArrayList Arrays HashMap)
           (java.util.concurrent ConcurrentHashMap)
           (java.util.concurrent.atomic AtomicLong)
           (mnemonic.datom IDatomView)
           (mnemonic.index IFastReduce)
           (mnemonic.schema Attr)))

(set! *warn-on-reflection* true)

;; ------------------------------------------------------------------ AST

;; `compiled` holds the pattern resolved against the last schema it was pulled with
;; (`compiled`), so per-entity work never looks an attribute up by keyword.
(defrecord PullPattern [wildcard? id? attrs override ^java.util.concurrent.atomic.AtomicReference cc])

(defrecord AttrSpec [attr reverse? key limit default xform sub recur rid])

(def ^:private default-limit 1000)

(defn- spec-error [kind msg data]
  (tx/error kind msg data))

(defn- invalid-attr-spec [x]
  (spec-error :db.error/invalid-attr-spec
              (str "Cannot interpret as an attribute spec: " (pr-str x) " of class: " (class x))
              {:spec x}))

(defn- invalid-attr-identifier [x]
  (spec-error :db.error/invalid-attr-spec
              (str "Attribute identifier " x " of class: class java.lang.String"
                   " does not start with a colon")
              {:spec x}))

(def ^:private ^AtomicLong rid-counter (AtomicLong. 0))

;; ------------------------------------------------------------------ xform allowlist

(def builtin-xforms
  "Symbols usable as :xform without configuration (Datomic's built-ins, probed)."
  '#{str name namespace keyword symbol clojure.edn/read-string
     clojure.core/str clojure.core/name clojure.core/namespace clojure.core/keyword
     clojure.core/symbol})

(def ^:dynamic *xforms*
  "Extra symbols allowed as :xform within a dynamic extent (a set of symbols)."
  #{})

(defn- read-extensions
  "Union of the :xforms of every mnemonic/extensions.edn resource on the classpath
  (the analogue of Datomic's datomic/extensions.edn)."
  []
  (let [urls (enumeration-seq (.getResources (clojure.lang.RT/baseLoader) "mnemonic/extensions.edn"))]
    (into #{}
          (mapcat (fn [^java.net.URL u]
                    (let [m (edn/read-string (slurp u))]
                      (when-not (or (nil? m) (map? m))
                        (throw (ex-info (str "Invalid " u ": expected a map")
                                        {:db/error :db.error/invalid-config
                                         :cognitect.anomalies/category :cognitect.anomalies/incorrect})))
                      (map symbol (:xforms m)))))
          urls)))

(def ^:private extension-xforms (delay (read-extensions)))

(defn- allowed-xform? [sym]
  (or (contains? builtin-xforms sym)
      (contains? *xforms* sym)
      (contains? @extension-xforms sym)))

(defn- resolve-xform
  "Function objects, vars and keywords from code are used as given. A symbol (pattern
  data) must be allowed: built in, in *xforms*, or listed under :xforms in
  mnemonic/extensions.edn; an allowed qualified symbol's namespace is required."
  [x]
  (cond
    (nil? x) nil
    (fn? x) x
    (var? x) x
    (keyword? x) x
    (symbol? x)
    (do (when-not (allowed-xform? x)
          (throw (ex-info (str "'" x "' needs to be listed under :xforms in mnemonic/extensions.edn")
                          {:db/error :db.error/xform-not-allowed
                           :cognitect.anomalies/category :cognitect.anomalies/forbidden
                           :xform x})))
        (or (if (namespace x)
              (try (requiring-resolve x) (catch Exception _ nil))
              (ns-resolve 'clojure.core x))
            (throw (spec-error :db.error/invalid-attr-spec
                               (str "Cannot resolve :xform " x) {:xform x}))))
    :else (throw (spec-error :db.error/invalid-attr-spec
                             (str "Cannot interpret as a :xform function: " (pr-str x))
                             {:xform x}))))

(defn- legacy-head? [x]
  (and (symbol? x) (#{"limit" "default"} (name x))))

(defn- parse-options
  "Option map {:as :limit :default :xform} from alternating key/value args."
  [spec-form args]
  (when (odd? (count args))
    (throw (invalid-attr-spec spec-form)))
  (reduce (fn [opts [k v]]
            (case k
              :as (assoc opts :as v)
              :limit (assoc opts :limit (if (or (nil? v) (number? v)) v default-limit))
              :default (assoc opts :default v)
              :xform (assoc opts :xform (resolve-xform v))
              opts))
          {} (partition 2 args)))

(defn- parse-attr-expr
  "[attr-keyword options] for an attribute expression: a keyword, a seq
  `(attr :opt val ...)`, or the legacy `(limit attr n)` / `(default attr v)`."
  [form]
  (cond
    (keyword? form) [form {}]
    (and (sequential? form) (seq form))
    (let [[head & more :as all] form]
      (cond
        (legacy-head? head)
        (let [[a v] more]
          (when-not (keyword? a) (throw (invalid-attr-spec form)))
          [a (if (< (count more) 2)
               {}
               (if (= "limit" (name head))
                 {:limit (if (or (nil? v) (number? v)) v default-limit)}
                 {:default v}))])

        (keyword? head) [head (parse-options form more)]
        (string? head) (throw (entity/not-a-keyword head))
        :else (throw (invalid-attr-spec all))))
    :else (throw (invalid-attr-spec form))))

(declare parse-seq)

(defn- parse-recursion-or-sub
  "[sub recur] for the value of a map spec entry."
  [v]
  (cond
    (= '... v) [nil {:depth nil}]
    (and (integer? v) (pos? (long v))) [nil {:depth (long v)}]
    (or (vector? v) (seq? v) (list? v)) [(parse-seq v) nil]
    :else (throw (spec-error :db.error/invalid-recur-limit
                             (str "Cannot interpret as a recursive pull specification: " v)
                             {:value v}))))

(defn- make-spec [kw opts sub recur]
  (let [fk (entity/reverse-attr-name kw)]
    (map->AttrSpec
     {:attr (or fk kw)
      :reverse? (boolean fk)
      :key (if (contains? opts :as) (:as opts) kw)
      :limit (if (contains? opts :limit) (:limit opts) default-limit)
      :default (:default opts)
      :xform (:xform opts)
      :sub sub
      :recur recur
      :rid (when recur (.incrementAndGet rid-counter))})))

(defn- parse-seq [pattern]
  (when-not (or (sequential? pattern) (nil? pattern))
    (throw (spec-error :db.error/invalid-attr-spec
                       (str "Cannot interpret as a pull pattern: " (pr-str pattern))
                       {:pattern pattern})))
  (let [seen (volatile! #{})
        wildcard? (volatile! false)
        id? (volatile! false)
        specs (volatile! [])
        add! (fn [kw opts sub recur]
               (if (= :db/id kw)
                 (when-not (or sub recur) (vreset! id? true))
                 (let [spec (make-spec kw opts sub recur)
                       k [(:attr spec) (:reverse? spec)]]
                   (when (contains? @seen k)
                     (throw (spec-error :pull/duplicate-attribute
                                        (str "Multiple specifications for " kw)
                                        {:attribute kw})))
                   (vswap! seen conj k)
                   (vswap! specs conj spec))))]
    (doseq [form pattern]
      (cond
        (= '* form) (vreset! wildcard? true)
        (map? form)
        (doseq [[ae v] form]
          (let [[kw opts] (parse-attr-expr ae)
                [sub recur] (parse-recursion-or-sub v)]
            (add! kw opts sub recur)))
        (or (keyword? form) (sequential? form))
        (let [[kw opts] (parse-attr-expr form)]
          (add! kw opts nil nil))
        (string? form) (throw (invalid-attr-identifier form))
        :else (throw (invalid-attr-identifier (str form)))))
    (let [specs @specs]
      (->PullPattern @wildcard? (or @wildcard? @id?) specs
                     (into #{} (comp (remove :reverse?) (map :attr)) specs)
                     (java.util.concurrent.atomic.AtomicReference.)))))

(def ^:private ^ConcurrentHashMap parse-cache (ConcurrentHashMap.))

(defn parse-pattern
  "Pattern -> AST. An AST is returned unchanged. Parsed patterns are cached."
  [pattern]
  (if (instance? PullPattern pattern)
    pattern
    (if (nil? pattern)
      (->PullPattern false false [] #{} (java.util.concurrent.atomic.AtomicReference.))
      ;; keyed by *xforms* too: an AST resolved under a binding must not leak out of it
      (let [k [pattern *xforms*]]
        (or (.get parse-cache k)
            (let [ast (parse-seq pattern)]
              (when (> (.size parse-cache) 1000) (.clear parse-cache))
              (.put parse-cache k ast)
              ast))))))

(def ^:private wildcard-ast (parse-seq '[*]))

;; ------------------------------------------------------------------ evaluation

(def ^:private empty-state {:seen #{} :depths {} :revisit false :comp #{}})

(declare pull-entity)

(defn- ref-attr? [^Attr attr]
  (= :db.type/ref (:value-type attr)))

(defn- default-ref-value
  "A ref target of `parent` without a sub-pattern: components expand, other refs are
  {:db/id}. A component already being expanded on this path (a component cycle, where
  Datomic overflows the stack) is rendered as {:db/id}."
  [db sch ^Attr attr parent eid st]
  (if (and (:component? attr) (not (contains? (:comp st) eid)) (not= parent eid))
    (pull-entity db sch eid wildcard-ast
                 (assoc empty-state :comp (conj (:comp st) parent eid)))
    {:db/id eid}))

(defn- child-state
  "The state the children of `spec` on node `eid` are pulled with: `st`, except for a
  recursive spec, where a revisited node (already expanded along this path) or one past
  the depth limit gets ::leaf (its children are not expanded), and any other gets `st`
  with `eid` seen and the depth counted down. The same for every child, so computed
  once per attribute."
  [eid st ^AttrSpec spec]
  (if-not (:recur spec)
    st
    (let [depth (:depth (:recur spec))
          remaining (when depth (get (:depths st) (:rid spec) depth))]
      (if (or (:revisit st) (and remaining (<= (long remaining) 0)))
        ::leaf
        (cond-> (update st :seen conj eid)
          remaining (assoc-in [:depths (:rid spec)] (dec (long remaining))))))))

(defn- child-value
  "The pulled value of target `c` of a ref (or, for sub-patterns, any) attribute of `eid`;
  `cst` is `(child-state eid st spec)`. A recursive child at a ::leaf is rendered as
  {:db/id c} when the pattern pulls :db/id and dropped otherwise."
  [db sch eid ^Attr attr c ast st cst ^AttrSpec spec]
  (cond
    (:recur spec) (when (integer? c)
                    (if (identical? cst ::leaf)
                      (when (:id? ast) {:db/id (long c)})
                      (pull-entity db sch (long c) ast cst)))
    (:sub spec) (cond
                  (integer? c) (pull-entity db sch (long c) (:sub spec) st)
                  (string? c) (throw (entity/not-a-keyword c))
                  :else nil)
    (:reverse? spec) {:db/id (long c)}
    (ref-attr? attr) (default-ref-value db sch attr eid (long c) st)
    :else c))

;; ------------------------------------------------------------------ compiled patterns

;; A spec resolved against a schema: its attribute (nil when not installed) and what
;; evaluation reads of it for every entity.
(deftype CSpec [^AttrSpec spec ^Attr attr ^long aid ^boolean many? ^boolean ref? ^boolean comp?])

;; A pattern resolved against schema `sch`. `fwd`: the sorted ids of the attributes its
;; specs read from the entity itself (not reverse ones); `one-read?`: an entity is read in
;; one pass for them (`read-entry`) rather than once per attribute.
(deftype Compiled [sch ^objects cspecs ^longs fwd ^boolean wild? ^boolean one-read?])

(defn- compile-ast ^Compiled [^PullPattern ast sch]
  (let [cs (object-array
            (for [^AttrSpec s (:attrs ast)]
              (let [^Attr a (schema/attr sch (:attr s))]
                (CSpec. s a (if a (long (:id a)) -1)
                        (boolean (and a (= :db.cardinality/many (:cardinality a))))
                        (boolean (and a (ref-attr? a)))
                        (boolean (and a (:component? a)))))))
        fwd (long-array (sort (distinct (for [^CSpec c cs
                                              :when (and (.-attr c) (not (:reverse? (.-spec c))))]
                                          (.-aid c)))))]
    (Compiled. sch cs fwd (boolean (:wildcard? ast)) (>= (alength fwd) 2))))

(defn- compiled
  "`ast` resolved against `sch`: kept on the AST for the last schema it was pulled with
  (a db's schema object is shared by its successors until the schema changes)."
  ^Compiled [^PullPattern ast sch]
  (let [^java.util.concurrent.atomic.AtomicReference r (.-cc ast)
        ^Compiled c (.get r)]
    (if (and c (identical? sch (.-sch c)))
      c
      (let [c (compile-ast ast sch)]
        (.set r c)
        c))))

;; ------------------------------------------------------------------ reading an entity

;; Datoms of one entity, in EAVT order (so grouped by attribute): attribute ids and values.
(deftype Entry [^ArrayList as ^ArrayList vs])

(defn- wanted?
  "Whether `a` is in the sorted `fwd` (nil: every attribute is)."
  [^longs fwd ^long a]
  (or (nil? fwd)
      (loop [i 0]
        (if (< i (alength fwd))
          (let [x (aget fwd i)]
            (cond (== x a) true
                  (> x a) false
                  :else (recur (inc i))))
          false))))

(def ^:private entry-cap
  "A pattern reads an entity with more datoms than this one attribute at a time instead
  (a wide cardinality-many attribute would cost more to walk than the reads it saves)."
  512)

(defn- read-entry
  "The asserted datoms of `eid` whose attribute is in `fwd` (nil: all of them), read in one
  pass over the entity; values of other attributes are not decoded. nil when `fwd` is
  given and the entity has more than `entry-cap` datoms."
  [db ^long eid ^longs fwd]
  (let [as (ArrayList.)
        vs (ArrayList.)
        r (index/reduce-views (index/datoms-iterable db :eavt (entity/e-prefix eid))
                              (fn [^long n ^IDatomView d]
                                (if (and fwd (>= n (long entry-cap)))
                                  (reduced -1)
                                  (do (when (.isAdded d)
                                        (let [a (.attr d)]
                                          (when (wanted? fwd a)
                                            (.add as a)
                                            (.add vs (.value d)))))
                                      (inc n))))
                              0)]
    (when-not (== -1 (long r))
      (Entry. as vs))))

(def ^:private prefetch-min
  "Fewer entities than this are read one at a time (through the segment cache)."
  64)

(defn- cached-entry
  "The datoms of `e` whose attribute is in `fwd` (nil: all), when its segment is cached
  (read straight from it), else nil."
  [db ^long e ^longs fwd]
  (when (instance? IFastReduce db)
    (let [as (ArrayList.)
          vs (ArrayList.)
          r (try (.fastReduce ^IFastReduce db :eavt (entity/e-prefix e)
                              (fn [_ ^IDatomView d]
                                (when (.isAdded d)
                                  (let [a (.attr d)]
                                    (when (wanted? fwd a)
                                      (.add as a)
                                      (.add vs (.value d)))))
                                nil)
                              nil)
                 (finally (Reference/reachabilityFence db)))]
      (when-not (identical? r index/miss)
        (Entry. as vs)))))

(defn- scan-entries!
  "Adds to `out` the datoms `fwd` names of each entity of `ids`, read in one EAVT range
  scan from the smallest id to the largest, when they are dense (span at most 4x their
  count); else leaves them to be read one at a time."
  [db ^java.util.Collection ids ^longs fwd ^HashMap out]
  (let [n (.size ids)
        it (.iterator ids)
        [lo hi] (loop [lo Long/MAX_VALUE hi Long/MIN_VALUE]
                  (if (.hasNext it)
                    (let [e (long (.next it))] (recur (min lo e) (max hi e)))
                    [lo hi]))
        lo (long lo)
        hi (long hi)]
    (when (<= (- hi lo) (* 4 n))
      (let [scanned (HashMap.)
            _ (doseq [e ids] (.put scanned e (Entry. (ArrayList.) (ArrayList.))))
            ^bytes stop (entity/e-prefix (inc hi))
            ^mnemonic.index.ICursor c (index/-seek-view db :eavt (entity/e-prefix lo))]
        (try
          (loop []
            (when (and (.valid c)
                       (neg? (Arrays/compareUnsigned (index/key-buf c) 0 (unchecked-int (index/key-len c))
                                                     stop 0 (alength stop))))
              (let [^IDatomView d (index/view-of c)]
                (when (.isAdded d)
                  (let [a (.attr d)]
                    (when (wanted? fwd a)
                      (when-let [^Entry en (.get scanned (.eid d))]
                        (.add ^ArrayList (.-as en) a)
                        (.add ^ArrayList (.-vs en) (.value d)))))))
              (.advance c)
              (recur)))
          (finally
            (.close c)
            (Reference/reachabilityFence db)))
        (.putAll out scanned)))))

(defn- prefetch
  "The datoms `cp` reads of each entity of `ids` (entity ids), as a HashMap eid -> Entry:
  those whose segment is cached straight from it, and the rest, when there are many dense
  ones, in one EAVT range scan (an entity missing from the map is read on its own). nil
  when there are fewer than `prefetch-min` ids or the pattern reads nothing of the
  entities themselves."
  [db ^java.util.Collection ids ^Compiled cp]
  (let [n (.size ids)
        ^longs fwd (when-not (.-wild? cp) (.-fwd cp))]
    (when (and (>= n (long prefetch-min)) (or (nil? fwd) (pos? (alength fwd))))
      (let [out (HashMap.)
            missed (ArrayList.)]
        (doseq [e ids]
          (if-let [en (cached-entry db (long e) fwd)]
            (.put out e en)
            (.add missed e)))
        (when (>= (.size missed) (long prefetch-min))
          (scan-entries! db missed fwd out))
        out))))

(defn- pre-values
  "ArrayList of the first `limit` (nil: all) values of attribute `aid` in a read entity,
  or nil when it has none (as `entity/values-by-prefix`)."
  [^Entry en ^long aid limit]
  (let [^ArrayList as (.-as en)
        ^ArrayList vs (.-vs en)
        limit (if limit (long limit) Long/MAX_VALUE)
        n (.size as)]
    (when (pos? limit)
      (let [out (ArrayList.)]
        (loop [i 0]
          (when (and (< i n) (< (.size out) limit))
            (when (== aid (long (.get as i))) (.add out (.get vs i)))
            (recur (inc i))))
        (when-not (.isEmpty out) out)))))

(defn- pre-first
  "The first value of attribute `aid` in a read entity, or ::entity/no-datom (as
  `entity/first-value`)."
  [^Entry en ^long aid]
  (let [^ArrayList as (.-as en)
        n (.size as)]
    (loop [i 0]
      (cond
        (== i n) ::entity/no-datom
        (== aid (long (.get as i))) (.get ^ArrayList (.-vs en) i)
        :else (recur (inc i))))))

;; ------------------------------------------------------------------ specs

(defn- collect-children
  "Vector of the non-nil `child-value`s of the values (or entity ids) `vs`, or nil when
  empty. `vs` was read before this runs, so nested pulls hold no cursor of ours open.
  Many dense ref targets pulled through a sub-pattern are read in one range scan first
  (`prefetch`), handed to their pulls in the state as :pre (for that sub-pattern only)."
  [db sch eid ^Attr attr ^ArrayList vs ast st ^AttrSpec spec]
  (let [n (.size vs)
        cst (child-state eid st spec)
        sub (:sub spec)
        st (if-let [pre (when (and sub (not (:recur spec)) (ref-attr? attr))
                          (prefetch db vs (compiled sub sch)))]
             (assoc st :pre pre :pre-ast sub)
             st)]
    (loop [i 0 out (transient [])]
      (if (< i n)
        (let [r (child-value db sch eid attr (.get vs i) ast st cst spec)]
          (recur (inc i) (if (nil? r) out (conj! out r))))
        (let [out (persistent! out)]
          (when (pos? (count out)) out))))))

(defn- spec-raw
  "Value of `c` for `eid` before :xform/:default, or nil when absent. `entry` holds the
  entity's datoms of the pattern's attributes (`read-entry`, `prefetch`), or nil to read
  each one."
  [db sch eid ast st ^Entry entry ^CSpec c]
  (let [^Attr attr (.-attr c)
        ^AttrSpec spec (.-spec c)]
    (when attr
      (let [aid (.-aid c)
            limit (:limit spec)]
        (if (:reverse? spec)
          (when (.-ref? c)
            (let [prefix (entity/ea-prefix eid aid)]
              (if (.-comp? c)
                ;; component: only the first datom is read
                (when-let [vs (entity/values-by-prefix db :vaet prefix 1 true)]
                  (first (collect-children db sch eid attr vs ast st spec)))
                (when-let [vs (entity/values-by-prefix db :vaet prefix limit true)]
                  (collect-children db sch eid attr vs ast st spec)))))
          (if (.-many? c)
            (when-let [vs (if entry
                            (pre-values entry aid limit)
                            (entity/values-by-prefix db :eavt (entity/ea-prefix eid aid) limit false))]
              (collect-children db sch eid attr vs ast st spec))
            (let [v (if entry
                      (pre-first entry aid)
                      (entity/first-value db :eavt (entity/ea-prefix eid aid)))]
              (when-not (identical? v ::entity/no-datom)
                (child-value db sch eid attr v ast st (child-state eid st spec) spec)))))))))

(defn- spec-value [db sch eid ast st entry ^CSpec c]
  (let [^AttrSpec spec (.-spec c)
        raw (spec-raw db sch eid ast st entry c)
        v (if-let [xf (:xform spec)] (xf raw) raw)]
    (if (and (nil? v) (some? (:default spec)))
      (:default spec)
      v)))

(defn- wildcard-into!
  "The transient map `m` with every attribute of `entry` (all of `eid`'s datoms) not
  overridden by an explicit spec; components expand after the entity was read."
  [m db sch eid override st ^Entry entry]
  (let [^ArrayList as (.-as entry)
        ^ArrayList vs (.-vs entry)
        n (.size as)
        attrs (:attrs sch)]
    (loop [i 0 m m]
      (if (< i n)
        (let [a (long (.get as i))
              j (loop [j (inc i)]
                  (if (and (< j n) (== a (long (.get as j)))) (recur (inc j)) j))
              ^Attr attr (get attrs a)]
          (recur (long j)
                 (if (and attr (not (contains? override (:ident attr))))
                   (let [ref? (ref-attr? attr)]
                     (assoc! m (:ident attr)
                             (if (= :db.cardinality/many (:cardinality attr))
                               (let [end (min (long j) (+ i (long default-limit)))]
                                 (loop [k i out (transient [])]
                                   (if (< k end)
                                     (let [x (.get vs k)]
                                       (recur (inc k) (conj! out (if ref? (default-ref-value db sch attr eid (long x) st) x))))
                                     (persistent! out))))
                               (let [x (.get vs i)]
                                 (if ref? (default-ref-value db sch attr eid (long x) st) x)))))
                   m)))
        m))))

(defn- pull-entity
  "Map for `eid` under `ast`, or nil when it has no content."
  [db sch eid ^PullPattern ast st]
  (let [cp (compiled ast sch)
        st (if (and (not (:revisit st)) (contains? (:seen st) eid)) (assoc st :revisit true) st)
        ;; this entity's datoms: prefetched by its parent for this pattern, or read once here
        entry (or (when-let [^HashMap pre (:pre st)]
                    (when (identical? ast (:pre-ast st)) (.get pre eid)))
                  (cond (:wildcard? ast) (read-entry db eid nil)
                        (.-one-read? cp) (read-entry db eid (.-fwd cp))))
        ^objects cs (.-cspecs cp)
        n (alength cs)
        m0 (if (:id? ast) {:db/id eid} {})
        add-specs (fn [m add]
                    (loop [i 0 acc m]
                      (if (< i n)
                        (let [^CSpec c (aget cs i)
                              k (:key (.-spec c))]
                          (recur (inc i)
                                 (if (contains? acc k)
                                   acc
                                   (let [v (spec-value db sch eid ast st entry c)]
                                     (if (nil? v) acc (add acc k v))))))
                        acc)))]
    ;; a transient pays off for a wildcard or many attributes; a few assocs are cheaper
    (if (or (:wildcard? ast) (> n 8))
      (let [t (transient m0)
            t (if (:wildcard? ast) (wildcard-into! t db sch eid (:override ast) st entry) t)
            m (add-specs t assoc!)]
        (when (pos? (count m)) (persistent! m)))
      (let [m (add-specs m0 assoc)]
        (when (pos? (count m)) m)))))

(defn- check-not-history [db]
  (when (index/-history? db)
    (throw (IllegalStateException. "Can't pull from history"))))

(defn- pull-from-ast*
  [db ast eid st]
  (let [ast (parse-pattern ast)
        e (entity/resolve-eid db eid)]
    (if (nil? e)
      (when (and (nil? eid) (:id? ast)) {:db/id nil})
      (pull-entity db (index/-schema db) e ast st))))

(defn pull-from-ast
  "Pulls `eid` (a long, or anything `entity/resolve-eid` accepts) from `db` under a
  parsed pattern. For use by the query engine."
  [db ast eid]
  (pull-from-ast* db ast eid empty-state))

(defn pull
  "Pulls `eid` (id, ident or lookup ref) from `db` using `pattern`."
  [db pattern eid]
  (check-not-history db)
  (when (some? pattern)
    (pull-from-ast db (parse-pattern pattern) eid)))

(defn pull-many
  "Pulls each of `eids`; a vector of results (nil for entities with no content)."
  [db pattern eids]
  (check-not-history db)
  (let [ast (parse-pattern pattern)
        ;; many dense ids are read in one range scan first (see `prefetch`)
        ids (into [] (keep #(when (integer? %) (long %))) eids)
        st (if-let [pre (prefetch db ids (compiled ast (index/-schema db)))]
             (assoc empty-state :pre pre :pre-ast ast)
             empty-state)]
    (mapv #(pull-from-ast* db ast % st) eids)))
