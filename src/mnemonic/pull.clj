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
           (mnemonic.schema Attr)))

(set! *warn-on-reflection* true)

;; ------------------------------------------------------------------ AST

(defrecord PullPattern [wildcard? id? attrs override])

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
                     (into #{} (comp (remove :reverse?) (map :attr)) specs)))))

(def ^:private ^ConcurrentHashMap parse-cache (ConcurrentHashMap.))

(defn parse-pattern
  "Pattern -> AST. An AST is returned unchanged. Parsed patterns are cached."
  [pattern]
  (if (instance? PullPattern pattern)
    pattern
    (if (nil? pattern)
      (->PullPattern false false [] #{})
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

(def ^:private prefetch-min
  "Fewer entities than this are read one at a time (through the segment cache)."
  64)

(defn- prefetch
  "Every asserted datom of each entity of `ids` (entity ids), read in one EAVT range scan
  from the smallest id to the largest, as a HashMap eid -> [attrs values] (the shape of
  `entity/attr-values`, in EAVT order). nil when there are fewer than `prefetch-min` or
  the ids are not dense (they span more than 4x their count): then reading each entity
  on its own is cheaper."
  [db ^java.util.Collection ids]
  (let [n (.size ids)]
    (when (>= n (long prefetch-min))
      (let [it (.iterator ids)
            [lo hi] (loop [lo Long/MAX_VALUE hi Long/MIN_VALUE]
                      (if (.hasNext it)
                        (let [e (long (.next it))] (recur (min lo e) (max hi e)))
                        [lo hi]))
            lo (long lo)
            hi (long hi)]
        (when (<= (- hi lo) (* 4 n))
          (let [out (HashMap.)
                _ (doseq [e ids] (.put out e [(ArrayList.) (ArrayList.)]))
                ^bytes stop (entity/e-prefix (inc hi))
                ^mnemonic.index.ICursor c (index/-seek-view db :eavt (entity/e-prefix lo))]
            (try
              (loop []
                (when (and (.valid c)
                           (neg? (Arrays/compareUnsigned (index/key-buf c) 0 (unchecked-int (index/key-len c))
                                                         stop 0 (alength stop))))
                  (let [^IDatomView d (index/view-of c)]
                    (when (.isAdded d)
                      (when-let [[^ArrayList as ^ArrayList vs] (.get out (.eid d))]
                        (.add as (.attr d))
                        (.add vs (.value d)))))
                  (.advance c)
                  (recur)))
              (finally
                (.close c)
                (Reference/reachabilityFence db)))
            out))))))

(defn- pre-values
  "ArrayList of the first `limit` (nil: all) values of attribute `aid` in a prefetched
  entity, or nil when it has none (as `entity/values-by-prefix`)."
  [[^ArrayList as ^ArrayList vs] ^long aid limit]
  (let [limit (if limit (long limit) Long/MAX_VALUE)
        n (.size as)]
    (when (pos? limit)
      (let [out (ArrayList.)]
        (loop [i 0]
          (when (and (< i n) (< (.size out) limit))
            (when (== aid (long (.get as i))) (.add out (.get vs i)))
            (recur (inc i))))
        (when-not (.isEmpty out) out)))))

(defn- pre-first
  "The first value of attribute `aid` in a prefetched entity, or ::entity/no-datom (as
  `entity/first-value`)."
  [[^ArrayList as ^ArrayList vs] ^long aid]
  (let [n (.size as)]
    (loop [i 0]
      (cond
        (== i n) ::entity/no-datom
        (== aid (long (.get as i))) (.get vs i)
        :else (recur (inc i))))))

(defn- collect-children
  "Vector of the non-nil `child-value`s of the values (or entity ids) `vs`, or nil when
  empty. `vs` was read before this runs, so nested pulls hold no cursor of ours open.
  Many dense ref targets pulled through a sub-pattern are read in one range scan first
  (`prefetch`), handed to their pulls in the state as :pre."
  [db sch eid ^Attr attr ^ArrayList vs ast st ^AttrSpec spec]
  (let [n (.size vs)
        cst (child-state eid st spec)
        st (if-let [pre (when (and (:sub spec) (not (:recur spec)) (ref-attr? attr))
                          (prefetch db vs))]
             (assoc st :pre pre)
             st)]
    (loop [i 0 out (transient [])]
      (if (< i n)
        (let [r (child-value db sch eid attr (.get vs i) ast st cst spec)]
          (recur (inc i) (if (nil? r) out (conj! out r))))
        (let [out (persistent! out)]
          (when (pos? (count out)) out))))))

(defn- spec-raw
  "Value of `spec` for `eid` before :xform/:default, or nil when absent. `entry` is the
  entity's prefetched datoms (`prefetch`), or nil to read them."
  [db sch eid ast st entry ^AttrSpec spec]
  (let [^Attr attr (schema/attr sch (:attr spec))]
    (when attr
      (let [many? (= :db.cardinality/many (:cardinality attr))
            limit (:limit spec)
            prefix (entity/ea-prefix eid (long (:id attr)))]
        (if (:reverse? spec)
          (when (ref-attr? attr)
            (if (:component? attr)
              ;; component: only the first datom is read
              (when-let [vs (entity/values-by-prefix db :vaet prefix 1 true)]
                (first (collect-children db sch eid attr vs ast st spec)))
              (when-let [vs (entity/values-by-prefix db :vaet prefix limit true)]
                (collect-children db sch eid attr vs ast st spec))))
          (if many?
            (when-let [vs (if entry
                            (pre-values entry (long (:id attr)) limit)
                            (entity/values-by-prefix db :eavt prefix limit false))]
              (collect-children db sch eid attr vs ast st spec))
            (let [v (if entry
                      (pre-first entry (long (:id attr)))
                      (entity/first-value db :eavt prefix))]
              (when-not (identical? v ::entity/no-datom)
                (child-value db sch eid attr v ast st (child-state eid st spec) spec)))))))))

(defn- spec-value [db sch eid ast st entry ^AttrSpec spec]
  (let [raw (spec-raw db sch eid ast st entry spec)
        v (if-let [xf (:xform spec)] (xf raw) raw)]
    (if (and (nil? v) (some? (:default spec)))
      (:default spec)
      v)))

(defn- wildcard-into!
  "The transient map `m` with every attribute of `eid` not overridden by an explicit spec.
  The entity's datoms are read first; components expand after the cursor is closed."
  [m db sch eid override st entry]
  (let [[^ArrayList as ^ArrayList vs] (or entry (entity/attr-values db eid))
        n (.size as)]
    (loop [i 0 m m]
      (if (< i n)
        (let [a (long (.get as i))
              j (loop [j (inc i)]
                  (if (and (< j n) (== a (long (.get as j)))) (recur (inc j)) j))
              ^Attr attr (get (:attrs sch) a)]
          (recur (long j)
                 (if (and attr (not (contains? override (:ident attr))))
                   (let [ref? (ref-attr? attr)
                         value (fn [x] (if ref? (default-ref-value db sch attr eid (long x) st) x))]
                     (assoc! m (:ident attr)
                             (if (= :db.cardinality/many (:cardinality attr))
                               (let [end (min (long j) (+ i (long default-limit)))]
                                 (loop [k i out (transient [])]
                                   (if (< k end)
                                     (recur (inc k) (conj! out (value (.get vs k))))
                                     (persistent! out))))
                               (value (.get vs i)))))
                   m)))
        m))))

(defn- pull-entity
  "Map for `eid` under `ast`, or nil when it has no content."
  [db sch eid ^PullPattern ast st]
  (let [st (if (and (not (:revisit st)) (contains? (:seen st) eid)) (assoc st :revisit true) st)
        ;; this entity's datoms, when its parent prefetched them
        entry (when-let [^HashMap pre (:pre st)] (.get pre eid))
        m0 (if (:id? ast) {:db/id eid} {})
        add-specs (fn [m add]
                    (reduce (fn [acc ^AttrSpec spec]
                              (let [k (:key spec)]
                                (if (contains? acc k)
                                  acc
                                  (let [v (spec-value db sch eid ast st entry spec)]
                                    (if (nil? v) acc (add acc k v))))))
                            m (:attrs ast)))]
    ;; a transient pays off for a wildcard or many attributes; a few assocs are cheaper
    (if (or (:wildcard? ast) (> (count (:attrs ast)) 8))
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
        st (if-let [pre (prefetch db ids)] (assoc empty-state :pre pre) empty-state)]
    (mapv #(pull-from-ast* db ast % st) eids)))
