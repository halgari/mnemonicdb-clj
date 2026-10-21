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
  (:import (java.util.concurrent ConcurrentHashMap)
           (java.util.concurrent.atomic AtomicLong)
           (mnemonic.datom Datom)
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

(defn- recursion-child
  "Child `c` of recursive attribute `spec` of node `eid`. A revisited node (already
  expanded along this path) is pulled in revisit mode: its recursive children are not
  expanded. Past the depth limit, or in revisit mode, a child is rendered as {:db/id c}
  when the pattern pulls :db/id and dropped otherwise."
  [db sch eid c ast st ^AttrSpec spec]
  (let [depth (:depth (:recur spec))
        remaining (when depth (get (:depths st) (:rid spec) depth))]
    (if (or (:revisit st) (and remaining (<= (long remaining) 0)))
      (when (:id? ast) {:db/id c})
      (pull-entity db sch c ast
                   (cond-> (update st :seen conj eid)
                     remaining (assoc-in [:depths (:rid spec)] (dec (long remaining))))))))

(defn- child-value
  "The pulled value of target `c` of a ref (or, for sub-patterns, any) attribute of `eid`."
  [db sch eid ^Attr attr c ast st ^AttrSpec spec]
  (cond
    (:recur spec) (when (integer? c) (recursion-child db sch eid (long c) ast st spec))
    (:sub spec) (cond
                  (integer? c) (pull-entity db sch (long c) (:sub spec) st)
                  (string? c) (throw (entity/not-a-keyword c))
                  :else nil)
    (:reverse? spec) {:db/id (long c)}
    (ref-attr? attr) (default-ref-value db sch attr eid (long c) st)
    :else c))

(defn- collect-children
  "Vector of the non-nil `(child-value ... (getter d) ...)` over the datoms `ds`, or nil
  when empty."
  [db sch eid ^Attr attr ds ast st ^AttrSpec spec e?]
  (let [n (count ds)]
    (loop [i 0 out (transient [])]
      (if (< i n)
        (let [^Datom d (nth ds i)
              r (child-value db sch eid attr (if e? (.-e d) (.-v d)) ast st spec)]
          (recur (inc i) (if (nil? r) out (conj! out r))))
        (let [out (persistent! out)]
          (when (pos? (count out)) out))))))

(defn- spec-raw
  "Value of `spec` for `eid` before :xform/:default, or nil when absent."
  [db sch eid ast st ^AttrSpec spec]
  (let [^Attr attr (schema/attr sch (:attr spec))]
    (when attr
      (let [many? (= :db.cardinality/many (:cardinality attr))
            limit (:limit spec)
            prefix (entity/ea-prefix eid (long (:id attr)))]
        (if (:reverse? spec)
          (when (ref-attr? attr)
            (if (:component? attr)
              ;; component: only the first datom is read
              (when-let [ds (entity/datoms-by-prefix db :vaet prefix 1)]
                (first (collect-children db sch eid attr ds ast st spec true)))
              (when-let [ds (entity/datoms-by-prefix db :vaet prefix limit)]
                (collect-children db sch eid attr ds ast st spec true))))
          (if many?
            (when-let [ds (entity/datoms-by-prefix db :eavt prefix limit)]
              (collect-children db sch eid attr ds ast st spec false))
            (let [v (entity/first-value db :eavt prefix)]
              (when-not (identical? v ::entity/no-datom)
                (child-value db sch eid attr v ast st spec)))))))))

(defn- spec-value [db sch eid ast st ^AttrSpec spec]
  (let [raw (spec-raw db sch eid ast st spec)
        v (if-let [xf (:xform spec)] (xf raw) raw)]
    (if (and (nil? v) (some? (:default spec)))
      (:default spec)
      v)))

(defn- wildcard-entries
  "[key value] for every attribute of `eid` not overridden by an explicit spec."
  [db sch eid override st]
  (let [ds (entity/datoms-by-prefix db :eavt (entity/e-prefix eid) nil)]
    (entity/keep-attr-groups
     ds
     (fn [a i j]
       (let [^Attr attr (get (:attrs sch) a)]
         (when (and attr (not (contains? override (:ident attr))))
           (let [value (fn [^Datom d] (if (ref-attr? attr)
                                        (default-ref-value db sch attr eid (long (.-v d)) st)
                                        (.-v d)))
                 i (long i)
                 j (long j)]
             [(:ident attr)
              (if (= :db.cardinality/many (:cardinality attr))
                (let [end (min j (+ i (long default-limit)))]
                  (loop [k i out (transient [])]
                    (if (< k end)
                      (recur (inc k) (conj! out (value (nth ds k))))
                      (persistent! out))))
                (value (nth ds i)))])))))))

(defn- pull-entity
  "Map for `eid` under `ast`, or nil when it has no content."
  [db sch eid ^PullPattern ast st]
  (let [st (if (and (not (:revisit st)) (contains? (:seen st) eid)) (assoc st :revisit true) st)
        m (if (:id? ast) {:db/id eid} {})
        m (if (:wildcard? ast)
            (reduce (fn [acc [k v]] (assoc acc k v)) m (wildcard-entries db sch eid (:override ast) st))
            m)
        m (reduce (fn [acc ^AttrSpec spec]
                    (let [k (:key spec)]
                      (if (contains? acc k)
                        acc
                        (let [v (spec-value db sch eid ast st spec)]
                          (if (nil? v) acc (assoc acc k v))))))
                  m (:attrs ast))]
    (when (seq m) m)))

(defn- check-not-history [db]
  (when (index/-history? db)
    (throw (IllegalStateException. "Can't pull from history"))))

(defn pull-from-ast
  "Pulls `eid` (a long, or anything `entity/resolve-eid` accepts) from `db` under a
  parsed pattern. For use by the query engine."
  [db ast eid]
  (let [ast (parse-pattern ast)
        e (entity/resolve-eid db eid)]
    (if (nil? e)
      (when (and (nil? eid) (:id? ast)) {:db/id nil})
      (pull-entity db (index/-schema db) e ast empty-state))))

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
  (let [ast (parse-pattern pattern)]
    (mapv #(pull-from-ast db ast %) eids)))
