(ns mnemonic.tx-fns
  "Transaction functions: `d/function` objects, `d/invoke`, the built-in :db/cas and
  :db/retractEntity functions, and expansion of tx-data lists that call a function (a
  classpath fn named by a qualified symbol, or a stored :db/fn named by an ident or id).
  Functions receive the db-before of the transaction and return tx-data, which is expanded
  recursively."
  (:require [clojure.edn :as edn]
            [mnemonic.index :as index]
            [mnemonic.tx :as tx])
  (:import (java.util ArrayList HashSet LinkedHashMap)
           (java.util.concurrent ConcurrentHashMap)
           (mnemonic.datom Datom)
           (mnemonic.schema Attr)))

(set! *warn-on-reflection* true)

;; ------------------------------------------------------------------ fn objects

(declare compile-fn)

(defrecord DbFn [lang imports requires params code]
  clojure.lang.IFn
  (invoke [this] ((compile-fn this)))
  (invoke [this a] ((compile-fn this) a))
  (invoke [this a b] ((compile-fn this) a b))
  (invoke [this a b c] ((compile-fn this) a b c))
  (invoke [this a b c d] ((compile-fn this) a b c d))
  (invoke [this a b c d e] ((compile-fn this) a b c d e))
  (invoke [this a b c d e f] ((compile-fn this) a b c d e f))
  (invoke [this a b c d e f g] ((compile-fn this) a b c d e f g))
  (invoke [this a b c d e f g h] ((compile-fn this) a b c d e f g h))
  (invoke [this a b c d e f g h i] ((compile-fn this) a b c d e f g h i))
  (invoke [this a b c d e f g h i j] ((compile-fn this) a b c d e f g h i j))
  (applyTo [this args] (apply (compile-fn this) args)))

(defmethod print-method DbFn [f ^java.io.Writer w]
  (.write w "#db/fn")
  (.write w (pr-str (into {} f))))

(defn- code-text ^String [code]
  (if (string? code) code (pr-str code)))

(defn function
  "Builds a function object from {:lang :params :code [:requires] [:imports]}. The code is
  kept as text and evaluated on first use."
  [m]
  (let [{:keys [lang params code requires imports]} m]
    (when-not (and lang params code)
      (throw (IllegalArgumentException. "Must supply lang, params and code")))
    (->DbFn (keyword (name lang)) (vec imports) (vec requires) (vec params) (code-text code))))

(defn- fn->string ^String [f]
  (binding [*print-length* nil *print-level* nil *print-meta* false]
    (pr-str (into {} f))))

(defn ->stored
  "The stored (string) form of a :db/fn value, or nil when `v` is not a function object or a
  valid stored function."
  [v]
  (cond
    (instance? DbFn v) (fn->string v)
    (string? v) (try
                  (let [m (edn/read-string v)]
                    (when (and (map? m) (:lang m) (vector? (:params m)) (string? (:code m)))
                      v))
                  (catch Exception _ nil))
    :else nil))

(defn- stored->fn ^DbFn [^String s]
  (let [m (edn/read-string s)]
    (->DbFn (keyword (name (:lang m))) (vec (:imports m)) (vec (:requires m))
            (vec (:params m)) (:code m))))

;; Compiled functions are cached (bounded, least recently used) by their definition, so a
;; stored function is evaluated once however often it is called. Each compiled function
;; lives in its own namespace, which is removed when the entry is evicted.
(def ^:private cache-limit 1000)

(defonce ^:private ^java.util.LinkedHashMap compiled
  (proxy [java.util.LinkedHashMap] [16 (float 0.75) true]
    (removeEldestEntry [eldest]
      (let [evict? (> (.size ^java.util.Map this) cache-limit)]
        (when evict?
          (remove-ns (second (.getValue ^java.util.Map$Entry eldest))))
        evict?))))

(defn- compile* [^DbFn f ns-sym]
  (when-not (= :clojure (:lang f))
    (throw (tx/error :db.error/unsupported (str "Unsupported function language " (:lang f))
                     :cognitect.anomalies/unsupported {:lang (:lang f)})))
  (binding [*ns* (create-ns ns-sym)]
    (refer-clojure)
    (doseq [r (:requires f)] (require r))
    (doseq [i (:imports f)] (eval (list 'clojure.core/import (list 'quote i))))
    (let [body (read-string (str "(do " (:code f) "\n)"))]
      (eval (list 'fn (vec (:params f)) body)))))

(defn- cached-fn
  "The compiled function for cache key `k`; `make-def` builds its DbFn on a miss."
  [k make-def]
  (or (locking compiled (first (.get compiled k)))
      (let [^DbFn f (make-def)
            ns-sym (gensym "mnemonic.txfn")
            c (try (compile* f ns-sym)
                   (catch Throwable t (remove-ns ns-sym) (throw t)))]
        (locking compiled
          (if-some [existing (.get compiled k)]
            (do (remove-ns ns-sym) (first existing))
            (do (.put compiled k [c ns-sym]) c))))))

(defn- compile-fn [^DbFn f] (cached-fn f (fn [] f)))

;; ------------------------------------------------------------------ built-ins

(defn- db-ctx [db] {:db db :schema (index/-schema db)})

(defn- entid-or-nil
  "d/entid semantics used by the built-in functions: ids and idents resolve (nil when
  unknown), lookup refs resolve (nil when absent), strings are not keywords; anything
  else is nil."
  [db x]
  (cond
    (integer? x) (long x)
    (keyword? x) (get (:idents (index/-schema db)) x)
    (vector? x) (tx/lookup-ref-eid (db-ctx db) x)
    (string? x) (throw (tx/error :db.error/not-a-keyword
                                 (str "Cannot interpret as a keyword: " x ", no leading :")
                                 {:value x}))
    :else nil))

(defn cas
  "Compare-and-swap of a cardinality-one attribute: `[[:db/add e a nv]]` when the current
  value is `ov` (nil: absent), else :db.error/cas-failed."
  [db e a ov nv]
  (let [data {:e e :a a :v-old ov :v-new nv :datomic/cancelled true}]
    (when (or (nil? e) (nil? a) (nil? nv))
      (throw (tx/error :db.error/invalid-cas "entity, attribute, and new-value must be specified"
                       :cognitect.anomalies/incorrect data)))
    (let [schema (index/-schema db)
          ^Attr attr (tx/resolve-attr schema a)]
      (when (= :db.cardinality/many (:cardinality attr))
        (throw (tx/error :db.error/invalid-cas-many "attribute must be cardinality-one"
                         :cognitect.anomalies/incorrect data)))
      (let [eid (or (entid-or-nil db e)
                    ;; an unresolvable entity has no current value: with a non-nil old
                    ;; value the compare fails (as in Datomic) rather than not-an-entity
                    (when (some? ov)
                      (throw (tx/error :db.error/cas-failed (str "Compare failed: " ov " nil")
                                       :cognitect.anomalies/conflict
                                       {:e e :a a :v-old ov :v nil :datomic/cancelled true})))
                    (throw (tx/error :db.error/not-an-entity
                                     (str "Unable to resolve entity: " (pr-str e) " in datom "
                                          (pr-str [e a ov]))
                                     {:entity e :datom [e a ov]})))
            cur (first (tx/datoms-of db :eavt [eid (:id attr)]))
            matches? (if (nil? ov)
                       (nil? cur)
                       (and cur
                            (if (= :db.type/ref (:value-type attr))
                              (and (integer? ov) (== (long ov) (long (:v cur))))
                              (try (tx/same-value? (tx/coerce-value attr ov) (:v cur))
                                   (catch clojure.lang.ExceptionInfo _ false)))))]
        (when-not matches?
          (let [actual (:v cur)]
            (throw (tx/error :db.error/cas-failed (str "Compare failed: " ov " " actual)
                             :cognitect.anomalies/conflict
                             {:e e :a a :v-old ov :v actual :datomic/cancelled true}))))
        [[:db/add e a nv]]))))

(defn retract-entity
  "Retraction forms for every datom of the entity, every reference to it, and (recursively)
  its component entities. Unknown entities retract nothing."
  [db e]
  (let [schema (index/-schema db)
        ids (:ids schema)
        out (LinkedHashMap.)
        seen (HashSet.)
        ident-of (fn [aid] (or (get ids aid) aid))
        add! (fn [^Datom d] (.putIfAbsent out [(.-e d) (.-a d) (tx/vkey (.-v d))]
                                          [:db/retract (.-e d) (ident-of (.-a d)) (.-v d)]))
        visit (fn visit [^long eid]
                (when (.add seen eid)
                  (doseq [^Datom d (tx/datoms-of db :eavt [eid])]
                    (add! d)
                    (let [^Attr attr (get (:attrs schema) (.-a d))]
                      (when (and attr (:component? attr) (= :db.type/ref (:value-type attr)))
                        (visit (long (.-v d))))))
                  (doseq [^Datom d (tx/datoms-of db :vaet [eid])]
                    (add! d))))]
    (when-some [eid (entid-or-nil db e)]
      (visit eid))
    (vec (.values out))))

;; ------------------------------------------------------------------ function lookup

(def ^:private builtin-fns
  {54 retract-entity
   55 cas})

(defn- stored-fn
  "The callable for entity `eid` (built-in or stored :db/fn), or nil."
  [db ^long eid]
  (or (get builtin-fns eid)
      (when-some [d (first (tx/datoms-of db :eavt [eid 52]))]
        (let [v (:v d)]
          (when (string? v)
            (cached-fn v #(stored->fn v)))))))

(defn- lookup-callable
  "[:fn f], [:unresolved] (unknown ident/symbol) or [:not-fn id]."
  [db x]
  (cond
    (symbol? x) (if (qualified-symbol? x)
                  [:fn (or (tx/resolve-callable x)
                           (throw (tx/error :db.error/not-a-data-function
                                            (str "Unable to resolve data function: " x)
                                            :cognitect.anomalies/not-found {:function x})))]
                  [:unresolved])
    (keyword? x) (if-some [eid (get (:idents (index/-schema db)) x)]
                   (if-some [f (stored-fn db eid)] [:fn f] [:not-fn eid])
                   [:unresolved])
    (integer? x) (if-some [f (stored-fn db (long x))] [:fn f] [:not-fn x])
    (string? x) (throw (tx/error :db.error/not-a-keyword
                                 (str "Cannot interpret as a keyword: " x ", no leading :")
                                 {:value x}))
    :else [:unresolved]))

(defn- not-a-data-function [msg x]
  (tx/error :db.error/not-a-data-function msg {:function x}))

(defn invoke
  "Calls the function named by `eid-or-ident` (a built-in or stored :db/fn) with `args`."
  [db eid-or-ident & args]
  (let [[tag f] (lookup-callable db eid-or-ident)]
    (case tag
      :fn (apply f args)
      :not-fn (throw (not-a-data-function (str "Not a data function: " eid-or-ident) eid-or-ident))
      :unresolved (throw (tx/error :db.error/not-an-entity
                                   (str "Unable to resolve entity: " eid-or-ident)
                                   {:entity eid-or-ident})))))

;; ------------------------------------------------------------------ expansion

(defn- call-forms
  "Expands the result of calling `f` with the transaction's db-before and the op's args."
  [ctx f op]
  (tx/expand-forms ctx (apply f (:db ctx) (rest op))))

(defn- check-arity [op n]
  (when-not (= n (count op))
    (throw (tx/error :db.error/invalid-tx-data (str "Wrong number of elements in " (pr-str op))
                     {:op op}))))

(defn expand-call
  "Expands a list-form op whose head is `head`: the built-ins :db/cas and
  :db/retractEntity (and their :db.fn aliases), or a transaction function call. Called by
  the expand-op methods in mnemonic.tx."
  [head ctx op]
  (case head
    (:db/cas :db.fn/cas) (do (check-arity op 5) (call-forms ctx cas op))
    (:db/retractEntity :db.fn/retractEntity) (do (check-arity op 2) (call-forms ctx retract-entity op))
    (let [[tag f] (lookup-callable (:db ctx) head)]
      (case tag
        :fn (call-forms ctx f op)
        :not-fn (throw (not-a-data-function (str "Not a data function: " f) head))
        :unresolved (throw (not-a-data-function (str "Unable to resolve data function: " (pr-str head))
                                                head))))))
