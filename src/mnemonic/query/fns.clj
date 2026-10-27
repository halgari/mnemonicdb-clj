(ns mnemonic.query.fns
  "Query functions: the built-in allowlist (Datomic's), function symbol resolution, and
  the db-aware built-ins (get-else, get-some, missing?). Also entity / value resolution
  shared with the engine."
  (:require [clojure.string :as str]
            [mnemonic.entity :as entity]
            [mnemonic.index :as index]
            [mnemonic.tx :as tx])
  (:import (clojure.lang Reflector Var)
           (java.util Arrays)
           (java.util.concurrent ConcurrentHashMap)
           (mnemonic.datom IDatomView)
           (mnemonic.schema Attr)))

(set! *warn-on-reflection* true)

;; ------------------------------------------------------------------ errors

(defn qerror
  "ex-info shaped like Datomic's query errors."
  ([kind msg] (tx/error kind msg nil))
  ([kind msg data] (tx/error kind msg data)))

(defn cannot-resolve [x]
  (qerror :db.error/not-an-entity (str "Cannot resolve key: " (pr-str x)) {:entity x}))

;; ------------------------------------------------------------------ entity / value resolution

(defn- resolve-eid-slow ^long [db x]
  (let [sch (index/-schema db)]
    (cond
      (integer? x) (long x)
      (keyword? x) (long (or (get (:idents sch) x) (throw (cannot-resolve x))))
      (vector? x) (long (or (tx/lookup-ref-eid {:db db :schema sch} x) (throw (cannot-resolve x))))
      (string? x) (throw (qerror :db.error/not-a-keyword
                                 (str "Cannot interpret as a keyword: " x ", no leading :")
                                 {:value x}))
      :else (throw (cannot-resolve x)))))

(defn resolve-eid
  "Entity id for a long, ident keyword or lookup ref. Throws when it does not resolve."
  ^long [db x]
  ;; a plain id (most tuples) needs no schema
  (if (instance? Long x)
    (.longValue ^Long x)
    (resolve-eid-slow db x)))

(defn normalize-value
  "Canonical form of a value used for matching when the attribute is not known."
  [v]
  (if (or (instance? Integer v) (instance? Short v) (instance? Byte v)) (long v) v))

(defn resolve-value
  "Value to match against datoms of `attr` (nil attr = unknown), or ::none when `v`
  cannot match anything. Ref attributes resolve idents and lookup refs."
  [db ^Attr attr v]
  (case (:value-type attr)
    :db.type/ref (cond
                   (integer? v) (long v)
                   (or (keyword? v) (vector? v)) (resolve-eid db v)
                   :else ::none)
    :db.type/bigint (if (integer? v) (biginteger v) v)
    (normalize-value v)))

(defn- first-value
  "Value of the first current datom of [e a], or ::none (decoded in place; no Datom)."
  [db ^long e ^long a]
  (index/reduce-views (index/datoms-iterable db :eavt (entity/ea-prefix e a))
                      (fn [_ ^IDatomView d] (reduced (.value d)))
                      ::none))

(defn- has-datom?
  "Whether [e a] has a current datom (nothing is decoded)."
  [db ^long e ^long a]
  (index/reduce-views (index/datoms-iterable db :eavt (entity/ea-prefix e a))
                      (fn [_ _] (reduced true))
                      false))

(defn- attr-of [db a]
  (tx/resolve-attr (index/-schema db) a))

;; ------------------------------------------------------------------ db-aware built-ins

(defn get-else [db e a default]
  (when (nil? default)
    (throw (qerror :db.error/invalid-query "get-else default value can't be nil")))
  (let [^Attr attr (attr-of db a)]
    (when (= :db.cardinality/many (:cardinality attr))
      (throw (qerror :db.error/invalid-query
                     (str "get-else does not support cardinality-many attribute " (:ident attr)))))
    (let [v (first-value db (resolve-eid db e) (:id attr))]
      (if (identical? v ::none) default v))))

(defn get-some [db e & attrs]
  (let [eid (resolve-eid db e)]
    (reduce (fn [_ a]
              (let [^Attr attr (attr-of db a)
                    v (first-value db eid (:id attr))]
                (when-not (identical? v ::none)
                  (reduced [(:id attr) v]))))
            nil
            attrs)))

(defn missing? [db e a]
  (let [sch (index/-schema db)
        aid (if (keyword? a) (get (:idents sch) a) (when (integer? a) (long a)))]
    (or (nil? aid)
        (not (has-datom? db (resolve-eid db e) aid)))))

(defn- differ?
  "True when the first half of the arguments differs from the second half."
  [& args]
  (let [v (vec args)
        n (quot (count v) 2)]
    (loop [i 0]
      (cond
        (== i n) (not (== (count v) (* 2 n)))   ; an odd extra argument differs from nothing
        (= (nth v i) (nth v (+ i n))) (recur (inc i))
        :else true))))

(defmacro ^:private both-long? [a b] `(and (instance? Long ~a) (instance? Long ~b)))

(defn- class-rank
  "Rank of the class of non-nil `x` in Datomic's order across classes (measured against
  Datomic: values of different classes order by class, numbers sharing one rank and
  comparing by value). Unknown classes rank last."
  ^long [x]
  (cond
    (instance? Number x) (if (instance? clojure.lang.Ratio x) 0 1)
    (string? x) 10
    (keyword? x) 6
    (instance? java.util.Date x) (cond (instance? java.sql.Timestamp x) 12
                                       :else 13)
    (instance? java.util.UUID x) 14
    (sequential? x) 2
    (map? x) 3
    (set? x) 4
    (bytes? x) 5
    (symbol? x) 7
    (instance? Boolean x) 8
    (instance? Character x) 9
    (instance? java.net.URI x) 11
    :else 15))

(defn- order-within-class
  "Order of two non-nil values of one class rank. Sequential values compare as vectors."
  ^long [a b]
  (cond
    (sequential? a) (long (compare (vec a) (vec b)))
    (bytes? a) (let [^bytes x a ^bytes y b
                     c (Integer/compare (alength x) (alength y))]
                 (if (zero? c) (long (Arrays/compare x y)) (long c)))
    :else (long (compare a b))))

(defn compare-values
  "Total order used by the query comparison built-ins (`<` `>` `<=` `>=`) as in Datomic:
  nil first, values of one class by `compare`, values of different classes by class rank
  (numbers of any class compare by value, NaN compares equal to every number)."
  ^long [a b]
  (cond
    (nil? a) (if (nil? b) 0 -1)
    (nil? b) 1
    :else
    (let [ra (class-rank a) rb (class-rank b)]
      (if (== ra rb)
        (order-within-class a b)
        (Long/compare ra rb)))))

(defmacro ^:private cmp-fast
  "Value of `slow` unless `a` and `b` are both Long / both String."
  [a b long-op str-op slow]
  `(cond (both-long? ~a ~b) (~long-op (long ~a) (long ~b))
         (and (string? ~a) (string? ~b)) (~str-op (.compareTo ~(with-meta a {:tag 'String}) ~(with-meta b {:tag 'String})) 0)
         :else ~slow))

(defn- lt [a b] (cmp-fast a b < < (neg? (compare-values a b))))
(defn- gt [a b] (cmp-fast a b > > (pos? (compare-values a b))))
(defn- lte [a b] (cmp-fast a b <= <= (not (pos? (compare-values a b)))))
(defn- gte [a b] (cmp-fast a b >= >= (not (neg? (compare-values a b)))))

(defn- unsupported-fulltext [& _]
  (throw (qerror :db.error/unsupported "fulltext is not supported")))

;; ------------------------------------------------------------------ allowlist

(def builtins
  "Unqualified function symbol -> function, Datomic's query built-ins."
  {'= =, '== ==, 'not= not=, '!= not=
   '< lt, '> gt, '<= lte, '>= gte
   '+ +, '- -, '* *, '/ /, 'quot quot, 'rem rem, 'mod mod, 'inc inc, 'dec dec
   'max max, 'min min
   'zero? zero?, 'pos? pos?, 'neg? neg?, 'even? even?, 'odd? odd?
   'compare compare, 'rand rand, 'rand-int rand-int
   'true? true?, 'false? false?, 'nil? nil?, 'some? some?, 'not not, 'identity identity
   'str str, 'subs subs, 'get get, 'count count, 'range range
   'vector vector, 'list list, 'hash-map hash-map, 'hash-set hash-set
   'keyword keyword, 'symbol symbol, 'name name, 'namespace namespace, 'type type, 'hash hash
   'ground identity, 'tuple vector, 'untuple identity
   're-find re-find, 're-matches re-matches, 're-seq re-seq, 're-pattern re-pattern
   '-differ? differ?
   'get-else get-else, 'get-some get-some, 'missing? missing?
   'fulltext unsupported-fulltext})

;; ------------------------------------------------------------------ resolution

(def ^:private ^ConcurrentHashMap fn-cache (ConcurrentHashMap.))

(defn- find-class ^Class [^String n]
  (try (Class/forName n)
       (catch ClassNotFoundException _
         (try (Class/forName (str "java.lang." n))
              (catch ClassNotFoundException _ nil)))))

(defn- unresolvable [sym ^Throwable cause]
  (ex-info (str "Unable to resolve function: " sym)
           {:db/error :db.error/invalid-query
            :cognitect.anomalies/category :cognitect.anomalies/incorrect
            :cognitect.anomalies/message (str "Unable to resolve function: " sym)
            :function sym}
           cause))

(defn- resolve-fn* [sym]
  (let [ns-part (namespace sym)
        nm (name sym)]
    (cond
      ns-part
      (if-let [cls (find-class ns-part)]
        (fn [& args] (Reflector/invokeStaticMethod ^Class cls ^String nm (object-array args)))
        (or (try (requiring-resolve sym)
                 (catch Exception e (throw (unresolvable sym e))))
            (throw (unresolvable sym nil))))

      (and (> (count nm) 1) (str/starts-with? nm "."))
      (let [m (subs nm 1)]
        (fn [obj & args] (Reflector/invokeInstanceMethod obj ^String m (object-array args))))

      :else
      (let [r (ns-resolve (the-ns 'clojure.core) sym)]
        (if (instance? Var r) r (throw (unresolvable sym nil)))))))

(defn fn-var?
  "True when function symbol `sym` names a query variable (`?f`) holding the function."
  [sym]
  (and (symbol? sym) (nil? (namespace sym)) (str/starts-with? (name sym) "?")))

(defn resolve-fn
  "Function for query symbol `sym`: the allowlist, else a clojure.core fn, a fully
  qualified (auto-required) fn, `Class/staticMethod` or `.instanceMethod`."
  [sym]
  (or (get builtins sym)
      (.get fn-cache sym)
      (let [f (resolve-fn* sym)]
        (.put fn-cache sym f)
        f)))
