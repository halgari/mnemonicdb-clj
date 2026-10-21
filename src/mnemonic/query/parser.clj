(ns mnemonic.query.parser
  "Parses Datomic Datalog queries and rules into a plain-data AST.

  The AST is specified in docs/query-ast.md. The parser is pure: it knows
  nothing about databases (attribute existence etc. is not validated here).
  Parsed queries and rules are cached by their input form."
  (:require [clojure.edn :as edn]
            [clojure.set :as set]
            [clojure.string :as str])
  (:import (java.util.concurrent ConcurrentHashMap)))

(set! *warn-on-reflection* true)

;; ------------------------------------------------------------------ errors

(defn- fail
  ([msg data] (fail :db.error/invalid-query msg data))
  ([kind msg data]
   (throw (ex-info msg (merge {:db/error kind
                               :cognitect.anomalies/category :cognitect.anomalies/incorrect
                               :cognitect.anomalies/message msg}
                              data)))))

;; -------------------------------------------------------------- primitives

(defn- plain-sym? [x] (and (symbol? x) (nil? (namespace x))))

(defn- var-sym? [x] (and (plain-sym? x) (str/starts-with? (name x) "?")))

(defn- src-sym? [x] (and (plain-sym? x) (str/starts-with? (name x) "$")))

(defn- parse-term [x]
  (cond (var-sym? x) {:type :var :sym x}
        (= '_ x) {:type :blank}
        (src-sym? x) {:type :src :sym x}
        :else {:type :const :val x}))

(defn- term-vars [terms]
  (keep #(when (= :var (:type %)) (:sym %)) terms))

(defn- binding-vars [b]
  (case (:bind b)
    :scalar [(:sym b)]
    (:blank :src :rules) []
    :coll (binding-vars (:elem b))
    (:tuple :rel) (vec (mapcat binding-vars (:elems b)))))

;; ---------------------------------------------------------------- bindings

(defn- scalar-or-blank [x form]
  (cond (var-sym? x) {:bind :scalar :sym x}
        (= '_ x) {:bind :blank}
        :else (fail (str "Invalid binding element " (pr-str x) " in " (pr-str form))
                    {:form form})))

(defn- parse-binding [form]
  (cond
    (or (var-sym? form) (= '_ form)) (scalar-or-blank form form)

    (and (vector? form) (seq form))
    (cond
      (and (= 2 (count form)) (= '... (second form)))
      {:bind :coll :elem (scalar-or-blank (first form) form)}

      (and (= 1 (count form)) (vector? (first form)) (seq (first form)))
      {:bind :rel :elems (mapv #(scalar-or-blank % form) (first form))}

      :else
      {:bind :tuple :elems (mapv #(scalar-or-blank % form) form)})

    :else (fail (str "Invalid binding form " (pr-str form)) {:form form})))

(defn- parse-in-binding [form]
  (cond (= '% form) {:bind :rules}
        (src-sym? form) {:bind :src :sym form}
        (or (var-sym? form) (= '_ form) (vector? form)) (parse-binding form)
        :else (fail (str "Argument " (pr-str form) " in :in is not a source, rule set, or binding")
                    {:form form})))

;; ----------------------------------------------------------------- clauses

(declare parse-clause)

(defn- parse-clauses [forms where]
  (when-not (sequential? forms)
    (fail (str "Expected a sequence of clauses in " where) {:form forms}))
  (mapv parse-clause forms))

(defn- clause-vars [c]
  (case (:clause c)
    :data-pattern (term-vars (:terms c))
    (:pred :rule) (term-vars (:args c))
    :fn (concat (term-vars (:args c)) (binding-vars (:binding c)))
    (:not :not-join :or :or-join) (:vars c)
    :and (mapcat clause-vars (:clauses c))))

(defn- distinct-vars [clauses]
  (vec (distinct (mapcat clause-vars clauses))))

(defn- parse-fn-arg [x call]
  (let [t (parse-term x)]
    (when (= :blank (:type t))
      (fail (str "Blank '_' is not allowed as a function argument in " (pr-str call))
            {:form call}))
    t))

(defn- parse-expr
  "`[(f args*)]` predicate or `[(f args*) binding]` function expression."
  [form]
  (let [call (first form)
        [f & args] call]
    (when (> (count form) 2)
      (fail (str "Can't have anything after binding expression in " (pr-str form))
            {:form form}))
    (when-not (symbol? f)
      (fail (str "Function in expression clause must be a symbol: " (pr-str form))
            {:form form}))
    (let [args (mapv #(parse-fn-arg % call) args)]
      (if (= 1 (count form))
        {:clause :pred :fn f :args args}
        {:clause :fn :fn f :args args :binding (parse-binding (second form))}))))

(defn- parse-pattern [form]
  (let [[src terms] (if (src-sym? (first form))
                      [(first form) (rest form)]
                      ['$ form])]
    (when-not (<= 1 (count terms) 5)
      (fail (str "Data pattern must have between 1 and 5 elements: " (pr-str form))
            {:form form}))
    {:clause :data-pattern :src src :terms (mapv parse-term terms)}))

(defn- parse-join-vars
  "Parses the var vector of not-join/or-join. Returns [required all]."
  [form clause]
  (when-not (and (vector? form) (seq form))
    (fail (str "Expected a non-empty vector of join variables in " clause) {:form form}))
  (let [[required others] (if (vector? (first form))
                            [(first form) (rest form)]
                            [[] form])]
    (doseq [x (concat required others)]
      (when-not (var-sym? x)
        (fail (str "Join variable " (pr-str x) " is not a variable in " clause) {:form form})))
    [(vec required) (vec (concat required others))]))

(defn- parse-not [src args]
  (when (empty? args) (fail "Empty 'not' clause" {}))
  (let [body (parse-clauses args "not")]
    {:clause :not :src src :vars (distinct-vars body) :body body}))

(defn- parse-not-join [src args]
  (let [[_ vars] (parse-join-vars (first args) "not-join")
        body (parse-clauses (rest args) "not-join")]
    (when (empty? body) (fail "Empty 'not-join' clause" {}))
    {:clause :not-join :src src :vars vars :body body}))

(defn- parse-branch [form]
  (if (and (seq? form) (= 'and (first form)))
    (let [cs (parse-clauses (rest form) "and")]
      (when (empty? cs) (fail "Empty 'and' clause" {:form form}))
      cs)
    [(parse-clause form)]))

(defn- parse-or [src args]
  (when (empty? args) (fail "Empty 'or' clause" {}))
  (let [branches (mapv parse-branch args)
        var-sets (mapv #(set (mapcat clause-vars %)) branches)]
    (when-not (apply = var-sets)
      (fail (str "All clauses in 'or' must use same set of vars, had " (pr-str var-sets))
            {:var-sets var-sets}))
    {:clause :or :src src :vars (distinct-vars (apply concat branches))
     :branches branches}))

(defn- parse-or-join [src args]
  (let [[required vars] (parse-join-vars (first args) "or-join")
        branches (mapv parse-branch (rest args))]
    (when (empty? branches) (fail "Empty 'or-join' clause" {}))
    {:clause :or-join :src src :vars vars :required required :branches branches}))

(defn- parse-list-clause [form]
  (let [has-src (src-sym? (first form))
        src (if has-src (first form) '$)
        [op & args] (if has-src (rest form) form)]
    (when-not (plain-sym? op)
      (fail (str "Invalid clause " (pr-str form)) {:form form}))
    (case (name op)
      "not" (parse-not src args)
      "not-join" (parse-not-join src args)
      "or" (parse-or src args)
      "or-join" (parse-or-join src args)
      "and" (do (when has-src (fail "'and' does not take a source" {:form form}))
                (when (empty? args) (fail "Empty 'and' clause" {:form form}))
                {:clause :and :clauses (parse-clauses args "and")})
      {:clause :rule :src src :name op :args (mapv parse-term args)})))

(defn- parse-clause [form]
  (cond
    (and (vector? form) (seq? (first form))) (parse-expr form)
    (vector? form) (parse-pattern form)
    (and (seq? form) (seq form)) (parse-list-clause form)
    :else (fail (str "Argument " (pr-str form) " in :where is not a list or vector")
                {:form form})))

;; ------------------------------------------------------------ binding check

(defn- flatten-and [clauses]
  (mapcat #(if (= :and (:clause %)) (flatten-and (:clauses %)) [%]) clauses))

(defn- branch-vars [c]
  (set (mapcat #(mapcat clause-vars %) (:branches c))))

(defn- ready? [c bound]
  (case (:clause c)
    (:data-pattern :rule :or) true
    (:pred :fn) (every? bound (term-vars (:args c)))
    (:not :not-join) (every? bound (:vars c))
    :or-join (every? bound (:required c))))

(defn- provides [c]
  (case (:clause c)
    (:data-pattern :rule) (term-vars (if (= :rule (:clause c)) (:args c) (:terms c)))
    :fn (binding-vars (:binding c))
    (:pred :not :not-join) []
    :or (:vars c)
    :or-join (filter (branch-vars c) (:vars c))))

(defn- unbound-for [c bound]
  (case (:clause c)
    (:pred :fn) (remove bound (term-vars (:args c)))
    (:not :not-join) (remove bound (:vars c))
    :or-join (remove bound (:required c))))

(defn- check-bindings
  "Checks that every clause can eventually be satisfied given `initial` bound
  vars (Datomic defers predicates/functions until their args are bound, so
  written order is not significant). Returns the final bound set."
  [clauses initial]
  (let [bound (loop [bound initial
                     pending (vec (flatten-and clauses))]
                (let [{ready true waiting false} (group-by #(boolean (ready? % bound)) pending)]
                  (cond
                    (seq ready) (recur (into bound (mapcat provides) ready) (vec waiting))
                    (seq waiting)
                    (let [c (first waiting)
                          missing (vec (distinct (unbound-for c bound)))
                          what (case (:clause c)
                                 (:pred :fn) "expression"
                                 :not "not"
                                 :not-join "not-join"
                                 :or-join "or-join")]
                      (fail :db.error/insufficient-binding
                            (str missing " not bound in " what " clause")
                            {:variables (set missing) :clause (:clause c)}))
                    :else bound)))]
    ;; nested bodies see everything the enclosing scope binds
    (doseq [c (flatten-and clauses)]
      (case (:clause c)
        (:not :not-join) (check-bindings (:body c) bound)
        (:or :or-join) (doseq [b (:branches c)] (check-bindings b bound))
        nil))
    bound))

(defn- find-elem-vars [e]
  (case (:type e)
    :var [(:sym e)]
    :agg (term-vars (:args e))
    :pull (cons (:var e) (term-vars [(:pattern e)]))))

(defn- check-query [{:keys [find with in where]}]
  (let [initial (set (mapcat binding-vars in))
        bound (check-bindings where initial)
        used (concat (mapcat find-elem-vars (:elements find)) with)
        missing (set (remove bound used))]
    (when (seq missing)
      (fail :db.error/unbound-query-variables
            (str "Query is referencing unbound variables: " (pr-str missing))
            {:variables missing}))))

;; -------------------------------------------------------------- find / top

(defn- parse-pull [args form]
  (let [[src x pattern] (cond (= 2 (count args)) ['$ (first args) (second args)]
                              (and (= 3 (count args)) (src-sym? (first args))) args
                              :else (fail (str "Invalid pull expression " (pr-str form)
                                               ", expected (pull ?e pattern) or (pull $ ?e pattern)")
                                          {:form form}))]
    (when-not (var-sym? x)
      (fail (str "Entity in " (pr-str form) " is not a variable") {:form form}))
    (when-not (or (vector? pattern) (map? pattern) (var-sym? pattern))
      (fail (str "Pull pattern in " (pr-str form) " must be a vector, map or variable")
            {:form form}))
    {:type :pull :src src :var x
     :pattern (if (var-sym? pattern) {:type :var :sym pattern} {:type :const :val pattern})}))

(defn- parse-find-elem [x]
  (cond
    (var-sym? x) {:type :var :sym x}
    (and (seq? x) (symbol? (first x)))
    (if (= 'pull (first x))
      (parse-pull (rest x) x)
      {:type :agg :agg (first x) :args (mapv parse-term (rest x))})
    :else (fail (str "Argument " (pr-str x) " in :find is not a variable") {:form x})))

(defn- validate-agg [e]
  (if (= :agg (:type e))
    (let [args (:args e)]
      (when-not (and (seq args) (= :var (:type (peek args)))
                     (every? #(#{:var :const} (:type %)) args))
        (fail (str "Aggregate (" (:agg e) " ...) must end with a variable argument")
              {:form e}))
      e)
    e))

(defn- parse-find [forms]
  (when (empty? forms) (fail "No :find clause specified" {}))
  (let [elem (comp validate-agg parse-find-elem)
        f0 (first forms)
        n (count forms)
        find (cond
               (and (= 1 n) (vector? f0))
               (cond (and (= 2 (count f0)) (= '... (second f0)))
                     {:spec :find-coll :elements [(elem (first f0))]}
                     (seq f0)
                     {:spec :find-tuple :elements (mapv elem f0)}
                     :else (fail "Empty find tuple" {:form f0}))

               (and (= 2 n) (= '. (second forms)))
               {:spec :find-scalar :elements [(elem f0)]}

               :else {:spec :find-rel :elements (mapv elem forms)})
        syms (map #(if (= :var (:type %)) (:sym %) %) (:elements find))]
    (when-not (apply distinct? syms)
      (fail "Duplicate elements in :find" {:find forms}))
    find))

(defn- parse-return-map [secs find]
  (let [present (filter secs [:keys :syms :strs])]
    (when (> (count present) 1)
      (fail "Only one of :keys, :syms or :strs may be specified" {}))
    (when-let [k (first present)]
      (let [names (vec (get secs k))]
        (when-not (and (#{:find-rel :find-tuple} (:spec find))
                       (= (count names) (count (:elements find))))
          (fail "Count of :keys/:strs/:syms must match count of :find" {:names names}))
        (doseq [n names]
          (when-not (or (symbol? n) (keyword? n) (string? n))
            (fail (str "Invalid " k " name " (pr-str n)) {:name n})))
        {:type k :names names}))))

(def ^:private section-keys #{:find :with :in :where :keys :syms :strs})

(defn- ->sections
  "Normalizes the map or sequence query form to {section [forms...]}."
  [q]
  (cond
    (map? q)
    (reduce-kv (fn [m k vs]
                 (when-not (section-keys k)
                   (fail (str "Unknown query key " (pr-str k)) {:key k}))
                 (when-not (sequential? vs)
                   (fail (str "Expected a sequence for " k) {:key k}))
                 (assoc m k (vec vs)))
               {} q)

    (sequential? q)
    (loop [xs (seq q) k nil m {}]
      (if-not xs
        m
        (let [x (first xs)]
          (cond
            (keyword? x) (do (when-not (section-keys x)
                               (fail (str "Unknown query keyword " (pr-str x)) {:key x}))
                             (recur (next xs) x (update m x #(or % []))))
            (nil? k) (fail (str "Query must start with a keyword, got " (pr-str x)) {:form q})
            :else (recur (next xs) k (update m k conj x))))))

    :else (fail (str "Query must be a map or sequence, got " (pr-str q)) {:form q})))

(defn- normalize [q]
  (cond
    (string? q) (let [r (try (edn/read-string q)
                             (catch Exception e
                               (fail (str "Unable to read query string: " (ex-message e))
                                     {:form q})))]
                  (normalize r))
    (and (seq? q) (= 'quote (first q)) (= 2 (count q))) (normalize (second q))
    :else q))

(defn- parse-query* [q]
  (let [secs (->sections (normalize q))]
    (when-not (contains? secs :find) (fail "No :find clause specified" {:form q}))
    (when-not (or (contains? secs :in) (contains? secs :where))
      (fail "No :in or :where clause specified" {:form q}))
    (let [find (parse-find (:find secs))
          with (vec (:with secs))
          _ (doseq [w with]
              (when-not (var-sym? w)
                (fail (str "Argument " (pr-str w) " in :with is not a variable") {:form w})))
          _ (when (seq (set/intersection (set with)
                                         (set (mapcat find-elem-vars (:elements find)))))
              (fail "Variables in :with must not also appear in :find" {:with with}))
          ast {:find find
               :return-map (parse-return-map secs find)
               :with with
               :in (if (contains? secs :in)
                     (mapv parse-in-binding (:in secs))
                     [{:bind :src :sym '$}])
               :where (parse-clauses (get secs :where []) ":where")}]
      (check-query ast)
      ast)))

;; ------------------------------------------------------------------- rules

(defn- parse-rule [form]
  (when-not (and (sequential? form) (seq? (first form)) (seq (first form)))
    (fail (str "Rule must be a vector starting with a head list: " (pr-str form)) {:form form}))
  (let [[name & hargs] (first form)
        body-forms (rest form)]
    (when-not (plain-sym? name)
      (fail (str "Rule name " (pr-str name) " is not a symbol") {:form form}))
    (let [[required others] (if (vector? (first hargs))
                              [(first hargs) (rest hargs)]
                              [[] hargs])]
      (doseq [x (concat required others)]
        (when-not (var-sym? x)
          (fail (str "Rule head argument " (pr-str x) " is not a variable") {:form form})))
      (when (empty? body-forms)
        (fail (str "Rule " name " has an empty body") {:form form}))
      [name {:head (vec (concat required others))
             :required (vec required)
             :body (parse-clauses (vec body-forms) (str "rule " name))}])))

(defn- parse-rules* [rules]
  (when-not (and (sequential? rules) (not (map? rules)))
    (fail (str "Rules must be a sequence of rule definitions, got " (pr-str rules))
          {:form rules}))
  (reduce (fn [m rule]
            (let [[name parsed] (parse-rule rule)
                  existing (first (get m name))]
              (when (and existing (not= (count (:head existing)) (count (:head parsed))))
                (fail (str "Arity mismatch for rule " name) {:rule name}))
              (update m name (fnil conj []) parsed)))
          {} rules))

;; ------------------------------------------------------------------- cache

(def ^:private ^:const cache-limit 1024)

(defn- cached [^ConcurrentHashMap cache f form]
  (let [k (if (nil? form) ::nil form)]
    (or (.get cache k)
        (let [r (f form)]
          (when (>= (.size cache) cache-limit) (.clear cache))
          (.put cache k r)
          r))))

(def ^:private query-cache (ConcurrentHashMap.))
(def ^:private rules-cache (ConcurrentHashMap.))

;; --------------------------------------------------------------------- API

(defn parse-query
  "Parses a Datomic query (map, vector, list, quoted or string form) into the
  AST documented in docs/query-ast.md. Throws ex-info with :db/error
  :db.error/invalid-query (or :db.error/unbound-query-variables,
  :db.error/insufficient-binding) on malformed queries. Cached by query form."
  [q]
  (cached query-cache parse-query* q))

(defn parse-rules
  "Parses a rule set (vector of `[(name args*) clause+]`) into
  `{rule-name [{:head [vars] :required [vars] :body [clauses]} ...]}`.
  Cached by form."
  [rules]
  (cached rules-cache parse-rules* rules))
