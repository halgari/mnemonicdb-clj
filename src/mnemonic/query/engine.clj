(ns mnemonic.query.engine
  "Query execution over the parsed AST (docs/query-ast.md).

  A relation is {:vars [sym ...] :tuples java.util.ArrayList<Object[]>}; every clause
  maps a relation to a relation. Clauses run in written order; predicates and function
  expressions whose arguments are not bound yet are deferred until they are.

  Extension points for rules / not / or / aggregates / pull (later task):
    - `execute-clause`  multimethod on the clause's :clause tag,
    - `find-element-fn` multimethod on the find element's :type,
    - `run-clauses`     runs a clause list (with deferral) against a relation."
  (:require [mnemonic.codec :as codec]
            [mnemonic.index :as index]
            [mnemonic.pull :as pull]
            [mnemonic.query.aggregates :as aggregates]
            [mnemonic.query.fns :as fns]
            [mnemonic.query.parser :as parser])
  (:import (java.util ArrayList Arrays Collection HashMap HashSet LinkedHashMap LinkedHashSet List)
           (java.util.concurrent TimeoutException)
           (mnemonic.datom Datom)))

(set! *warn-on-reflection* true)

(def ^:private qerror fns/qerror)

;; ------------------------------------------------------------------ relations

(defn unit-rel
  "The relation with no variables and one empty tuple (identity of join)."
  []
  {:vars [] :tuples (doto (ArrayList.) (.add (object-array 0)))})

(defn rel-size ^long [rel] (.size ^ArrayList (:tuples rel)))

(defn var-index
  "Map of variable symbol -> column index of `rel`."
  [rel]
  (zipmap (:vars rel) (range)))

(defn- extend-tuple
  "`t` with the elements of `extra` appended."
  ^objects [^objects t ^objects extra]
  (let [n (alength t)
        m (alength extra)
        r (object-array (+ n m))]
    (System/arraycopy t 0 r 0 n)
    (System/arraycopy extra 0 r n m)
    r))

(defn check-deadline!
  "Throws :db.error/query-timeout when the query deadline in `ctx` has passed."
  [ctx]
  (when-let [d (:deadline ctx)]
    (when (> (System/nanoTime) (long d))
      (throw (ex-info "Query canceled: timeout elapsed"
                      {:db/error :db.error/query-timeout
                       :cognitect.anomalies/category :cognitect.anomalies/interrupted
                       :cognitect.anomalies/message "Query canceled: timeout elapsed"}
                      (TimeoutException. "Query canceled: timeout elapsed"))))))

(defmacro ^:private each-tuple
  "Binds `t` to each tuple of `tuples` (an ArrayList), checking the deadline now and then."
  [[t tuples ctx] & body]
  `(let [^ArrayList tuples# ~tuples
         n# (.size tuples#)]
     (dotimes [i# n#]
       (when (zero? (bit-and i# 4095)) (check-deadline! ~ctx))
       (let [~(with-meta t {:tag 'objects}) (.get tuples# i#)]
         ~@body))))

;; ------------------------------------------------------------------ sources

(defn- db? [x] (satisfies? index/IDb x))

(defn source
  "The source (db or collection) bound to src-var `sym` in `ctx`."
  [ctx sym]
  (let [s (get (:sources ctx) sym ::missing)]
    (cond
      (identical? s ::missing)
      (throw (qerror :db.error/invalid-query (str "Data source not supplied: " sym) {:source sym}))
      (nil? s)
      (throw (qerror :db.error/invalid-data-source
                     "Nil or missing data source. Did you forget to pass a database argument?"))
      (or (db? s) (instance? Collection s)) s
      :else
      (throw (qerror :db.error/invalid-data-source
                     (str (class s) " is not a valid data source type."))))))

;; ------------------------------------------------------------------ bindings -> rows

(defn binding-syms
  "Variables bound by `b` in column order (blanks are skipped)."
  [b]
  (case (:bind b)
    :scalar [(:sym b)]
    :blank []
    :tuple (into [] (mapcat binding-syms) (:elems b))
    :coll (binding-syms (:elem b))
    :rel (into [] (mapcat binding-syms) (:elems b))
    []))

(defn- coll-like? [x]
  (or (sequential? x) (instance? Collection x) (and x (.isArray (class x)))))

(defn- bad-binding [msg v]
  (qerror :db.error/invalid-query msg {:value v}))

(defn- row-of
  "Object[] of the non-blank positions of sequential `v` matched against `elems`."
  ^objects [elems v]
  (when-not (coll-like? v)
    (throw (bad-binding (str "Cannot bind a non-collection value to a tuple: " (pr-str v)) v)))
  (let [v (if (or (sequential? v) (instance? List v)) v (vec v))
        n (count elems)]
    (when (< (count v) n)
      (throw (bad-binding (str "Binding tuple is too short: " (pr-str v)) v)))
    (let [out (ArrayList.)]
      (dotimes [i n]
        (when (= :scalar (:bind (nth elems i)))
          (.add out (nth v i))))
      (.toArray out))))

(defn- scalar-row ^objects [v]
  (doto (object-array 1) (aset 0 v)))

(defn binding-rows
  "Rows (Object[] aligned with `(binding-syms b)`) for value `v` bound by `b`. A nil
  value binds no rows, except for inputs (`input?`) where a nil scalar is an error."
  [b v input?]
  (case (:bind b)
    :scalar (cond (some? v) [(scalar-row v)]
                  input? (throw (bad-binding "nil is not a valid input value" v))
                  :else [])
    :blank [(object-array 0)]
    :tuple (if (nil? v) [] [(row-of (:elems b) v)])
    :coll (cond
            (nil? v) []
            (coll-like? v) (let [elem (:elem b)]
                             (if (= :scalar (:bind elem))
                               (mapv scalar-row (seq v))
                               (mapv (fn [_] (object-array 0)) (seq v))))
            :else (throw (bad-binding (str "Cannot bind a non-collection value to a collection: "
                                           (pr-str v)) v)))
    :rel (cond
           (nil? v) []
           (coll-like? v) (mapv #(row-of (:elems b) %) (seq v))
           :else (throw (bad-binding (str "Cannot bind a non-collection value to a relation: "
                                          (pr-str v)) v)))
    (throw (bad-binding (str "Unsupported binding " (pr-str b)) v))))

(defn extend-rel
  "Joins `rel` with the rows produced by `(rows-fn tuple)` for every tuple. `bsyms`
  names the row columns; columns whose variable is already bound must be equal."
  [ctx rel bsyms rows-fn]
  (let [vars (:vars rel)
        vidx (var-index rel)
        existing (vec (keep-indexed (fn [i s] (when-let [j (get vidx s)] [i j])) bsyms))
        new-cols (vec (keep-indexed (fn [i s] (when-not (contains? vidx s) i)) bsyms))
        new-syms (mapv #(nth bsyms %) new-cols)
        out (ArrayList.)]
    (each-tuple [t (:tuples rel) ctx]
      (doseq [^objects r (rows-fn t)]
        (when (every? (fn [[i j]] (= (aget r (long i)) (aget t (long j)))) existing)
          (.add out (if (empty? new-cols)
                      t
                      (extend-tuple t (let [e (object-array (count new-cols))]
                                        (dotimes [k (count new-cols)]
                                          (aset e k (aget r (long (nth new-cols k)))))
                                        e)))))))
    {:vars (into vars new-syms) :tuples out}))

;; ------------------------------------------------------------------ data patterns

(defn- compile-pattern
  "Pattern plan for `terms` against relation vars: per position (e a v tx added) a
  spec {:kind :const|:bound|:free|:same|:blank ...}."
  [rel-vars terms]
  (let [vidx (zipmap rel-vars (range))]
    (loop [i 0 specs [] new-syms [] first-pos {}]
      (if (== i 5)
        {:specs specs
         :new-syms new-syms
         :new-pos (mapv first-pos new-syms)
         :bound-pos (vec (keep-indexed (fn [p s] (when (= :bound (:kind s)) p)) specs))
         :same (vec (keep-indexed (fn [p s] (when (= :same (:kind s)) [(:pos s) p])) specs))}
        (let [t (get terms i {:type :blank})]
          (case (:type t)
            :const (recur (inc i) (conj specs {:kind :const :val (:val t)}) new-syms first-pos)
            :blank (recur (inc i) (conj specs {:kind :blank}) new-syms first-pos)
            :var (let [s (:sym t)]
                   (cond
                     (contains? vidx s)
                     (recur (inc i) (conj specs {:kind :bound :idx (long (vidx s))}) new-syms first-pos)
                     (contains? first-pos s)
                     (recur (inc i) (conj specs {:kind :same :pos (first-pos s)}) new-syms first-pos)
                     :else
                     (recur (inc i) (conj specs {:kind :free}) (conj new-syms s) (assoc first-pos s i))))
            (throw (qerror :db.error/invalid-query "Invalid term in data pattern" {:term t}))))))))

(defn- datom-comp [^Datom d ^long pos]
  (case pos
    0 (.-e d)
    1 (.-a d)
    2 (.-v d)
    3 (.-tx d)
    4 (.-added d)))

(defn- spec-raw [spec ^objects tuple consts-only?]
  (case (:kind spec)
    :const (:val spec)
    :bound (if consts-only? ::free (aget tuple (long (:idx spec))))
    ::free))

(defn- resolve-pattern
  "Resolved components [e a v tx added] of the pattern for `tuple`: values or ::free for
  unconstrained positions. Returns nil when nothing can match. With `consts-only?` bound
  variables count as unconstrained."
  [db specs tuple consts-only?]
  (let [sch (index/-schema db)
        raw (fn [p] (spec-raw (nth specs p) tuple consts-only?))
        ra (raw 1)
        a (cond
            (identical? ra ::free) ::free
            (keyword? ra) (or (get (:idents sch) ra) (throw (fns/qerror :db.error/not-an-entity
                                                                         (str "Unable to resolve entity: " ra)
                                                                         {:entity ra})))
            (integer? ra) (long ra)
            :else ::none)
        re (raw 0)
        e (if (identical? re ::free) ::free (fns/resolve-eid db re))
        rv (raw 2)
        v (cond
            (identical? rv ::free) ::free
            (identical? a ::free) (fns/normalize-value rv)
            (identical? a ::none) ::none
            :else (fns/resolve-value db (get (:attrs sch) a) rv))
        rt (raw 3)
        tx (cond (identical? rt ::free) ::free (integer? rt) (long rt) :else ::none)
        rd (raw 4)
        added (cond (identical? rd ::free) ::free (instance? Boolean rd) rd :else ::none)]
    (when-not (or (identical? a ::none) (identical? v ::none) (identical? tx ::none)
                  (identical? added ::none))
      [e a v tx added])))

(defn- free? [x] (identical? x ::free))

(defn- scan-plan
  "[index prefix-bytes covered-v?] for resolved pattern components, nil when nothing can
  match. Throws for patterns that would need a full scan."
  [[e a v _ _] clause-terms]
  (let [ev? (not (free? e)) av? (not (free? a)) vv? (not (free? v))
        [idx comps covered?]
        (cond
          ev? [:eavt (cond-> [e] av? (conj a) (and av? vv?) (conj v)) (and av? vv?)]
          av? (if vv? [:avet [a v] true] [:aevt [a] false])
          (and vv? (instance? Long v)) [:vaet [v] true]
          vv? nil
          :else (throw (qerror :db.error/insufficient-binding
                               (str "Insufficient binding of db clause: " (pr-str clause-terms)
                                    " would cause full scan")
                               {:clause clause-terms})))]
    (when idx
      (try [idx (codec/encode-prefix idx comps) covered?]
           (catch clojure.lang.ExceptionInfo _ nil)))))

(defn- datom-filter
  "Predicate over datoms for the constraints not covered by the index prefix."
  [[_ _ v tx added] covered? same]
  (let [vbytes (when-not (or covered? (free? v))
                 (try (codec/value->bytes v) (catch clojure.lang.ExceptionInfo _ ::unencodable)))]
    (when-not (identical? vbytes ::unencodable)
      (let [tx? (not (free? tx))
            added? (not (free? added))
            tx (if tx? (long tx) 0)]
        (if (and (nil? vbytes) (not tx?) (not added?) (empty? same))
          (constantly true)
          (fn [^Datom d]
            (and (or (nil? vbytes) (Arrays/equals ^bytes vbytes (codec/value->bytes (.-v d))))
                 (or (not tx?) (== tx (.-tx d)))
                 (or (not added?) (= added (.-added d)))
                 (every? (fn [[p q]] (= (datom-comp d p) (datom-comp d q))) same))))))))

(defn- scan!
  "Calls `(f datom)` for every datom of `db` in the index range that passes `pass?`,
  checking the query deadline every 4096 datoms."
  [ctx db [idx prefix] pass? f]
  (let [cnt (long-array 1)]
    (reduce (fn [_ d]
              (let [c (aget cnt 0)]
                (aset cnt 0 (inc c))
                (when (zero? (bit-and c 4095)) (check-deadline! ctx)))
              (when (pass? d) (f d))
              nil)
            nil
            (index/datoms-iterable db idx prefix))))

(defn- new-values
  "Object[] of the free-variable values taken from datom `d`."
  ^objects [^Datom d new-pos]
  (let [r (object-array (count new-pos))]
    (dotimes [i (count new-pos)]
      (aset r i (datom-comp d (long (nth new-pos i)))))
    r))

(def ^:private hash-join-threshold 1000)

(defn- exec-db-pattern [ctx db rel plan terms]
  (let [{:keys [specs new-syms new-pos bound-pos same]} plan
        ^ArrayList tuples (:tuples rel)
        out (ArrayList.)
        n (.size tuples)
        no-new? (empty? new-pos)
        emit! (fn [^objects t ^Datom d]
                (.add out (if no-new? t (extend-tuple t (new-values d new-pos)))))
        result {:vars (into (:vars rel) new-syms) :tuples out}]
    (when (pos? n)
      (cond
        ;; no variable from the relation is involved: one scan, then a product
        (empty? bound-pos)
        (when-let [res (resolve-pattern db specs (object-array 0) true)]
          (when-let [sp (scan-plan res terms)]
            (when-let [pass? (datom-filter res (nth sp 2) same)]
              (scan! ctx db sp pass?
                     (fn [^Datom d]
                       (check-deadline! ctx)
                       (each-tuple [t tuples ctx] (emit! t d)))))))

        ;; large relation, constant attribute: scan once and hash join
        (and (> n hash-join-threshold) (= :const (:kind (nth specs 1))))
        (when-let [res (resolve-pattern db specs (object-array 0) true)]
          (when-let [sp (scan-plan res terms)]
            (when-let [pass? (datom-filter res (nth sp 2) same)]
              (let [groups (HashMap.)]
                (each-tuple [t tuples ctx]
                  (when-let [full (resolve-pattern db specs t false)]
                    (let [k (mapv #(nth full %) bound-pos)
                          ^ArrayList g (or (.get groups k)
                                           (let [g (ArrayList.)] (.put groups k g) g))]
                      (.add g t))))
                (scan! ctx db sp pass?
                       (fn [^Datom d]
                         (when-let [^ArrayList g (.get groups (mapv #(datom-comp d %) bound-pos))]
                           (dotimes [i (.size g)] (emit! (.get g i) d)))))))))

        ;; otherwise a seek per tuple
        :else
        (each-tuple [t tuples ctx]
          (when-let [res (resolve-pattern db specs t false)]
            (when-let [sp (scan-plan res terms)]
              (when-let [pass? (datom-filter res (nth sp 2) same)]
                (scan! ctx db sp pass? (fn [^Datom d] (emit! t d)))))))))
    result))

(defn- exec-coll-pattern [ctx coll rel plan]
  (let [{:keys [specs new-syms new-pos bound-pos same]} plan
        ^ArrayList tuples (:tuples rel)
        out (ArrayList.)
        no-new? (empty? new-pos)
        groups (HashMap.)
        const-specs (vec (keep-indexed (fn [p s] (when (= :const (:kind s)) [p (:val s)])) specs))]
    (each-tuple [t tuples ctx]
      (let [k (mapv #(fns/normalize-value (aget t (long (:idx (nth specs %))))) bound-pos)
            ^ArrayList g (or (.get groups k) (let [g (ArrayList.)] (.put groups k g) g))]
        (.add g t)))
    (when-not (.isEmpty groups)
      (doseq [row (seq coll)]
        (check-deadline! ctx)
        (when (and (every? (fn [[p c]] (= c (nth row p))) const-specs)
                   (every? (fn [[p q]] (= (nth row p) (nth row q))) same))
          (when-let [^ArrayList g (.get groups (mapv #(fns/normalize-value (nth row %)) bound-pos))]
            (let [extra (when-not no-new?
                          (let [e (object-array (count new-pos))]
                            (dotimes [i (count new-pos)] (aset e i (nth row (nth new-pos i))))
                            e))]
              (dotimes [i (.size g)]
                (let [^objects t (.get g i)]
                  (.add out (if no-new? t (extend-tuple t extra))))))))))
    {:vars (into (:vars rel) new-syms) :tuples out}))

;; ------------------------------------------------------------------ clause execution

(declare run-clauses)

(defmulti execute-clause
  "Runs one parsed clause against `rel`, returning the new relation."
  (fn [_ctx _rel clause] (:clause clause)))

(defmethod execute-clause :default [_ _ clause]
  (throw (qerror :db.error/unsupported (str "Unsupported clause: " (pr-str (:clause clause)))
                 {:clause clause})))

(defmethod execute-clause :data-pattern [ctx rel {:keys [src terms]}]
  (let [s (source ctx src)
        plan (compile-pattern (:vars rel) terms)]
    (if (db? s)
      (exec-db-pattern ctx s rel plan terms)
      (exec-coll-pattern ctx s rel plan))))

(defmethod execute-clause :and [ctx rel {:keys [clauses]}]
  (run-clauses ctx rel clauses))

;; ---- predicates and function expressions

(defn- var-args [clause]
  (keep #(when (= :var (:type %)) (:sym %)) (:args clause)))

(defn- rule-required-syms
  "Variables the caller must have bound for rule-call `clause` (the call arguments at the
  rule's required head positions)."
  [ctx clause]
  (let [defs (get (:rules ctx) (:name clause))
        args (:args clause)]
    (when (and (seq defs) (= (count (:head (first defs))) (count args)))
      (let [positions (into #{} (mapcat (fn [{:keys [head required]}]
                                          (keep-indexed (fn [i h] (when (some #{h} required) i)) head)))
                            defs)]
        (keep-indexed (fn [i a] (when (and (contains? positions i) (= :var (:type a))) (:sym a)))
                      args)))))

(defn- needs
  "Variables that must be bound before `clause` can run."
  [ctx clause]
  (case (:clause clause)
    (:pred :fn) (concat (var-args clause)
                        (let [f (:fn clause)] (when (fns/fn-var? f) [f])))
    (:not :not-join) (:vars clause)
    :or-join (:required clause)
    :rule (rule-required-syms ctx clause)
    nil))

(defn- clause-ready? [ctx rel clause]
  (let [vidx (var-index rel)]
    (every? #(contains? vidx %) (needs ctx clause))))

(defn- arg-getter
  "A fn of tuple for argument `term`."
  [ctx vidx term]
  (case (:type term)
    :const (let [v (:val term)] (fn [_] v))
    :var (let [i (long (vidx (:sym term)))] (fn [^objects t] (aget t i)))
    :src (let [s (source ctx (:sym term))] (fn [_] s))
    (throw (qerror :db.error/invalid-query "Invalid function argument" {:term term}))))

(defn- fn-caller
  "A fn of tuple calling the clause's function on its arguments."
  [ctx rel {:keys [args] :as clause}]
  (let [vidx (var-index rel)
        gs (mapv #(arg-getter ctx vidx %) args)
        n (count gs)
        g0 (first gs) g1 (second gs) g2 (nth gs 2 nil)
        sym (:fn clause)
        fidx (when (fns/fn-var? sym) (long (vidx sym)))
        f (when-not fidx (fns/resolve-fn sym))
        getf (if fidx (fn [^objects t] (aget t fidx)) (fn [_] f))]
    (case n
      0 (fn [t] ((getf t)))
      1 (fn [t] ((getf t) (g0 t)))
      2 (fn [t] ((getf t) (g0 t) (g1 t)))
      3 (fn [t] ((getf t) (g0 t) (g1 t) (g2 t)))
      (fn [t] (apply (getf t) (map #(% t) gs))))))

(defmethod execute-clause :pred [ctx rel clause]
  (let [call (fn-caller ctx rel clause)
        ^ArrayList tuples (:tuples rel)]
    (if (empty? (var-args clause))
      ;; Datomic evaluates constant-only predicates but does not filter on them
      (do (when (pos? (.size tuples)) (call (object-array 0)))
          rel)
      (let [out (ArrayList.)]
        (each-tuple [t tuples ctx]
          (when (call t) (.add out t)))
        {:vars (:vars rel) :tuples out}))))

(defmethod execute-clause :fn [ctx rel {:keys [binding] :as clause}]
  (let [call (fn-caller ctx rel clause)]
    (extend-rel ctx rel (binding-syms binding)
                (fn [t] (binding-rows binding (call t) false)))))

(defn- deferrable? [clause]
  (contains? #{:pred :fn :rule :not :not-join :or-join} (:clause clause)))

(defn run-clauses
  "Executes `clauses` in written order against `rel`. Predicates and function
  expressions with unbound arguments wait until a later clause binds them."
  [ctx rel clauses]
  (letfn [(drain [rel pending]
            (loop [rel rel pending pending]
              (if-let [c (first (filter #(clause-ready? ctx rel %) pending))]
                (recur (execute-clause ctx rel c)
                       (vec (remove #(identical? c %) pending)))
                [rel pending])))]
    (loop [rel rel cs (seq clauses) pending []]
      (check-deadline! ctx)
      (if-let [c (first cs)]
        (if (and (deferrable? c) (not (clause-ready? ctx rel c)))
          (recur rel (next cs) (conj pending c))
          (let [[rel' pending'] (drain (execute-clause ctx rel c) pending)]
            (recur rel' (next cs) pending')))
        (if (seq pending)
          (let [vidx (var-index rel)
                missing (into #{} (comp (mapcat #(needs ctx %)) (remove #(contains? vidx %))) pending)]
            (throw (qerror :db.error/insufficient-binding
                           (str (vec missing) " not bound in clause: "
                                (pr-str (let [c (first pending)]
                                          (or (:fn c) (:name c) (:clause c)))))
                           {:variables missing :clause (first pending)})))
          rel)))))

;; ------------------------------------------------------------------ inputs

(defn- parse-rules-input
  "Rule map for the `%` argument (a rule-set vector); nil means no rules."
  [a]
  (cond
    (nil? a) {}
    (or (sequential? a) (string? a)) (if (and (sequential? a) (empty? a)) {} (parser/parse-rules a))
    :else (throw (qerror :db.error/invalid-query "Rules must be a collection of rule definitions"
                         {:rules a}))))

(defn- bind-inputs
  "Matches `args` with the :in bindings; returns ctx (sources, rules) and the relation of
  all non-source inputs."
  [ctx ast args]
  (let [ins (:in ast)
        nargs (count args)]
    ;; a query without :in has the implicit [$] and Datomic accepts it with no arguments
    ;; (the parser cannot tell this from an explicit `:in $`)
    (when (and (< nargs (count ins))
               (not (and (zero? nargs) (= ins [{:bind :src :sym '$}]))))
      (throw (qerror :db.error/too-few-inputs
                     (str "Query expected " (count ins) " inputs but received " nargs)
                     {:expected (count ins) :received nargs})))
    (loop [bs (seq ins) as (seq args) ctx ctx rel (unit-rel)]
      (if bs
        (let [b (first bs) a (first as)]
          (case (:bind b)
            :src (recur (next bs) (next as) (assoc-in ctx [:sources (:sym b)] a) rel)
            :rules (recur (next bs) (next as) (assoc ctx :rules (parse-rules-input a)) rel)
            (let [rows (binding-rows b a true)]
              (recur (next bs) (next as) ctx
                     (extend-rel ctx rel (binding-syms b) (fn [_] rows))))))
        [ctx rel]))))

;; ------------------------------------------------------------------ source checks

(defn- term-srcs [terms]
  (into #{} (keep #(when (= :src (:type %)) (:sym %))) terms))

(defn clause-srcs [c]
  (case (:clause c)
    :data-pattern #{(:src c)}
    (:pred :fn) (term-srcs (:args c))
    :rule (conj (term-srcs (:args c)) (:src c))
    (:not :not-join) (into #{(:src c)} (mapcat clause-srcs) (:body c))
    (:or :or-join) (into #{(:src c)} (mapcat #(mapcat clause-srcs %)) (:branches c))
    :and (into #{} (mapcat clause-srcs) (:clauses c))
    #{}))

(defn- check-sources! [ast]
  (let [declared (into #{} (keep #(when (= :src (:bind %)) (:sym %))) (:in ast))
        used (into (into #{} (mapcat clause-srcs) (:where ast))
                   (keep #(when (= :pull (:type %)) (:src %)))
                   (:elements (:find ast)))]
    (doseq [s (sort used)]
      (when (and s (not (contains? declared s)))
        (throw (qerror :db.error/invalid-query (str "Data source not supplied: " s) {:source s}))))))

;; ------------------------------------------------------------------ find

(defmulti find-element-fn
  "A fn of tuple returning the raw value of find element `elem` against relation `rel`.
  For :pull elements the raw value is what `finish-element-fn` turns into the pulled map."
  (fn [_ctx _rel elem] (:type elem)))

(defmethod find-element-fn :default [_ _ elem]
  (throw (qerror :db.error/unsupported
                 (str "Find element " (pr-str (:type elem)) " is not supported yet")
                 {:element elem})))

(defn- column-fn
  "Getter of the column of variable `sym` in `rel`; throws when unbound."
  [rel sym]
  (let [i (get (var-index rel) sym)]
    (when-not i
      (throw (qerror :db.error/unbound-query-variables
                     (str "Query is referencing unbound variables: #{" sym "}")
                     {:variables #{sym}})))
    (let [i (long i)]
      (fn [^objects t] (aget t i)))))

(defmethod find-element-fn :var [_ rel {:keys [sym]}]
  (column-fn rel sym))

(defmethod find-element-fn :pull [_ rel {:keys [var pattern]}]
  (let [ef (column-fn rel var)]
    (if (= :var (:type pattern))
      (let [pf (column-fn rel (:sym pattern))]
        (fn [t] [(ef t) (pf t)]))
      ef)))

(defmulti finish-element-fn
  "nil, or a fn turning the raw value of find element `elem` into its result value
  (pull). Applied after de-duplication, and lazily for qseq."
  (fn [_ctx _rel elem] (:type elem)))

(defmethod finish-element-fn :default [_ _ _] nil)

(defmethod finish-element-fn :pull [ctx _ {:keys [src pattern]}]
  (let [db (source ctx src)]
    (when-not (db? db)
      (throw (qerror :db.error/invalid-data-source
                     (str (class db) " is not a valid data source type for pull."))))
    (when (index/-history? db)
      (throw (IllegalStateException. "Can't pull from history")))
    (if (= :var (:type pattern))
      (let [cache (volatile! nil)
            parsed (fn [pat]
                     (let [c @cache]
                       (if (and c (identical? (key c) pat))
                         (val c)
                         (let [ast (pull/parse-pattern pat)]
                           (vreset! cache (clojure.lang.MapEntry. pat ast))
                           ast))))]
        (fn [[e pat]] (pull/pull-from-ast db (parsed pat) e)))
      (let [ast (pull/parse-pattern (:val pattern))]
        (fn [e] (pull/pull-from-ast db ast e))))))

(defn- key-fn [x kind]
  (case kind
    :keys (if (keyword? x) x (keyword x))
    :strs (cond (string? x) x (keyword? x) (subs (str x) 1) :else (str x))
    :syms (cond (symbol? x) x (keyword? x) (symbol (namespace x) (name x)) :else (symbol x))))

(defn- tuple-vec [fs ^objects t]
  (let [r (object-array (count fs))]
    (dotimes [i (count fs)] (aset r i ((nth fs i) t)))
    (vec r)))

(defn- distinct-rows
  "Raw rows (vectors aligned with the find elements), de-duplicated together with the
  :with values."
  [ctx rel ffs wfs]
  (let [seen (LinkedHashSet.)
        rows (ArrayList.)]
    (each-tuple [t (:tuples rel) ctx]
      (let [fv (tuple-vec ffs t)
            k (if (seq wfs) (conj fv (tuple-vec wfs t)) fv)]
        (when (.add seen k) (.add rows fv))))
    (vec rows)))

(defn- group-rows
  "Raw rows for a :find with aggregates: the relation is projected to the non-aggregate
  elements, the aggregate arguments and :with, de-duplicated, grouped by the non-aggregate
  elements and each aggregate applied to the values of its group."
  [ctx rel elems kfs afs wfs aggs]
  (let [seen (HashSet.)
        groups (LinkedHashMap.)
        nagg (count afs)]
    (each-tuple [t (:tuples rel) ctx]
      (let [k (tuple-vec kfs t)
            a (tuple-vec afs t)]
        (when (.add seen [k a (tuple-vec wfs t)])
          (let [^objects cols (or (.get groups k)
                                  (let [c (object-array nagg)]
                                    (dotimes [i nagg] (aset c i (ArrayList.)))
                                    (.put groups k c)
                                    c))]
            (dotimes [i nagg] (.add ^ArrayList (aget cols i) (nth a i)))))))
    (let [out (ArrayList.)]
      (doseq [[k ^objects cols] groups]
        (check-deadline! ctx)
        (let [row (object-array (count elems))]
          (loop [i 0 ki 0 ai 0]
            (when (< i (count elems))
              (if (= :agg (:type (nth elems i)))
                (do (aset row i ((nth aggs ai) (vec (aget cols ai))))
                    (recur (inc i) ki (inc ai)))
                (do (aset row i (nth k ki))
                    (recur (inc i) (inc ki) ai)))))
          (.add out (vec row))))
      (vec out))))

(defn- lazy-map
  "Like `map` over a vector but one element at a time (no chunking), so that pulls are
  realized only as the seq is consumed."
  [f coll]
  (lazy-seq
   (when-let [s (seq coll)]
     (cons (f (first s)) (lazy-map f (rest s))))))

(defn- shape-result
  "Projects the final relation per the :find spec, :with and :keys/:syms/:strs. With
  `lazy?` (qseq) the result is a seq whose pulls are realized as it is consumed."
  [ctx ast rel lazy?]
  (let [{:keys [find with return-map]} ast
        elems (:elements find)
        agg? (boolean (some #(= :agg (:type %)) elems))
        wfs (mapv #(column-fn rel %) with)
        finishers (mapv #(finish-element-fn ctx rel %) elems)
        pull? (boolean (some some? finishers))
        rows (if agg?
               (let [plain (remove #(= :agg (:type %)) elems)
                     aggs (filter #(= :agg (:type %)) elems)]
                 (group-rows ctx rel elems
                             (mapv #(find-element-fn ctx rel %) plain)
                             (mapv #(column-fn rel (:sym (peek (:args %)))) aggs)
                             wfs
                             (mapv aggregates/resolve-agg aggs)))
               (distinct-rows ctx rel (mapv #(find-element-fn ctx rel %) elems) wfs))
        ;; aggregate rows carry results in place; pull finishing applies to pull elements
        pulled (if pull?
                 (fn [row] (vec (map-indexed (fn [i v] (if-let [f (nth finishers i)] (f v) v)) row)))
                 identity)
        to-map (when return-map
                 (let [ks (mapv #(key-fn % (:type return-map)) (:names return-map))]
                   (fn [row] (zipmap ks row))))
        spec (:spec find)]
    (if lazy?
      (case spec
        :find-rel (when (seq rows) (lazy-map (if to-map (comp to-map pulled) pulled) rows))
        :find-coll (when (seq rows) (lazy-map (comp first pulled) rows))
        :find-scalar (when-let [row (first rows)]
                       (let [v (first (pulled row))]
                         (cond
                           (nil? v) nil
                           (or (coll? v) (instance? Collection v) (string? v)) (seq v)
                           :else (throw (IllegalArgumentException.
                                         (str "Don't know how to create ISeq from: "
                                              (.getName (class v))))))))
        :find-tuple (when-let [row (first rows)]
                      (seq (let [r (pulled row)] (if to-map (to-map r) r)))))
      (case spec
        :find-rel (cond
                    to-map (mapv (comp to-map pulled) rows)
                    (or agg? pull? (seq wfs)) (mapv pulled rows)
                    :else (HashSet. ^Collection rows))
        :find-coll (mapv (comp first pulled) rows)
        :find-scalar (some-> (first rows) pulled first)
        :find-tuple (when-let [row (first rows)]
                      (let [r (pulled row)] (if to-map (to-map r) r)))))))

;; ------------------------------------------------------------------ entry point

(defn execute
  "Runs parsed query `ast` against `args`. `opts`: {:timeout ms, :lazy true}."
  [ast args opts]
  (let [ctx {:deadline (when-let [ms (:timeout opts)]
                         (+ (System/nanoTime) (* (long ms) 1000000)))}]
    (check-sources! ast)
    (doseq [el (:elements (:find ast))]
      (case (:type el)
        :agg (aggregates/resolve-agg el)
        (:var :pull) nil
        (when-not (contains? (methods find-element-fn) (:type el))
          (throw (qerror :db.error/unsupported
                         (str "Find element " (pr-str (:type el)) " is not supported yet")
                         {:element el})))))
    (let [[ctx rel] (bind-inputs ctx ast args)
          rel (run-clauses ctx rel (:where ast))]
      (shape-result ctx ast rel (boolean (:lazy opts))))))

(defn parse [q] (parser/parse-query q))
