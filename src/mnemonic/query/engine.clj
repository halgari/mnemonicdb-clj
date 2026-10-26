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
  (:import (clojure.lang ArraySeq IFn LazilyPersistentVector PersistentArrayMap)
           (java.nio ByteBuffer)
           (java.util ArrayList Arrays Collection Date HashMap HashSet LinkedHashMap List RandomAccess UUID WeakHashMap)
           (java.util.concurrent ConcurrentHashMap TimeoutException)
           (mnemonic.datom IDatomView)
           (mnemonic.schema Attr)))

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
  "Joins `rel` with the rows produced by `(rows-fn tuple)` (nil, or a vector / ArrayList /
  seq of Object[]) for every tuple. `bsyms` names the row columns; columns whose variable is
  already bound must be equal."
  [ctx rel bsyms rows-fn]
  (let [vars (:vars rel)
        vidx (var-index rel)
        existing (vec (keep-indexed (fn [i s] (when-let [j (get vidx s)] [i j])) bsyms))
        ne (count existing)
        ex-i (int-array (map first existing))
        ex-j (int-array (map second existing))
        new-cols (vec (keep-indexed (fn [i s] (when-not (contains? vidx s) i)) bsyms))
        nn (count new-cols)
        nc (int-array new-cols)
        new-syms (mapv #(nth bsyms %) new-cols)
        out (ArrayList.)]
    (each-tuple [t (:tuples rel) ctx]
      (when-let [rows (rows-fn t)]
        (let [^List rows (if (instance? RandomAccess rows) rows (vec rows))
              m (.size rows)]
          (dotimes [ri m]
            (let [^objects r (.get rows ri)]
              (when (loop [k 0]
                      (or (== k ne)
                          (and (= (aget r (aget ex-i k)) (aget t (aget ex-j k)))
                               (recur (inc k)))))
                (.add out (if (zero? nn)
                            t
                            (let [n (alength t)
                                  e (object-array (+ n nn))]
                              (System/arraycopy t 0 e 0 n)
                              (dotimes [k nn] (aset e (+ n k) (aget r (aget nc k))))
                              e)))))))))
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

;; Scans hand clauses flyweight datom views (index/reduce-views): a view is valid only
;; during the call that receives it, so everything a clause keeps is copied out of it
;; with `datom-comp`, never the view itself.

(defn- datom-comp [^IDatomView d ^long pos]
  (case pos
    0 (.eid d)
    1 (.attr d)
    2 (.value d)
    3 (.txid d)
    4 (.isAdded d)))

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

(defn- same-pass
  "Datom predicate for the repeated-variable constraints `same` ([pos pos] pairs), nil
  when there are none."
  [same]
  (when (seq same)
    (let [^ints ps (int-array (map first same))
          ^ints qs (int-array (map second same))
          n (alength ps)]
      (fn [^IDatomView d]
        (loop [i 0]
          (or (== i n)
              (and (= (datom-comp d (aget ps i)) (datom-comp d (aget qs i)))
                   (recur (inc i)))))))))

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
          (let [sp (same-pass same)]
            (fn [^IDatomView d]
              (and (or (nil? vbytes) (Arrays/equals ^bytes vbytes (codec/value->bytes (.value d))))
                   (or (not tx?) (== tx (.txid d)))
                   (or (not added?) (= added (.isAdded d)))
                   (or (nil? sp) (sp d))))))))))

(defn- scan!
  "Calls `(f datom)` for every datom of `db` in the index range that passes `pass?`
  (nil = all), checking the query deadline every 4096 datoms counted across scans in
  `cnt` (a long[1] shared by the scans of one clause)."
  [ctx ^longs cnt db idx prefix pass? f]
  (let [dl? (some? (:deadline ctx))]
    (index/reduce-views (index/datoms-iterable db idx prefix)
                        (fn [_ d]
                          (when dl?
                            (let [c (aget cnt 0)]
                              (aset cnt 0 (inc c))
                              (when (zero? (bit-and c 4095)) (check-deadline! ctx))))
                          (when (or (nil? pass?) (pass? d)) (f d))
                          nil)
                        nil)))

(defn- scan-budget!
  "Like `scan!` but gives up once more than `limit` datoms of the range were visited.
  Returns the number of datoms of the range when it was exhausted, else -(limit + 1)
  (a lower bound of the range size, negated)."
  [ctx ^longs cnt db idx prefix pass? f limit]
  (let [dl? (some? (:deadline ctx))
        limit (long limit)
        seen (long-array 1)]
    (if (index/reduce-views (index/datoms-iterable db idx prefix)
                            (fn [_ d]
                              (when dl?
                                (let [c (aget cnt 0)]
                                  (aset cnt 0 (inc c))
                                  (when (zero? (bit-and c 4095)) (check-deadline! ctx))))
                              (let [s (inc (aget seen 0))]
                                (aset seen 0 s)
                                (if (> s limit)
                                  (reduced false)
                                  (do (when (or (nil? pass?) (pass? d)) (f d))
                                      true))))
                            true)
      (aget seen 0)
      (- (inc limit)))))

(defn- new-values
  "Object[] of the free-variable values taken from datom `d`."
  ^objects [^IDatomView d ^ints new-pos]
  (let [n (alength new-pos)
        r (object-array n)]
    (dotimes [i n]
      (aset r i (datom-comp d (aget new-pos i))))
    r))

;; ---- AVET range pushdown

(def ^:private no-extra (object-array 0))

(defn- cache-key?
  "True when `x` is a plain value whose equals / hashCode agree with how the pattern
  resolves it (so equal keys always resolve to the same seek)."
  [x]
  (or (instance? Long x) (instance? String x) (keyword? x) (instance? Boolean x)
      (instance? UUID x) (instance? Date x)))

(def ^:private cmp-ops {'< :lt, '<= :le, '> :gt, '>= :ge, '= :eq})
(def ^:private flip-op {:lt :gt, :le :ge, :gt :lt, :ge :le, :eq :eq})

(defn- range-hints
  "Comparison predicates on `vsym` found at the head of `clauses` (the clauses that will
  run right after the pattern binding it): [{:op :lt|:le|:gt|:ge|:eq :bound term} ...],
  with `vsym` as the left operand. Stops at the first other clause so that no clause
  that could observe or fail on the out-of-range values is skipped."
  [vsym clauses]
  (loop [cs (seq clauses) acc []]
    (if-let [c (first cs)]
      (let [op (when (= :pred (:clause c)) (get cmp-ops (:fn c)))
            [x y :as args] (:args c)
            var? (fn [t] (and (= :var (:type t)) (= vsym (:sym t))))
            ok? (fn [t] (#{:var :const} (:type t)))]
        (if (and op (== 2 (count args)) (ok? x) (ok? y) (not (and (var? x) (var? y))))
          (cond
            (var? x) (recur (next cs) (conj acc {:op op :bound y}))
            (var? y) (recur (next cs) (conj acc {:op (flip-op op) :bound x}))
            :else acc)
          acc))
      acc)))

(defn- hint-clause
  "Data pattern `c` annotated with the range predicates that follow it."
  [c cs pending]
  (if (and (= :data-pattern (:clause c)) (= :var (:type (get (:terms c) 2))))
    (let [hs (range-hints (:sym (get (:terms c) 2))
                          (concat pending (take-while #(= :pred (:clause %)) cs)))]
      (if (seq hs) (assoc c ::range-hints hs) c))
    c))

(defn- range-vt-ok?
  "True when bound `b` has exactly the class of the values of value type `vt`, so that
  the comparison of the post-filter agrees with the order of the index. NaN is refused
  (it compares equal to every number); Timestamps are refused (they order below Dates)."
  [vt b]
  (case vt
    (:db.type/long :db.type/ref) (instance? Long b)
    :db.type/instant (identical? Date (class b))
    :db.type/string (instance? String b)
    :db.type/double (and (instance? Double b) (not (Double/isNaN (double b))))
    false))

(defn- range-bounds
  "[lo hi] (either may be nil = open) covering, for every tuple of the relation, the
  values that can pass all `hints`; nil when no usable bound exists or a bound has a
  type other than the attribute's (the predicate then runs unfiltered)."
  [hints vidx ^ArrayList tuples vt]
  (let [hs (into [] (keep (fn [{:keys [op bound]}]
                            (when-let [b (if (= :const (:type bound))
                                           bound
                                           (when-let [i (get vidx (:sym bound))] {:idx (long i)}))]
                              [op b])))
                 hints)]
    (when (seq hs)
      ;; per hint, resolved once: the tuple column (-1 = constant), the constant, and
      ;; whether it bounds from below / above; per tuple, a loop into a reused [lo hi]
      (let [m (count hs)
            ^ints hidx (int-array (map (fn [[_ b]] (if (contains? b :idx) (:idx b) -1)) hs))
            ^objects hval (object-array (map (fn [[_ b]] (:val b)) hs))
            ^booleans lo? (boolean-array (map (fn [[op _]] (contains? #{:gt :ge :eq} op)) hs))
            ^booleans hi? (boolean-array (map (fn [[op _]] (contains? #{:lt :le :eq} op)) hs))
            lh (object-array 2)
            n (.size tuples)]
        (loop [ti 0 hull-lo nil hull-hi nil lo-open? false hi-open? false]
          (if (< ti n)
            (let [^objects t (.get tuples ti)
                  ok? (loop [k 0 lo nil hi nil]
                        (if (< k m)
                          (let [i (aget hidx k)
                                bv (if (neg? i) (aget hval k) (aget t i))]
                            (if-not (range-vt-ok? vt bv)
                              false
                              (recur (inc k)
                                     (if (and (aget lo? k) (or (nil? lo) (pos? (compare bv lo)))) bv lo)
                                     (if (and (aget hi? k) (or (nil? hi) (neg? (compare bv hi)))) bv hi))))
                          (do (aset lh 0 lo) (aset lh 1 hi) true)))]
              (when ok?
                (let [lo (aget lh 0) hi (aget lh 1)]
                  (recur (inc ti)
                         (cond lo-open? nil (nil? lo) nil (nil? hull-lo) lo (neg? (compare lo hull-lo)) lo :else hull-lo)
                         (cond hi-open? nil (nil? hi) nil (nil? hull-hi) hi (pos? (compare hi hull-hi)) hi :else hull-hi)
                         (or lo-open? (nil? lo))
                         (or hi-open? (nil? hi))))))
            (when (or hull-lo hull-hi)
              [hull-lo hull-hi])))))))

(defn- prefix-successor
  "Smallest byte string greater than every string starting with `p`; nil when none."
  ^bytes [^bytes p]
  (loop [i (dec (alength p))]
    (when (>= i 0)
      (if (== 0xFF (bit-and 0xFF (aget p i)))
        (recur (dec i))
        (let [r (Arrays/copyOf p (inc i))]
          (aset r i (unchecked-byte (inc (bit-and 0xFF (aget r i)))))
          r)))))

;; compares the cursor's key in place (no copy per entry) and passes flyweight views on
(deftype StopCursor [^mnemonic.index.ICursor c ^bytes stop]
  mnemonic.index.ICursor
  (valid [_] (and (.valid c)
                  (or (nil? stop)
                      (neg? (Arrays/compareUnsigned (index/key-buf c) 0 (unchecked-int (index/key-len c))
                                                    stop 0 (alength stop))))))
  (advance [_] (.advance c))
  (key [_] (.key c))
  (datom [_] (.datom c))
  (retractTx [_] (.retractTx c))
  (close [_] (.close c))
  mnemonic.index.IViewCursor
  (view [_] (index/view-of c))
  mnemonic.index.IKeyView
  (keyBuf [_] (index/key-buf c))
  (keyLen [_] (index/key-len c)))

(defn- range-db
  "A db view whose reads are the `idx` entries of `db` from `start` up to (excluding)
  `stop`; everything else is delegated to `db`."
  [db idx ^bytes start ^bytes stop]
  (reify index/IDb
    (-view [_ _ _] (->StopCursor (index/-seek-view db idx start) stop))
    (-seek-view [_ index s] (index/-seek-view db index s))
    (-seek-current [_ index p] (index/-seek-current db index p))
    (-seek-history [_ index p] (index/-seek-history db index p))
    (-basis-t [_] (index/-basis-t db))
    (-schema [_] (index/-schema db))
    (-as-of-t [_] (index/-as-of-t db))
    (-since-t [_] (index/-since-t db))
    (-history? [_] (index/-history? db))
    (-filter-pred [_] (index/-filter-pred db))
    (-raw [_] (index/-raw db))))

(defn- double-edge
  "Encoded-order edge for a Double bound: -0.0 and 0.0 compare equal but encode apart,
  so a lower bound of zero starts at -0.0 and an upper bound of zero ends at 0.0."
  [^double x lower?]
  (if (== x 0.0) (if lower? -0.0 0.0) x))

(defn- range-plan
  "A range-db for a bounded AVET scan of the pattern (constant attribute `a`, free
  entity and value), or nil when not applicable. The predicates stay as post-filters,
  so the bounds only have to cover every passing value."
  [db rel specs hints a]
  (when (and hints a
             (contains? #{:free :blank} (:kind (nth specs 0)))
             (identical? :free (:kind (nth specs 2))))
    (let [^Attr attr (get (:attrs (index/-schema db)) a)
          vt (:value-type attr)]
      (when (contains? #{:db.type/long :db.type/ref :db.type/instant :db.type/string :db.type/double} vt)
        (when-let [[lo hi] (range-bounds hints (var-index rel) (:tuples rel) vt)]
          (let [dbl? (= vt :db.type/double)
                ;; a stored NaN compares equal to everything: it passes <= and >= (and
                ;; nothing else), so unless a strict / equality predicate rejects it the
                ;; upper end stays open to include it (NaN encodes above +Infinity)
                nan-passes? (and dbl? (every? #(contains? #{:le :ge} (:op %)) hints))
                lo (if (and dbl? (some? lo)) (double-edge lo true) lo)
                hi (if (and dbl? (some? hi)) (double-edge hi false) hi)
                hi (if nan-passes? nil hi)]
            (range-db db :avet
                      (codec/encode-prefix :avet (if (nil? lo) [a] [a lo]))
                      (prefix-successor (codec/encode-prefix :avet (if (nil? hi) [a] [a hi]))))))))))

;; ---- access to the datoms of a pattern
;;
;; An access describes how one clause reaches its datoms. Two implementations:
;; `fast-access` (constant attribute, no tx / added constraint: everything is resolved
;; once per clause) and `generic-access` (any other shape, resolved per tuple). Both
;; return {:scan-const (fn [] [idx prefix pass? a] | nil)   the range of the constant parts
;;         :key-fn     (fn [tuple] join-key | nil)          key of a tuple's bound positions
;;         :datom-key  (fn [datom] join-key)
;;         :seek       (fn [tuple f])}                      scan of one tuple's datoms

(defn- datom-key-fn
  "Join key of a datom at the (sorted) bound positions `bound-pos`."
  [bound-pos]
  (if (== 1 (count bound-pos))
    (let [p (long (first bound-pos))] (fn [^IDatomView d] (datom-comp d p)))
    (let [ps (long-array bound-pos)]
      (fn [^IDatomView d]
        (let [ks (object-array (alength ps))]
          (dotimes [i (alength ps)] (aset ks i (datom-comp d (aget ps i))))
          (Arrays/asList ks))))))

(defn- unconstrained? [kind]
  (or (identical? kind :free) (identical? kind :blank) (identical? kind :same)))

(defn- fast-access
  "Access for patterns with a constant attribute and no tx / added constraint, nil for
  other shapes, ::no-match when nothing can match (attribute is not an entity id)."
  [ctx ^longs cnt db specs bound-pos same]
  (when (and (identical? :const (:kind (nth specs 1)))
             (unconstrained? (:kind (nth specs 3)))
             (unconstrained? (:kind (nth specs 4))))
    (let [sch (index/-schema db)
          ra (:val (nth specs 1))
          a (cond (keyword? ra) (or (get (:idents sch) ra)
                                    (throw (fns/qerror :db.error/not-an-entity
                                                       (str "Unable to resolve entity: " ra)
                                                       {:entity ra})))
                  (integer? ra) (long ra)
                  :else ::none)]
      (if (identical? a ::none)
        ::no-match
        (let [a (long a)
              attr (get (:attrs sch) a)
              ke (:kind (nth specs 0))
              kv (:kind (nth specs 2))
              ;; resolution of position 0 / 2 for the tuple (bound) or once (constant)
              e-of (case ke
                     :const (let [d (delay (fns/resolve-eid db (:val (nth specs 0))))]
                              (fn ^long [_] (long @d)))
                     :bound (let [i (int (:idx (nth specs 0)))]
                              (fn ^long [^objects t] (fns/resolve-eid db (aget t i))))
                     nil)
              v-of (case kv
                     :const (let [d (delay (fns/resolve-value db attr (:val (nth specs 2))))]
                              (fn [_] @d))
                     :bound (let [i (int (:idx (nth specs 2)))]
                              (fn [^objects t] (fns/resolve-value db attr (aget t i))))
                     nil)
              e-const? (identical? ke :const)
              v-const? (identical? kv :const)
              ;; prefix of the entries for position values (e, v), nil e / v = unconstrained
              build (fn [ev? vv? e v]
                      (try
                        (let [kb (codec/new-key-buf)]
                          (cond
                            ev? (do (.putLong kb (long e)) (.putLong kb a)
                                    (when vv? (codec/encode-value kb v)))
                            vv? (do (.putLong kb a) (codec/encode-value kb v)))
                          (codec/key-buf-bytes kb))
                        (catch clojure.lang.ExceptionInfo _ nil)))
              aevt-prefix (codec/encode-prefix :aevt [a])
              prefix-fn (fn [t consts-only?]
                          (let [ev? (and (some? e-of) (or e-const? (not consts-only?)))
                                vv? (and (some? v-of) (or v-const? (not consts-only?)))
                                e (if ev? (e-of t) 0)
                                v (when vv? (v-of t))]
                            (cond
                              (and vv? (identical? v ::none)) nil
                              (or ev? vv?) (build ev? vv? e v)
                              :else aevt-prefix)))
              idx-of (fn [consts-only?]
                       (cond (and e-of (or e-const? (not consts-only?))) :eavt
                             (and v-of (or v-const? (not consts-only?))) :avet
                             :else :aevt))
              pass? (same-pass same)
              empty-t (object-array 0)
              bound-e? (identical? ke :bound)
              bound-v? (identical? kv :bound)
              seek-idx (idx-of false)
              key-fn (cond
                       (and bound-e? bound-v?)
                       (fn [^objects t]
                         (let [e (e-of t) v (v-of t)]
                           (when-not (identical? v ::none)
                             (Arrays/asList (doto (object-array 2) (aset 0 e) (aset 1 v))))))
                       bound-e? (fn [^objects t] (e-of t))
                       bound-v? (fn [^objects t] (let [v (v-of t)] (when-not (identical? v ::none) v)))
                       :else (fn [_] nil))]
          {:scan-const (fn []
                         (when-let [p (prefix-fn empty-t true)]
                           [(idx-of true) p pass? a]))
           :key-fn (if (or bound-e? bound-v?) key-fn (fn [_] nil))
           :datom-key (datom-key-fn bound-pos)
           :seek (fn [^objects t f]
                   (when-let [p (prefix-fn t false)]
                     (scan! ctx cnt db seek-idx p pass? f)))})))))

(defn- generic-access
  [ctx ^longs cnt db specs terms bound-pos same]
  (let [bp (vec bound-pos)
        ^ints bpa (int-array bp)
        nbp (alength bpa)
        keyf (if (== 1 nbp)
               (let [p (aget bpa 0)] (fn [full] (nth full p)))
               (fn [full]
                 (let [ks (object-array nbp)]
                   (dotimes [i nbp] (aset ks i (nth full (aget bpa i))))
                   (Arrays/asList ks))))]
    {:scan-const (fn []
                   (when-let [res (resolve-pattern db specs (object-array 0) true)]
                     (when-let [[idx prefix covered?] (scan-plan res terms)]
                       (when-let [pass? (datom-filter res covered? same)]
                         [idx prefix pass? (let [a (nth res 1)] (when-not (free? a) a))]))))
     :key-fn (fn [t] (when-let [full (resolve-pattern db specs t false)] (keyf full)))
     :datom-key (datom-key-fn bp)
     :seek (fn [t f]
             (when-let [res (resolve-pattern db specs t false)]
               (when-let [[idx prefix covered?] (scan-plan res terms)]
                 (when-let [pass? (datom-filter res covered? same)]
                   (scan! ctx cnt db idx prefix pass? f)))))}))

;; ---- executing a data pattern against a db

(def ^:private hash-join-min-tuples
  "Relations smaller than this seek per tuple without considering a scan."
  8)

(def ^:private hash-join-scan-factor
  "A scan + hash join is abandoned for per-tuple seeks once its index range turns out to
  have more than this many datoms per relation tuple (a seek costs about as much as
  scanning this many datoms)."
  3)

(def ^:private ^WeakHashMap range-size-cache
  ;; db value -> {range key -> size}; a negative size is a lower bound (-size)
  (WeakHashMap.))

(defn- range-sizes
  "The map of range sizes learned so far for `db` (db values are immutable, so a size
  stays exact for as long as the db lives)."
  ^ConcurrentHashMap [db]
  (locking range-size-cache
    (or (.get range-size-cache db)
        (let [m (ConcurrentHashMap.)]
          (.put range-size-cache db m)
          m))))

(defn- range-key [idx ^bytes prefix]
  (let [b (byte-array (inc (alength prefix)))]
    (aset b 0 (unchecked-byte (case idx :eavt 0 :aevt 1 :avet 2 3)))
    (System/arraycopy prefix 0 b 1 (alength prefix))
    (ByteBuffer/wrap b)))

(defn- eid-window
  "[start stop] of the AEVT entries of attribute `a` whose entity is between the smallest
  and the largest id at column `col` of `tuples`; nil when a value there is not a plain
  long id (an ident or lookup ref: the unbounded scan resolves those)."
  [^ArrayList tuples ^long col ^long a]
  (let [n (.size tuples)]
    (loop [i 0 lo Long/MAX_VALUE hi Long/MIN_VALUE]
      (if (< i n)
        (let [x (aget ^objects (.get tuples i) col)]
          (when (instance? Long x)
            (let [e (long x)]
              (recur (inc i) (min lo e) (max hi e)))))
        [(codec/encode-prefix :aevt [a lo])
         (prefix-successor (codec/encode-prefix :aevt [a hi]))]))))

(defn- window-key
  "range-sizes key of an AEVT window [start stop]."
  [[^bytes start ^bytes stop]]
  (let [sl (alength start)
        tl (if stop (alength stop) 0)
        b (byte-array (+ 2 sl tl))]
    (aset b 0 (unchecked-byte 4))
    (aset b 1 (unchecked-byte sl))
    (System/arraycopy start 0 b 2 sl)
    (when stop (System/arraycopy stop 0 b (+ 2 sl) tl))
    (ByteBuffer/wrap b)))

(defn- exec-db-pattern [ctx db rel plan terms hints]
  (let [{:keys [specs new-syms new-pos bound-pos same]} plan
        ^ArrayList tuples (:tuples rel)
        out (ArrayList.)
        n (.size tuples)
        no-new? (empty? new-pos)
        np (int-array new-pos)
        emit! (fn [^objects t ^IDatomView d]
                (.add out (if no-new? t (extend-tuple t (new-values d np)))))
        result {:vars (into (:vars rel) new-syms) :tuples out}]
    (when (pos? n)
      (let [cnt (long-array 1)
            fast (fast-access ctx cnt db specs bound-pos same)]
        (when-not (identical? fast ::no-match)
          (let [{:keys [scan-const key-fn datom-key seek]}
                (or fast (generic-access ctx cnt db specs terms bound-pos same))
                seeks!
                ;; a seek per tuple, once per distinct bound key
                (fn []
                  (let [cache (HashMap.)
                        ^ints bidx (int-array (map #(:idx (nth specs %)) bound-pos))
                        nb (alength bidx)
                        single? (== 1 nb)
                        b0 (aget bidx 0)]
                    (letfn [(cached! [k ^objects t]
                              (let [^ArrayList rows
                                    (or (.get cache k)
                                        (let [rows (ArrayList.)]
                                          (seek t (fn [^IDatomView d]
                                                    (.add rows (if no-new? no-extra (new-values d np)))))
                                          (.put cache k rows)
                                          rows))]
                                (dotimes [i (.size rows)]
                                  (.add out (if no-new? t (extend-tuple t (.get rows i)))))))
                            (direct! [^objects t]
                              (seek t (fn [^IDatomView d] (emit! t d))))]
                      (each-tuple [t tuples ctx]
                        (if single?
                          (let [k (aget t b0)]
                            (if (cache-key? k) (cached! k t) (direct! t)))
                          (let [ks (object-array nb)]
                            (dotimes [i nb] (aset ks i (aget t (aget bidx i))))
                            (if (loop [i 0] (or (== i nb) (and (cache-key? (aget ks i)) (recur (inc i)))))
                              (cached! (Arrays/asList ks) t)
                              (direct! t))))))))]
            (cond
              ;; no variable from the relation is involved: one scan, then a product
              (empty? bound-pos)
              (when-let [[idx prefix pass? a] (scan-const)]
                (scan! ctx cnt (or (range-plan db rel specs hints a) db) idx prefix pass?
                       (fn [^IDatomView d]
                         (each-tuple [t tuples ctx] (emit! t d)))))

              ;; constant attribute, enough tuples: scan the constant part of the pattern
              ;; once and hash join, unless the range is much larger than the relation
              (and (>= n hash-join-min-tuples) (identical? :const (:kind (nth specs 1))))
              (if-let [[idx prefix pass? a] (scan-const)]
                (let [hinted (range-plan db rel specs hints a)
                      ;; entity bound by the relation: scan only the AEVT window from its
                      ;; smallest to its largest id (one transaction's entities are
                      ;; contiguous, so the window is often about the relation's size)
                      win (when (and (nil? hinted) (some? a) (identical? idx :aevt)
                                     (identical? :bound (:kind (nth specs 0))))
                            (eid-window tuples (long (:idx (nth specs 0))) (long a)))
                      rdb (or hinted (when win (range-db db :aevt (nth win 0) (nth win 1))))
                      limit (* hash-join-scan-factor n)
                      sizes (when (or (nil? rdb) win) (range-sizes db))
                      skey (when sizes (if win (window-key win) (range-key idx prefix)))
                      known (when sizes (.get sizes skey))]
                  (if (and known (> (Math/abs (long known)) limit))
                    (seeks!)
                    (let [groups (HashMap.)]
                      (each-tuple [t tuples ctx]
                        (when-let [k (key-fn t)]
                          (let [^ArrayList g (or (.get groups k)
                                                 (let [g (ArrayList.)] (.put groups k g) g))]
                            (.add g t))))
                      (let [r (long (scan-budget! ctx cnt (or rdb db) idx prefix pass?
                                                  (fn [^IDatomView d]
                                                    (when-let [^ArrayList g (.get groups (datom-key d))]
                                                      (dotimes [i (.size g)] (emit! (.get g i) d))))
                                                  limit))]
                        (when (and sizes (< (.size sizes) 256)
                                   (or (nil? known) (neg? (long known))))
                          (.put sizes skey r))
                        (when (neg? r)
                          (.clear out)
                          (seeks!))))))
                nil)

              :else (seeks!))))))
    result))

(defn- exec-coll-pattern [ctx coll rel plan]
  (let [{:keys [specs new-syms new-pos bound-pos same]} plan
        ^ArrayList tuples (:tuples rel)
        out (ArrayList.)
        no-new? (empty? new-pos)
        groups (HashMap.)
        ;; per clause: positions as int arrays, so the per-tuple / per-row work is loops
        nb (count bound-pos)
        ^ints bpos (int-array bound-pos)
        ^ints bidx (int-array (map #(:idx (nth specs %)) bound-pos))
        consts (vec (keep-indexed (fn [p s] (when (= :const (:kind s)) [p (:val s)])) specs))
        nc (count consts)
        ^ints cpos (int-array (map first consts))
        ^objects cval (object-array (map second consts))
        nsame (count same)
        ^ints sp (int-array (map first same))
        ^ints sq (int-array (map second same))
        nn (count new-pos)
        ^ints npos (int-array new-pos)
        ;; join key: the one normalized bound value, else a vector of them
        tkey (fn [^objects t]
               (if (== nb 1)
                 (fns/normalize-value (aget t (aget bidx 0)))
                 (let [k (object-array nb)]
                   (dotimes [i nb] (aset k i (fns/normalize-value (aget t (aget bidx i)))))
                   (LazilyPersistentVector/createOwning k))))
        rkey (fn [row]
               (if (== nb 1)
                 (fns/normalize-value (nth row (aget bpos 0)))
                 (let [k (object-array nb)]
                   (dotimes [i nb] (aset k i (fns/normalize-value (nth row (aget bpos i)))))
                   (LazilyPersistentVector/createOwning k))))]
    (each-tuple [t tuples ctx]
      (let [k (tkey t)
            ^ArrayList g (or (.get groups k) (let [g (ArrayList.)] (.put groups k g) g))]
        (.add g t)))
    (when-not (.isEmpty groups)
      (let [cnt (long-array 1)]
        (doseq [row0 (seq coll)]
          (let [c (aget cnt 0)]
            (aset cnt 0 (inc c))
            (when (zero? (bit-and c 4095)) (check-deadline! ctx)))
          ;; nth is O(1) from here on (a list or seq row is copied once)
          (let [row (if (instance? RandomAccess row0) row0 (vec row0))]
            (when (and (loop [i 0]
                         (or (== i nc) (and (= (aget cval i) (nth row (aget cpos i))) (recur (inc i)))))
                       (loop [i 0]
                         (or (== i nsame) (and (= (nth row (aget sp i)) (nth row (aget sq i))) (recur (inc i))))))
              (when-let [^ArrayList g (.get groups (rkey row))]
                (let [extra (when-not no-new?
                              (let [e (object-array nn)]
                                (dotimes [i nn] (aset e i (nth row (aget npos i))))
                                e))]
                  (dotimes [i (.size g)]
                    (let [^objects t (.get g i)]
                      (.add out (if no-new? t (extend-tuple t extra))))))))))))
    {:vars (into (:vars rel) new-syms) :tuples out}))

;; ------------------------------------------------------------------ clause execution

(declare run-clauses)

(defmulti execute-clause
  "Runs one parsed clause against `rel`, returning the new relation."
  (fn [_ctx _rel clause] (:clause clause)))

(defmethod execute-clause :default [_ _ clause]
  (throw (qerror :db.error/unsupported (str "Unsupported clause: " (pr-str (:clause clause)))
                 {:clause clause})))

(defmethod execute-clause :data-pattern [ctx rel {:keys [src terms] hints ::range-hints}]
  (let [s (source ctx src)
        plan (compile-pattern (:vars rel) terms)]
    (if (db? s)
      (exec-db-pattern ctx s rel plan terms hints)
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
      4 (let [g3 (nth gs 3)] (fn [t] ((getf t) (g0 t) (g1 t) (g2 t) (g3 t))))
      5 (let [g3 (nth gs 3) g4 (nth gs 4)] (fn [t] ((getf t) (g0 t) (g1 t) (g2 t) (g3 t) (g4 t))))
      ;; more arguments: one array per call, no lazy seq
      (let [^objects ga (object-array gs)]
        (fn [t]
          (let [a (object-array n)]
            (dotimes [i n] (aset a i (.invoke ^IFn (aget ga i) t)))
            (.applyTo ^IFn (getf t) (ArraySeq/create a))))))))

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
    (if (= :scalar (:bind binding))
      ;; [(f ...) ?x], the common case: one value per tuple, no row arrays or vectors
      (let [sym (:sym binding)
            j (get (var-index rel) sym)
            out (ArrayList.)]
        (each-tuple [t (:tuples rel) ctx]
          (let [v (call t)]
            (when (some? v)
              (if j
                (when (= v (aget t (long j))) (.add out t))
                (let [n (alength t)
                      r (object-array (inc n))]
                  (System/arraycopy t 0 r 0 n)
                  (aset r n v)
                  (.add out r))))))
        {:vars (if j (:vars rel) (conj (:vars rel) sym)) :tuples out})
      (extend-rel ctx rel (binding-syms binding)
                  (fn [t] (binding-rows binding (call t) false))))))

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
          (let [[rel' pending'] (drain (execute-clause ctx rel (hint-clause c (next cs) pending)) pending)]
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

(defn- fn-array
  "Object[] of the fns of `fs`."
  ^objects [fs]
  (object-array fs))

(defn- tuple-array
  "Object[] of the values of fn-array `fs` applied to tuple `t`."
  ^objects [^objects fs ^objects t]
  (let [n (alength fs)
        r (object-array n)]
    (dotimes [i n] (aset r i (.invoke ^clojure.lang.IFn (aget fs i) t)))
    r))

(deftype RowKey [^objects a ^int h]
  Object
  (hashCode [_] h)
  (equals [_ o] (and (instance? RowKey o) (Arrays/equals a ^objects (.-a ^RowKey o)))))

(defn- row-key [^objects a] (->RowKey a (Arrays/hashCode a)))

(defn- distinct-rows
  "Raw rows (vectors aligned with the find elements), de-duplicated together with the
  :with values."
  [ctx rel ffs wfs]
  (let [allfs (fn-array (concat ffs wfs))
        ffs (fn-array ffs)
        nf (alength ffs)
        with? (pos? (- (alength allfs) nf))
        n (rel-size rel)
        seen (HashSet. (int (min 1048576 (+ 16 (* 2 n)))))
        rows (ArrayList. (int (min 1048576 n)))]
    (each-tuple [t (:tuples rel) ctx]
      (if with?
        (let [all (tuple-array allfs t)]
          (when (.add seen (row-key all))
            (.add rows (LazilyPersistentVector/createOwning (Arrays/copyOf all nf)))))
        (let [fv (tuple-array ffs t)]
          (when (.add seen (row-key fv))
            (.add rows (LazilyPersistentVector/createOwning fv))))))
    (vec rows)))

(defn- distinct-row-set
  "The de-duplicated rows as a HashSet (the plain :find-rel result)."
  [ctx rel ffs]
  (let [ffs (fn-array ffs)
        out (HashSet. (int (min 1048576 (+ 16 (* 2 (rel-size rel))))))]
    (each-tuple [t (:tuples rel) ctx]
      (.add out (LazilyPersistentVector/createOwning (tuple-array ffs t))))
    out))

(defn- group-rows
  "Raw rows for a :find with aggregates: the relation is projected to the non-aggregate
  elements, the aggregate arguments and :with, de-duplicated, grouped by the non-aggregate
  elements and each aggregate applied to the values of its group."
  [ctx rel elems kfs afs wfs aggs]
  (let [nk (count kfs)
        allfs (fn-array (concat kfs afs wfs))
        seen (HashSet.)
        groups (LinkedHashMap.)
        nagg (count afs)]
    (each-tuple [t (:tuples rel) ctx]
      (let [all (tuple-array allfs t)]
        (when (.add seen (row-key all))
          (let [k (LazilyPersistentVector/createOwning (Arrays/copyOf all (int nk)))
                ^objects cols (or (.get groups k)
                                  (let [c (object-array nagg)]
                                    (dotimes [i nagg] (aset c i (ArrayList.)))
                                    (.put groups k c)
                                    c))]
            (dotimes [i nagg] (.add ^ArrayList (aget cols i) (aget all (+ nk i))))))))
    (let [out (ArrayList.)
          ne (count elems)
          ;; per element, resolved once: aggregate or plain
          ^booleans agg? (boolean-array (map #(= :agg (:type %)) elems))
          ^objects aggf (object-array aggs)]
      (doseq [[k ^objects cols] groups]
        (check-deadline! ctx)
        (let [row (object-array ne)]
          (loop [i 0 ki 0 ai 0]
            (when (< i ne)
              (if (aget agg? i)
                (do (aset row i (.invoke ^IFn (aget aggf ai) (vec (aget cols ai))))
                    (recur (inc i) ki (inc ai)))
                (do (aset row i (nth k ki))
                    (recur (inc i) (inc ki) ai)))))
          (.add out (LazilyPersistentVector/createOwning row))))
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
        plain-set? (and (not lazy?) (= :find-rel (:spec find)) (not return-map)
                        (not agg?) (not pull?) (empty? wfs))
        rows (if agg?
               (let [plain (remove #(= :agg (:type %)) elems)
                     aggs (filter #(= :agg (:type %)) elems)]
                 (group-rows ctx rel elems
                             (mapv #(find-element-fn ctx rel %) plain)
                             (mapv #(column-fn rel (:sym (peek (:args %)))) aggs)
                             wfs
                             (mapv aggregates/resolve-agg aggs)))
               (if plain-set?
                 (distinct-row-set ctx rel (mapv #(find-element-fn ctx rel %) elems))
                 (distinct-rows ctx rel (mapv #(find-element-fn ctx rel %) elems) wfs)))
        ;; aggregate rows carry results in place; pull finishing applies to pull elements
        pulled (if pull?
                 (let [^objects fa (object-array finishers)
                       nf (alength fa)]
                   (fn [row]
                     (let [out (object-array nf)]
                       (dotimes [i nf]
                         (let [v (nth row i)
                               f (aget fa i)]
                           (aset out i (if f (f v) v))))
                       (LazilyPersistentVector/createOwning out))))
                 identity)
        to-map (when return-map
                 (let [ks (mapv #(key-fn % (:type return-map)) (:names return-map))
                       nk (count ks)
                       ^objects ka (object-array ks)]
                   (if (and (<= nk 8) (== nk (count (set ks))))
                     ;; few distinct keys: an array map built straight from the row
                     (fn [row]
                       (let [kvs (object-array (* 2 nk))]
                         (dotimes [i nk]
                           (aset kvs (* 2 i) (aget ka i))
                           (aset kvs (inc (* 2 i)) (nth row i)))
                         (PersistentArrayMap. kvs)))
                     (fn [row] (zipmap ks row)))))
        first-of (fn [row] (nth row 0))
        spec (:spec find)]
    (if lazy?
      (case spec
        :find-rel (when (seq rows) (lazy-map (if to-map (comp to-map pulled) pulled) rows))
        :find-coll (when (seq rows) (lazy-map (comp first-of pulled) rows))
        :find-scalar (when-let [row (first rows)]
                       (let [v (first-of (pulled row))]
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
                    plain-set? rows
                    pull? (mapv pulled rows)
                    (or agg? (seq wfs)) rows   ; already a vector of row vectors
                    :else (HashSet. ^Collection rows))
        :find-coll (if pull?
                     (mapv (comp first-of pulled) rows)
                     (mapv first-of rows))
        :find-scalar (some-> (first rows) pulled first-of)
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
