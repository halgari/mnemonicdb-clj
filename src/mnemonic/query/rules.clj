(ns mnemonic.query.rules
  "Rules, `not` / `not-join` and `or` / `or-join` for the query engine.

  not / or: the join variables are projected out of the relation, the body (or each
  branch) runs against that small relation, and the result is anti-joined / unioned back.

  Rule calls: bottom-up semi-naive evaluation, per top-level rule call. Calls form
  classes (rule name, which argument positions are bound, source). A class has a set of
  demanded bound-argument tuples (seeded from the calling relation, and from calls in rule
  bodies) and a set of answers (full head tuples). Rounds evaluate every definition of every
  class against its new demand, and against the new answers (the delta) of one recursive
  call at a time, until nothing changes. Calls inside `not` start their own evaluation."
  (:require [mnemonic.query.engine :as e]
            [mnemonic.query.fns :as fns])
  (:import (clojure.lang IFn LazilyPersistentVector)
           (java.util ArrayList HashMap HashSet IdentityHashMap LinkedHashSet List)))

(set! *warn-on-reflection* true)

(def ^:private qerror fns/qerror)

;; ------------------------------------------------------------------ helpers

(defn- project-distinct
  "Relation of the distinct tuples of `rel` restricted to `syms` (which must be bound)."
  [ctx rel syms]
  (let [vidx (e/var-index rel)
        ^ints cols (int-array (map vidx syms))
        n (alength cols)
        ^ArrayList tuples (:tuples rel)
        seen (HashSet.)
        out (ArrayList.)]
    (dotimes [i (.size tuples)]
      (when (zero? (bit-and i 4095)) (e/check-deadline! ctx))
      (let [^objects t (.get tuples i)
            r (object-array n)]
        (dotimes [j n] (aset r j (aget t (aget cols j))))
        ;; the key vector shares the tuple's array (tuples are never written once built)
        (when (.add seen (LazilyPersistentVector/createOwning r)) (.add out r))))
    {:vars (vec syms) :tuples out}))

(defn- cols-of
  "Column indexes (an int[]) for `key-of`."
  ^ints [cols]
  (int-array cols))

(defn- key-of
  "Vector of the values of tuple `t` at the columns `cols` (an int[]); equal to (and
  hashing as) the vector `mapv` would build, without the closure or the copy."
  [^objects t ^ints cols]
  (let [n (alength cols)
        r (object-array n)]
    (dotimes [j n] (aset r j (aget t (aget cols j))))
    (LazilyPersistentVector/createOwning r)))

(defn- insufficient [syms clause]
  (qerror :db.error/insufficient-binding
          (str (vec syms) " not bound in " (name (:clause clause)) " clause")
          {:variables (set syms) :clause clause}))

;; ------------------------------------------------------------------ not

(defn- anti-join [ctx rel {:keys [vars body] :as clause}]
  (let [vars (vec (distinct vars))
        vidx (e/var-index rel)]
    (when-let [missing (seq (remove #(contains? vidx %) vars))]
      (throw (insufficient missing clause)))
    (let [depth (inc (long (::depth ctx 0)))
          _ (when (> depth 64)
              (throw (qerror :db.error/invalid-query "Rule recursion through negation is too deep"
                             {:clause clause})))
          sub (project-distinct ctx rel vars)
          ;; rule calls under negation are evaluated to completion on their own
          res (e/run-clauses (-> ctx (dissoc ::session ::delta) (assoc ::depth depth)) sub body)
          ridx (e/var-index res)
          matched (HashSet.)
          ^ArrayList rtuples (:tuples res)
          ;; built only when there are results (as before, an unbound var fails only then)
          _ (when (pos? (.size rtuples))
              (let [rcols (cols-of (map ridx vars))]
                (dotimes [i (.size rtuples)] (.add matched (key-of (.get rtuples i) rcols)))))
          cols (cols-of (map vidx vars))
          out (ArrayList.)
          ^ArrayList tuples (:tuples rel)]
      (dotimes [i (.size tuples)]
        (when (zero? (bit-and i 4095)) (e/check-deadline! ctx))
        (let [^objects t (.get tuples i)]
          (when-not (.contains matched (key-of t cols))
            (.add out t))))
      {:vars (:vars rel) :tuples out})))

(defmethod e/execute-clause :not [ctx rel clause] (anti-join ctx rel clause))
(defmethod e/execute-clause :not-join [ctx rel clause] (anti-join ctx rel clause))

;; ------------------------------------------------------------------ or

(defn- exec-or [ctx rel {:keys [vars branches] :as clause}]
  (let [vars (vec (distinct vars))
        vidx (e/var-index rel)
        bound (filterv #(contains? vidx %) vars)
        fresh (filterv #(not (contains? vidx %)) vars)
        sub (project-distinct ctx rel bound)
        results (LinkedHashSet.)]
    (doseq [branch branches]
      (let [res (e/run-clauses ctx sub branch)
            ridx (e/var-index res)
            _ (when-let [missing (seq (remove #(contains? ridx %) fresh))]
                (throw (insufficient missing clause)))
            bcols (cols-of (map ridx bound))
            fcols (cols-of (map ridx fresh))
            ^ArrayList rtuples (:tuples res)]
        (dotimes [i (.size rtuples)]
          (when (zero? (bit-and i 4095)) (e/check-deadline! ctx))
          (let [^objects t (.get rtuples i)]
            (.add results [(key-of t bcols) (key-of t fcols)])))))
    (let [groups (HashMap.)
          bcols (cols-of (map vidx bound))]
      (doseq [[k f] results]
        (let [^ArrayList l (or (.get groups k)
                               (let [l (ArrayList.)] (.put groups k l) l))]
          (.add l (object-array f))))
      (e/extend-rel ctx rel fresh (fn [^objects t] (.get groups (key-of t bcols)))))))

(defmethod e/execute-clause :or [ctx rel clause] (exec-or ctx rel clause))
(defmethod e/execute-clause :or-join [ctx rel clause] (exec-or ctx rel clause))

;; ------------------------------------------------------------------ rule evaluation state

(defn- new-session []
  {:classes (HashMap.) :srcs (IdentityHashMap.)})

(defn- src-id ^long [session src-val]
  (let [^IdentityHashMap m (:srcs session)]
    (if-let [i (.get m src-val)]
      (long i)
      (let [i (.size m)] (.put m src-val i) i))))

(defn- get-class
  "The class for rule `name` called with argument positions `mask` (vector of booleans)
  against source value `src-val`, created on first use."
  [session name mask src-val]
  (let [k [name mask (src-id session src-val)]
        ^HashMap cs (:classes session)]
    (or (.get cs k)
        (let [c {:name name :mask mask :src src-val
                 :pos (vec (keep-indexed (fn [i b] (when b i)) mask))
                 :posa (int-array (keep-indexed (fn [i b] (when b i)) mask))
                 :demand (HashSet.) :dpend (ArrayList.) :dd (ArrayList.)
                 :ans (HashSet.) :aidx (HashMap.) :apend (ArrayList.) :da (ArrayList.)}]
          (.put cs k c)
          c))))

(defn- add-demand! [c v]
  (when (.add ^HashSet (:demand c) v)
    (.add ^ArrayList (:dpend c) v)))

(defn- bound-key
  "The bound-argument values of answer `ans` (a vector) for class `c`, as a vector."
  [c ans]
  (let [^ints pos (:posa c)
        n (alength pos)
        r (object-array n)]
    (dotimes [i n] (aset r i (nth ans (aget pos i))))
    (LazilyPersistentVector/createOwning r)))

(defn- add-answer! [c ans]
  (when (.add ^HashSet (:ans c) ans)
    (.add ^ArrayList (:apend c) ans)
    (let [k (bound-key c ans)
          ^HashMap idx (:aidx c)
          ^ArrayList l (or (.get idx k) (let [l (ArrayList.)] (.put idx k l) l))]
      (.add l ans))))

(defn- snapshot!
  "Moves the pending demand / answers of class `c` into the round's delta."
  [c]
  (let [^ArrayList dd (:dd c) ^ArrayList dp (:dpend c)
        ^ArrayList da (:da c) ^ArrayList ap (:apend c)]
    (.clear dd) (.addAll dd dp) (.clear dp)
    (.clear da) (.addAll da ap) (.clear ap)))

(defn- collect-sites
  "Rule-call clauses of `clauses` whose answers can grow during evaluation (not those
  under negation, which are evaluated separately)."
  [clauses]
  (mapcat (fn [c]
            (case (:clause c)
              :rule [c]
              :and (collect-sites (:clauses c))
              (:or :or-join) (mapcat collect-sites (:branches c))
              nil))
          clauses))

(defn- eval-def!
  "Evaluates one definition `d` for class `c` seeded with demand tuples `seeds`, adding
  the head tuples found to the answers of `c`."
  [ctx session c d seeds delta]
  (let [{:keys [head body]} d]
    (when (not= (count head) (count (distinct head)))
      (throw (qerror :db.error/unsupported "Rule heads with repeated variables are not supported"
                     {:head head})))
    (let [tuples (ArrayList.)
          _ (doseq [v seeds] (.add tuples (object-array v)))
          seed-rel {:vars (mapv #(nth head %) (:pos c)) :tuples tuples}
          ctx' (-> ctx
                   (assoc ::session session ::delta delta)
                   (assoc-in [:sources '$] (:src c)))
          res (e/run-clauses ctx' seed-rel body)
          ridx (e/var-index res)
          cols (cols-of (map (fn [h]
                               (or (ridx h)
                                   (throw (qerror :db.error/insufficient-binding
                                                  (str "Rule head variable " h " is not bound by the rule body")
                                                  {:variables #{h}}))))
                             head))
          ^ArrayList rtuples (:tuples res)]
      (dotimes [i (.size rtuples)]
        (when (zero? (bit-and i 4095)) (e/check-deadline! ctx))
        (add-answer! c (key-of (.get rtuples i) cols))))))

(defn- eval-class!
  "One round for class `c`. `delta-rules`: the names of the rules some class of which has
  new answers this round (a class's delta only changes at `snapshot!`)."
  [ctx session c delta-rules]
  (let [dd (vec (:dd c))
        ^HashSet demand (:demand c)
        ;; the demand as a vector, rebuilt only when it has grown (calls in rule bodies
        ;; add to it during the round)
        dvec (volatile! nil)
        dsize (volatile! -1)
        demand-vec (fn []
                     (let [s (.size demand)]
                       (when-not (== s (long @dsize))
                         (vreset! dvec (vec demand))
                         (vreset! dsize s))
                       @dvec))]
    (doseq [d (get (:rules ctx) (:name c))]
      (when (seq dd)
        (eval-def! ctx session c d dd nil))
      (when (pos? (.size demand))
        (doseq [site (collect-sites (:body d))]
          (when (contains? delta-rules (:name site))
            (eval-def! ctx session c d (demand-vec) {:clause site})))))))

(defn- fixpoint! [ctx session]
  (loop []
    (e/check-deadline! ctx)
    (let [cs (vec (.values ^HashMap (:classes session)))]
      (doseq [c cs] (snapshot! c))
      (when (some #(or (pos? (.size ^ArrayList (:dd %))) (pos? (.size ^ArrayList (:da %)))) cs)
        ;; classes created during the round start with no delta, so this set holds
        (let [delta-rules (into #{} (keep #(when (pos? (.size ^ArrayList (:da %))) (:name %))) cs)]
          (doseq [c cs] (eval-class! ctx session c delta-rules)))
        (recur)))))

;; ------------------------------------------------------------------ rule calls

(defn- call-plan
  "Per argument: how the call argument relates to the relation `vidx`."
  [vidx args]
  (loop [i 0 plan [] firsts {}]
    (if (== i (count args))
      plan
      (let [a (nth args i)]
        (case (:type a)
          :const (recur (inc i) (conj plan {:kind :const :val (:val a)}) firsts)
          :blank (recur (inc i) (conj plan {:kind :blank}) firsts)
          :var (let [s (:sym a)]
                 (cond
                   (contains? vidx s)
                   (recur (inc i) (conj plan {:kind :bound :idx (long (vidx s))}) firsts)
                   (contains? firsts s)
                   (recur (inc i) (conj plan {:kind :same :pos (firsts s)}) firsts)
                   :else
                   (recur (inc i) (conj plan {:kind :free :sym s}) (assoc firsts s i))))
          (throw (qerror :db.error/invalid-query "Invalid argument in rule call"
                         {:arg a})))))))

(defn- check-rule-sources!
  "Datomic rejects explicit source vars in rule bodies; we allow declared ones but an
  undeclared source var is an error when the rule is called."
  [ctx name defs]
  (doseq [s (into #{} (comp (mapcat :body) (mapcat e/clause-srcs)) defs)]
    (when (and s (not= s '$) (not (contains? (:sources ctx) s)))
      (throw (qerror :db.error/invalid-query
                     (str "Data source not supplied: " s " (in rule " name ")")
                     {:source s :rule name})))))

(defmethod e/execute-clause :rule [ctx rel {:keys [src name args] :as clause}]
  (let [defs (get (:rules ctx) name)]
    (when (empty? defs)
      (throw (qerror :db.error/invalid-query (str "Unable to resolve rule: " name)
                     {:rule name})))
    (when-not (every? #(= (count (:head %)) (count args)) defs)
      (throw (qerror :db.error/invalid-query
                     (str "Rule " name " called with " (count args) " arguments, expected "
                          (vec (distinct (map (comp count :head) defs))))
                     {:rule name})))
    (check-rule-sources! ctx name defs)
    (let [plan (call-plan (e/var-index rel) args)
          mask (mapv #(contains? #{:const :bound} (:kind %)) plan)
          bound-plans (filterv #(contains? #{:const :bound} (:kind %)) plan)
          getters (mapv (fn [{:keys [kind val idx]}]
                          (if (= kind :const)
                            (fn [_] val)
                            (fn [^objects t] (aget t (long idx)))))
                        bound-plans)
          ^objects garr (object-array getters)
          ng (alength garr)
          key-of-tuple (fn [t]
                         (let [r (object-array ng)]
                           (dotimes [i ng] (aset r i (.invoke ^IFn (aget garr i) t)))
                           (LazilyPersistentVector/createOwning r)))
          top? (nil? (::session ctx))
          session (or (::session ctx) (new-session))
          ctx (assoc ctx ::session session)
          c (get-class session name mask (get (:sources ctx) src))
          ^ArrayList tuples (:tuples rel)]
      (dotimes [i (.size tuples)]
        (when (zero? (bit-and i 4095)) (e/check-deadline! ctx))
        (add-demand! c (key-of-tuple (.get tuples i))))
      (when top? (fixpoint! ctx session))
      (let [delta (::delta ctx)
            ;; the delta's answers by bound key (an index like :aidx, for this call only)
            ^HashMap delta-index (when (and delta (identical? (:clause delta) clause))
                                   (let [m (HashMap.)
                                         ^ArrayList da (:da c)]
                                     (dotimes [i (.size da)]
                                       (let [ans (.get da i)
                                             k (bound-key c ans)
                                             ^ArrayList l (or (.get m k) (let [l (ArrayList.)] (.put m k l) l))]
                                         (.add l ans)))
                                     m))
            ^HashMap aidx (or delta-index (:aidx c))
            free (filterv #(= :free (:kind (nth plan %))) (range (count plan)))
            new-syms (mapv #(:sym (nth plan %)) free)
            same (vec (keep-indexed (fn [i p] (when (= :same (:kind p)) [i (:pos p)])) plan))
            ^ints fa (int-array free)
            nf (alength fa)
            ^ints si (int-array (map first same))
            ^ints sj (int-array (map second same))
            nsm (alength si)
            ;; answers are distinct head tuples sharing the lookup's bound values, and a
            ;; :same position repeats a free one: only blank positions can make two answers
            ;; project to the same row
            dedupe? (boolean (some #(= :blank (:kind %)) plan))]
        (e/extend-rel
         ctx rel new-syms
         (fn [t]
           (when-let [^List as (.get aidx (key-of-tuple t))]
             (let [m (.size as)
                   out (ArrayList. m)
                   seen (when dedupe? (HashSet.))]
               (dotimes [x m]
                 (let [ans (.get as x)]
                   (when (loop [k 0]
                           (or (== k nsm)
                               (and (= (nth ans (aget si k)) (nth ans (aget sj k))) (recur (inc k)))))
                     (let [r (object-array nf)]
                       (dotimes [k nf] (aset r k (nth ans (aget fa k))))
                       (when (or (not dedupe?) (.add ^HashSet seen (LazilyPersistentVector/createOwning r)))
                         (.add out r))))))
               out))))))))
