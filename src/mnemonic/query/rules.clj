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
  (:import (java.util ArrayList HashMap HashSet IdentityHashMap LinkedHashSet)))

(set! *warn-on-reflection* true)

(def ^:private qerror fns/qerror)

;; ------------------------------------------------------------------ helpers

(defn- project-distinct
  "Relation of the distinct tuples of `rel` restricted to `syms` (which must be bound)."
  [ctx rel syms]
  (let [vidx (e/var-index rel)
        cols (mapv #(long (vidx %)) syms)
        n (count cols)
        ^ArrayList tuples (:tuples rel)
        seen (HashSet.)
        out (ArrayList.)]
    (dotimes [i (.size tuples)]
      (when (zero? (bit-and i 4095)) (e/check-deadline! ctx))
      (let [^objects t (.get tuples i)
            r (object-array n)]
        (dotimes [j n] (aset r j (aget t (long (nth cols j)))))
        (when (.add seen (vec r)) (.add out r))))
    {:vars (vec syms) :tuples out}))

(defn- key-of [^objects t cols]
  (mapv #(aget t (long %)) cols))

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
          rcols (mapv ridx vars)
          matched (HashSet.)
          ^ArrayList rtuples (:tuples res)
          _ (dotimes [i (.size rtuples)] (.add matched (key-of (.get rtuples i) rcols)))
          cols (mapv vidx vars)
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
            bcols (mapv ridx bound)
            fcols (mapv ridx fresh)
            ^ArrayList rtuples (:tuples res)]
        (dotimes [i (.size rtuples)]
          (when (zero? (bit-and i 4095)) (e/check-deadline! ctx))
          (let [^objects t (.get rtuples i)]
            (.add results [(key-of t bcols) (key-of t fcols)])))))
    (let [groups (HashMap.)
          bcols (mapv vidx bound)]
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
                 :demand (HashSet.) :dpend (ArrayList.) :dd (ArrayList.)
                 :ans (HashSet.) :aidx (HashMap.) :apend (ArrayList.) :da (ArrayList.)}]
          (.put cs k c)
          c))))

(defn- add-demand! [c v]
  (when (.add ^HashSet (:demand c) v)
    (.add ^ArrayList (:dpend c) v)))

(defn- bound-key [c ans]
  (mapv #(nth ans %) (:pos c)))

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
          cols (mapv (fn [h]
                       (or (ridx h)
                           (throw (qerror :db.error/insufficient-binding
                                          (str "Rule head variable " h " is not bound by the rule body")
                                          {:variables #{h}}))))
                     head)
          ^ArrayList rtuples (:tuples res)]
      (dotimes [i (.size rtuples)]
        (when (zero? (bit-and i 4095)) (e/check-deadline! ctx))
        (add-answer! c (key-of (.get rtuples i) cols))))))

(defn- has-delta? [session rule-name]
  (some (fn [c] (and (= rule-name (:name c)) (pos? (.size ^ArrayList (:da c)))))
        (.values ^HashMap (:classes session))))

(defn- eval-class! [ctx session c]
  (let [dd (vec (:dd c))]
    (doseq [d (get (:rules ctx) (:name c))]
      (when (seq dd)
        (eval-def! ctx session c d dd nil))
      (when (pos? (.size ^HashSet (:demand c)))
        (doseq [site (collect-sites (:body d))]
          (when (has-delta? session (:name site))
            (eval-def! ctx session c d (vec (:demand c)) {:clause site})))))))

(defn- fixpoint! [ctx session]
  (loop []
    (e/check-deadline! ctx)
    (let [cs (vec (.values ^HashMap (:classes session)))]
      (doseq [c cs] (snapshot! c))
      (when (some #(or (pos? (.size ^ArrayList (:dd %))) (pos? (.size ^ArrayList (:da %)))) cs)
        (doseq [c cs] (eval-class! ctx session c))
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
          key-of-tuple (fn [t] (mapv #(% t) getters))
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
            delta-index (when (and delta (identical? (:clause delta) clause))
                          (group-by #(bound-key c %) (vec ^ArrayList (:da c))))
            ^HashMap aidx (:aidx c)
            lookup (if delta-index
                     (fn [k] (get delta-index k))
                     (fn [k] (.get aidx k)))
            free (filterv #(= :free (:kind (nth plan %))) (range (count plan)))
            new-syms (mapv #(:sym (nth plan %)) free)
            same (vec (keep-indexed (fn [i p] (when (= :same (:kind p)) [i (:pos p)])) plan))]
        (e/extend-rel
         ctx rel new-syms
         (fn [t]
           (let [seen (HashSet.)]
             (keep (fn [ans]
                     (when (every? (fn [[i j]] (= (nth ans i) (nth ans j))) same)
                       (let [vals (mapv #(nth ans %) free)]
                         (when (.add seen vals) (object-array vals)))))
                   (lookup (key-of-tuple t))))))))))
