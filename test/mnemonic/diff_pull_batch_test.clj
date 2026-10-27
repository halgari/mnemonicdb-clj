(ns mnemonic.diff-pull-batch-test
  "Differential tests for the paths pull takes over many entities: an entity read once for
  the attributes a pattern names, many dense children read in one scan (prefetch), sparse
  and interleaved ids, wide reverse refs, entities past the one-read cap, nested
  sub-patterns that differ by level, and time views. Results must equal Datomic's."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datomic.api :as d]
            [mnemonic.api :as m]
            [mnemonic.conn :as conn]
            [mnemonic.oracle :as o]))

(use-fixtures :each (fn [f] (try (f) (finally (conn/reset-registry!)))))

(defn- attr [id vt card & {:as opts}]
  (merge {:db/id (str "attr-" id) :db/ident id :db/valueType vt :db/cardinality card} opts))

(def ^:private one :db.cardinality/one)
(def ^:private many :db.cardinality/many)

(def ^:private schema-tx
  [(attr :lo/name :db.type/string one :db/unique :db.unique/identity)
   (attr :mod/lo :db.type/ref one)
   (attr :mod/name :db.type/string one)
   (attr :mod/enabled :db.type/boolean one)
   (attr :mod/added :db.type/instant one)
   (attr :mod/tags :db.type/string many)
   (attr :mod/archive :db.type/ref one)
   (attr :mod/meta :db.type/ref one :db/isComponent true)
   (attr :meta/note :db.type/string one)
   (attr :ar/key :db.type/string one)
   (attr :ar/size :db.type/long one)
   (attr :file/mod :db.type/ref one)
   (attr :file/rel :db.type/string one)
   (attr :file/hash :db.type/string one)
   (attr :file/size :db.type/long one)
   {:db/id "attr-file/path" :db/ident :file/path :db/valueType :db.type/tuple
    :db/tupleAttrs [:file/mod :file/rel] :db/cardinality one}])

(defn- files-of [i]
  ;; mostly a few files; some none; two mods past 512 (wide reverse refs)
  (cond (= i 7) 600 (= i 40) 520 (zero? (mod i 9)) 0 :else (inc (mod i 5))))

(def ^:private n-mods 120)

(def ^:private data-tx
  ;; each mod is followed by its files, so mod and file ids interleave
  (vec
   (concat
    [{:db/id "lo" :lo/name "L"}]
    (mapcat (fn [i]
              (concat
               [(cond-> {:db/id (str "m" i) :mod/lo "lo" :mod/name (str "mod " i)
                         :mod/enabled (even? i) :mod/added #inst "2026-01-01"
                         :mod/archive {:db/id (str "a" i) :ar/key (str "k" i) :ar/size i}}
                  (zero? (mod i 4)) (assoc :mod/tags ["x" "y"])
                  (zero? (mod i 6)) (assoc :mod/meta {:meta/note (str "n" i)}))]
               (for [j (range (files-of i))]
                 {:db/id (str "f" i "-" j) :file/mod (str "m" i) :file/rel (str "r/" j)
                  :file/hash (str "h" i "-" j) :file/size j})))
            (range n-mods))
    ;; past the one-read cap: 600 values of a cardinality-many attribute
    [{:db/id "wide" :mod/lo "lo" :mod/name "wide" :mod/enabled true :mod/tags (mapv #(str "t" %) (range 600))}])))

(defn- setup [cd cm]
  @(d/transact cd schema-tx)
  @(m/transact cm schema-tx)
  (let [rd @(d/transact cd data-tx)
        rm @(m/transact cm data-tx)
        named (fn [r] (into {} (filter (comp string? key)) (:tempids r)))]
    (is (= (named rd) (named rm)) "same entity ids on both sides")
    {:d (d/db cd) :m (m/db cm) :ids (named rm)}))

(defn- run [f]
  (try (f)
       (catch Throwable t
         [:error (or (:db/error (ex-data t)) [:unclassified (class t)])])))

(def ^:private node
  '[:db/id :mod/name :mod/enabled :mod/added :mod/tags
    {:mod/archive [:ar/key :ar/size]}
    {(:file/_mod :limit nil) [:file/rel :file/hash]}])

(def ^:private loadout-patterns
  [['[:lo/name] "pull one attribute"]
   [(conj '[:lo/name] {'(:mod/_lo :limit nil) node}) "loadout, mods, archives, files"]
   [(conj '[:lo/name] {'(:mod/_lo :limit nil) '[:mod/name :mod/meta]}) "component default expansion"]
   [(conj '[:lo/name] {'(:mod/_lo :limit nil) '[*]}) "every mod wildcard"]
   [(conj '[:lo/name] {'(:mod/_lo :limit nil) '[* {:mod/archive [*]}]}) "wildcard plus sub-pattern"]
   [(conj '[:lo/name] {:mod/_lo '[:mod/name]}) "default limit 1000"]
   [(conj '[:lo/name] {'(:mod/_lo :limit 5) '[:mod/name {:mod/archive [:ar/key]}]}) "limit 5"]])

(deftest whole-loadout-pulls-match-datomic
  (o/with-both [cd cm]
    (let [{:keys [d m ids]} (setup cd cm)
          lo (ids "lo")]
      (doseq [[pattern label] loadout-patterns]
        (testing label
          (is (= (run #(d/pull d pattern lo)) (run #(m/pull m pattern lo)))))))))

(defn- mod-ids [ids] (mapv #(ids (str "m" %)) (range n-mods)))

(defn- file-ids [ids i] (mapv #(ids (str "f" i "-" %)) (range (files-of i))))

(def ^:private many-patterns
  ['[:file/rel :file/hash]
   '[:file/rel]
   '[*]
   '[:db/id :file/rel {:file/mod [:mod/name]}]
   '[(:file/rel :as :x) :file/hash]
   '[(:file/rel :as :x) (:file/hash :as :x)]
   '[(:file/rel :xform str) (:file/missing :default 0) :file/size]
   '[:file/path :file/rel]
   '[:mod/name :mod/enabled {:mod/archive [:ar/key]}]
   '[:mod/name {(:file/_mod :limit nil) [:file/rel {:file/mod [:mod/name {:mod/archive [:ar/size]}]}]}]
   '[:mod/name {(:file/_mod :limit 2) [:file/rel]} {:mod/archive [*]}]])

(deftest pull-many-matches-datomic-over-id-shapes
  (o/with-both [cd cm]
    (let [{:keys [d m ids]} (setup cd cm)
          shapes {"dense files of one mod" (file-ids ids 7)
                  "files spanning just under 4x their count" (vec (take-nth 3 (file-ids ids 7)))
                  "sparse files" (vec (take-nth 5 (file-ids ids 7)))
                  "interleaved mods" (mod-ids ids)
                  "mods and files mixed, with repeats and a missing id"
                  (vec (concat (take 40 (mod-ids ids)) (file-ids ids 40) [99999999] (take 10 (mod-ids ids))))
                  "past the one-read cap" [(ids "wide") (ids "m1")]}]
      (doseq [[label eids] shapes
              pattern many-patterns]
        (testing (str label " " (pr-str pattern))
          (is (= (run #(d/pull-many d pattern eids)) (run #(m/pull-many m pattern eids)))))))))

(deftest one-read-cap-matches-datomic
  (o/with-both [cd cm]
    (let [{:keys [d m ids]} (setup cd cm)
          e (ids "wide")]
      (doseq [pattern ['[:mod/name :mod/enabled] '[:mod/name (:mod/tags :limit 3)] '[:mod/tags]
                       '[(:mod/tags :limit nil) :mod/name] '[*] '[:mod/name {:mod/lo [:lo/name]}]]]
        (testing (pr-str pattern)
          (is (= (run #(d/pull d pattern e)) (run #(m/pull m pattern e)))))))))

(deftest batched-pulls-match-datomic-on-time-views
  (o/with-both [cd cm]
    (let [{:keys [d m ids]} (setup cd cm)
          t0 (d/basis-t d)
          tx (vec (concat
                   (for [j (range 0 600 7)] {:db/id (ids (str "f7-" j)) :file/rel (str "moved/" j)})
                   (for [j (range 1 600 11)] [:db/retractEntity (ids (str "f7-" j))])
                   [{:db/id (ids "m3") :mod/name "renamed" :mod/enabled true}
                    [:db/retract (ids "m4") :mod/tags "x"]]))]
      (is (= t0 (m/basis-t m)))
      @(d/transact cd tx)
      @(m/transact cm tx)
      (let [d2 (d/db cd) m2 (m/db cm)
            lo (ids "lo")
            files (file-ids ids 7)]
        (doseq [[dd mm label] [[(d/as-of d2 t0) (m/as-of m2 t0) "as-of"]
                               [(d/since d2 t0) (m/since m2 t0) "since"]
                               [d2 m2 "now"]]]
          (testing label
            (doseq [pattern ['[:file/rel :file/hash] '[*] '[:file/rel {:file/mod [:mod/name]}]]]
              (is (= (run #(d/pull-many dd pattern files)) (run #(m/pull-many mm pattern files)))
                  (pr-str pattern)))
            (is (= (run #(d/pull dd (first (second loadout-patterns)) lo))
                   (run #(m/pull mm (first (second loadout-patterns)) lo)))
                "the whole loadout")))))))
