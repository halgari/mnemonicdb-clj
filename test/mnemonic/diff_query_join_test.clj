(ns mnemonic.diff-query-join-test
  "Differential tests for joins big enough to take the scan + hash join path of a data
  pattern, small enough to take the per-tuple seek path, and the ones in between (the
  engine starts a hash join and gives up when the index range is much larger than the
  relation), also repeated on one db value so the learned range sizes are used."
  (:require [clojure.test :refer [deftest is]]
            [datomic.api :as d]
            [mnemonic.api :as m]
            [mnemonic.oracle :as o]))

(def ^:private schema
  [{:db/id "a" :db/ident :j/id :db/valueType :db.type/long :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/id "b" :db/ident :j/grp :db/valueType :db.type/long :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/id "c" :db/ident :j/name :db/valueType :db.type/string :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/id "d" :db/ident :j/tag :db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   {:db/id "e" :db/ident :j/self :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/id "f" :db/ident :j/score :db/valueType :db.type/double :db/cardinality :db.cardinality/one
    :db/index true}])

(def ^:private n-entities 2500)

(def ^:private tags
  (vec (for [i (range 20)] {:db/id (str "tag" i) :j/id (+ 100000 i) :j/name (str "tag-" i)})))

(def ^:private people
  (vec (for [i (range n-entities)]
         (cond-> {:db/id (str "e" i) :j/id i :j/grp (mod i 10) :j/name (str "nm" (mod i 700))
                  :j/tag [(str "tag" (mod i 20)) (str "tag" (mod (* 3 i) 20))]
                  :j/score (double (mod (* 7 i) 301))}
           (zero? (mod i 5)) (assoc :j/self (str "e" i))
           (and (zero? (mod i 7)) (pos? (mod i 5))) (assoc :j/self (str "e" (mod (+ i 1) n-entities)))))))

(def ^:private queries
  [;; whole-attribute joins: the range is about as large as the relation (hash join)
   '[:find ?e ?g ?n :where [?e :j/grp ?g] [?e :j/name ?n]]
   '[:find ?e ?g ?s :where [?e :j/id ?i] [?e :j/grp ?g] [?e :j/score ?s]]
   ;; selective: a relation of ~250 against a 2500-entry range (seeks, after a probe)
   '[:find ?e ?n :where [?e :j/grp 3] [?e :j/name ?n]]
   '[:find ?e ?s :where [?e :j/grp 3] [?e :j/score ?s]]
   '[:find ?e ?t :where [?e :j/grp 3] [?e :j/tag ?t]]
   ;; tiny relation (below the hash join minimum)
   '[:find ?e ?n :where [?e :j/id 5] [?e :j/name ?n]]
   ;; bound value (not entity): an attribute joined on its value
   '[:find ?e ?e2 :where [?e :j/id 77] [?e :j/name ?n] [?e2 :j/name ?n]]
   '[:find ?t ?e :where [?t :j/id ?i] [(> ?i 100000)] [?e :j/tag ?t]]
   '[:find ?t ?e :where [?t :j/id 100003] [?e :j/tag ?t]]
   ;; both entity and value bound
   '[:find ?e ?g :where [?e :j/grp ?g] [?e :j/grp ?g]]
   '[:find ?e ?g ?g2 :where [?e :j/grp ?g] [?e :j/grp ?g2] [(= ?g ?g2)]]
   '[:find ?e ?t :where [?e :j/tag ?t] [?e :j/tag ?t]]
   ;; a repeated variable within one pattern
   '[:find ?e :where [?e :j/id ?i] [?e :j/self ?e]]
   '[:find ?e ?n :where [?e :j/grp 2] [?e :j/self ?e] [?e :j/name ?n]]
   ;; ranges on the scanned pattern together with a relation
   '[:find ?e ?s :where [?e :j/grp 4] [?e :j/score ?s] [(< ?s 50.0)]]
   '[:find ?e ?s :where [?e :j/id ?i] [(< ?i 1000)] [?e :j/score ?s] [(>= ?s 100.0)] [(< ?s 150.0)]]
   '[:find ?e ?n :where [?e :j/grp ?g] [(< ?g 3)] [?e :j/name ?n] [(>= ?n "nm3")] [(< ?n "nm5")]]
   ;; the relation from :in
   '[:find ?e ?n :in $ [?i ...] :where [?e :j/id ?i] [?e :j/name ?n]]
   '[:find ?e ?g :in $ [[?i ?g]] :where [?e :j/id ?i] [?e :j/grp ?g]]
   ;; unknown bound values
   '[:find ?e :in $ [?v ...] :where [?e :j/name ?v]]
   ;; a contiguous run of entities joined on (an AEVT window about the relation's size,
   ;; scanned instead of the attribute's whole range), card-one and card-many
   '[:find ?e ?n ?t :where [?e :j/id ?i] [(< ?i 400)] [?e :j/name ?n] [?e :j/tag ?t]]
   '[:find ?e ?s :where [?e :j/id ?i] [(>= ?i 1200)] [(< ?i 1260)] [?e :j/score ?s]]
   ;; a run plus one far entity: the window is wide, so the join falls back to seeks
   '[:find ?e ?n :in $ [?k ...] :where [?e :j/id ?k] [?e :j/name ?n]]
   ;; aggregates and :with over a hash join
   '[:find ?g (count ?e) (sum ?s) :where [?e :j/grp ?g] [?e :j/score ?s]]
   '[:find ?g (count ?n) :with ?e :where [?e :j/grp ?g] [?e :j/name ?n]]])

(defn- inputs [q]
  (cond
    (some #{'[?i ...]} q) [(vec (range 0 n-entities 3))]
    (some #{'[[?i ?g]]} q) [(vec (for [i (range 0 n-entities 7)] [i (mod (* i 3) 10)]))]
    (some #{'[?v ...]} q) [(vec (concat (map #(str "nm" %) (range 0 700 3)) ["zzz" 5]))]
    (some #{'[?k ...]} q) [(vec (concat (range 0 300) [(dec n-entities)]))]
    :else []))

(defn- canon [r]
  (cond (instance? java.util.Set r) [:set (set (map vec r))]
        (vector? r) [:vec (frequencies (map #(if (coll? %) (vec %) %) r))]
        :else [:val r]))

(defn- run [q-fn q db args]
  (try {:ok (canon (apply q-fn q db args))}
       (catch Throwable t {:error (or (:db/error (ex-data t)) (class t))})))

(defn- check [label db-d db-m]
  (doseq [q queries
          :let [args (inputs q)
                expected (run d/q q db-d args)]
          round (range 3)]
    (is (= expected (run m/q q db-m args)) (str label " round " round " " (pr-str q)))))

(deftest joins-equal-datomic
  (o/with-both [cd cm]
    (doseq [tx [schema (into tags people)]]
      (let [{:keys [datomic mnemonic]} (o/transact-both [cd cm] tx)]
        (assert (not (:error datomic)) (pr-str (:error datomic)))
        (assert (not (:error mnemonic)) (pr-str (:error mnemonic)))))
    (let [t0 (d/basis-t (d/db cd))]
      (check :current (d/db cd) (m/db cm))
      (o/transact-both [cd cm] [[:db/add [:j/id 3] :j/grp 4] [:db/retract [:j/id 4] :j/name "nm4"]
                                [:db/add [:j/id 5] :j/score 50.5]])
      (check :as-of (d/as-of (d/db cd) t0) (m/as-of (m/db cm) t0))
      (check :after (d/db cd) (m/db cm))
      (check :since (d/since (d/db cd) t0) (m/since (m/db cm) t0))
      (check :history (d/history (d/db cd)) (m/history (m/db cm))))))
