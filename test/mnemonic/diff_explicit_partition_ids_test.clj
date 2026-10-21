(ns mnemonic.diff-explicit-partition-ids-test
  "Explicit entity ids in :db.part/db, :db.part/tx and the user partition that were never
  allocated: Datomic (probed, 1.0.7745) accepts them iff n < max(next-t, next-db-n), one rule
  for all three partitions, with next-t the t of the tx being processed (so the current tx's
  own id is rejected). Accepted ids are ordinary entities (a second card-one assert replaces
  the first, retract works). Boundaries are checked in fresh and non-fresh db states."
  (:require [clojure.test :refer [deftest is]]
            [datomic.api :as d]
            [mnemonic.oracle :as o]))

(def ^:private tx-base 13194139533312)

(defn- step! [conns data expect]
  (let [r (o/transact-both conns data)
        kind (fn [x] (if (contains? x :error) (:error x) :ok))]
    (is (= (:datomic r) (:mnemonic r)) (pr-str data))
    (is (= expect (kind (:datomic r))) (pr-str data))))

(deftest explicit-db-and-tx-partition-ids
  (o/with-both [cd cm]
    (let [conns [cd cm]
          far-db 123456789
          far-tx (+ tx-base 123456789)]
      (step! conns [[:db/add far-db :db/doc "a"]] :db.error/invalid-entity-id)
      (step! conns [[:db/add far-tx :db/doc "a"]] :db.error/invalid-entity-id)
      (step! conns [[:db/retract far-db :db/doc "a"]] :db.error/invalid-entity-id)
      (doseq [e [500 (+ tx-base 500)]]
        (step! conns [[:db/add e :db/doc "a"]] :ok)
        (step! conns [[:db/add e :db/doc "b"]] :ok)
        (step! conns [[:db/retract e :db/doc "b"]] :ok)
        (step! conns [[:db/retract e :db/doc "b"]] :ok)))))

(defn- attr [i]
  {:db/ident (keyword "p" (str "a" i)) :db/valueType :db.type/string
   :db/cardinality :db.cardinality/one})

(def ^:private states
  {:fresh (fn [_ _] nil)
   :schema-5-txs (fn [cs step] (dotimes [i 5] (step cs [(attr i)])))
   :attrs-100-one-tx (fn [cs step] (step cs (mapv attr (range 100))))
   :attrs-300-one-tx (fn [cs step] (step cs (mapv attr (range 300))))
   :data-30-txs (fn [cs step]
                  (step cs [(attr 0)])
                  (dotimes [i 30] (step cs [{:db/id "x" :p/a0 (str i)}])))
   ;; db-partition ids outrun t: Datomic (and MnemonicDB) lift t past them
   :db-idents-1200-then-schema (fn [cs step]
                                 (step cs (fn [tempid]
                                           (mapv (fn [i] {:db/id (tempid :db.part/db)
                                                          :db/ident (keyword "q" (str "i" i))})
                                                 (range 1200))))
                                 (dotimes [i 5] (step cs [(attr i)])))})

(defn- kind [x] (if (contains? x :error) (:error x) :ok))

(defn- ok-step! [cs data]
  (let [r (o/transact-both cs data)]
    (is (not (contains? (:datomic r) :error)) (pr-str (:datomic r)))
    (is (not (contains? (:mnemonic r) :error)) (pr-str (:mnemonic r)))))

(deftest explicit-ids-follow-datomic-window-in-non-fresh-states
  (doseq [[sname setup] states
          [pname base] [[:db 0] [:tx tx-base] [:user (bit-shift-left 4 42)]]]
    (o/with-both [cd cm]
      (let [conns [cd cm]]
        (setup conns ok-step!)
        ;; every accepted tx moves the window, so each probe re-reads it; probes are
        ;; picked relative to the window as it is at round start, in descending order of
        ;; acceptance so rejected ones (which change nothing) come last
        (dotimes [_ 2]
          (let [limit (d/next-t (d/db cd))
                probe (fn [n]
                        (let [limit (d/next-t (d/db cd))
                              data [[:db/add (+ base n) :db/doc (str sname pname n)]]
                              r (o/transact-both conns data)
                              expect (if (< n limit) :ok :db.error/invalid-entity-id)]
                          (is (= (:datomic r) (:mnemonic r)) (pr-str [sname pname n limit]))
                          (is (= expect (kind (:datomic r)))
                              (pr-str [sname pname n limit]))))]
            (doseq [n (distinct [(+ limit 2) (inc limit) limit])] (probe n))
            (doseq [n [999 (- limit 2)]] (probe n))
            (probe (dec (d/next-t (d/db cd))))
            (probe (d/next-t (d/db cd)))))))))
