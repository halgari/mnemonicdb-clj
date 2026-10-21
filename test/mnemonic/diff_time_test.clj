(ns mnemonic.diff-time-test
  "Differential test: as-of / since / tx-range with java.util.Date arguments follow
  Datomic's semantics (the first transaction whose :db/txInstant is >= the date)."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datomic.api :as d]
            [mnemonic.api :as m]
            [mnemonic.conn :as conn]
            [mnemonic.oracle :as o]))

(use-fixtures :each (fn [f] (try (f) (finally (conn/reset-registry!)))))

(def ^:private base 1700000000000)
(defn- at [ms] (java.util.Date. (long ms)))

(defn- scenario [c]
  (let [mn? (instance? mnemonic.conn.Connection c)
        transact (if mn? m/transact d/transact)
        db-of (if mn? m/db d/db)
        q (if mn? m/q d/q)
        as-of (if mn? m/as-of d/as-of)
        since (if mn? m/since d/since)
        as-of-t (if mn? m/as-of-t d/as-of-t)
        since-t (if mn? m/since-t d/since-t)
        tx-range (if mn? m/tx-range d/tx-range)
        log (if mn? m/log d/log)
        out (atom [])
        note! (fn [k f] (swap! out conj [k (try (f) (catch Throwable t [:error (class t) (ex-message t)]))]))]
    @(transact c [{:db/id "datomic.tx" :db/txInstant (at base)}
                  {:db/ident :p/n :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])
    (doseq [i (range 1 6)]
      @(transact c [{:db/id "datomic.tx" :db/txInstant (at (+ base (* i 1000)))} {:p/n i}]))
    @(transact c [{:db/id "datomic.tx" :db/txInstant (at (+ base 6000))} {:p/n 6}])
    @(transact c [{:db/id "datomic.tx" :db/txInstant (at (+ base 6000))} {:p/n 7}])
    (let [db (db-of c)
          ns-of (fn [x] (sort (q '[:find [?v ...] :where [_ :p/n ?v]] x)))]
      (doseq [off [-100000 -1 0 1 999 1000 1500 5000 6000 6001 100000]]
        (let [dt (at (+ base off))]
          (note! [:as-of off] #(let [x (as-of db dt)] [(as-of-t x) (ns-of x)]))
          (note! [:since off] #(let [x (since db dt)] [(since-t x) (ns-of x)]))))
      (note! :as-of-epoch #(as-of-t (as-of db (at 0))))
      (note! :as-of-then-date #(let [x (as-of (as-of db 1005) (at (+ base 1500)))] [(as-of-t x) (ns-of x)])))
    (let [lg (log c)
          ts (fn [s e] (mapv :t (tx-range lg s e)))]
      (note! :tx-range-1 #(ts (at (+ base 1000)) (at (+ base 3000))))
      (note! :tx-range-2 #(ts (at (+ base 1500)) (at (+ base 6000))))
      (note! :tx-range-open-end #(ts (at (+ base 1500)) nil))
      (note! :tx-range-open-start #(ts nil (at (+ base 2000))))
      (note! :tx-range-mixed #(ts (at (+ base 1500)) 1004))
      (note! :tx-range-before-all #(ts (at 0) (at (+ base 1000))))
      (note! :tx-range-after-all #(ts (at (+ base 100000)) nil)))
    @out))

(deftest date-time-views-match-datomic
  (o/with-both [cd cm]
    (doseq [[[k vd] [_ vm]] (map vector (scenario cd) (scenario cm))]
      (testing (str k)
        (is (= vd vm))))))
