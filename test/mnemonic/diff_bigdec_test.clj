(ns mnemonic.diff-bigdec-test
  "Differential test: BigDecimal values keep their scale (1.0M, 1.00M, 10.50M, 100M
  round-trip exactly; a query for 10.5M does not match 10.50M), while index lookups and
  ranges by value match numerically, as in Datomic."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datomic.api :as d]
            [mnemonic.api :as m]
            [mnemonic.conn :as conn]
            [mnemonic.oracle :as o]))

(use-fixtures :each (fn [f] (try (f) (finally (conn/reset-registry!)))))

(def ^:private values
  [1.0M 1M 1.00M 10.5M 10.50M 0M 0.0M 0.00M -1.0M -1M -1.00M 100M 1E+2M 1E+3M 1000M
   0.001M -0.5M -0.50M 123.4500M])

(defn- exact [v] (if (decimal? v) [(str v) (.scale ^BigDecimal v)] v))

(defn- scenario [c]
  (let [mn? (instance? mnemonic.conn.Connection c)
        transact (if mn? m/transact d/transact)
        db-of (if mn? m/db d/db)
        q (if mn? m/q d/q)
        datoms (if mn? m/datoms d/datoms)
        index-range (if mn? m/index-range d/index-range)
        out (atom [])
        note! (fn [k v] (swap! out conj [k v]))]
    @(transact c [{:db/ident :p/d :db/valueType :db.type/bigdec :db/cardinality :db.cardinality/one :db/index true}
                  {:db/ident :p/k :db/valueType :db.type/long :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}])
    @(transact c (map-indexed (fn [i v] {:p/k i :p/d v}) values))
    (let [db (db-of c)]
      (note! :round-trip (sort (map (fn [[k v]] [k (exact v)])
                                    (q '[:find ?k ?v :where [?e :p/d ?v] [?e :p/k ?k]] db))))
      (note! :q-10.5 (q '[:find ?k :in $ ?x :where [?e :p/d ?x] [?e :p/k ?k]] db 10.5M))
      (note! :q-10.50 (q '[:find ?k :in $ ?x :where [?e :p/d ?x] [?e :p/k ?k]] db 10.50M))
      (note! :q-100 (q '[:find ?k :in $ ?x :where [?e :p/d ?x] [?e :p/k ?k]] db 100M))
      (note! :pred-= (q '[:find ?k :in $ ?x :where [?e :p/d ?v] [(= ?v ?x)] [?e :p/k ?k]] db 10.5M))
      (note! :datoms-1M (sort (map (comp exact :v) (datoms db :avet :p/d 1M))))
      (note! :datoms-1.00M (sort (map (comp exact :v) (datoms db :avet :p/d 1.00M))))
      (note! :index-range (sort (map (comp exact :v) (index-range db :p/d 1.00M 10.5M))))
      (note! :index-range-open (sort (map (comp exact :v) (index-range db :p/d 100M nil))))
      (note! :avet-all (sort (map (comp exact :v) (datoms db :avet :p/d)))))
    (let [r @(transact c [{:p/k 3 :p/d 10.50M}])]
      (note! :card-one-scale-change (sort (keep (fn [dd] (when (decimal? (:v dd)) [(exact (:v dd)) (:added dd)]))
                                                (:tx-data r)))))
    (let [r @(transact c [{:p/k 3 :p/d 10.50M}])]
      (note! :same-scale-no-op (count (:tx-data r))))
    @out))

(deftest bigdec-scale-matches-datomic
  (o/with-both [cd cm]
    (doseq [[[k vd] [_ vm]] (map vector (scenario cd) (scenario cm))]
      (testing (str k)
        (is (= vd vm))))))
