(ns mnemonic.diff-layers-test
  "Differential tests: as-of / since / history (and compositions) over scripted histories
  must produce the same datoms, in the same order, as Datomic for every index."
  (:require [clojure.test :refer [deftest is testing]]
            [datomic.api :as d]
            [mnemonic.api :as m]
            [mnemonic.oracle :as o]))

(def one :db.cardinality/one)
(def many :db.cardinality/many)

(def schema
  [{:db/id "n" :db/ident :p/name :db/valueType :db.type/string :db/cardinality one
    :db/unique :db.unique/identity}
   {:db/id "a" :db/ident :p/age :db/valueType :db.type/long :db/cardinality one :db/index true}
   {:db/id "t" :db/ident :p/tags :db/valueType :db.type/string :db/cardinality many :db/index true}
   {:db/id "f" :db/ident :p/friends :db/valueType :db.type/ref :db/cardinality many}
   {:db/id "b" :db/ident :p/boss :db/valueType :db.type/ref :db/cardinality one}
   {:db/id "h" :db/ident :p/nick :db/valueType :db.type/string :db/cardinality one
    :db/noHistory true}
   {:db/id "c" :db/ident :p/addr :db/valueType :db.type/ref :db/cardinality one
    :db/isComponent true}
   {:db/id "k" :db/ident :a/city :db/valueType :db.type/string :db/cardinality one :db/index true}])

(def al [:p/name "Al"])
(def bo [:p/name "Bo"])

(def histories
  [["card-one flip-flop"
    [[{:p/name "Al" :p/age 1}]
     [[:db/add al :p/age 2]]
     [[:db/add al :p/age 1]]
     [[:db/add al :p/age 3]]
     [[:db/retract al :p/age 3]]
     [[:db/add al :p/age 1]]]]
   ["card-many churn"
    [[{:p/name "Al" :p/tags ["a" "b"]}]
     [[:db/retract al :p/tags "a"]]
     [[:db/add al :p/tags "a"] [:db/add al :p/tags "c"]]
     [[:db/retract al :p/tags "b"] [:db/retract al :p/tags "c"]]
     [[:db/retract al :p/tags]]
     [[:db/add al :p/tags "a"]]]]
   ["refs and VAET"
    [[{:p/name "Al"} {:p/name "Bo"} {:p/name "Cy"}]
     [{:db/id al :p/friends [bo [:p/name "Cy"]] :p/boss bo}]
     [[:db/retract al :p/friends bo]]
     [[:db/add al :p/boss [:p/name "Cy"]]]
     [[:db/add al :p/friends bo] [:db/retract al :p/boss [:p/name "Cy"]]]]]
   ["no-history attribute"
    [[{:p/name "Al" :p/nick "one" :p/age 5}]
     [[:db/add al :p/nick "two"]]
     [[:db/add al :p/age 6] [:db/retract al :p/nick "two"]]
     [[:db/add al :p/nick "three"]]]]
   ["retract all facts"
    [[{:p/name "Al" :p/age 1 :p/tags ["x" "y"] :p/addr {:a/city "Denver"}}
      {:p/name "Bo" :p/boss al}]
     [[:db/add bo :p/age 9]]
     [[:db/retract al :p/age 1] [:db/retract al :p/tags "x"] [:db/retract al :p/tags "y"]]
     [{:p/name "Al" :p/age 100}]]]
   ["bulk with churn"
    [(vec (for [i (range 12)] {:p/name (str "P" i) :p/age (mod (* i 5) 7) :p/tags [(str "t" (mod i 3))]}))
     (vec (for [i (range 0 12 2)] [:db/add [:p/name (str "P" i)] :p/age (+ 20 i)]))
     (vec (for [i (range 0 12 3)] [:db/retract [:p/name (str "P" i)] :p/tags (str "t" (mod i 3))]))
     (vec (for [i (range 0 12 4)] [:db/add [:p/name (str "P" i)] :p/age (mod (* i 5) 7)]))]]
   ["schema changes interleaved"
    [[{:p/name "Al" :p/age 1}]
     [{:db/id "x" :db/ident :q/late :db/valueType :db.type/string :db/cardinality one :db/index true}]
     [[:db/add al :q/late "l1"]]
     [[:db/add al :q/late "l2"]]
     [{:db/id :p/age :db/doc "age doc"}]
     [[:db/add al :p/age 2]]]]
   ["same value many txs"
    [[{:p/name "Al" :a/city "Denver"}]
     [[:db/add al :a/city "Denver"]]
     [[:db/add al :a/city "Austin"]]
     [[:db/add al :a/city "Denver"]]
     [[:db/retract al :a/city "Denver"]]
     [[:db/add al :a/city "Denver"]]
     [[:db/add al :a/city "Austin"]]]]
   ["tx metadata and empty txs"
    [[{:p/name "Al" :p/age 1}]
     []
     [{:db/id "datomic.tx" :p/name "TxName"} [:db/add al :p/age 2]]
     []
     [[:db/add al :p/age 3]]]]
   ["unique value moves"
    [[{:p/name "Al" :p/age 1} {:p/name "Bo" :p/age 2}]
     [[:db/retract al :p/age 1] [:db/add bo :p/age 1]]
     [[:db/add al :p/age 2] [:db/retract bo :p/age 1]]
     [[:db/add bo :p/age 3]]]]])

(def ^:private idx-args
  "Index + leading components exercised per db."
  [[:eavt] [:aevt] [:avet] [:vaet]
   [:aevt :p/age] [:avet :p/age] [:avet :p/tags] [:avet :a/city]])

(defn- user-only
  "Normalized datoms of user transactions only. Datomic's bootstrap history holds two
  retractions (the :db.fn/cas and :db.fn/retractEntity renames, at t 0) that the bootstrap
  snapshot does not reproduce; the plain bootstrap indexes are compared in diff-m1-test."
  [ds]
  (filterv (fn [[_ _ _ tx _]] (>= (m/tx->t tx) 1000)) ds))

(defn- check-db [label dd dm]
  (let [hist? (d/is-history dd)]
    (doseq [[idx & comps] idx-args]
    (let [nick (d/entid dd :p/nick)
          ;; Datomic mem keeps un-indexed history of :db/noHistory attributes (it is only
          ;; dropped when indexing runs); MnemonicDB never keeps it. Not compared.
          norm (fn [ds] (cond->> (remove #(= nick (nth % 1)) (o/normalize-datoms ds))
                          hist? user-only
                          true vec))
          dat (try (norm (seq (apply d/datoms dd idx comps)))
                   (catch Exception e [:err (:db/error (ex-data e))]))
          mem (try (norm (seq (apply m/datoms dm idx comps)))
                   (catch Exception e [:err (:db/error (ex-data e))]))]
      (is (= dat mem) (str label " " idx " " comps))
      (let [dat (try (norm (seq (apply d/seek-datoms dd idx comps)))
                     (catch Exception e [:err (:db/error (ex-data e))]))
            mem (try (norm (seq (apply m/seek-datoms dm idx comps)))
                     (catch Exception e [:err (:db/error (ex-data e))]))]
        (is (= dat mem) (str label " seek " idx " " comps)))))))

(defn- run-history [[nm steps]]
  (o/with-both [cd cm]
    (o/transact-both [cd cm] schema)
    (doseq [s steps] (o/transact-both [cd cm] s))
    (let [dd (d/db cd) dm (m/db cm)
          basis (d/basis-t dd)
          ts (range 1000 (inc basis))
          sample (distinct (concat [999 1000 (inc basis) (+ basis 50)]
                                   (take-nth 2 ts) [basis (dec basis)]))]
      (is (= basis (m/basis-t dm)))
      (check-db (str nm " plain") dd dm)
      (check-db (str nm " history") (d/history dd) (m/history dm))
      (doseq [t sample]
        (testing (str nm " t=" t)
          (check-db (str nm " as-of " t) (d/as-of dd t) (m/as-of dm t))
          (check-db (str nm " since " t) (d/since dd t) (m/since dm t))
          (check-db (str nm " history as-of " t) (d/history (d/as-of dd t)) (m/history (m/as-of dm t)))
          (check-db (str nm " as-of history " t) (d/as-of (d/history dd) t) (m/as-of (m/history dm) t))
          (check-db (str nm " history since " t) (d/history (d/since dd t)) (m/history (m/since dm t)))
          (check-db (str nm " since history " t) (d/since (d/history dd) t) (m/since (m/history dm) t))
          (is (= (d/as-of-t (d/as-of dd t)) (m/as-of-t (m/as-of dm t))))
          (is (= (d/since-t (d/since dd t)) (m/since-t (m/since dm t))))
          (is (= (d/basis-t (d/as-of dd t)) (m/basis-t (m/as-of dm t))))))
      (testing "compositions"
        (let [t1 (nth (vec ts) (quot (count ts) 3))
              t2 (nth (vec ts) (quot (* 2 (count ts)) 3))]
          (check-db (str nm " since+as-of") (d/since (d/as-of dd t2) t1) (m/since (m/as-of dm t2) t1))
          (check-db (str nm " as-of+since") (d/as-of (d/since dd t1) t2) (m/as-of (m/since dm t1) t2))
          (check-db (str nm " as-of override") (d/as-of (d/as-of dd t1) t2) (m/as-of (m/as-of dm t1) t2))
          (check-db (str nm " since override") (d/since (d/since dd t2) t1) (m/since (m/since dm t2) t1))
          (check-db (str nm " history of both")
                    (d/history (d/since (d/as-of dd t2) t1)) (m/history (m/since (m/as-of dm t2) t1)))))
      (testing "log"
        (let [dl (map (fn [e] [(:t e) (o/normalize-datoms (:data e))]) (d/tx-range (d/log cd) nil nil))
              ml (map (fn [e] [(:t e) (o/normalize-datoms (:data e))]) (m/tx-range (m/log cm) nil nil))]
          (is (= (map first dl) (map first ml)))
          (is (= (map (comp set second) dl) (map (comp set second) ml)) nm))))))

(deftest layers-differential-test
  (is (= 10 (count histories)))
  (doseq [h histories]
    (testing (first h)
      (run-history h))))
