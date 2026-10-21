(ns mnemonic.test-fns
  "Transaction functions, attribute predicates and entity predicates shared by the advanced
  transaction tests. They run unchanged under Datomic (the oracle) and MnemonicDB, so any db
  access goes through whichever API matches the db they are handed."
  (:require [clojure.string :as str]))

(defn- api-fn [db sym]
  (requiring-resolve
   (symbol (if (str/starts-with? (.getName (class db)) "datomic") "datomic.api" "mnemonic.api") sym)))

(defn datoms [db & args] (apply (api-fn db "datoms") db args))

;; ------------------------------------------------------------------ tx functions

(defn add-age [_db e n] [[:db/add e :p/age n]])
(defn nested [_db e] [['mnemonic.test-fns/add-age e 5] {:db/id "new" :p/name "nn"}])
(defn bad [_db] (throw (ex-info "boom" {:x 1})))
(defn ret-nil [_db] nil)
(defn ret-map [_db] {:db/id "m" :p/name "mm"})
(defn ret-lazy [_db] (list [:db/add "z" :p/name "zz"]))
(defn ret-set [_db] #{[:db/add "s" :p/name "s1"]})
(defn db-basis [db] [[:db/add "q" :p/age ((api-fn db "basis-t") db)]])
(defn tempid-fn [_db] [[:db/add "tt" :p/name "tt-name"] [:db/add "tt" :p/age 9]])
(defn sees-before? [db e] [[:db/add e :p/age (count (datoms db :eavt e :p/age))]])
(def not-a-fn 5)

;; ------------------------------------------------------------------ attribute predicates

(defn pos-val? [v] (pos? v))
(defn nil-pred [_v] nil)
(defn str-pred [_v] "yes")
(defn throws-pred [_v] (throw (ex-info "pred boom" {})))
(defn short-name? [v] (< (count v) 4))
(defn pair-sum-pos? [v] (pos? (+ (long (nth v 0 0)) (long (nth v 1 0)))))

;; ------------------------------------------------------------------ entity predicates

(defn person-ok? [db eid] (boolean (seq (datoms db :eavt eid :p/age))))
(defn age-is-99? [db eid] (= 99 (:v (first (datoms db :eavt eid :p/age)))))
(defn no-ret-ok? [_db _eid] nil)
(defn throws-ent? [_db _eid] (throw (ex-info "ent boom" {})))
