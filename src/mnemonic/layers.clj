(ns mnemonic.layers
  "Db layers (as-of, since, history, filter, with). Stubs until the layers task."
  (:refer-clojure :exclude [filter]))

(set! *warn-on-reflection* true)

(defn- unsupported [what]
  (throw (ex-info (str what " is not implemented yet")
                  {:db/error :db.error/unsupported
                   :cognitect.anomalies/category :cognitect.anomalies/unsupported
                   :feature what})))

(defn as-of [db t] (unsupported "as-of"))
(defn since [db t] (unsupported "since"))
(defn history [db] (unsupported "history"))
(defn filter [db pred] (unsupported "filter"))
(defn with [db tx-data] (unsupported "with"))
(defn as-of-t [db] (unsupported "as-of-t"))
(defn since-t [db] (unsupported "since-t"))
(defn history? [db] (unsupported "is-history"))
