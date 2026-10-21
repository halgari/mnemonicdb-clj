(ns mnemonic.query
  "Datalog query engine. Stubs until the query task.")

(set! *warn-on-reflection* true)

(defn- unsupported [what]
  (throw (ex-info (str what " is not implemented yet")
                  {:db/error :db.error/unsupported
                   :cognitect.anomalies/category :cognitect.anomalies/unsupported
                   :feature what})))

(defn q [query & inputs] (unsupported "q"))
(defn query [query-map] (unsupported "query"))
(defn qseq [query & inputs] (unsupported "qseq"))
