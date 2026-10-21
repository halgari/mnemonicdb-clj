(ns mnemonic.query
  "Datalog query entry points: q, query, qseq. Execution lives in mnemonic.query.engine;
  rules, not/or and aggregates are loaded for their multimethod registrations."
  (:require [mnemonic.query.aggregates]
            [mnemonic.query.engine :as engine]
            [mnemonic.query.rules])
  )

(set! *warn-on-reflection* true)

(defn- query-map? [x]
  (and (map? x) (contains? x :query) (not (contains? x :find))))

(defn- run [q args opts]
  (engine/execute (engine/parse q) (vec args) opts))

(defn- run-map
  ([m] (run-map m nil))
  ([m extra-opts]
   (when-not (contains? m :query)
     (throw (ex-info "Query map requires :query"
                     {:db/error :db.error/invalid-query
                      :cognitect.anomalies/category :cognitect.anomalies/incorrect})))
   (run (:query m) (:args m) (merge (select-keys m [:timeout]) extra-opts))))

(defn q
  "Runs a query. `query` is the vector/list/map/string form; `inputs` match its :in.
  A query-map ({:query .. :args [..] :timeout ms}) is also accepted."
  [query & inputs]
  (if (query-map? query)
    (run-map query)
    (run query inputs nil)))

(defn query
  "Runs {:query q :args [inputs] :timeout ms}."
  [query-map]
  (run-map query-map))

(defn qseq
  "Like q (also accepts a query-map), returning a seq of the result. The seq is lazy:
  pull expressions are realized as it is consumed. nil when empty."
  [query & inputs]
  (let [opts {:lazy true}]
    (if (query-map? query)
      (run-map query opts)
      (run query inputs opts))))
