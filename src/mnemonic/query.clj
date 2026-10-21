(ns mnemonic.query
  "Datalog query entry points: q, query, qseq. Execution lives in mnemonic.query.engine."
  (:require [mnemonic.query.engine :as engine])
  (:import (java.util Collection)))

(set! *warn-on-reflection* true)

(defn- query-map? [x]
  (and (map? x) (contains? x :query) (not (contains? x :find))))

(defn- run [q args opts]
  (engine/execute (engine/parse q) (vec args) opts))

(defn- run-map [m]
  (when-not (contains? m :query)
    (throw (ex-info "Query map requires :query"
                    {:db/error :db.error/invalid-query
                     :cognitect.anomalies/category :cognitect.anomalies/incorrect})))
  (run (:query m) (:args m) (select-keys m [:timeout])))

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
  "Like q (also accepts a query-map), returning a seq of the result. Eager."
  [query & inputs]
  (let [r (if (query-map? query) (run-map query) (run query inputs nil))]
    (cond
      (instance? Collection r) (seq r)
      (nil? r) nil
      :else (list r))))
