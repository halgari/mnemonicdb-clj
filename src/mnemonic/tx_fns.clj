(ns mnemonic.tx-fns
  "Transaction functions. Stubs until the advanced-tx task.")

(set! *warn-on-reflection* true)

(defn- unsupported [what]
  (throw (ex-info (str what " is not implemented yet")
                  {:db/error :db.error/unsupported
                   :cognitect.anomalies/category :cognitect.anomalies/unsupported
                   :feature what})))

(defn function [m] (unsupported "function"))
(defn invoke [db eid-or-ident & args] (unsupported "invoke"))
