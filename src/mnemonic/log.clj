(ns mnemonic.log
  "Transaction log and tx-report queues. Stubs until the layers task.")

(set! *warn-on-reflection* true)

(defn- unsupported [what]
  (throw (ex-info (str what " is not implemented yet")
                  {:db/error :db.error/unsupported
                   :cognitect.anomalies/category :cognitect.anomalies/unsupported
                   :feature what})))

(defn log [conn] (unsupported "log"))
(defn tx-range [log start end] (unsupported "tx-range"))
(defn tx-report-queue [conn] (unsupported "tx-report-queue"))
(defn remove-tx-report-queue [conn] (unsupported "remove-tx-report-queue"))
