(ns mnemonic.pull
  "Pull. Stubs until the pull task.")

(set! *warn-on-reflection* true)

(defn- unsupported [what]
  (throw (ex-info (str what " is not implemented yet")
                  {:db/error :db.error/unsupported
                   :cognitect.anomalies/category :cognitect.anomalies/unsupported
                   :feature what})))

(defn pull [db pattern eid] (unsupported "pull"))
(defn pull-many [db pattern eids] (unsupported "pull-many"))
