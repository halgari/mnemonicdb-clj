(ns mnemonic.entity
  "Lazy entity maps. Stubs until the entity task.")

(set! *warn-on-reflection* true)

(defn- unsupported [what]
  (throw (ex-info (str what " is not implemented yet")
                  {:db/error :db.error/unsupported
                   :cognitect.anomalies/category :cognitect.anomalies/unsupported
                   :feature what})))

(defn entity [db eid] (unsupported "entity"))
(defn touch [e] (unsupported "touch"))
(defn entity-db [e] (unsupported "entity-db"))
