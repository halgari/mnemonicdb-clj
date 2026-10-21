(ns mnemonic.r25-mem
  "Prints RocksDB memory estimates (table readers = index+filter outside the cache, block
  cache usage, memtables) for a db after a read workload.
  clojure -M:bench -m mnemonic.r25-mem <dir>"
  (:require [mnemonic.rocks :as rocks])
  (:import (org.rocksdb RocksDB)))

(defn -main [dir]
  (let [store (rocks/open-store {:path dir})
        ^RocksDB db (:db store)]
    (try
      (doseq [cf [:default :eavt :aevt :avet :vaet :eavt-h :aevt-h :avet-h :vaet-h :log]
              :let [h (get (:cfs store) cf)]]
        (println (name cf)
                 "table-readers-mb" (/ (.getLongProperty db h "rocksdb.estimate-table-readers-mem") 1048576.0)
                 "sst-mb" (/ (.getLongProperty db h "rocksdb.total-sst-files-size") 1048576.0)))
      (finally (rocks/close-store store))))
  (shutdown-agents))
