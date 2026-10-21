(ns mnemonic.db
  "Base db: a RocksDB snapshot plus basis-t and schema."
  (:require [mnemonic.bootstrap :as bootstrap]
            [mnemonic.codec :as codec]
            [mnemonic.index :as index]
            [mnemonic.rocks :as rocks]
            [mnemonic.schema :as schema])
  (:import (mnemonic.rocks IRawCursor Snap)))

(set! *warn-on-reflection* true)

(def ^:private history-cf
  {:eavt :eavt-h :aevt :aevt-h :avet :avet-h :vaet :vaet-h})

;; Snapshot release action. Must not reference the Base (it is the Cleaner action).
(deftype Releaser [^Snap snap]
  Runnable
  (run [_] (.markReleased snap)))

(defn- open-raw ^IRawCursor [store snap cf]
  (rocks/raw-cursor store cf snap))

(deftype Base [store ^Snap snap basis-t schema meta ^Releaser releaser]
  index/IDb
  (-seek-current [_ idx prefix]
    (index/prefix-cursor (open-raw store snap idx) prefix idx false))
  (-seek-history [_ idx prefix]
    (index/prefix-cursor (open-raw store snap (history-cf idx)) prefix idx true))
  (-view [_ idx prefix]
    (index/prefix-cursor (open-raw store snap idx) prefix idx false))
  (-seek-view [_ idx start]
    (index/seek-cursor (open-raw store snap idx) start idx false))
  (-basis-t [_] basis-t)
  (-schema [_] schema)
  (-as-of-t [_] nil)
  (-since-t [_] nil)
  (-history? [_] false)
  (-filter-pred [_] nil)
  (-raw [this] this))

(defn history-seek-cursor
  "Cursor over the history index of `idx` from the first key >= `start` to its end."
  [^Base b idx start]
  (index/seek-cursor (open-raw (.-store b) (.-snap b) (history-cf idx)) start idx true))

(defn released?
  "True once `release-db` was called on `db` (or it was collected)."
  [^Base db]
  (rocks/snapshot-released? (.-snap db)))

(defn db-meta
  "The store meta (counters) as of this db's snapshot."
  [^Base db]
  (.-meta db))

(defn release-db
  "Release the snapshot held by `db` (idempotent). Later reads of `db` throw
  :db.error/db-released; iterations already in progress complete."
  [^Base db]
  (.run ^Runnable (.-releaser db))
  nil)

(defn- last-log-tx
  "Highest tx in the log as seen by `snap`, or nil."
  [store snap]
  (with-open [^IRawCursor c (rocks/raw-cursor store :log snap)]
    (.seekToLast c)
    (when (.isValid c)
      (codec/bytes->long (.key c)))))

(defn- scan-attr
  "All current datoms of attribute `aid`, via AEVT on `snap`."
  [store snap aid]
  (let [^mnemonic.index.ICursor cur (index/prefix-cursor (rocks/raw-cursor store :aevt snap)
                                 (codec/encode-prefix :aevt [aid]) :aevt false)]
    (try
      (loop [acc []]
        (if (.valid cur)
          (let [d (index/copy-datom (.datom cur))]
            (.advance cur)
            (recur (conj acc d)))
          acc))
      (finally (.close cur)))))

(defn- load-schema
  "Only the schema-defining attributes are scanned (AEVT), not all of EAVT."
  [store snap]
  (schema/schema-from-datoms
   (into [] (mapcat #(scan-attr store snap %)) (sort schema/schema-attr-ids))))

(defn base-db
  "Db value over a fresh snapshot of `store`."
  [store]
  (let [snap (rocks/snapshot store)
        releaser (->Releaser snap)]
    (try
      (let [tx (last-log-tx store snap)
            basis (if tx (codec/tx->t tx) 0)
            sch (load-schema store snap)
            db (->Base store snap basis sch (bootstrap/read-meta store snap) releaser)]
        (rocks/register-cleanup! db releaser)
        db)
      (catch Throwable t
        (.run ^Releaser releaser)
        (throw t)))))
