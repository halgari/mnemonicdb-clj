(ns mnemonic.db
  "Base db: a RocksDB snapshot plus basis-t and schema."
  (:require [mnemonic.bootstrap :as bootstrap]
            [mnemonic.codec :as codec]
            [mnemonic.index :as index]
            [mnemonic.rocks :as rocks]
            [mnemonic.schema :as schema])
  (:import (mnemonic.datom Datom)
           (mnemonic.rocks IRawCursor Snap)))

(set! *warn-on-reflection* true)

(def ^:private history-cf
  {:eavt :eavt-h :aevt :aevt-h :avet :avet-h :vaet :vaet-h})

;; Snapshot release action. Must not reference the Base (it is the Cleaner action).
(deftype Releaser [^Snap snap]
  Runnable
  (run [_] (.markReleased snap)))

(defn- open-raw
  "A raw cursor over `cf`. `prefix` non-nil: the caller reads only keys starting with it
  (a prefix cursor), which lets RocksDB use the prefix bloom filter; nil: total order."
  ^IRawCursor [store snap cf prefix]
  (rocks/raw-cursor store cf snap prefix))

(deftype Base [store ^Snap snap basis-t schema meta ^Releaser releaser]
  index/IDb
  (-seek-current [_ idx prefix]
    (index/prefix-cursor (open-raw store snap idx prefix) prefix idx false))
  (-seek-history [_ idx prefix]
    (index/prefix-cursor (open-raw store snap (history-cf idx) prefix) prefix idx true))
  (-view [_ idx prefix]
    (index/prefix-cursor (open-raw store snap idx prefix) prefix idx false))
  (-seek-view [_ idx start]
    (index/seek-cursor (open-raw store snap idx nil) start idx false))
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
  (index/seek-cursor (open-raw (.-store b) (.-snap b) (history-cf idx) nil) start idx true))

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

(defn ident-history
  "[eid keyword retract-tx] of every retracted :db/ident datom, read from `cur` (a
  history cursor over the AEVT prefix of :db/ident); closes `cur`."
  [^mnemonic.index.ICursor cur]
  (try
    (loop [acc []]
      (if (.valid cur)
        (let [^Datom d (.datom cur)
              x [(.-e d) (.-v d) (.retractTx cur)]]
          (.advance cur)
          (recur (conj acc x)))
        acc))
    (finally (.close cur))))

(defn load-schema
  "Only the schema-defining attributes are scanned (AEVT), not all of EAVT; retracted
  idents (AEVT history of :db/ident) become aliases."
  [store snap]
  (schema/with-ident-aliases
   (schema/schema-from-datoms
    (into [] (mapcat #(scan-attr store snap %)) (sort schema/schema-attr-ids)))
   (ident-history (index/prefix-cursor (rocks/raw-cursor store :aevt-h snap)
                                       (codec/encode-prefix :aevt [schema/ident-attr-id])
                                       :aevt true))))

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

(defn successor-db
  "Db value over a fresh snapshot of `store` taken right after the writer committed the
  tx that moved `prev` to `basis-t` / `meta` (writer thread only). The schema object of
  `prev` is reused unless `schema-changed?`, in which case it is reloaded from the new
  snapshot. No log or meta reads."
  [store ^Base prev basis-t meta schema-changed?]
  (let [snap (rocks/snapshot store)
        releaser (->Releaser snap)]
    (try
      (let [sch (if schema-changed? (load-schema store snap) (.-schema prev))
            db (->Base store snap basis-t sch meta releaser)]
        (rocks/register-cleanup! db releaser)
        db)
      (catch Throwable t
        (.run ^Releaser releaser)
        (throw t)))))
