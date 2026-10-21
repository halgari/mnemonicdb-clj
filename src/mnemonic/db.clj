(ns mnemonic.db
  "Base db: a RocksDB snapshot plus basis-t and schema."
  (:require [mnemonic.codec :as codec]
            [mnemonic.index :as index]
            [mnemonic.rocks :as rocks]
            [mnemonic.schema :as schema])
  (:import (java.util.concurrent.atomic AtomicBoolean)
           (mnemonic.rocks IRawCursor)
           (org.rocksdb RocksDB Snapshot)))

(set! *warn-on-reflection* true)

(def ^:private history-cf
  {:eavt :eavt-h :aevt :aevt-h :avet :avet-h :vaet :vaet-h})

(defn- store-open? [store]
  (.isOwningHandle ^RocksDB (:db store)))

;; Snapshot release state. Must not reference the Base (it is the Cleaner action).
(deftype Releaser [store ^Snapshot snap ^AtomicBoolean done]
  Runnable
  (run [_]
    (when (.compareAndSet done false true)
      ;; close-store does not release snapshots; releasing against a closed db would crash.
      (when (store-open? store)
        (rocks/release-snapshot store snap)))))

;; Wraps a raw cursor so that closing after the store was closed is a no-op
;; (a RocksIterator must not be closed on a closed db).
(deftype GuardedCursor [store ^IRawCursor raw]
  IRawCursor
  (seek [_ k] (.seek raw k))
  (seekToFirst [_] (.seekToFirst raw))
  (seekToLast [_] (.seekToLast raw))
  (seekForPrev [_ k] (.seekForPrev raw k))
  (isValid [_] (.isValid raw))
  (next [_] (.next raw))
  (prev [_] (.prev raw))
  (key [_] (.key raw))
  (value [_] (.value raw))
  (close [_] (when (store-open? store) (.close raw))))

(defn- open-raw ^IRawCursor [store snap cf]
  (->GuardedCursor store (rocks/raw-cursor store cf snap)))

(deftype Base [store ^Snapshot snap basis-t schema meta ^Releaser releaser]
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

(defn release-db
  "Release the snapshot held by `db` (idempotent)."
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
        releaser (->Releaser store snap (AtomicBoolean. false))]
    (try
      (let [tx (last-log-tx store snap)
            basis (if tx (codec/tx->t tx) 0)
            sch (load-schema store snap)
            db (->Base store snap basis sch nil releaser)]
        (rocks/register-cleanup! db releaser)
        db)
      (catch Throwable t
        (.run ^Releaser releaser)
        (throw t)))))
