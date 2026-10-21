(ns mnemonic.log
  "Transaction log (tx-range) and tx-report queues."
  (:refer-clojure :exclude [chunk])
  (:require [mnemonic.bootstrap :as bootstrap]
            [mnemonic.codec :as codec]
            [mnemonic.rocks :as rocks])
  (:import (java.util Date)
           (java.util.concurrent LinkedBlockingQueue)
           (mnemonic.datom Datom)
           (mnemonic.rocks IRawCursor)))

(set! *warn-on-reflection* true)

;; The log is read live from the store (every entry is immutable once written).
(defrecord Log [store])

(defn log
  "The transaction log of `conn`."
  [conn]
  (->Log (:store conn)))

(def ^:private chunk 64)

(def ^:private first-user-tx
  "The log also holds the bootstrap transactions; like Datomic, the range starts at the
  first user transaction."
  (codec/t->tx (long (:next-t bootstrap/initial-meta))))

(def ^:private t-base (codec/t->tx 0))

(defn- first-tx-at-or-after
  "Tx id of the first transaction whose :db/txInstant is >= `date` (AVET seek on the
  live store), or Long/MAX_VALUE when there is none."
  ^long [store ^Date date]
  (with-open [^IRawCursor c (rocks/raw-cursor store :avet nil)]
    (if-let [k (.seekKey c (codec/encode-prefix :avet [50 date]))]
      (let [^Datom d (codec/decode-key :avet k)]
        (if (== 50 (.-a d)) (.-e d) Long/MAX_VALUE))
      Long/MAX_VALUE)))

(defn- ->tx
  "A t, a tx id or a Date -> tx id; nil stays nil. A Date maps to the first transaction
  whose :db/txInstant is >= the date (as in Datomic)."
  [store x]
  (cond
    (nil? x) nil
    (instance? Date x) (first-tx-at-or-after store x)
    (integer? x) (let [x (long x)]
                   (if (>= x t-base) x (codec/t->tx x)))
    :else (throw (ex-info (str "Invalid tx-range bound " (pr-str x) ": expected a t, tx id or Date")
                          {:db/error :db.error/invalid-t
                           :cognitect.anomalies/category :cognitect.anomalies/incorrect}))))

(defn- read-chunk
  "Up to `chunk` [tx bytes] pairs with start <= tx < end (end may be nil)."
  [store ^long start end]
  (with-open [^IRawCursor c (rocks/raw-cursor store :log nil)]
    (.seek c (codec/long->bytes start))
    (loop [acc [] n 0]
      (if (and (< n chunk) (.isValid c))
        (let [tx (codec/bytes->long (.key c))]
          (if (and end (>= tx (long end)))
            acc
            (let [v (.value c)]
              (.next c)
              (recur (conj acc [tx v]) (inc n)))))
        acc))))

(defn- range-seq [store ^long start end]
  (lazy-seq
   (when (or (nil? end) (< start (long end)))
     (let [entries (read-chunk store start end)]
       (when (seq entries)
         (concat (map (fn [[tx bs]] {:t (codec/tx->t tx) :data (codec/decode-log-entry tx bs)})
                      entries)
                 (when (== chunk (count entries))
                   (range-seq store (inc (long (first (peek entries)))) end))))))))

(defn tx-range
  "Seq of {:t t :data [Datom ...]} for transactions with start <= t < end. `start` and
  `end` are t values, tx ids, java.util.Dates, or nil (open)."
  [^Log lg start end]
  (let [store (:store lg)]
    (range-seq store
               (max first-user-tx (or (->tx store start) first-user-tx))
               (->tx store end))))

(defn tx-report-queue
  "The connection's tx-report queue (created on first call, the same queue after)."
  [conn]
  (first (swap! (:queues conn) (fn [s] (if (seq s) s #{(LinkedBlockingQueue.)})))))

(defn remove-tx-report-queue
  "Stops publishing tx reports to the connection's queue."
  [conn]
  (reset! (:queues conn) #{})
  nil)
