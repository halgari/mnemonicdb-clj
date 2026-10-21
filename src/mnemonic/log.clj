(ns mnemonic.log
  "Transaction log (tx-range) and tx-report queues."
  (:refer-clojure :exclude [chunk])
  (:require [mnemonic.bootstrap :as bootstrap]
            [mnemonic.codec :as codec]
            [mnemonic.rocks :as rocks])
  (:import (java.util.concurrent LinkedBlockingQueue)
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

(defn- ->tx
  "A t or a tx id -> tx id; nil stays nil."
  [x]
  (when (some? x)
    (let [x (long x)]
      (if (>= x t-base) x (codec/t->tx x)))))

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
   (let [entries (read-chunk store start end)]
     (when (seq entries)
       (concat (map (fn [[tx bs]] {:t (codec/tx->t tx) :data (codec/decode-log-entry tx bs)})
                    entries)
               (when (== chunk (count entries))
                 (range-seq store (inc (long (first (peek entries)))) end)))))))

(defn tx-range
  "Seq of {:t t :data [Datom ...]} for transactions with start <= t < end. `start` and
  `end` are t values, tx ids, or nil (open)."
  [^Log lg start end]
  (range-seq (:store lg)
             (max first-user-tx (or (->tx start) first-user-tx))
             (->tx end)))

(defn tx-report-queue
  "The connection's tx-report queue (created on first call, the same queue after)."
  [conn]
  (first (swap! (:queues conn) (fn [s] (if (seq s) s #{(LinkedBlockingQueue.)})))))

(defn remove-tx-report-queue
  "Stops publishing tx reports to the connection's queue."
  [conn]
  (reset! (:queues conn) #{})
  nil)
