(ns mnemonic.index
  "Cursor and db protocols plus cursor combinators over encoded index keys."
  (:require [mnemonic.codec :as codec]
            [mnemonic.datom :as datom]
            [mnemonic.rocks :as rocks])
  (:import (clojure.lang ChunkBuffer ChunkedCons IReduceInit Reduced Seqable Sequential)
           (java.lang.ref Reference)
           (java.util Arrays)
           (java.util.concurrent.atomic AtomicLong)
           (mnemonic.datom Datom)
           (mnemonic.rocks IRawCursor)))

(set! *warn-on-reflection* true)

(definterface ICursor
  (^boolean valid [])
  (advance [])
  (^bytes key [])
  (datom [])
  (^long retractTx [])
  (close []))

(defprotocol IDb
  (-seek-current [db index prefix-bytes])
  (-seek-history [db index prefix-bytes])
  (-view [db index prefix-bytes])
  (-seek-view [db index start-bytes])
  (-basis-t [db])
  (-schema [db])
  (-as-of-t [db])
  (-since-t [db])
  (-history? [db])
  (-filter-pred [db])
  (-raw [db]))

(defonce ^{:doc "Test hook: number of range cursors opened and not yet closed."
           :tag AtomicLong}
  open-cursors (AtomicLong. 0))

;; ------------------------------------------------------------------ range cursor

(defn- starts-with? [^bytes k ^bytes prefix]
  (let [n (alength prefix)]
    (and (>= (alength k) n)
         (Arrays/equals k 0 n prefix 0 n))))

(defn- in-prefix
  "`k` when non-nil and (when `prefix` is non-nil) within the prefix, else nil."
  ^bytes [^bytes k ^bytes prefix]
  (when (and k (or (nil? prefix) (starts-with? k prefix)))
    k))

(deftype RangeCursor [^IRawCursor raw ^bytes prefix index history?
                      ^:unsynchronized-mutable ^bytes k
                      ^:unsynchronized-mutable d
                      ^:unsynchronized-mutable ^boolean closed?]
  ICursor
  (valid [_] (some? k))
  (advance [_]
    (when k
      (set! k (in-prefix (.nextKey raw) prefix))
      (set! d nil))
    nil)
  (key [_] k)
  (datom [_]
    (when k
      (or d (set! d (codec/decode-key index k)))))
  (retractTx [_]
    (if (and k history?)
      (codec/bytes->long (.value raw))
      -1))
  (close [_]
    (when-not closed?
      (set! closed? (boolean true))
      (set! k nil)
      (set! d nil)
      (.decrementAndGet open-cursors)
      (.close raw))
    nil))

(defn- range-cursor [^IRawCursor raw ^bytes start ^bytes prefix index history?]
  (.incrementAndGet open-cursors)
  (try
    (->RangeCursor raw prefix index history? (in-prefix (.seekKey raw start) prefix) nil false)
    (catch Throwable t
      (.decrementAndGet open-cursors)
      (.close raw)
      (throw t))))

(defn prefix-cursor
  "Cursor over keys of `raw-cursor` starting with `prefix`; owns and closes the raw cursor."
  [raw-cursor ^bytes prefix index history?]
  (range-cursor raw-cursor prefix prefix index history?))

(defn seek-cursor
  "Cursor from the first key >= `start` to the end of the index; owns the raw cursor."
  [raw-cursor ^bytes start index history?]
  (range-cursor raw-cursor start nil index history?))

;; ------------------------------------------------------------------ combinators

(deftype EmptyCursor []
  ICursor
  (valid [_] false)
  (advance [_] nil)
  (key [_] nil)
  (datom [_] nil)
  (retractTx [_] -1)
  (close [_] nil))

(defn empty-cursor [] (EmptyCursor.))

(defn- min-index
  "Index of the valid cursor with the smallest key (first on ties), or -1."
  ^long [^objects cs]
  (loop [i 0 best -1]
    (if (< i (alength cs))
      (let [^mnemonic.index.ICursor c (aget cs i)]
        (if (.valid c)
          (recur (inc i)
                 (if (or (neg? best)
                         (neg? (codec/compare-bytes
                                (.key c) (.key ^mnemonic.index.ICursor (aget cs best)))))
                   i
                   best))
          (recur (inc i) best)))
      best)))

(deftype MergeCursor [^objects cs
                      ^:unsynchronized-mutable ^long cur
                      ^:unsynchronized-mutable ^boolean closed?]
  ICursor
  (valid [_] (>= cur 0))
  (advance [_]
    (when (>= cur 0)
      (.advance ^mnemonic.index.ICursor (aget cs cur))
      (set! cur (min-index cs)))
    nil)
  (key [_] (when (>= cur 0) (.key ^mnemonic.index.ICursor (aget cs cur))))
  (datom [_] (when (>= cur 0) (.datom ^mnemonic.index.ICursor (aget cs cur))))
  (retractTx [_] (if (>= cur 0) (.retractTx ^mnemonic.index.ICursor (aget cs cur)) -1))
  (close [_]
    (when-not closed?
      (set! closed? (boolean true))
      (set! cur -1)
      (let [errs (volatile! nil)]
        (dotimes [i (alength cs)]
          (try (.close ^mnemonic.index.ICursor (aget cs i))
               (catch Throwable t (vswap! errs #(or % t)))))
        (when-let [t @errs] (throw t))))
    nil))

(defn merge-cursors
  "K-way merge of cursors by key bytes (ties resolved in argument order).
  Closing the merge closes every source."
  [cursors]
  (let [cs (object-array cursors)]
    (->MergeCursor cs (min-index cs) false)))

(deftype FilterCursor [^mnemonic.index.ICursor c pred
                       ^:unsynchronized-mutable ^boolean closed?]
  ICursor
  (valid [_] (and (not closed?) (.valid c)))
  (advance [_]
    (when-not closed?
      (.advance c)
      (loop []
        (when (and (.valid c) (not (pred (.datom c))))
          (.advance c)
          (recur))))
    nil)
  (key [_] (when-not closed? (.key c)))
  (datom [_] (when-not closed? (.datom c)))
  (retractTx [_] (if closed? -1 (.retractTx c)))
  (close [_]
    (when-not closed?
      (set! closed? (boolean true))
      (.close c))
    nil))

(defn filter-cursor
  "Cursor over `c` keeping only entries whose Datom satisfies `pred`."
  [^mnemonic.index.ICursor c pred]
  (try
    (loop []
      (when (and (.valid c) (not (pred (.datom c))))
        (.advance c)
        (recur)))
    (catch Throwable t
      (.close c)
      (throw t)))
  (->FilterCursor c pred false))

;; ------------------------------------------------------------------ iterable

(def ^:private ^:const chunk-size 512)

(defn copy-datom
  "Immutable copy of a (possibly flyweight) datom."
  ^Datom [^Datom d]
  (datom/datom (.-e d) (.-a d) (.-v d) (.-tx d) (.-added d)))

(defn- cursor-seq
  "Lazy chunked seq over `cur`, closing it at exhaustion. `holder` is kept
  reachable by the seq so the Cleaner closes `cur` if the seq is abandoned; `db` is
  kept reachable so its snapshot cannot be released under a live iteration."
  [^mnemonic.index.ICursor cur holder db]
  (lazy-seq
   (try
     (let [buf (ChunkBuffer. chunk-size)]
       (loop [n 0]
         (when (and (< n chunk-size) (.valid cur))
           (.add buf (copy-datom (.datom cur)))
           (.advance cur)
           (recur (inc n))))
       (let [more? (.valid cur)]
         (when-not more? (.close cur))
         (when (pos? (.count buf))
           (ChunkedCons. (.chunk buf)
                         (when more? (cursor-seq cur holder db))))))
     (catch Throwable t
       (.close cur)
       (throw t))
     (finally
       (Reference/reachabilityFence db)))))

(defn datoms-iterable
  "Iterable of immutable Datoms over `(-view db index prefix)`. Each seq / iterator /
  reduce opens its own cursor."
  [db index prefix]
  (reify
    Iterable
    (iterator [this]
      (let [^Iterable s (or (seq this) [])] (.iterator s)))
    Seqable
    (seq [_]
      (let [^mnemonic.index.ICursor cur (-view db index prefix)
            holder (Object.)]
        (rocks/register-cleanup! holder #(.close cur))
        (seq (cursor-seq cur holder db))))
    IReduceInit
    (reduce [_ f init]
      (let [^mnemonic.index.ICursor cur (-view db index prefix)]
        (try
          (loop [acc init]
            (if (.valid cur)
              (let [ret (f acc (copy-datom (.datom cur)))]
                (if (reduced? ret)
                  (.deref ^Reduced ret)
                  (do (.advance cur) (recur ret))))
              acc))
          (finally
            (.close cur)
            (Reference/reachabilityFence db)))))
    Sequential))
