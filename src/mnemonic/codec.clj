(ns mnemonic.codec
  "Order-preserving byte encoding of values and index keys.

  Every encoding is self-delimiting and compares correctly under unsigned
  bytewise comparison, so RocksDB's default comparator is sufficient."
  (:require [mnemonic.datom :as datom])
  (:import (java.io ByteArrayOutputStream)
           (java.math BigDecimal BigInteger)
           (java.net URI)
           (java.nio ByteBuffer)
           (java.nio.charset StandardCharsets)
           (java.util Arrays Date UUID)
           (mnemonic.datom Datom)))

(set! *warn-on-reflection* true)

;; ------------------------------------------------------------------ tags

(def ^:const tag-tuple-end 0x00)
(def ^:const tag-nil 0x01)
(def ^:const tag-boolean 0x10)
(def ^:const tag-long 0x11)
(def ^:const tag-bigint 0x12)
(def ^:const tag-float 0x13)
(def ^:const tag-double 0x14)
(def ^:const tag-bigdec 0x15)
(def ^:const tag-instant 0x16)
(def ^:const tag-uuid 0x17)
(def ^:const tag-string 0x18)
(def ^:const tag-keyword 0x19)
(def ^:const tag-symbol 0x1A)
(def ^:const tag-uri 0x1B)
(def ^:const tag-bytes 0x1C)
(def ^:const tag-tuple 0x1D)

;; ------------------------------------------------------------------ ids

(def ^:private ^:const part-shift 42)
(def ^:private ^:const n-mask (dec (bit-shift-left 1 42)))

(defn part-of ^long [^long eid] (unsigned-bit-shift-right eid part-shift))
(defn eid ^long [^long part ^long n] (bit-or (bit-shift-left part part-shift) n))
(defn t->tx ^long [^long t] (eid 3 t))
(defn tx->t ^long [^long tx] (bit-and tx n-mask))

;; ------------------------------------------------------------------ low-level writers/readers

(defn compare-bytes
  "Unsigned lexicographic comparison."
  ^long [^bytes a ^bytes b]
  (long (Arrays/compareUnsigned a b)))

(defn- write-byte! [^ByteArrayOutputStream out ^long b]
  (.write out (int b)))

(defn- write-raw! [^ByteArrayOutputStream out ^bytes bs]
  (.write out bs 0 (alength bs)))

(defn- write-long!
  "8 bytes big-endian with the sign bit flipped (unsigned order == signed order)."
  [^ByteArrayOutputStream out ^long x]
  (write-raw! out (.array (.putLong (ByteBuffer/allocate 8) (bit-xor x Long/MIN_VALUE)))))

(defn- write-int!
  "4 bytes big-endian with the sign bit flipped, XORed with `mask` (0 or -1)."
  [^ByteArrayOutputStream out ^long x ^long mask]
  (write-raw! out (.array (.putInt (ByteBuffer/allocate 4)
                                   (unchecked-int (bit-xor (bit-xor x 0x80000000) mask))))))

(defn- read-long ^long [^ByteBuffer buf]
  (bit-xor (.getLong buf) Long/MIN_VALUE))

(defn- write-escaped!
  "Bytes with 0x00 -> 0x00 0xFF, terminated by 0x00 0x00. Every byte is XORed
  with `mask` (0x00, or 0xFF to invert the order)."
  [^ByteArrayOutputStream out ^bytes bs ^long mask]
  (dotimes [i (alength bs)]
    (let [b (bit-and 0xFF (aget bs i))]
      (write-byte! out (bit-xor b mask))
      (when (zero? b)
        (write-byte! out (bit-xor 0xFF mask)))))
  (write-byte! out mask)
  (write-byte! out mask))

(defn- read-escaped ^bytes [^ByteBuffer buf ^long mask]
  (let [out (ByteArrayOutputStream.)]
    (loop []
      (let [b (bit-xor (bit-and 0xFF (.get buf)) mask)]
        (if (zero? b)
          (let [n (bit-xor (bit-and 0xFF (.get buf)) mask)]
            (when-not (zero? n)
              (write-byte! out 0)
              (recur)))
          (do (write-byte! out b)
              (recur)))))
    (.toByteArray out)))

(defn- utf8 ^bytes [^String s] (.getBytes s StandardCharsets/UTF_8))
(defn- from-utf8 ^String [^bytes bs] (String. bs StandardCharsets/UTF_8))

;; ------------------------------------------------------------------ value encoding

(defn- write-sortable-double! [^ByteArrayOutputStream out ^double d]
  (let [bits (Double/doubleToLongBits d)]               ; canonical NaN
    (write-raw! out (.array (.putLong (ByteBuffer/allocate 8)
                                      (bit-xor bits (if (neg? bits) -1 Long/MIN_VALUE)))))))

(defn- write-sortable-float! [^ByteArrayOutputStream out ^double f]
  (let [bits (Float/floatToIntBits (unchecked-float f))]
    (write-raw! out (.array (.putInt (ByteBuffer/allocate 4)
                                     (unchecked-int (bit-xor bits (if (neg? bits) -1 Integer/MIN_VALUE))))))))

(defn- write-bigint! [^ByteArrayOutputStream out ^BigInteger n]
  (let [sg (.signum n)]
    (case sg
      0 (write-byte! out 1)
      (let [neg (neg? sg)
            mask (if neg -1 0)
            mag (let [m (.toByteArray (.abs n))]       ; strip sign-padding byte
                  (if (and (> (alength m) 1) (zero? (aget m 0)))
                    (Arrays/copyOfRange m 1 (alength m))
                    m))]
        (write-byte! out (if neg 0 2))
        (write-int! out (alength mag) mask)
        (dotimes [i (alength mag)]
          (write-byte! out (bit-xor (bit-and 0xFF (aget mag i)) (bit-and mask 0xFF))))))))

(defn- read-bigint ^BigInteger [^ByteBuffer buf]
  (let [sg (bit-and 0xFF (.get buf))]
    (if (== sg 1)
      BigInteger/ZERO
      (let [neg (zero? sg)
            mask (if neg -1 0)
            len (bit-and 0xFFFFFFFF (bit-xor (bit-xor (.getInt buf) 0x80000000) mask))
            mag (byte-array len)]
        (.get buf mag)
        (when neg
          (dotimes [i len] (aset mag i (unchecked-byte (bit-xor (aget mag i) -1)))))
        (BigInteger. (if neg -1 1) mag)))))

(defn- write-bigdec! [^ByteArrayOutputStream out ^BigDecimal d]
  (if (zero? (.signum d))
    (write-byte! out 1)
    (let [d (.stripTrailingZeros d)
          neg (neg? (.signum d))
          mask (if neg 0xFF 0)
          digits (utf8 (str (.abs (.unscaledValue d))))
          exp (- (.precision d) (.scale d) 1)           ; value = 0.d1d2.. scaled: d1.d2.. x 10^exp
          expb (.array (.putLong (ByteBuffer/allocate 8) (bit-xor exp Long/MIN_VALUE)))]
      (write-byte! out (if neg 0 2))
      (dotimes [i 8] (write-byte! out (bit-xor (bit-and 0xFF (aget expb i)) mask)))
      (write-escaped! out digits mask))))

(defn- read-bigdec ^BigDecimal [^ByteBuffer buf]
  (let [sg (bit-and 0xFF (.get buf))]
    (if (== sg 1)
      BigDecimal/ZERO
      (let [neg (zero? sg)
            mask (if neg 0xFF 0)
            eb (byte-array 8)
            _ (.get buf eb)
            _ (dotimes [i 8] (aset eb i (unchecked-byte (bit-xor (aget eb i) mask))))
            exp (bit-xor (.getLong (ByteBuffer/wrap eb)) Long/MIN_VALUE)
            digits (from-utf8 (read-escaped buf mask))
            unscaled (BigInteger. ^String digits)]
        (BigDecimal. (if neg (.negate unscaled) unscaled)
                     (int (- (count digits) exp 1)))))))

(declare encode-value decode-value)

(defn encode-value
  "Append the tagged, order-preserving encoding of `v` to `out`."
  [^ByteArrayOutputStream out v]
  (cond
    (nil? v) (write-byte! out tag-nil)
    (or (instance? Long v) (instance? Integer v) (instance? Short v) (instance? Byte v))
    (do (write-byte! out tag-long) (write-long! out (long v)))
    (instance? Boolean v) (do (write-byte! out tag-boolean) (write-byte! out (if v 1 0)))
    (instance? BigInteger v) (do (write-byte! out tag-bigint) (write-bigint! out v))
    (instance? clojure.lang.BigInt v)
    (do (write-byte! out tag-bigint) (write-bigint! out (biginteger v)))
    (instance? Float v) (do (write-byte! out tag-float) (write-sortable-float! out (double v)))
    (instance? Double v) (do (write-byte! out tag-double) (write-sortable-double! out (double v)))
    (instance? BigDecimal v) (do (write-byte! out tag-bigdec) (write-bigdec! out v))
    (instance? Date v) (do (write-byte! out tag-instant) (write-long! out (.getTime ^Date v)))
    (instance? UUID v) (let [^UUID u v]
                         (write-byte! out tag-uuid)
                         (write-long! out (.getMostSignificantBits u))
                         (write-long! out (.getLeastSignificantBits u)))
    (string? v) (do (write-byte! out tag-string) (write-escaped! out (utf8 v) 0))
    (keyword? v) (do (write-byte! out tag-keyword)
                     (write-escaped! out (utf8 (or (namespace v) "")) 0)
                     (write-escaped! out (utf8 (name v)) 0))
    (symbol? v) (do (write-byte! out tag-symbol)
                    (write-escaped! out (utf8 (or (namespace v) "")) 0)
                    (write-escaped! out (utf8 (name v)) 0))
    (instance? URI v) (do (write-byte! out tag-uri) (write-escaped! out (utf8 (str v)) 0))
    (bytes? v) (do (write-byte! out tag-bytes) (write-escaped! out v 0))
    (vector? v) (do (write-byte! out tag-tuple)
                    (doseq [x v] (encode-value out x))
                    (write-byte! out tag-tuple-end))
    :else (throw (ex-info (str "Unsupported value type: " (class v))
                          {:db/error :db.error/not-an-entity-value
                           :cognitect.anomalies/category :cognitect.anomalies/incorrect
                           :value v}))))

(defn value->bytes ^bytes [v]
  (let [out (ByteArrayOutputStream.)]
    (encode-value out v)
    (.toByteArray out)))

(defn- ns-name->symbol [ns-bytes name-bytes make]
  (let [ns (from-utf8 ns-bytes)]
    (make (when-not (.isEmpty ns) ns) (from-utf8 name-bytes))))

(defn decode-value
  "Decode one value from `buf`, advancing its position."
  [^ByteBuffer buf]
  (let [tag (bit-and 0xFF (.get buf))]
    (case tag
      0x01 nil
      0x10 (not (zero? (.get buf)))
      0x11 (read-long buf)
      0x12 (read-bigint buf)
      0x13 (let [b (.getInt buf)]
             (Float/intBitsToFloat (bit-xor b (if (neg? b) Integer/MIN_VALUE -1))))
      0x14 (let [b (.getLong buf)]
             (Double/longBitsToDouble (bit-xor b (if (neg? b) Long/MIN_VALUE -1))))
      0x15 (read-bigdec buf)
      0x16 (Date. (read-long buf))
      0x17 (let [msb (read-long buf)] (UUID. msb (read-long buf)))
      0x18 (from-utf8 (read-escaped buf 0))
      0x19 (let [n (read-escaped buf 0)] (ns-name->symbol n (read-escaped buf 0) keyword))
      0x1A (let [n (read-escaped buf 0)] (ns-name->symbol n (read-escaped buf 0) symbol))
      0x1B (URI. (from-utf8 (read-escaped buf 0)))
      0x1C (read-escaped buf 0)
      0x1D (loop [acc (transient [])]
             (if (zero? (bit-and 0xFF (.get buf (.position buf))))
               (do (.get buf) (persistent! acc))
               (recur (conj! acc (decode-value buf)))))
      (throw (ex-info (str "Unknown value tag: " tag)
                      {:db/error :db.error/invalid-encoding
                       :cognitect.anomalies/category :cognitect.anomalies/fault})))))

;; ------------------------------------------------------------------ longs as bytes

(defn long->bytes ^bytes [^long x]
  (.array (.putLong (ByteBuffer/allocate 8) x)))

(defn bytes->long ^long [^bytes b]
  (.getLong (ByteBuffer/wrap b)))

;; ------------------------------------------------------------------ keys

(def ^:private layouts
  "Component kinds per index, in key order. :l long id, :v tagged value, :r raw ref."
  {:eavt [:l :l :v :l]
   :aevt [:l :l :v :l]
   :avet [:l :v :l :l]
   :vaet [:r :l :l :l]})

(defn- layout [index]
  (or (get layouts index)
      (throw (ex-info (str "Unknown index " index)
                      {:db/error :db.error/unsupported
                       :cognitect.anomalies/category :cognitect.anomalies/incorrect}))))

(defn- write-component! [^ByteArrayOutputStream out kind x]
  (case kind
    :v (encode-value out x)
    (write-long! out (long x))))

(defn encode-prefix
  "Encode the leading `components` (in index order) of a key for `index`."
  ^bytes [index components]
  (let [out (ByteArrayOutputStream.)]
    (loop [kinds (seq (layout index)) cs (seq components)]
      (when (and kinds cs)
        (write-component! out (first kinds) (first cs))
        (recur (next kinds) (next cs))))
    (.toByteArray out)))

(defn encode-key
  ^bytes [index e a v tx]
  (encode-prefix index (case index
                         :eavt [e a v tx]
                         :aevt [a e v tx]
                         :avet [a v e tx]
                         :vaet [v a e tx])))

(defn decode-key
  "Decode an index key into a Datom (added = true)."
  ^Datom [index ^bytes k]
  (let [buf (ByteBuffer/wrap k)
        [c1 c2 c3 c4] (mapv (fn [kind] (if (= kind :v) (decode-value buf) (read-long buf)))
                            (layout index))]
    (case index
      :eavt (datom/datom c1 c2 c3 c4 true)
      :aevt (datom/datom c2 c1 c3 c4 true)
      :avet (datom/datom c3 c1 c2 c4 true)
      :vaet (datom/datom c3 c2 c1 c4 true))))

;; ------------------------------------------------------------------ log entries

(defn encode-log-entry
  "Count (int), then per datom: E A V added(1 byte). TX is the log key."
  ^bytes [datoms]
  (let [out (ByteArrayOutputStream.)]
    (write-raw! out (.array (.putInt (ByteBuffer/allocate 4) (int (count datoms)))))
    (doseq [^Datom d datoms]
      (write-long! out (.-e d))
      (write-long! out (.-a d))
      (encode-value out (.-v d))
      (write-byte! out (if (.-added d) 1 0)))
    (.toByteArray out)))

(defn decode-log-entry
  [^long tx ^bytes b]
  (let [buf (ByteBuffer/wrap b)
        n (.getInt buf)]
    (loop [i 0 acc (transient [])]
      (if (< i n)
        (let [e (read-long buf)
              a (read-long buf)
              v (decode-value buf)
              added (not (zero? (.get buf)))]
          (recur (inc i) (conj! acc (datom/datom e a v tx added))))
        (persistent! acc)))))
