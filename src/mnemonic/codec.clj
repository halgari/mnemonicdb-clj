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

;; ------------------------------------------------------------------ format

(def format-version
  "On-disk format version, stored in the store meta and checked on connect. Bump it on
  any change to key or value encodings.
  1: initial format. 2: BigDecimal values keep their scale."
  2)

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

(defn prefix-end
  "Smallest key greater than every key starting with `p`, or nil when unbounded."
  ^bytes [^bytes p]
  (loop [i (dec (alength p))]
    (cond
      (neg? i) nil
      (== 255 (bit-and (aget p i) 0xff)) (recur (dec i))
      :else (let [r (Arrays/copyOf p (int (inc i)))]
              (aset r i (unchecked-byte (inc (bit-and (aget r i) 0xff))))
              r))))


(defn compare-bytes
  "Unsigned lexicographic comparison."
  ^long [^bytes a ^bytes b]
  (long (Arrays/compareUnsigned a b)))

(definterface IKeyBuf
  (reserve [^long n])
  (putByte [^long b])
  (putLong [^long x])
  (putLongRaw [^long x])
  (putIntRaw [^long x])
  (putEscapedString [^String s])
  (putByteArray [^bytes bs])
  (toBytes []))

(deftype KeyBuf [^:unsynchronized-mutable ^bytes buf ^:unsynchronized-mutable ^long pos]
  IKeyBuf
  (reserve [_ n]
    (let [need (+ pos n)]
      (when (> need (alength buf))
        (set! buf (Arrays/copyOf buf (unchecked-int (max need (* 2 (alength buf))))))))
    nil)
  (putByte [this b]
    (.reserve this 1)
    (aset buf (unchecked-int pos) (unchecked-byte b))
    (set! pos (+ pos 1))
    nil)
  (putLong [this x]
    ;; 8 bytes big-endian with the sign bit flipped (unsigned order == signed order)
    (.putLongRaw this (bit-xor x Long/MIN_VALUE)))
  (putLongRaw [this x]
    (.reserve this 8)
    (let [b buf p pos]
      (aset b (unchecked-int p) (unchecked-byte (unsigned-bit-shift-right x 56)))
      (aset b (unchecked-int (+ p 1)) (unchecked-byte (unsigned-bit-shift-right x 48)))
      (aset b (unchecked-int (+ p 2)) (unchecked-byte (unsigned-bit-shift-right x 40)))
      (aset b (unchecked-int (+ p 3)) (unchecked-byte (unsigned-bit-shift-right x 32)))
      (aset b (unchecked-int (+ p 4)) (unchecked-byte (unsigned-bit-shift-right x 24)))
      (aset b (unchecked-int (+ p 5)) (unchecked-byte (unsigned-bit-shift-right x 16)))
      (aset b (unchecked-int (+ p 6)) (unchecked-byte (unsigned-bit-shift-right x 8)))
      (aset b (unchecked-int (+ p 7)) (unchecked-byte x))
      (set! pos (+ p 8)))
    nil)
  (putIntRaw [this x]
    (.reserve this 4)
    (let [b buf p pos]
      (aset b (unchecked-int p) (unchecked-byte (unsigned-bit-shift-right x 24)))
      (aset b (unchecked-int (+ p 1)) (unchecked-byte (unsigned-bit-shift-right x 16)))
      (aset b (unchecked-int (+ p 2)) (unchecked-byte (unsigned-bit-shift-right x 8)))
      (aset b (unchecked-int (+ p 3)) (unchecked-byte x))
      (set! pos (+ p 4)))
    nil)
  ;; Strings are stored as CESU-8: each UTF-16 code unit is encoded on its own (1-3 bytes, so a
  ;; supplementary character becomes two 3-byte surrogate encodings). Unlike UTF-8, the byte
  ;; order of this encoding equals Java's String.compareTo (UTF-16 code unit order), which is
  ;; Datomic's value order; it also round-trips unpaired surrogates. Then 0x00 -> 0x00 0xFF,
  ;; terminated by 0x00 0x00.
  (putEscapedString [this s]
    (let [n (.length s)]
      (.reserve this (+ (* 3 n) 2))
      (let [b buf]
        (loop [i 0 p pos]
          (if (< i n)
            (let [c (long (int (.charAt s i)))]
              (cond
                (== c 0) (do (aset b (unchecked-int p) (unchecked-byte 0))
                             (aset b (unchecked-int (+ p 1)) (unchecked-byte 0xFF))
                             (recur (inc i) (+ p 2)))
                (< c 0x80) (do (aset b (unchecked-int p) (unchecked-byte c))
                               (recur (inc i) (+ p 1)))
                (< c 0x800) (do (aset b (unchecked-int p) (unchecked-byte (bit-or 0xC0 (unsigned-bit-shift-right c 6))))
                                (aset b (unchecked-int (+ p 1)) (unchecked-byte (bit-or 0x80 (bit-and c 0x3F))))
                                (recur (inc i) (+ p 2)))
                :else (do (aset b (unchecked-int p) (unchecked-byte (bit-or 0xE0 (unsigned-bit-shift-right c 12))))
                          (aset b (unchecked-int (+ p 1)) (unchecked-byte (bit-or 0x80 (bit-and (unsigned-bit-shift-right c 6) 0x3F))))
                          (aset b (unchecked-int (+ p 2)) (unchecked-byte (bit-or 0x80 (bit-and c 0x3F))))
                          (recur (inc i) (+ p 3)))))
            (do (aset b (unchecked-int p) (unchecked-byte 0))
                (aset b (unchecked-int (+ p 1)) (unchecked-byte 0))
                (set! pos (+ p 2)))))))
    nil)
  ;; Datomic orders byte arrays by length, then by signed byte values: a 4-byte length followed
  ;; by the bytes with the sign bit flipped (self-delimiting, no escaping needed).
  (putByteArray [this bs]
    (let [n (alength bs)]
      (.putIntRaw this n)
      (.reserve this n)
      (let [b buf p pos]
        (dotimes [i n]
          (aset b (unchecked-int (+ p i)) (unchecked-byte (bit-xor 0x80 (aget bs i)))))
        (set! pos (+ p n))))
    nil)
  (toBytes [_] (Arrays/copyOf buf (unchecked-int pos))))

(defn new-key-buf
  "A growable, unsynchronized key writer."
  (^KeyBuf [] (KeyBuf. (byte-array 48) 0))
  (^KeyBuf [^long cap] (KeyBuf. (byte-array (max cap 16)) 0)))

(defn key-buf-bytes
  "Copy of the bytes written to `kb` so far."
  ^bytes [^KeyBuf kb]
  (.toBytes kb))

(defn- write-byte! [^KeyBuf out ^long b]
  (.putByte out b))

(defn- write-long! [^KeyBuf out ^long x]
  (.putLong out x))

(defn- write-int!
  "4 bytes big-endian with the sign bit flipped, XORed with `mask` (0 or -1)."
  [^KeyBuf out ^long x ^long mask]
  (.putIntRaw out (unchecked-int (bit-xor (bit-xor x 0x80000000) mask))))

(defn- read-long ^long [^ByteBuffer buf]
  (bit-xor (.getLong buf) Long/MIN_VALUE))

(defn- read-byte-array ^bytes [^ByteBuffer buf]
  (let [n (.getInt buf)
        bs (byte-array n)]
    (dotimes [i n]
      (aset-byte bs i (unchecked-byte (bit-xor 0x80 (bit-and 0xFF (.get buf))))))
    bs))

(defn- read-escaped ^bytes [^ByteBuffer buf ^long mask]
  (let [out (ByteArrayOutputStream.)]
    (loop []
      (let [b (bit-xor (bit-and 0xFF (.get buf)) mask)]
        (if (zero? b)
          (let [n (bit-xor (bit-and 0xFF (.get buf)) mask)]
            (when-not (zero? n)
              (.write out 0)
              (recur)))
          (do (.write out (int b))
              (recur)))))
    (.toByteArray out)))

(defn- from-utf8 ^String [^bytes bs]
  (let [n (alength bs)
        sb (StringBuilder. n)]
    (loop [i 0]
      (when (< i n)
        (let [b (bit-and 0xFF (aget bs i))]
          (cond
            (< b 0x80) (do (.append sb (char b)) (recur (inc i)))
            (< b 0xE0) (do (.append sb (char (bit-or (bit-shift-left (bit-and b 0x1F) 6)
                                                     (bit-and 0x3F (aget bs (+ i 1))))))
                           (recur (+ i 2)))
            :else (do (.append sb (char (bit-or (bit-shift-left (bit-and b 0x0F) 12)
                                                (bit-shift-left (bit-and 0x3F (aget bs (+ i 1))) 6)
                                                (bit-and 0x3F (aget bs (+ i 2))))))
                      (recur (+ i 3)))))))
    (.toString sb)))

(defn- read-string-escaped
  "Read an escaped, terminated CESU-8 string (mask 0) straight from the buffer."
  ^String [^ByteBuffer buf]
  (if-not (.hasArray buf)
    (from-utf8 (read-escaped buf 0))
    (let [arr (.array buf)
          off (.arrayOffset buf)
          start (+ off (.position buf))
          lim (+ off (.limit buf))
          ;; ASCII fast path: no zero and no high byte before the terminator
          i (loop [i start]
              (if (and (< i lim) (pos? (aget arr i))) (recur (inc i)) i))]
      (if (and (< (inc i) lim) (zero? (aget arr i)) (zero? (aget arr (inc i))))
        (do (.position buf (unchecked-int (- (+ i 2) off)))
            (String. arr (unchecked-int start) (unchecked-int (- i start)) StandardCharsets/ISO_8859_1))
        (let [sb (StringBuilder. (unchecked-int (max 16 (- i start))))]
          (loop [i start]
            (when (>= i lim) (throw (java.nio.BufferUnderflowException.)))
            (let [b (bit-and 0xFF (aget arr i))]
              (cond
                (zero? b)
                (do (when (>= (inc i) lim) (throw (java.nio.BufferUnderflowException.)))
                    (if (zero? (aget arr (inc i)))
                      (.position buf (unchecked-int (- (+ i 2) off)))
                      (do (.append sb (char 0)) (recur (+ i 2)))))
                (< b 0x80) (do (.append sb (char b)) (recur (inc i)))
                (< b 0xE0) (do (.append sb (char (bit-or (bit-shift-left (bit-and b 0x1F) 6)
                                                         (bit-and 0x3F (aget arr (+ i 1))))))
                               (recur (+ i 2)))
                :else (do (.append sb (char (bit-or (bit-shift-left (bit-and b 0x0F) 12)
                                                    (bit-shift-left (bit-and 0x3F (aget arr (+ i 1))) 6)
                                                    (bit-and 0x3F (aget arr (+ i 2))))))
                          (recur (+ i 3))))))
          (.toString sb))))))

;; ------------------------------------------------------------------ value encoding

(defn- write-sortable-double! [^KeyBuf out ^double d]
  (let [bits (Double/doubleToLongBits d)]               ; canonical NaN
    (.putLongRaw out (bit-xor bits (if (neg? bits) -1 Long/MIN_VALUE)))))

(defn- write-sortable-float! [^KeyBuf out ^double f]
  (let [bits (Float/floatToIntBits (unchecked-float f))]
    (.putIntRaw out (unchecked-int (bit-xor bits (if (neg? bits) -1 Integer/MIN_VALUE))))))

(defn- write-bigint! [^KeyBuf out ^BigInteger n]
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

(defn- write-bigdec-numeric!
  "The numeric part: zero, or sign + exponent + significant digits (trailing zeros
  stripped), so numerically equal values share it."
  [^KeyBuf out ^BigDecimal d]
  (if (zero? (.signum d))
    (write-byte! out 1)
    (let [d (.stripTrailingZeros d)
          neg (neg? (.signum d))
          mask (if neg 0xFF 0)
          ^String digits (str (.abs (.unscaledValue d)))
          exp (- (.precision d) (.scale d) 1)           ; value = 0.d1d2.. scaled: d1.d2.. x 10^exp
          n (.length digits)]
      (write-byte! out (if neg 0 2))
      (.putLongRaw out (bit-xor (bit-xor exp Long/MIN_VALUE) (if neg -1 0)))
      ;; digits are ASCII and never zero, so escaping is a no-op
      (dotimes [i n] (write-byte! out (bit-xor (long (int (.charAt digits i))) mask)))
      (write-byte! out mask)
      (write-byte! out mask))))

(defn- write-bigdec!
  "Numeric part, then the scale (format 2) as an order-preserving int: values sort
  numerically, numerically equal values by scale, and decode with their exact scale."
  [^KeyBuf out ^BigDecimal d]
  (write-bigdec-numeric! out d)
  (write-int! out (.scale d) 0))

(defn bigdec-numeric-prefix
  "Tag plus numeric part of `d`: a key prefix matching `d` at every scale."
  ^bytes [^BigDecimal d]
  (let [out (new-key-buf)]
    (write-byte! out tag-bigdec)
    (write-bigdec-numeric! out d)
    (.toBytes out)))

(defn- read-bigdec ^BigDecimal [^ByteBuffer buf]
  (let [sg (bit-and 0xFF (.get buf))
        ^BigDecimal v
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
                         (int (- (count digits) exp 1)))))
        scale (bit-xor (.getInt buf) (unchecked-int 0x80000000))]
    (.setScale v (int scale) java.math.RoundingMode/UNNECESSARY)))

(defn encode-value
  "Append the tagged, order-preserving encoding of `v` to the key writer `out`."
  [^KeyBuf out v]
  (cond
    (nil? v) (write-byte! out tag-nil)
    (instance? Long v) (do (write-byte! out tag-long) (write-long! out (long v)))
    (string? v) (do (write-byte! out tag-string) (.putEscapedString out ^String v))
    (keyword? v) (let [^clojure.lang.Keyword k v]
                   (write-byte! out tag-keyword)
                   (.putEscapedString out (or (.getNamespace k) ""))
                   (.putEscapedString out (.getName k)))
    (or (instance? Integer v) (instance? Short v) (instance? Byte v))
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
    (symbol? v) (let [^clojure.lang.Symbol s v]
                  (write-byte! out tag-symbol)
                  (.putEscapedString out (or (.getNamespace s) ""))
                  (.putEscapedString out (.getName s)))
    (instance? URI v) (do (write-byte! out tag-uri) (.putEscapedString out (str v)))
    (bytes? v) (do (write-byte! out tag-bytes) (.putByteArray out ^bytes v))
    (vector? v) (do (write-byte! out tag-tuple)
                    (doseq [x v] (encode-value out x))
                    (write-byte! out tag-tuple-end))
    :else (throw (ex-info (str "Unsupported value type: " (class v))
                          {:db/error :db.error/not-an-entity-value
                           :cognitect.anomalies/category :cognitect.anomalies/incorrect
                           :value v}))))

(defn value->bytes ^bytes [v]
  (let [out (new-key-buf)]
    (encode-value out v)
    (.toBytes out)))

(defn decode-value
  "Decode one value from `buf`, advancing its position."
  [^ByteBuffer buf]
  (let [tag (bit-and 0xFF (.get buf))]
    (case tag
      0x01 nil
      0x10 (not (zero? (.get buf)))
      0x11 (read-long buf)
      0x12 (bigint (read-bigint buf))
      0x13 (let [b (.getInt buf)]
             (Float/intBitsToFloat (bit-xor b (if (neg? b) Integer/MIN_VALUE -1))))
      0x14 (let [b (.getLong buf)]
             (Double/longBitsToDouble (bit-xor b (if (neg? b) Long/MIN_VALUE -1))))
      0x15 (read-bigdec buf)
      0x16 (Date. (read-long buf))
      0x17 (let [msb (read-long buf)] (UUID. msb (read-long buf)))
      0x18 (read-string-escaped buf)
      0x19 (let [ns (read-string-escaped buf)
                 n (read-string-escaped buf)]
             (keyword (when-not (.isEmpty ns) ns) n))
      0x1A (let [ns (read-string-escaped buf)
                 n (read-string-escaped buf)]
             (symbol (when-not (.isEmpty ns) ns) n))
      0x1B (URI. (read-string-escaped buf))
      0x1C (read-byte-array buf)
      0x1D (loop [acc (transient [])]
             (if (zero? (bit-and 0xFF (.get buf (.position buf))))
               (do (.get buf) (persistent! acc))
               (recur (conj! acc (decode-value buf)))))
      (throw (ex-info (str "Unknown value tag: " tag)
                      {:db/error :db.error/invalid-encoding
                       :cognitect.anomalies/category :cognitect.anomalies/fault})))))

;; ------------------------------------------------------------------ longs as bytes

(defn long->bytes ^bytes [^long x]
  (let [b (byte-array 8)]
    (dotimes [i 8]
      (aset b i (unchecked-byte (unsigned-bit-shift-right x (* 8 (- 7 i))))))
    b))

(defn bytes->long ^long [^bytes b]
  (loop [i 0 acc 0]
    (if (< i 8)
      (recur (inc i) (bit-or (bit-shift-left acc 8) (bit-and 0xFF (aget b i))))
      acc)))

;; ------------------------------------------------------------------ keys

(defn- unknown-index [index]
  (throw (ex-info (str "Unknown index " index)
                  {:db/error :db.error/unsupported
                   :cognitect.anomalies/category :cognitect.anomalies/incorrect})))

(defn- value-pos
  "Position of the tagged value component in the key of `index`; every other component
  is a long (VAET's leading value is a raw ref)."
  ^long [index]
  (case index
    :eavt 2
    :aevt 2
    :avet 1
    :vaet -1
    (unknown-index index)))

(defn- write-prefix!
  [^KeyBuf out index components numeric?]
  (let [vp (value-pos index)
        n (min 4 (count components))]
    (dotimes [i n]
      (let [x (nth components i)]
        (if (== i vp)
          (if (and numeric? (== i (dec n)) (instance? BigDecimal x))
            (do (write-byte! out tag-bigdec)
                (write-bigdec-numeric! out x))
            (encode-value out x))
          (write-long! out (long x)))))))

(defn encode-prefix
  "Encode the leading `components` (in index order) of a key for `index`."
  ^bytes [index components]
  (let [out (new-key-buf)]
    (write-prefix! out index components false)
    (.toBytes out)))

(defn encode-prefix-numeric
  "Like `encode-prefix`, but a BigDecimal as the last component is encoded without its
  scale, so the prefix matches every numerically equal value (as Datomic's index
  lookups and ranges do)."
  ^bytes [index components]
  (let [out (new-key-buf)]
    (write-prefix! out index components true)
    (.toBytes out)))

(defn encode-key
  ^bytes [index e a v tx]
  (let [out (new-key-buf)]
    (case index
      :eavt (do (write-long! out (long e)) (write-long! out (long a))
                (encode-value out v) (write-long! out (long tx)))
      :aevt (do (write-long! out (long a)) (write-long! out (long e))
                (encode-value out v) (write-long! out (long tx)))
      :avet (do (write-long! out (long a)) (encode-value out v)
                (write-long! out (long e)) (write-long! out (long tx)))
      :vaet (do (write-long! out (long v)) (write-long! out (long a))
                (write-long! out (long e)) (write-long! out (long tx)))
      (unknown-index index))
    (.toBytes out)))

;; Key construction from a value that is already encoded (`vb` = `value->bytes`): the same
;; layouts as `encode-key`, `encode-prefix` and `encode-log-entry`, without re-encoding.

(defn- put-long!
  "Writes `x` at `off` of `bb` (big-endian, sign bit flipped); returns the next offset."
  ^long [^ByteBuffer bb ^long off ^long x]
  (.putLong bb (int off) (bit-xor x Long/MIN_VALUE))
  (+ off 8))

(defn encode-key-vb
  "Like `encode-key` for :eavt, :aevt and :avet, given the value's encoding `vb`."
  ^bytes [index e a ^bytes vb tx]
  (let [e (long e) a (long a) tx (long tx)
        n (alength vb)
        arr (byte-array (+ 24 n))
        bb (ByteBuffer/wrap arr)]
    (case index
      :eavt (do (put-long! bb 0 e) (put-long! bb 8 a)
                (System/arraycopy vb 0 arr 16 n)
                (put-long! bb (+ 16 n) tx))
      :aevt (do (put-long! bb 0 a) (put-long! bb 8 e)
                (System/arraycopy vb 0 arr 16 n)
                (put-long! bb (+ 16 n) tx))
      :avet (do (put-long! bb 0 a)
                (System/arraycopy vb 0 arr 8 n)
                (put-long! bb (+ 8 n) e)
                (put-long! bb (+ 16 n) tx))
      (unknown-index index))
    arr))

(defn encode-vaet-key
  "VAET key; the value `v` is an entity id, stored as a plain long."
  ^bytes [^long v ^long a ^long e ^long tx]
  (let [arr (byte-array 32)
        bb (ByteBuffer/wrap arr)]
    (put-long! bb 0 v) (put-long! bb 8 a) (put-long! bb 16 e) (put-long! bb 24 tx)
    arr))

(defn encode-prefix-eav-vb
  "Like `(encode-prefix :eavt [e a v])` given the value's encoding `vb`."
  ^bytes [^long e ^long a ^bytes vb]
  (let [arr (byte-array (+ 16 (alength vb)))
        bb (ByteBuffer/wrap arr)]
    (put-long! bb 0 e) (put-long! bb 8 a)
    (System/arraycopy vb 0 arr 16 (alength vb))
    arr))

(defn encode-prefix-av-vb
  "Like `(encode-prefix :avet [a v])` given the value's encoding `vb`."
  ^bytes [^long a ^bytes vb]
  (let [arr (byte-array (+ 8 (alength vb)))
        bb (ByteBuffer/wrap arr)]
    (put-long! bb 0 a)
    (System/arraycopy vb 0 arr 8 (alength vb))
    arr))

;; ------------------------------------------------------------------ in-place access
;; Flyweight datom views read components directly from the key bytes. Component offsets:
;;   eavt  E@0 A@8  V@16..n-8   TX@n-8
;;   aevt  A@0 E@8  V@16..n-8   TX@n-8
;;   avet  A@0 V@8..n-16        E@n-16 TX@n-8
;;   vaet  V@0 (raw ref) A@8 E@16 TX@24   (n = 32)

(defn index-code
  "Small int for an index keyword (0 eavt, 1 aevt, 2 avet, 3 vaet)."
  ^long [index]
  (case index :eavt 0 :aevt 1 :avet 2 :vaet 3 (unknown-index index)))

(defn key-long-at
  "The order-preserving long stored at `off` of `k`."
  ^long [^bytes k ^long off]
  (let [i (unchecked-int off)]
    (bit-xor (bit-or (bit-shift-left (long (aget k i)) 56)
                     (bit-shift-left (bit-and 0xFF (long (aget k (unchecked-add-int i 1)))) 48)
                     (bit-shift-left (bit-and 0xFF (long (aget k (unchecked-add-int i 2)))) 40)
                     (bit-shift-left (bit-and 0xFF (long (aget k (unchecked-add-int i 3)))) 32)
                     (bit-shift-left (bit-and 0xFF (long (aget k (unchecked-add-int i 4)))) 24)
                     (bit-shift-left (bit-and 0xFF (long (aget k (unchecked-add-int i 5)))) 16)
                     (bit-shift-left (bit-and 0xFF (long (aget k (unchecked-add-int i 6)))) 8)
                     (bit-and 0xFF (long (aget k (unchecked-add-int i 7)))))
             Long/MIN_VALUE)))

(defn key-e ^long [^long code ^bytes k ^long n]
  (case code 0 (key-long-at k 0) 1 (key-long-at k 8) 2 (key-long-at k (- n 16)) (key-long-at k 16)))

(defn key-a ^long [^long code ^bytes k ^long _n]
  (case code 0 (key-long-at k 8) 1 (key-long-at k 0) 2 (key-long-at k 0) (key-long-at k 8)))

(defn key-tx ^long [^bytes k ^long n] (key-long-at k (- n 8)))

(defn key-value
  "Decode the value component of the key (first `n` bytes of `k`)."
  [^long code ^bytes k ^long n]
  (case code
    3 (key-long-at k 0)
    (let [off (if (== code 2) 8 16)]
      (if (== 0x11 (bit-and 0xFF (long (aget k off))))
        (key-long-at k (inc off))
        (decode-value (ByteBuffer/wrap k (unchecked-int off) (unchecked-int (- n off))))))))

(defn key-value-long
  "The value component as a primitive long (long-tagged value, or a VAET ref)."
  ^long [^long code ^bytes k ^long _n]
  (case code
    3 (key-long-at k 0)
    (let [off (if (== code 2) 8 16)]
      (if (== 0x11 (bit-and 0xFF (long (aget k off))))
        (key-long-at k (inc off))
        (throw (ClassCastException. "datom value is not a long"))))))

(defn decode-key-code
  "Decode the index key held in the first `n` bytes of `k` (index code, see `index-code`)
  into a Datom (added = true); components are read in place, no ByteBuffer."
  ^Datom [^long code ^bytes k ^long n]
  (Datom. (key-e code k n) (key-a code k n) (key-value code k n) (key-tx k n) true))

(defn decode-key-len
  "Decode the index key held in the first `n` bytes of `k` into a Datom (added = true)."
  ^Datom [index ^bytes k ^long n]
  (decode-key-code (index-code index) k n))

(defn decode-key
  "Decode an index key into a Datom (added = true)."
  ^Datom [index ^bytes k]
  (decode-key-len index k (alength k)))

;; ------------------------------------------------------------------ log entries

(defn encode-log-entry
  "Count (int), then per datom: E A V added(1 byte). TX is the log key."
  ^bytes [datoms]
  (let [n (count datoms)
        ^KeyBuf out (new-key-buf (+ 8 (* 40 n)))]
    (.putIntRaw out n)
    (doseq [^Datom d datoms]
      (write-long! out (.-e d))
      (write-long! out (.-a d))
      (encode-value out (.-v d))
      (write-byte! out (if (.-added d) 1 0)))
    (.toBytes out)))

(defn encode-log-entry-vb
  "Like `encode-log-entry`, with `vbs` the value encodings parallel to `datoms`."
  ^bytes [datoms vbs]
  (let [size (reduce (fn [^long n ^bytes vb] (+ n 17 (alength vb))) 4 vbs)
        arr (byte-array size)
        bb (ByteBuffer/wrap arr)]
    (.putInt bb 0 (int (count datoms)))
    (loop [ds (seq datoms) vs (seq vbs) off 4]
      (when ds
        (let [^Datom d (first ds) ^bytes vb (first vs)
              off (long off)
              off (put-long! bb off (.-e d))
              off (put-long! bb off (.-a d))
              n (alength vb)]
          (System/arraycopy vb 0 arr (int off) n)
          (aset arr (+ (int off) n) (byte (if (.-added d) 1 0)))
          (recur (next ds) (next vs) (+ off n 1)))))
    arr))

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
          (recur (inc i) (conj! acc (Datom. e a v tx added))))
        (persistent! acc)))))
