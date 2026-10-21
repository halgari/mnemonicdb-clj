(ns mnemonic.codec-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [mnemonic.codec :as c]
            [mnemonic.datom :as d])
  (:import (java.math BigInteger BigDecimal)
           (java.net URI)
           (java.nio ByteBuffer)
           (java.util Arrays Date UUID)))

(set! *warn-on-reflection* true)

;; ---------------------------------------------------------------- generators

(def gen-long gen/large-integer)

(def gen-double (gen/double* {:NaN? true :infinite? true}))

(def gen-float (gen/fmap #(unchecked-float (double %)) gen-double))

(def gen-bmp-char
  (gen/fmap char (gen/one-of [(gen/choose 0 0xD7FF) (gen/choose 0xE000 0xFFFF)])))

(def gen-str
  (gen/one-of [gen/string
               (gen/fmap #(apply str %) (gen/vector gen-bmp-char 0 12))
               (gen/fmap #(apply str %) (gen/vector (gen/elements [\a \b (char 0) (char 255)]) 0 8))]))

(def gen-kw (gen/one-of [gen/keyword gen/keyword-ns]))
(def gen-sym (gen/one-of [gen/symbol gen/symbol-ns]))

(def gen-bigint
  (gen/fmap (fn [xs] (reduce (fn [^BigInteger a x] (.multiply a (BigInteger/valueOf (long x))))
                             BigInteger/ONE xs))
            (gen/vector gen/large-integer 1 5)))

(def gen-bigdec
  (gen/fmap (fn [[^BigInteger u s]] (BigDecimal. u (int s)))
            (gen/tuple gen-bigint (gen/choose -20 20))))

(def gen-date (gen/fmap #(Date. (long %)) gen/large-integer))
(def gen-uuid (gen/fmap (fn [[a b]] (UUID. (long a) (long b))) (gen/tuple gen/large-integer gen/large-integer)))
(def gen-uri (gen/fmap #(URI. (str "http://x/" %)) gen/string-alphanumeric))
(def gen-bytes (gen/one-of [gen/bytes
                            (gen/fmap byte-array (gen/vector (gen/elements [0 -1 1 127 -128]) 0 8))]))
(def gen-tuple-long (gen/vector gen-long 0 5))
(def gen-tuple-mixed
  (gen/vector (gen/one-of [gen-long gen-str gen-kw (gen/return nil) gen/boolean]) 0 5))

;; ---------------------------------------------------------------- helpers

(defn- bb ^ByteBuffer [^bytes b] (ByteBuffer/wrap b))
(defn- rt [v] (c/decode-value (bb (c/value->bytes v))))

(defn- same? [a b]
  (cond
    (bytes? a) (and (bytes? b) (Arrays/equals ^bytes a ^bytes b))
    (and (float? a) (Double/isNaN (double a))) (and (float? b) (Double/isNaN (double b)))
    (vector? a) (and (vector? b) (= (count a) (count b)) (every? true? (map same? a b)))
    (decimal? a) (and (decimal? b) (zero? (.compareTo ^BigDecimal a ^BigDecimal b)))
    :else (and (= a b) (= (class a) (class b)))))

(defn- order-prop [g cmp]
  (prop/for-all [a g b g]
    (= (Integer/signum (int (cmp a b)))
       (Integer/signum (c/compare-bytes (c/value->bytes a) (c/value->bytes b))))))

(defn- lex-compare [xs ys]
  (loop [xs (seq xs) ys (seq ys)]
    (cond (and (nil? xs) (nil? ys)) 0
          (nil? xs) -1
          (nil? ys) 1
          :else (let [r (compare (first xs) (first ys))]
                  (if (zero? r) (recur (next xs) (next ys)) r)))))

;; ---------------------------------------------------------------- round trip

(defspec round-trip-long 1000 (prop/for-all [v gen-long] (same? v (rt v))))
(defspec round-trip-double 1000 (prop/for-all [v gen-double] (same? v (rt v))))
(defspec round-trip-float 1000 (prop/for-all [v gen-float] (same? v (rt v))))
(defspec round-trip-string 1000 (prop/for-all [v gen-str] (same? v (rt v))))
(defspec round-trip-keyword 1000 (prop/for-all [v gen-kw] (same? v (rt v))))
(defspec round-trip-symbol 1000 (prop/for-all [v gen-sym] (same? v (rt v))))
(defspec round-trip-bigint 1000 (prop/for-all [v gen-bigint] (same? v (rt v))))
(defspec round-trip-bigdec 1000 (prop/for-all [v gen-bigdec] (same? v (rt v))))
(defspec round-trip-instant 1000 (prop/for-all [v gen-date] (same? v (rt v))))
(defspec round-trip-uuid 1000 (prop/for-all [v gen-uuid] (same? v (rt v))))
(defspec round-trip-uri 1000 (prop/for-all [v gen-uri] (same? v (rt v))))
(defspec round-trip-bytes 1000 (prop/for-all [v gen-bytes] (same? v (rt v))))
(defspec round-trip-boolean 10 (prop/for-all [v gen/boolean] (same? v (rt v))))
(defspec round-trip-tuple 1000 (prop/for-all [v gen-tuple-mixed] (same? v (rt v))))

(defspec round-trip-sequence 1000
  ;; several values concatenated decode back one by one (self-delimiting)
  (prop/for-all [vs (gen/vector (gen/one-of [gen-long gen-str gen-kw gen-bigdec gen-bigint
                                             gen-bytes gen-tuple-mixed gen-double])
                                1 6)]
    (let [out (java.io.ByteArrayOutputStream.)]
      (doseq [v vs] (c/encode-value out v))
      (let [buf (bb (.toByteArray out))
            back (vec (repeatedly (count vs) #(c/decode-value buf)))]
        (and (same? (vec vs) back) (zero? (.remaining buf)))))))

(deftest input-types
  (is (same? 5 (rt (int 5))))
  (is (same? (biginteger 5) (rt 5N)))
  (is (thrown? clojure.lang.ExceptionInfo (c/value->bytes {:a 1}))))

;; ---------------------------------------------------------------- order

(defspec order-long 2000 (order-prop gen-long compare))
(defspec order-double 2000 (order-prop gen-double #(Double/compare (double %1) (double %2))))
(defspec order-float 2000 (order-prop gen-float #(Float/compare (float %1) (float %2))))
(defspec order-string 2000
  (order-prop (gen/one-of [gen/string
                           (gen/fmap #(apply str %) (gen/vector gen-bmp-char 0 6))
                           (gen/fmap #(apply str %) (gen/vector (gen/elements [\a \b (char 0)]) 0 5))])
              compare))
(defspec order-keyword 2000 (order-prop gen-kw compare))
(defspec order-symbol 2000 (order-prop gen-sym compare))
(defspec order-bigint 2000 (order-prop gen-bigint compare))
(defspec order-bigdec 2000 (order-prop gen-bigdec compare))
(defspec order-instant 2000 (order-prop gen-date compare))
(defspec order-uuid 2000 (order-prop gen-uuid compare))
(defspec order-boolean 100 (order-prop gen/boolean compare))
(defspec order-bytes 2000 (order-prop gen-bytes #(Arrays/compareUnsigned ^bytes %1 ^bytes %2)))
(defspec order-tuple 2000 (order-prop gen-tuple-long lex-compare))

(deftest bigdec-order-is-numeric
  (let [vs [-100M -1.5M -1.25M -0.001M 0M 0.001M 1M 1.25M 1.5M 1E+10M]
        bs (map c/value->bytes vs)]
    (is (every? neg? (map c/compare-bytes bs (rest bs))))
    (is (zero? (c/compare-bytes (c/value->bytes 1.0M) (c/value->bytes 1M))))))

;; ---------------------------------------------------------------- edges

(deftest edge-values
  (doseq [s ["" "\u0000" "a\u0000b" "😀" "￿"]]
    (is (= s (rt s))))
  (doseq [x [Double/NaN -0.0 0.0 Double/POSITIVE_INFINITY Double/NEGATIVE_INFINITY Double/MIN_VALUE]]
    (is (same? x (rt x))))
  (is (Double/isNaN (double (rt Double/NaN))))
  (is (= (Double/doubleToLongBits -0.0) (Double/doubleToLongBits (double (rt -0.0)))))
  (is (neg? (c/compare-bytes (c/value->bytes -0.0) (c/value->bytes 0.0))))
  (is (neg? (c/compare-bytes (c/value->bytes Double/POSITIVE_INFINITY) (c/value->bytes Double/NaN))))
  (doseq [x [Long/MIN_VALUE Long/MAX_VALUE]] (is (= x (rt x))))
  (doseq [k [:a :a/b :a.b/c]] (is (= k (rt k))))
  (doseq [x [1.0M 1M -0.001M 1E+10M 0M 0.0M]] (is (same? x (rt x))))
  (is (nil? (rt nil)))
  (is (= [] (rt [])))
  (is (= [nil [1 nil]] (rt [nil [1 nil]])))
  (is (same? (byte-array [0 0 1]) (rt (byte-array [0 0 1])))))

(deftest tags-pinned
  (is (= [0x00 0x01 0x10 0x11 0x12 0x13 0x14 0x15 0x16 0x17 0x18 0x19 0x1A 0x1B 0x1C 0x1D]
         [c/tag-tuple-end c/tag-nil c/tag-boolean c/tag-long c/tag-bigint c/tag-float
          c/tag-double c/tag-bigdec c/tag-instant c/tag-uuid c/tag-string c/tag-keyword
          c/tag-symbol c/tag-uri c/tag-bytes c/tag-tuple]))
  (is (= c/tag-long (bit-and 0xFF (aget (c/value->bytes 1) 0))))
  (is (= 9 (alength ^bytes (c/value->bytes 1)))))

;; ---------------------------------------------------------------- keys

(def indexes [:eavt :aevt :avet :vaet])

(def gen-key-val (gen/one-of [gen-long gen-str gen-kw gen-uuid gen/boolean]))
(def gen-id gen/nat)
(def gen-small (gen/choose 0 2))

(defn- components [idx e a v tx]
  (case idx
    :eavt [e a v tx]
    :aevt [a e v tx]
    :avet [a v e tx]
    :vaet [v a e tx]))

(defn- starts-with? [^bytes k ^bytes p]
  (and (<= (alength p) (alength k))
       (zero? (Arrays/compareUnsigned k 0 (alength p) p 0 (alength p)))))

(defn- gen-v [idx] (if (= idx :vaet) gen-small (gen/one-of [gen-small gen-key-val])))

(defspec key-prefix 2000
  (prop/for-all [[idx e1 a1 v1 tx1 e2 a2 v2 tx2 n]
                 (gen/bind (gen/elements indexes)
                           (fn [idx]
                             (gen/tuple (gen/return idx)
                                        gen-small gen-small (gen-v idx) gen-small
                                        gen-small gen-small (gen-v idx) gen-small
                                        (gen/choose 0 4))))]
    (let [c1 (components idx e1 a1 v1 tx1)
          c2 (components idx e2 a2 v2 tx2)
          pre (c/encode-prefix idx (take n c1))
          k1 (c/encode-key idx e1 a1 v1 tx1)
          k2 (c/encode-key idx e2 a2 v2 tx2)]
      (and (starts-with? k1 pre)
           (= (starts-with? k2 pre)
              (= (take n c1) (take n c2)))))))

(deftest prefix-lengths
  (doseq [idx indexes]
    (is (zero? (alength (c/encode-prefix idx []))))
    (is (= 8 (alength (c/encode-prefix idx [1]))))
    (is (= (if (= idx :avet) 17 16) (alength ^bytes (c/encode-prefix idx [1 2])))))
  ;; avet [a v e]: a (8) + tagged long v (9) + e (8)
  (is (= 25 (alength (c/encode-prefix :avet [1 2 3])))))

(defspec key-round-trip 1000
  (prop/for-all [idx (gen/elements [:eavt :aevt :avet])
                 e gen-id a gen-id v gen-key-val tx gen-id]
    (= (d/datom e a v tx true) (c/decode-key idx (c/encode-key idx e a v tx)))))

(defspec key-round-trip-vaet 1000
  (prop/for-all [e gen-id a gen-id v gen-id tx gen-id]
    (= (d/datom e a v tx true) (c/decode-key :vaet (c/encode-key :vaet e a v tx)))))

(defspec key-order-eavt 1000
  (prop/for-all [x (gen/tuple gen-id gen-id gen-long gen-id)
                 y (gen/tuple gen-id gen-id gen-long gen-id)]
    (= (Integer/signum (int (lex-compare x y)))
       (Integer/signum (c/compare-bytes (apply c/encode-key :eavt x)
                                        (apply c/encode-key :eavt y))))))

;; ---------------------------------------------------------------- misc

(defspec long-bytes-round-trip 1000
  (prop/for-all [x gen-long]
    (and (= x (c/bytes->long (c/long->bytes x)))
         (= 8 (alength ^bytes (c/long->bytes x))))))

(deftest long-bytes-order
  (is (neg? (c/compare-bytes (c/long->bytes 1) (c/long->bytes 256))))
  (is (= [0 0 0 0 0 0 1 0] (mapv long (c/long->bytes 256)))))

(defspec log-entry-round-trip 500
  (prop/for-all [tx gen-id
                 ds (gen/vector (gen/tuple gen-id gen-id
                                           (gen/one-of [gen-key-val gen-bigdec gen-date gen-bytes])
                                           gen/boolean)
                                0 6)]
    (let [datoms (mapv (fn [[e a v added]] (d/datom e a v tx added)) ds)
          back (c/decode-log-entry tx (c/encode-log-entry datoms))]
      (and (vector? back)
           (= (count datoms) (count back))
           (every? true? (map (fn [x y] (and (= (:e x) (:e y)) (= (:a x) (:a y))
                                             (same? (:v x) (:v y))
                                             (= (:tx x) (:tx y)) (= (:added x) (:added y))))
                              datoms back))))))

(deftest ids
  (is (= 4 (c/part-of (c/eid 4 1002))))
  (is (= 1002 (bit-and (c/eid 4 1002) (dec (bit-shift-left 1 42)))))
  (is (= (bit-or (bit-shift-left 3 42) 1000) (c/t->tx 1000)))
  (is (= 1000 (c/tx->t (c/t->tx 1000))))
  (is (= 3 (c/part-of (c/t->tx 1000))))
  (is (= 0 (c/part-of 72))))
