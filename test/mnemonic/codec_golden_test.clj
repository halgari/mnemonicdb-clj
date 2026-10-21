(ns mnemonic.codec-golden-test
  "The optimized encoder must produce byte-identical output to the frozen original
  (mnemonic.codec-old), and the optimized decoder must agree with the original."
  (:require [clojure.test :refer [deftest is]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [mnemonic.codec :as c]
            [mnemonic.codec-old :as old]
            [mnemonic.codec-test :as ct]
            [mnemonic.datom :as d])
  (:import (java.util Arrays)))

(set! *warn-on-reflection* true)

(def gen-value
  (gen/one-of [ct/gen-long ct/gen-double ct/gen-float ct/gen-str ct/gen-kw ct/gen-sym
               ct/gen-bigint ct/gen-bigdec ct/gen-date ct/gen-uuid ct/gen-uri ct/gen-bytes
               ct/gen-tuple-mixed gen/boolean (gen/return nil)
               (gen/fmap int (gen/choose -100000 100000))
               (gen/fmap short (gen/choose -32768 32767))
               (gen/fmap byte (gen/choose -128 127))
               (gen/fmap #(clojure.lang.BigInt/fromBigInteger (biginteger %)) ct/gen-bigint)
               (gen/vector (gen/vector ct/gen-str 0 3) 0 3)]))

(def gen-eid (gen/one-of [gen/large-integer (gen/choose 0 100) (gen/return Long/MIN_VALUE)
                          (gen/return Long/MAX_VALUE)]))

(defn- same-bytes? [^bytes a ^bytes b] (Arrays/equals a b))

(defspec golden-value-bytes 3000
  (prop/for-all [v gen-value] (same-bytes? (c/value->bytes v) (old/value->bytes v))))

(defspec golden-key-bytes 3000
  (prop/for-all [e gen-eid a gen-eid v gen-value tx gen-eid
                 index (gen/elements [:eavt :aevt :avet])]
    (same-bytes? (c/encode-key index e a v tx) (old/encode-key index e a v tx))))

(defspec golden-vaet-key-bytes 1000
  (prop/for-all [e gen-eid a gen-eid v gen-eid tx gen-eid]
    (same-bytes? (c/encode-key :vaet e a v tx) (old/encode-key :vaet e a v tx))))

(defn- comps [index e a v tx n]
  (vec (take n (case index
                 :eavt [e a v tx]
                 :aevt [a e v tx]
                 :avet [a v e tx]
                 :vaet [v a e tx]))))

(defspec golden-prefix-bytes 3000
  (prop/for-all [e gen-eid a gen-eid v gen-value tx gen-eid n (gen/choose 0 4)
                 index (gen/elements [:eavt :aevt :avet])]
    (let [cs (comps index e a v tx n)]
      (and (same-bytes? (c/encode-prefix index cs) (old/encode-prefix index cs))
           (same-bytes? (c/encode-prefix-numeric index cs) (old/encode-prefix-numeric index cs))
           ;; lists/seqs as components behave like vectors
           (same-bytes? (c/encode-prefix index (apply list cs)) (old/encode-prefix index cs))))))

(defspec golden-prefix-numeric-bigdec 2000
  (prop/for-all [e gen-eid a gen-eid v ct/gen-bigdec index (gen/elements [:eavt :aevt :avet])]
    (every? (fn [n]
              (let [cs (comps index e a v 0 n)]
                (same-bytes? (c/encode-prefix-numeric index cs)
                             (old/encode-prefix-numeric index cs))))
            (range 0 4))))

(defspec golden-vaet-prefix 500
  (prop/for-all [v gen-eid a gen-eid e gen-eid tx gen-eid n (gen/choose 0 4)]
    (let [cs (comps :vaet e a v tx n)]
      (same-bytes? (c/encode-prefix :vaet cs) (old/encode-prefix :vaet cs)))))

(defspec golden-bigdec-numeric-prefix 1000
  (prop/for-all [v ct/gen-bigdec]
    (same-bytes? (c/bigdec-numeric-prefix v) (old/bigdec-numeric-prefix v))))

(defspec golden-decode-key 2000
  (prop/for-all [e gen-eid a gen-eid v gen-value tx gen-eid
                 index (gen/elements [:eavt :aevt :avet])]
    (let [k (old/encode-key index e a v tx)
          x (c/decode-key index k)
          y (old/decode-key index k)]
      (and (== (.-e x) e (.-e y)) (== (.-a x) a (.-a y)) (== (.-tx x) tx (.-tx y))
           (.-added x) (.-added y)
           ;; compare by encoding: NaN and widened integer types are not `=`
           (same-bytes? (c/value->bytes (.-v x)) (c/value->bytes (.-v y)))
           (same-bytes? (c/value->bytes (.-v x)) (c/value->bytes v))))))

(defspec golden-log-entry 500
  (prop/for-all [ds (gen/vector (gen/tuple gen-eid gen-eid gen-value gen/boolean) 0 6)
                 tx gen-eid]
    (let [datoms (mapv (fn [[e a v added]] (d/datom e a v tx added)) ds)
          bs (c/encode-log-entry datoms)]
      (and (same-bytes? bs (old/encode-log-entry datoms))
           (let [x (c/decode-log-entry tx bs) y (old/decode-log-entry tx bs)]
             ;; re-encode: NaN is not `=` to itself
             (and (= (count x) (count y) (count datoms))
                  (same-bytes? (c/encode-log-entry x) (old/encode-log-entry y))
                  (same-bytes? (c/encode-log-entry x) bs)))))))

(defn- old-hash [e a v tx added] (hash [e a (d/value-hash v) tx added]))

(defspec golden-datom-hash 2000
  (prop/for-all [e gen-eid a gen-eid v gen-value tx gen-eid added gen/boolean]
    (let [^Object x (d/datom e a v tx added)]
      (and (= (hash x) (old-hash e a v tx added))
           (= (.hashCode x) (old-hash e a v tx added))))))

(deftest unknown-index-still-throws
  (is (thrown? clojure.lang.ExceptionInfo (c/encode-key :nope 1 2 3 4)))
  (is (thrown? clojure.lang.ExceptionInfo (c/encode-prefix :nope [1])))
  (is (thrown? clojure.lang.ExceptionInfo (c/decode-key :nope (byte-array 8)))))
