(ns mnemonic.segcache-unit-test
  "The index segment cache's core rules in isolation: granule hashing, the AVET value
  scanner, stamp validity, superseded versions, epochs, admission and the byte budget."
  (:require [clojure.test :refer [deftest is testing]]
            [mnemonic.codec :as codec]
            [mnemonic.segcache :as sc])
  (:import (java.util Date UUID)))

(defn- log-op [t] [:put :log (codec/long->bytes (codec/t->tx t)) (byte-array 0)])

(defn- eavt-key [e t] (codec/encode-key :eavt e 100 "x" (codec/t->tx t)))

(defn- e-prefix [e] (codec/encode-prefix :eavt [e]))

(defn- reader [b ep]
  (reify mnemonic.segcache.IReader
    (basisT [_] b)
    (epoch [_] ep)))

(defn- fill!
  "Stores a one-key segment for entity `e` read at basis `r` (stamp read now)."
  [c e r ep]
  (let [k (sc/gkey 1 (e-prefix e) 8)]
    (sc/store! c (sc/point-pool c) k (reader r ep) (sc/stamp c (sc/gkey-hash k))
               (object-array [(eavt-key e r)]) nil false)))

(defn- hit? [c e b ep]
  (some? (sc/lookup c (sc/point-pool c) (sc/gkey 1 (e-prefix e) 8) (reader b ep))))

(deftest value-end-test
  (doseq [v [true false 0 -5 Long/MAX_VALUE (float 1.5) 2.5 (Date. 7) (UUID. 1 2)
             "" "abc" "a\u0000b" "é中" :k :ns/k 'sym 'ns/sym (java.net.URI. "http://x")
             (byte-array [1 2 3]) ["a" 1 nil :k] [nil nil] [[1 "x"] 2]]]
    (let [vb (codec/value->bytes v)
          b (byte-array (concat [0 0 0 0 0 0 0 0] vb [9 9 9]))]
      (is (= (+ 8 (alength vb)) (sc/value-end b 8 (alength b))) (pr-str v))
      (is (= -1 (sc/value-end b 8 (+ 8 (dec (alength vb))))) (str "truncated " (pr-str v)))))
  (testing "encodings that are not handled here"
    (doseq [v [(bigint 5) 1.5M]]
      (let [vb (codec/value->bytes v)]
        (is (= -1 (sc/value-end vb 0 (alength vb))))))))

(deftest chunk-hash-test
  (let [k (codec/encode-key :aevt 72 (+ 5000 (bit-shift-left 4 42)) 1 (codec/t->tx 9))
        ch (sc/key-chunk k)]
    (is (= (sc/hash-chunk 4 k ch) (sc/hash-range 4 (sc/chunk-granule k ch) 16)))
    (is (= ch (sc/key-chunk (sc/chunk-start k ch))))
    (is (= (inc ch) (sc/key-chunk (sc/chunk-start k (inc ch)))))
    (is (neg? (codec/compare-bytes (sc/chunk-start k ch) k)))))

(deftest hash-spread-test
  (is (< 99000 (count (into #{} (map #(sc/hash-range 1 (e-prefix %) 8)) (range 100000))))))

(deftest stamp-validity-test
  (let [c (sc/new-cache 16)
        ep (sc/epoch c)]
    (sc/before-write! c [(log-op 5) [:put :eavt (eavt-key 1 5) (byte-array 0)]])
    (fill! c 1 10 ep)
    (testing "valid from the last write to the granule up to the read basis, and beyond"
      (is (not (hit? c 1 4 ep)))
      (is (hit? c 1 5 ep))
      (is (hit? c 1 10 ep))
      (is (hit? c 1 12 ep) "nothing written since"))
    (sc/before-write! c [(log-op 13) [:put :eavt (eavt-key 1 13) (byte-array 0)]])
    (testing "a write to the granule ends the validity at the highest basis seen"
      (is (not (hit? c 1 13 ep)))
      (is (hit? c 1 12 ep))
      (is (hit? c 1 7 ep)))
    (testing "the superseded segment keeps serving old bases"
      (is (some? (fill! c 1 13 ep)))
      (is (hit? c 1 13 ep))
      (is (hit? c 1 20 ep))
      (is (hit? c 1 8 ep) "older version")
      (is (not (hit? c 1 4 ep))))
    (testing "other granules are unaffected"
      (fill! c 2 13 ep)
      (is (hit? c 2 0 ep)))
    (testing "a fill whose granule changed after its basis is dropped"
      (sc/before-write! c [(log-op 14) [:put :eavt (eavt-key 3 14) (byte-array 0)]])
      (is (nil? (fill! c 3 13 ep))))
    (testing "an epoch mismatch never hits"
      (is (not (hit? c 2 13 (+ ep 2)))))))

(deftest irregular-write-test
  (let [c (sc/new-cache 16)
        ep (sc/epoch c)]
    (is (false? (sc/before-write! c [(log-op 5) [:put :eavt (eavt-key 1 5) (byte-array 0)]])))
    (testing "no log entry"
      (is (true? (sc/before-write! c [[:put :eavt (eavt-key 1 5) (byte-array 0)]])))
      (is (odd? (sc/epoch c)))
      (sc/after-write! c)
      (is (= (+ ep 2) (sc/epoch c))))
    (testing "a t not above the last one"
      (is (true? (sc/before-write! c [(log-op 5) [:put :eavt (eavt-key 1 5) (byte-array 0)]])))
      (sc/after-write! c))
    (testing "batches without index entries are ignored"
      (is (nil? (sc/before-write! c [[:put :default (byte-array 1) (byte-array 0)]]))))))

(deftest admission-test
  (let [c (sc/new-cache 16)
        h (sc/hash-range 1 (e-prefix 9) 8)
        other (sc/hash-range 1 (e-prefix 10) 8)]
    (is (false? (sc/admit? c h)))
    (testing "consecutive misses of one granule count as one read"
      (is (false? (sc/admit? c h)))
      (is (false? (sc/admit? c h))))
    (is (false? (sc/admit? c other)))
    (is (true? (sc/admit? c h)) "missed again by a later read")
    (testing "a long run of misses of one granule is admitted"
      (is (some true? (repeatedly 9 #(sc/admit? c other)))))
    (testing "the window slides: seen two windows of first misses ago is forgotten"
      (let [c (sc/new-cache 1)]
        (is (false? (sc/admit? c h)))
        (dotimes [i 20000] (sc/admit? c (sc/hash-range 1 (e-prefix (+ 100 i)) 8)))
        (is (false? (sc/admit? c h)))))))

(deftest budget-test
  (let [c (sc/new-cache 1)
        ep (sc/epoch c)]
    (dotimes [e 20000] (fill! c e 1 ep))
    (let [{:keys [bytes evictions point-bytes]} (sc/stats c)]
      (is (pos? evictions))
      (is (<= point-bytes (* 1.1 (- 1048576 (quot 1048576 4)))))
      (is (pos? bytes)))
    (sc/clear! c)
    (is (zero? (:bytes (sc/stats c))))
    (is (nil? (sc/new-cache 0)))))
