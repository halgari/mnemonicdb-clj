(ns mnemonic.tx-ops-golden-test
  "The transactor builds index keys and the log entry from one encoding of each value; these
  must stay byte-identical to the codec's own key and log-entry encoders."
  (:require [clojure.edn]
            [clojure.test :refer [deftest is testing]]
            [mnemonic.bootstrap :as bootstrap]
            [mnemonic.codec :as codec]
            [mnemonic.datom :as datom]
            [mnemonic.tx :as tx])
  (:import (java.util Arrays Date UUID IdentityHashMap)
           (java.net URI)))

(def values
  [0 1 -1 Long/MAX_VALUE Long/MIN_VALUE 42
   "" "abc" "a\u0000b" "héllo 😀" (apply str (repeat 300 "x"))
   :a :ns/name :a/b.c 'sym 'ns/sym true false
   1.5 -0.0 Double/NaN (float 2.5) 12345678901234567890N (biginteger 7)
   1.50M 0M -3.14159M (Date. 1700000000000) (UUID. 1 2) (URI. "http://x.org/a")
   (byte-array [1 2 0 3 -1]) (byte-array 0) [1 "a" :k nil] []])

(def ids [0 1 5 (codec/eid 4 123) (codec/eid 3 77) (codec/eid 0 10) 4398046511103])

(deftest keys-match-codec
  (doseq [v values, e ids, a [0 10 73 1000], tx [(codec/t->tx 1) (codec/t->tx 99999)]]
    (let [d (datom/datom e a v tx true)
          vb (codec/value->bytes v)]
      (doseq [idx [:eavt :aevt :avet]]
        (is (Arrays/equals ^bytes (codec/encode-key idx e a v tx) ^bytes (#'tx/ekey idx d vb))
            (str idx " " (pr-str v))))
      (is (Arrays/equals ^bytes (codec/encode-prefix :eavt [e a v]) ^bytes (#'tx/prefix-ea+v e a vb)))
      (is (Arrays/equals ^bytes (codec/encode-prefix :avet [a v]) ^bytes (#'tx/prefix-av a vb)))))
  (doseq [v ids, e ids, a [10 73], tx [(codec/t->tx 5)]]
    (is (Arrays/equals ^bytes (codec/encode-key :vaet e a v tx)
                       ^bytes (#'tx/vaet-key (datom/datom e a v tx true))))))

(deftest log-op-matches-bootstrap
  (let [tx (codec/t->tx 7)
        ds (vec (concat (map-indexed (fn [i v] (datom/datom (nth ids (mod i 7)) 10 v tx (even? i))) values)
                        [(datom/datom 1 2 3 tx false)]))
        info (IdentityHashMap.)]
    (let [[op1 cf1 k1 b1] (bootstrap/log-op tx ds)
          [op2 cf2 k2 b2] (#'tx/log-op tx ds info)]
      (is (= [op1 cf1] [op2 cf2]))
      (is (Arrays/equals ^bytes k1 ^bytes k2))
      (is (Arrays/equals ^bytes b1 ^bytes b2)))))

(deftest codec-vb-fns-match-encode-key
  (doseq [v values, e ids, a [0 10 73], tx [(codec/t->tx 3)]]
    (let [vb (codec/value->bytes v)]
      (doseq [idx [:eavt :aevt :avet]]
        (is (Arrays/equals ^bytes (codec/encode-key idx e a v tx) ^bytes (codec/encode-key-vb idx e a vb tx))))
      (is (Arrays/equals ^bytes (codec/encode-prefix :eavt [e a v]) ^bytes (codec/encode-prefix-eav-vb e a vb)))
      (is (Arrays/equals ^bytes (codec/encode-prefix :avet [a v]) ^bytes (codec/encode-prefix-av-vb a vb)))))
  (doseq [v ids, e ids, a [10 73]]
    (is (Arrays/equals ^bytes (codec/encode-key :vaet e a v 9) ^bytes (codec/encode-vaet-key v a e 9)))))

(deftest meta-op-text
  (doseq [m [{:format 2 :next-t 1000 :next-db-n 73}
             {:format 2 :next-t 123456789012 :next-db-n 4096}
             {:format 2 :next-t 5 :next-db-n 6 :extra "x"}
             {:next-t 5}]]
    (let [[_ _ _ ^bytes b] (bootstrap/meta-op m)]
      (is (= m (clojure.edn/read-string (String. b "UTF-8"))))
      (is (= (pr-str m) (String. b "UTF-8"))))))
