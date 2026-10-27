(ns mnemonic.query.longmap
  "A map from primitive longs (entity ids) to groups of tuples, for hash joins keyed on
  one entity position: open addressing over a long[], no boxed key or map node per entry.")

(set! *warn-on-reflection* true)

(definterface ILongGroups
  (^void add [^long k v] "Appends `v` to the group of `k`.")
  (get [^long k] "The group of `k` (an ArrayList), or nil.")
  (^long size []))

(defn- mix ^long [^long k]
  ;; Murmur3 finalizer: entity ids are dense and sequential
  (let [k (bit-xor k (unsigned-bit-shift-right k 33))
        k (unchecked-multiply k (unchecked-long 0xff51afd7ed558ccd))
        k (bit-xor k (unsigned-bit-shift-right k 33))
        k (unchecked-multiply k (unchecked-long 0xc4ceb9fe1a85ec53))]
    (bit-xor k (unsigned-bit-shift-right k 33))))

(deftype LongGroups [^:unsynchronized-mutable ^longs ks
                     ^:unsynchronized-mutable ^objects vs
                     ^:unsynchronized-mutable ^long n
                     ^:unsynchronized-mutable ^long mask]
  ILongGroups
  (add [this k v]
    (when (>= (* 2 (inc n)) (alength ks))
      ;; grow to keep the load under one half
      (let [oks ks ovs vs
            cap (* 2 (alength oks))]
        (set! ks (long-array cap))
        (set! vs (object-array cap))
        (set! mask (dec cap))
        (dotimes [i (alength oks)]
          (when-let [g (aget ovs i)]
            (let [ok (aget oks i)]
              (loop [j (bit-and (mix ok) mask)]
                (if (nil? (aget vs j))
                  (do (aset ks j ok) (aset vs j g))
                  (recur (bit-and (inc j) mask)))))))))
    (loop [j (bit-and (mix k) mask)]
      (let [g (aget vs j)]
        (cond
          (nil? g) (let [l (java.util.ArrayList. 2)]
                     (.add l v)
                     (aset ks j k)
                     (aset vs j l)
                     (set! n (inc n)))
          (== k (aget ks j)) (.add ^java.util.ArrayList g v)
          :else (recur (bit-and (inc j) mask)))))
    nil)
  (get [_ k]
    (loop [j (bit-and (mix k) mask)]
      (let [g (aget vs j)]
        (cond
          (nil? g) nil
          (== k (aget ks j)) g
          :else (recur (bit-and (inc j) mask))))))
  (size [_] n))

(defn long-groups
  "An empty LongGroups sized for about `expected` keys."
  ^LongGroups [^long expected]
  (let [cap (loop [c 16] (if (< c (* 2 expected)) (recur (* 2 c)) c))]
    (LongGroups. (long-array cap) (object-array cap) 0 (dec cap))))
