(ns mnemonic.query.longmap-test
  (:require [clojure.test :refer [deftest is testing]]
            [mnemonic.query.longmap :as lm])
  (:import (java.util ArrayList HashMap)
           (mnemonic.query.longmap ILongGroups)))

(defn- check [label keys]
  (testing label
    (let [^ILongGroups g (lm/long-groups 4)
          ref (HashMap.)]
      (doseq [[i k] (map-indexed vector keys)]
        (.add g (long k) i)
        (.add ^ArrayList (or (.get ref k) (let [l (ArrayList.)] (.put ref k l) l)) i))
      (is (= (.size ref) (.size g)))
      (doseq [k (.keySet ref)]
        (is (= (vec (.get ref k)) (vec (.get g (long k)))) (str "key " k)))
      (doseq [k [-7 Long/MIN_VALUE Long/MAX_VALUE 123456789123]]
        (when-not (.containsKey ref k)
          (is (nil? (.get g (long k)))))))))

(deftest groups-match-a-hash-map
  (let [r (java.util.Random. 7)]
    (check "sequential" (range 17592186045418 17592186055418))
    (check "strided by 1024" (map #(* 1024 %) (range 5000)))
    (check "strided by 2^20" (map #(bit-shift-left % 20) (range 3000)))
    (check "negative and extremes" (concat (range -500 500) [Long/MIN_VALUE Long/MAX_VALUE]))
    (check "random" (repeatedly 20000 #(.nextLong r)))
    (check "repeats" (map #(mod % 37) (range 5000)))))
