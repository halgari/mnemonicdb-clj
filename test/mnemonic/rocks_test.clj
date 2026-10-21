(ns mnemonic.rocks-test
  (:require [clojure.test :refer [deftest is testing]]
            [mnemonic.rocks :as r])
  (:import (java.nio.file Files LinkOption Path)
           (java.nio.file.attribute FileAttribute)
           (java.util Comparator)))

(defn- bs ^bytes [& xs] (byte-array (map unchecked-byte xs)))
(defn- ub [^bytes b] (mapv #(bit-and (long %) 0xFF) b))

;; No IRawCursor hints here on purpose: mnemonic.reflection-test reloads
;; mnemonic.rocks, which redefines the interface class under already-compiled tests.
(defn- scan [cur]
  (loop [acc []]
    (if (.isValid cur)
      (let [k (ub (.key cur))]
        (.next cur)
        (recur (conj acc k)))
      acc)))

(defn- temp-dir ^Path []
  (Files/createTempDirectory "mnemonic-rocks-test" (make-array FileAttribute 0)))

(defn- delete-tree [^Path p]
  (when (Files/exists p (make-array LinkOption 0))
    (with-open [s (Files/walk p (make-array java.nio.file.FileVisitOption 0))]
      (doseq [^Path x (sort (Comparator/reverseOrder) (iterator-seq (.iterator s)))]
        (Files/deleteIfExists x)))))

(deftest cf-names-test
  (is (= [:default :eavt :aevt :avet :vaet :eavt-h :aevt-h :avet-h :vaet-h :log] r/cf-names)))

(deftest mem-store-has-all-cfs
  (let [s (r/open-store {:path "mem-cfs" :mem? true})]
    (try
      (is (= (set r/cf-names) (set (keys (:cfs s)))))
      (is (every? some? (vals (:cfs s))))
      (finally (r/close-store s)))))

(deftest atomic-write-across-cfs
  (let [s (r/open-store {:path "mem-write" :mem? true})]
    (try
      (r/write! s [[:put :eavt (bs 1) (bs 9)]
                   [:put :log (bs 2) (bs)]
                   [:put :aevt (bs 3) (bs 7)]] {})
      (is (= [9] (ub (r/get-bytes s :eavt nil (bs 1)))))
      (is (= [] (ub (r/get-bytes s :log nil (bs 2)))))
      (is (nil? (r/get-bytes s :eavt nil (bs 5))))
      (r/write! s [[:delete :eavt (bs 1)] [:put :aevt (bs 4) (bs 1)]] {:sync? true})
      (is (nil? (r/get-bytes s :eavt nil (bs 1))))
      (is (= [1] (ub (r/get-bytes s :aevt nil (bs 4)))))
      (is (= [7] (ub (r/get-bytes s :aevt nil (bs 3)))))
      (finally (r/close-store s)))))

(deftest cursor-bytewise-unsigned-order
  (let [s (r/open-store {:path "mem-cursor" :mem? true})]
    (try
      (r/write! s (for [k [[0xFF] [0x01] [0x80] [0x7F]]] [:put :eavt (apply bs k) (bs 0)]) {})
      (with-open [c (r/raw-cursor s :eavt nil)]
        (.seekToFirst c)
        (is (= [[1] [0x7F] [0x80] [0xFF]] (scan c)))
        (.seek c (bs 0x80))
        (is (= [0x80] (ub (.key c))))
        (is (= [0] (ub (.value c))))
        (.seek c (bs 0x81))
        (is (= [0xFF] (ub (.key c))))
        (.seekToLast c)
        (is (= [0xFF] (ub (.key c))))
        (.prev c)
        (is (= [0x80] (ub (.key c))))
        (.seekForPrev c (bs 0x90))
        (is (= [0x80] (ub (.key c))))
        (.seek c (bs 0xFF 0x01))
        (is (false? (.isValid c))))
      (finally (r/close-store s)))))

(deftest snapshot-isolation
  (let [s (r/open-store {:path "mem-snap" :mem? true})]
    (try
      (r/write! s [[:put :eavt (bs 1) (bs 1)]] {})
      (let [snap (r/snapshot s)]
        (r/write! s [[:put :eavt (bs 2) (bs 2)]] {})
        (with-open [c (r/raw-cursor s :eavt snap)]
          (.seekToFirst c)
          (is (= [[1]] (scan c))))
        (is (nil? (r/get-bytes s :eavt snap (bs 2))))
        (is (some? (r/get-bytes s :eavt nil (bs 2))))
        (with-open [c (r/raw-cursor s :eavt nil)]
          (.seekToFirst c)
          (is (= [[1] [2]] (scan c))))
        (r/release-snapshot s snap))
      (finally (r/close-store s)))))

(deftest cursor-close-idempotent
  (let [s (r/open-store {:path "mem-close" :mem? true})]
    (try
      (let [c (r/raw-cursor s :eavt nil)]
        (.close c)
        (.close c)
        (is true))
      (finally (r/close-store s)))))

(deftest on-disk-persist-and-destroy
  (let [dir (temp-dir)
        path (str (.resolve dir "db"))]
    (try
      (let [s (r/open-store {:path path})]
        (r/write! s [[:put :vaet (bs 5) (bs 6)]] {:sync? true})
        (r/close-store s))
      (let [s (r/open-store {:path path})]
        (try
          (is (= [6] (ub (r/get-bytes s :vaet nil (bs 5)))))
          (finally (r/close-store s))))
      (r/destroy-store path)
      (is (not (Files/exists (.resolve dir "db") (make-array LinkOption 0))))
      (finally (delete-tree dir)))))

(deftest cleanup-runs-after-gc
  (testing "register-cleanup! fires after the registered object is collected"
    (let [p (promise)]
      (r/register-cleanup! (Object.) #(deliver p true))
      (dotimes [_ 40] (when-not (realized? p) (System/gc) (Thread/sleep 50)))
      (is (realized? p)))))
