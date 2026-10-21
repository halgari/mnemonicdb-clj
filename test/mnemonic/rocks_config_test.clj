(ns mnemonic.rocks-config-test
  "Storage configuration (round 2.5): block cache / write buffer / compression options,
  flush on close."
  (:require [clojure.test :refer [deftest is testing]]
            [mnemonic.rocks :as r])
  (:import (java.nio.file Files LinkOption Path)
           (java.nio.file.attribute FileAttribute)
           (java.util Comparator)))

(defn- bs ^bytes [& xs] (byte-array (map unchecked-byte xs)))
(defn- ub [^bytes b] (mapv #(bit-and (long %) 0xFF) b))

(defn- temp-dir ^Path []
  (Files/createTempDirectory "mnemonic-rocks-config-test" (make-array FileAttribute 0)))

(defn- delete-tree [^Path p]
  (when (Files/exists p (make-array LinkOption 0))
    (with-open [s (Files/walk p (make-array java.nio.file.FileVisitOption 0))]
      (doseq [^Path x (sort (Comparator/reverseOrder) (iterator-seq (.iterator s)))]
        (Files/delete x)))))

(defn- wal-bytes ^long [^Path dir]
  (reduce + (map #(.length ^java.io.File %)
                 (filter #(.endsWith (.getName ^java.io.File %) ".log")
                         (file-seq (.toFile dir))))))

(deftest close-flushes-memtables-so-reopen-replays-no-wal
  (let [dir (temp-dir)
        path (str (.resolve dir "db"))]
    (try
      (let [s (r/open-store {:path path})]
        ;; unsynced writes: the data only lives in the memtable and the WAL
        (r/write! s (for [i (range 2000)] [:put :eavt (bs 0 0 0 (quot i 256) (mod i 256)) (bs 7)]) {})
        (r/close-store s))
      (is (zero? (wal-bytes (.resolve dir "db"))) "no WAL left to replay after close")
      (let [s (r/open-store {:path path})]
        (try
          (is (= [7] (ub (r/get-bytes s :eavt nil (bs 0 0 0 7 207)))))
          (finally (r/close-store s))))
      (finally (delete-tree dir)))))

(deftest store-options-and-compression-variants
  (testing "every option combination opens, writes, reads and closes (native objects freed)"
    (doseq [opts [{:cache-mb 1 :write-buffer-mb 1}
                  {:cache-mb 600 :write-buffer-mb 8 :compression "lz4+zstd"}
                  {:compression "none"}]]
      (let [dir (temp-dir)
            path (str (.resolve dir "db"))]
        (try
          (let [s (r/open-store (assoc opts :path path))]
            (r/write! s (for [i (range 500)] [:put :aevt (bs 0 0 0 (quot i 256) (mod i 256)) (bs 1)]) {})
            (r/flush! s)
            (is (= [1] (ub (r/get-bytes s :aevt nil (bs 0 0 0 1 3)))))
            (r/close-store s)
            (r/close-store s))
          ;; files written under one compression setting are readable under another
          (let [s (r/open-store {:path path :compression "lz4"})]
            (try (is (= [1] (ub (r/get-bytes s :aevt nil (bs 0 0 0 1 3)))))
                 (finally (r/close-store s))))
          (finally (delete-tree dir)))))))

(deftest query-store-opts-parsing
  (is (= {:cache-mb 512 :write-buffer-mb 64 :compression "lz4+zstd"}
         (r/query-store-opts {"cache-mb" "512" "write-buffer-mb" "64" "compression" "lz4+zstd" "sync" "true"})))
  (is (= {} (r/query-store-opts {"sync" "false"})))
  (doseq [bad [{"cache-mb" "0"} {"cache-mb" "x"} {"write-buffer-mb" "-1"} {"compression" "gzip"}]]
    (is (thrown? clojure.lang.ExceptionInfo (r/query-store-opts bad)))))
