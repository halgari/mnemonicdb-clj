(ns mnemonic.uri-options-test
  "Store options (cache-mb, write-buffer-mb, compression) as URI query parameters."
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [mnemonic.api :as m]
            [mnemonic.conn :as conn]
            [mnemonic.rocks :as rocks])
  (:import (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)))

(use-fixtures :each (fn [f] (try (f) (finally (conn/reset-registry!)))))

(defn- tmp-dir []
  (str (Files/createTempDirectory "mnemonic-uri-test" (make-array FileAttribute 0))))

(defn- delete-tree [dir]
  (doseq [f (reverse (file-seq (io/file dir)))] (.delete ^java.io.File f)))

(defn- cause-error [^Throwable t]
  (loop [x t] (cond (nil? x) nil (:db/error (ex-data x)) (:db/error (ex-data x)) :else (recur (.getCause x)))))

(defn- err [f] (try (f) :no-error (catch Throwable t (cause-error t))))

(def ^:private schema
  [{:db/ident :t/n :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])

(deftest valid-options-open-test
  (let [dir (tmp-dir)
        uri (str "mnemonic:file://" dir "?sync=false&cache-mb=16&write-buffer-mb=8&compression=none")]
    (try
      (is (true? (m/create-database uri)))
      (let [c (m/connect uri)]
        @(m/transact c schema)
        @(m/transact c [{:t/n 7}])
        (is (= #{[7]} (set (m/q '[:find ?n :where [_ :t/n ?n]] (m/db c))))))
      (finally (conn/reset-registry!) (delete-tree dir)))))

(deftest options-reach-open-store-test
  (let [dir (tmp-dir)
        uri (str "mnemonic:file://" dir "?cache-mb=16&write-buffer-mb=8&compression=lz4")
        seen (atom [])
        orig rocks/open-store]
    (try
      (with-redefs [rocks/open-store (fn [o] (swap! seen conj o) (orig o))]
        (m/create-database uri)
        (m/connect uri))
      (is (= 2 (count @seen)))
      (doseq [o @seen]
        (is (= {:cache-mb 16 :write-buffer-mb 8 :compression "lz4" :path (.getCanonicalPath (io/file dir))}
               o)))
      (finally (conn/reset-registry!) (delete-tree dir)))))

(deftest invalid-options-test
  (is (= :db.error/invalid-uri (err #(m/create-database "mnemonic:file:///tmp/x?cache-mb=abc"))))
  (is (= :db.error/invalid-uri (err #(m/create-database "mnemonic:file:///tmp/x?cache-mb=0"))))
  (is (= :db.error/invalid-uri (err #(m/create-database "mnemonic:file:///tmp/x?compression=gzip"))))
  (is (= :db.error/invalid-db-uri (err #(m/create-database "mnemonic:file:///tmp/x?cache=1"))))
  (is (= :db.error/invalid-db-uri (err #(m/create-database "mnemonic:file:///tmp/x?sync=maybe")))))

(deftest identity-and-mem-test
  (let [dir (tmp-dir)]
    (try
      (m/create-database (str "mnemonic:file://" dir))
      (is (identical? (m/connect (str "mnemonic:file://" dir "?cache-mb=8"))
                      (m/connect (str "mnemonic:file://" dir "?sync=false"))))
      (let [seen (atom nil) orig rocks/open-store]
        (with-redefs [rocks/open-store (fn [o] (reset! seen o) (orig o))]
          (is (true? (m/create-database "mnemonic:mem://uri-opts?cache-mb=8"))))
        (is (= 8 (:cache-mb @seen)))
        (is (= "uri-opts" (:path @seen))))
      (is (some? (m/connect "mnemonic:mem://uri-opts")))
      (finally (conn/reset-registry!) (delete-tree dir)))))
