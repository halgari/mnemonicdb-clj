(ns mnemonic.conn-test
  "Connection-level API: shutdown, get-database-names, format / counter checks on
  connect, post-commit failures."
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [mnemonic.api :as m]
            [mnemonic.bootstrap :as bootstrap]
            [mnemonic.codec :as codec]
            [mnemonic.conn :as conn]
            [mnemonic.rocks :as rocks])
  (:import (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)
           (java.util.concurrent ExecutionException)))

(use-fixtures :each (fn [f] (try (f) (finally (conn/reset-registry!)))))

(defn- tmp-dir []
  (str (Files/createTempDirectory "mnemonic-conn-test" (make-array FileAttribute 0))))

(defn- delete-tree [dir]
  (doseq [f (reverse (file-seq (io/file dir)))] (.delete ^java.io.File f)))

(defn- err [f]
  (try (f) :ok
       (catch Throwable t
         (loop [^Throwable x t]
           (cond (nil? x) t
                 (:db/error (ex-data x)) (:db/error (ex-data x))
                 :else (recur (.getCause x)))))))

(deftest get-database-names-test
  (testing "mem"
    (is (nil? (m/get-database-names "mnemonic:mem://*")))
    (m/create-database "mnemonic:mem://b")
    (m/create-database "mnemonic:mem://a")
    (is (= ["a" "b"] (vec (m/get-database-names "mnemonic:mem://*"))))
    (m/delete-database "mnemonic:mem://a")
    (is (= ["b"] (vec (m/get-database-names "mnemonic:mem://*")))))
  (testing "a uri without * is invalid (as in Datomic)"
    (is (= :db.error/invalid-db-uri (err #(m/get-database-names "mnemonic:mem://"))))
    (is (= :db.error/invalid-db-uri (err #(m/get-database-names "mnemonic:mem://a"))))
    (is (= :db.error/invalid-db-uri (err #(m/get-database-names "foo://*")))))
  (testing "file: databases that are direct subdirectories"
    (let [dir (tmp-dir)]
      (try
        (is (nil? (m/get-database-names (str "mnemonic:file://" dir "/*"))))
        (m/create-database (str "mnemonic:file://" dir "/one"))
        (m/create-database (str "mnemonic:file://" dir "/two"))
        (.mkdirs (io/file dir "not-a-db"))
        (is (= ["one" "two"] (vec (m/get-database-names (str "mnemonic:file://" dir "/*")))))
        (finally (conn/reset-registry!) (delete-tree dir))))))

(deftest shutdown-test
  (let [dir (tmp-dir)
        fu (str "mnemonic:file://" dir "/db")
        mu "mnemonic:mem://sd"]
    (try
      (m/create-database fu)
      (m/create-database mu)
      (let [fc (m/connect fu)
            mc (m/connect mu)]
        @(m/transact mc [{:db/ident :x/y}])
        (is (nil? (m/shutdown false)))
        (is (= :db.error/connection-released (err #(m/db fc))))
        (is (= :db.error/connection-released (err #(m/db mc))))
        (testing "file stores are closed: the database can be opened again"
          (is (= 72 (m/basis-t (m/db (m/connect fu))))))
        (testing "in-memory databases stay available"
          (is (= 1000 (m/basis-t (m/db (m/connect mu)))))))
      (finally (conn/reset-registry!) (delete-tree dir)))))

(defn- rewrite-meta! [path f]
  (let [s (rocks/open-store {:path path})]
    (try
      (rocks/write! s [(bootstrap/meta-op (f (bootstrap/read-meta s)))] {:sync? true})
      (finally (rocks/close-store s)))))

(deftest format-and-counter-checks-test
  (let [dir (tmp-dir)
        path (str dir "/db")
        u (str "mnemonic:file://" path)]
    (try
      (m/create-database u)
      (is (= codec/format-version (:format (bootstrap/read-meta (:store (m/connect u))))))
      (m/release (m/connect u))
      (doseq [fmt [(dec codec/format-version) (inc codec/format-version) nil "2"]]
        (testing (str "format " (pr-str fmt) " is refused")
          (rewrite-meta! path #(assoc % :format fmt))
          (let [e (try (m/connect u) nil (catch clojure.lang.ExceptionInfo e e))]
            (is (= :db.error/incompatible-format (:db/error (ex-data e))))
            (is (= :cognitect.anomalies/unsupported (:cognitect.anomalies/category (ex-data e)))))))
      (rewrite-meta! path #(assoc % :format codec/format-version))
      (testing "next-t must be greater than the last t in the log"
        (rewrite-meta! path #(assoc % :next-t 50))
        (is (= :db.error/invalid-metadata (err #(m/connect u)))))
      (testing "after a failed connect the store is closed again"
        (rewrite-meta! path #(assoc % :next-t 1000))
        (is (= 72 (m/basis-t (m/db (m/connect u))))))
      (finally (conn/reset-registry!) (delete-tree dir)))))

(deftest post-commit-failure-says-committed-test
  (let [u "mnemonic:mem://pc"
        _ (m/create-database u)
        c (m/connect u)
        real mnemonic.db/base-db
        calls (atom 0)
        r (with-redefs [mnemonic.db/base-db (fn [store]
                                              (if (<= (swap! calls inc) 2)
                                                (throw (RuntimeException. "boom"))
                                                (real store)))]
            (try @(m/transact c [{:db/ident :x/committed}]) (catch ExecutionException e e)))
        cause (.getCause ^Throwable r)
        data (ex-data cause)]
    (is (instance? ExecutionException r))
    (is (= :db.error/db-after-unavailable (:db/error data)))
    (is (= :cognitect.anomalies/fault (:cognitect.anomalies/category data)))
    (is (true? (:committed? data)))
    (is (= 1000 (:t data)))
    (is (re-find #"committed" (ex-message cause)))
    (is (= "boom" (ex-message (.getCause ^Throwable cause))))
    (is (= 1000 (m/basis-t (m/db c))) "the commit is visible")))
