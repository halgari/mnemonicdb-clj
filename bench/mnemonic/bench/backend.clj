(ns mnemonic.bench.backend
  "Uniform adapter over mnemonic.api and datomic.api so every workload runs unchanged
  against mnemonic mem, mnemonic file and Datomic mem."
  (:require [clojure.java.io :as io]
            [datomic.api :as d]
            [mnemonic.api :as m])
  (:import (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)))

(def datomic-fns
  {:create-database d/create-database :delete-database d/delete-database
   :connect d/connect :release d/release :db d/db :transact d/transact
   :datoms d/datoms :entity d/entity :pull d/pull :q d/q
   :as-of d/as-of :history d/history :basis-t d/basis-t})

(def mnemonic-fns
  {:create-database m/create-database :delete-database m/delete-database
   :connect m/connect :release m/release :db m/db :transact m/transact
   :datoms m/datoms :entity m/entity :pull m/pull :q m/q
   :as-of m/as-of :history m/history :basis-t m/basis-t})

(def tmp-root
  (delay (str (Files/createTempDirectory "mnemonic-bench" (make-array FileAttribute 0)))))

(def ^:private counter (atom 0))

(defn all-backends []
  [{:name "mnemonic-mem" :kind :mnemonic :fns mnemonic-fns
    :uri-fn #(str "mnemonic:mem://bench-" (swap! counter inc))}
   {:name "mnemonic-file" :kind :mnemonic :fns mnemonic-fns
    :uri-fn #(str "mnemonic:file://" @tmp-root "/db-" (swap! counter inc))}
   {:name "datomic-mem" :kind :datomic :fns datomic-fns
    :uri-fn #(str "datomic:mem://bench-" (swap! counter inc))}])

(defn call [b k & args] (apply (get-in b [:fns k]) args))

(defn new-db!
  "Creates a fresh database; returns {:uri :conn :backend}."
  [b]
  (let [uri ((:uri-fn b))]
    (call b :create-database uri)
    {:uri uri :backend b :conn (call b :connect uri)}))

(defn drop-db! [{:keys [backend uri conn]}]
  (try (call backend :release conn) (catch Throwable _))
  (try (call backend :delete-database uri) (catch Throwable _)))

(defn delete-tmp! []
  (when (realized? tmp-root)
    (doseq [f (reverse (file-seq (io/file @tmp-root)))] (.delete ^java.io.File f))))

(defn tx! [{:keys [backend conn]} data]
  @(call backend :transact conn data))

(defn db [{:keys [backend conn]}] (call backend :db conn))
