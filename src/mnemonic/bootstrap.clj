(ns mnemonic.bootstrap
  "Writes the Datomic bootstrap datoms into a fresh store."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.walk :as walk]
            [mnemonic.codec :as codec]
            [mnemonic.datom :as datom]
            [mnemonic.rocks :as rocks])
  (:import (java.nio.charset StandardCharsets)
           (java.util Date)
           (mnemonic.datom Datom)))

(set! *warn-on-reflection* true)

(def ^:private meta-key (.getBytes "meta" StandardCharsets/UTF_8))
(def ^:private empty-bytes (byte-array 0))

(def initial-meta {:format 1 :next-t 1000 :next-db-n 73})

(defn bootstrap-datoms
  "The bootstrap datoms as a vector of Datoms (added = true), in file order."
  []
  (let [raw (with-open [r (java.io.PushbackReader. (io/reader (io/resource "mnemonic/bootstrap.edn")))]
              (edn/read r))
        conv (fn [x] (if (and (seq? x) (= 'inst (first x))) (Date. (long (second x))) x))]
    (mapv (fn [[e a v tx]]
            (let [[v] (walk/postwalk conv [v])]
              (datom/datom e a v tx true)))
          raw)))

(defn- ref-attr-ids
  "Attr ids whose :db/valueType (40) is :db.type/ref (entity 20)."
  [datoms]
  (into #{} (comp (filter (fn [^Datom d] (and (== (.-a d) 40) (== (long (.-v d)) 20))))
                  (map (fn [^Datom d] (.-e d))))
        datoms))

(defn datom-ops
  "Put ops for the current CFs of `d`; VAET only when its attribute is a ref."
  [^Datom d ref-attrs]
  (let [k (fn [idx] (codec/encode-key idx (.-e d) (.-a d) (.-v d) (.-tx d)))]
    (cond-> [[:put :eavt (k :eavt) empty-bytes]
             [:put :aevt (k :aevt) empty-bytes]
             [:put :avet (k :avet) empty-bytes]]
      (contains? ref-attrs (.-a d)) (conj [:put :vaet (k :vaet) empty-bytes]))))

(defn log-op [tx datoms]
  [:put :log (codec/long->bytes tx) (codec/encode-log-entry datoms)])

(defn meta-op
  "Op that stores `meta` (EDN) in :default."
  [meta]
  [:put :default meta-key (.getBytes ^String (pr-str meta) StandardCharsets/UTF_8)])

(defn read-meta
  "Stored meta map, or nil. With `snap`, reads as of that snapshot."
  ([store] (read-meta store nil))
  ([store snap]
   (when-let [^bytes b (rocks/get-bytes store :default snap meta-key)]
     (edn/read-string (String. b StandardCharsets/UTF_8)))))

(defn bootstrap!
  "Write the bootstrap datoms, their log entries and initial meta in one batch."
  [store]
  (let [datoms (bootstrap-datoms)
        refs (ref-attr-ids datoms)
        by-tx (into (sorted-map) (group-by #(.-tx ^Datom %) datoms))]
    (rocks/write! store
                  (concat (mapcat #(datom-ops % refs) datoms)
                          (map (fn [[tx ds]] (log-op tx ds)) by-tx)
                          [(meta-op initial-meta)])
                  {:sync? true})))
