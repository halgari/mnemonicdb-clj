(ns mnemonic.reflection-test
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is]]))

(defn- src-namespaces []
  (->> (file-seq (io/file "src"))
       (filter #(str/ends-with? (.getName ^java.io.File %) ".clj"))
       (map #(-> (.getPath ^java.io.File %)
                 (str/replace #"^src[/\\]" "")
                 (str/replace #"\.clj$" "")
                 (str/replace #"[/\\]" ".")
                 (str/replace "_" "-")
                 symbol))
       sort))

(deftest no-reflection-warnings
  (let [err (java.io.StringWriter.)]
    (binding [*err* err
              *warn-on-reflection* true]
      (doseq [ns-sym (src-namespaces)]
        (require ns-sym :reload)))
    (is (not (str/includes? (str err) "Reflection warning"))
        (str err))))
