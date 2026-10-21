(ns mnemonic.api-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [mnemonic.api :as m]
            [mnemonic.conn :as conn]
            [mnemonic.index :as index])
  (:import (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)
           (java.util.concurrent ExecutionException)))

(use-fixtures :each (fn [f] (try (f) (finally (conn/reset-registry!)))))

(defn- uri [] (str "mnemonic:mem://" (java.util.UUID/randomUUID)))

(defn- fresh []
  (let [u (uri)]
    (m/create-database u)
    (m/connect u)))

(def ^:private schema
  [{:db/id "n" :db/ident :person/name :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/id "a" :db/ident :person/age :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/id "c" :db/ident :person/city :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one :db/index true}
   {:db/id "f" :db/ident :person/friend :db/valueType :db.type/ref :db/cardinality :db.cardinality/many}])

(defn- tx! [c data] @(m/transact c data))

(deftest lifecycle-test
  (let [u (uri)]
    (is (true? (m/create-database u)))
    (is (false? (m/create-database u)))
    (let [c (m/connect u)]
      (is (identical? c (m/connect u)) "one connection per uri")
      (is (= 72 (m/basis-t (m/db c))))
      (is (= 1000 (m/next-t (m/db c)))))
    (is (true? (m/delete-database u)))
    (is (false? (m/delete-database u)))
    (is (= :db.error/db-not-found
           (try (m/connect u) (catch clojure.lang.ExceptionInfo e (:db/error (ex-data e))))))
    (is (= :db.error/invalid-db-uri
           (try (m/connect "foo:bar") (catch clojure.lang.ExceptionInfo e (:db/error (ex-data e))))))))

(deftest release-mem-test
  (let [u (uri)
        _ (m/create-database u)
        c (m/connect u)]
    (tx! c schema)
    (m/release c)
    (is (thrown? clojure.lang.ExceptionInfo (m/db c)))
    (let [c2 (m/connect u)]
      (is (not (identical? c c2)))
      (is (= 1000 (m/basis-t (m/db c2))) "in-memory data survives release"))))

(deftest transact-test
  (let [c (fresh)
        r (tx! c schema)]
    (is (= #{:db-before :db-after :tx-data :tempids} (set (keys r))))
    (is (= 72 (m/basis-t (:db-before r))))
    (is (= 1000 (m/basis-t (:db-after r))))
    (is (= 1000 (m/basis-t (m/db c))))
    (is (= 1001 (m/next-t (m/db c))) "attrs use the db counter, not the shared one")
    (testing "failed transaction throws on deref and writes nothing"
      (let [f (m/transact c [[:db/add "x" :person/age "not a long"]])
            e (try @f nil (catch ExecutionException e e))]
        (is (some? e))
        (is (= :db.error/wrong-type-for-attribute (:db/error (ex-data (.getCause ^Throwable e)))))
        (is (= 1000 (m/basis-t (m/db c))))
        (is (= 1001 (m/next-t (m/db c))))))
    (testing "transact-async works the same"
      (let [r2 @(m/transact-async c [{:db/id "x" :person/name "Al"}])]
        (is (= 1001 (m/basis-t (:db-after r2))))))))

(deftest datoms-test
  (let [c (fresh)
        _ (tx! c schema)
        r (tx! c [{:db/id "al" :person/name "Al" :person/age 3 :person/city "Denver"}
                  {:db/id "bo" :person/name "Bo" :person/age 5 :person/city "Austin"
                   :person/friend "al"}])
        al (get (:tempids r) "al")
        bo (get (:tempids r) "bo")
        db (m/db c)
        ds (fn [& args] (mapv (juxt :e :a :v) (seq (apply m/datoms db args))))]
    (testing "ident components"
      (is (= [[al 74 3]] (ds :eavt al :person/age)))
      (is (= [[al 74 3] [bo 74 5]] (ds :aevt :person/age)))
      (is (= [[al 74 3]] (ds :eavt [:person/name "Al"] :person/age)))
      (is (= [[bo 76 al]] (ds :vaet al)))
      (is (= [[bo 76 al]] (ds :vaet [:person/name "Al"] :person/friend)))
      (is (= [[al 73 "Al"]] (ds :avet :person/name "Al")))
      (is (= [[al 75 "Denver"]] (ds :avet :person/city "Denver")))
      (is (= [] (ds :eavt al :nope/attr)) "unknown attribute ident matches nothing")
      (is (= [] (ds :eavt [:person/name "Nobody"]))))
    (testing "tx component"
      (is (= [[al 74 3]] (ds :eavt al :person/age 3 (:tx (first (seq (m/datoms db :eavt al :person/age))))))))
    (testing ":avet on a non-indexed attribute"
      (is (= :db.error/attribute-not-indexed
             (try (doall (m/datoms db :avet :person/age))
                  (catch clojure.lang.ExceptionInfo e (:db/error (ex-data e))))))
      (is (= :db.error/attribute-not-indexed
             (try (doall (m/seek-datoms db :avet :person/age))
                  (catch clojure.lang.ExceptionInfo e (:db/error (ex-data e)))))))
    (testing "full :avet scan holds only indexed attributes"
      (is (every? #(contains? #{10 73 75 50 39} (:a %)) (seq (m/datoms db :avet)))))
    (testing "datoms is an Iterable, not Counted"
      (is (instance? Iterable (m/datoms db :eavt)))
      (is (not (instance? clojure.lang.Counted (m/datoms db :eavt)))))
    (testing "seek-datoms continues past the prefix"
      (is (= [[bo 75 "Austin"] [al 75 "Denver"]]
             (take 2 (mapv (juxt :e :a :v) (seq (m/seek-datoms db :avet :person/city)))))))
    (testing "index-range"
      (let [rng (fn [s e] (mapv :v (seq (m/index-range db :person/city s e))))]
        (is (= ["Austin" "Denver"] (rng nil nil)))
        (is (= ["Austin"] (rng "A" "C")))
        (is (= ["Denver"] (rng "Denver" "Denverz")))
        (is (= [] (rng "Denver" "Denver")) "end is exclusive")
        (is (= ["Austin" "Denver"] (rng "Austin" nil)))
        (is (= :db.error/attribute-not-indexed
               (try (doall (m/index-range db :person/age 1 5))
                    (catch clojure.lang.ExceptionInfo e (:db/error (ex-data e))))))))
    (testing "bad index"
      (is (thrown? clojure.lang.ExceptionInfo (m/datoms db :foo))))))

(deftest ids-test
  (let [c (fresh)
        r (tx! c schema)
        db (m/db c)]
    (is (= 73 (m/entid db :person/name)))
    (is (= 73 (m/entid db 73)))
    (is (nil? (m/entid db :nope)))
    (is (= :person/name (m/ident db 73)))
    (is (= :person/name (m/ident db :person/name)))
    (is (= {:id 73 :ident :person/name :value-type :db.type/string
            :cardinality :db.cardinality/one :unique :db.unique/identity
            :indexed false :has-avet true :is-component false :no-history false
            :fulltext false :tuple-discontinued false}
           (m/attribute db :person/name)))
    (is (nil? (m/attribute db :nope)))
    (is (= (bit-shift-left 3 42) (bit-and (m/t->tx 1000) (bit-shift-left 3 42))))
    (is (= 1000 (m/tx->t (m/t->tx 1000))))
    (is (= 4 (m/part (m/entid-at db :db.part/user 1005))))
    (is (= (m/t->tx 1005) (m/entid-at db :db.part/tx 1005)))
    (let [t1 (m/tempid :db.part/user)
          t2 (m/tempid :db.part/user -5)
          r2 @(m/transact c [[:db/add t1 :person/name "T1"] [:db/add t2 :person/name "T2"]])]
      (is (= (m/entid db [:person/name "T1"]) nil) "old db unchanged")
      (is (= (:e (first (seq (m/datoms (:db-after r2) :avet :person/name "T1"))))
             (m/resolve-tempid (:db-after r2) (:tempids r2) t1)))
      (is (= (:e (first (seq (m/datoms (:db-after r2) :avet :person/name "T2"))))
             (m/resolve-tempid (:db-after r2) (:tempids r2) t2)))
      (is (not= t1 (m/tempid :db.part/user)))
      (is (= (m/tempid :db.part/user -5) t2)))
    (is (some? r))
    (testing "squuid"
      (let [u (m/squuid)]
        (is (instance? java.util.UUID u))
        (is (< (Math/abs (- (m/squuid-time-millis u) (System/currentTimeMillis))) 5000))))))

(deftest stubs-test
  (let [c (fresh)
        db (m/db c)
        kind (fn [f] (try (f) nil (catch clojure.lang.ExceptionInfo e (:db/error (ex-data e)))))]
    (doseq [f [#(m/entity db 1) #(m/pull db [:db/ident] 1)
               #(m/q '[:find ?e :where [?e :db/ident]] db)
               #(m/function {})]]
      (is (= :db.error/unsupported (kind f))))))

(deftest file-db-reopen-test
  (let [dir (str (Files/createTempDirectory "mnemonic-api-test" (make-array FileAttribute 0)))
        u (str "mnemonic:file://" dir "/db")]
    (try
      (is (true? (m/create-database u)))
      (let [c (m/connect u)]
        (tx! c schema)
        (tx! c [{:db/id "al" :person/name "Al" :person/age 3}])
        (tx! c [{:db/id "bo" :person/name "Bo"} [:db/add [:person/name "Al"] :person/age 4]])
        (let [db (m/db c)
              before (mapv (juxt :e :a :v :tx :added) (seq (m/datoms db :eavt)))
              next-before (m/next-t db)]
          (is (= 1003 (m/basis-t db)))
          (m/release c)
          (conn/reset-registry!)
          (let [c2 (m/connect u)
                db2 (m/db c2)]
            (is (not (identical? c c2)))
            (is (= 1003 (m/basis-t db2)))
            (is (= next-before (m/next-t db2)))
            (is (= before (mapv (juxt :e :a :v :tx :added) (seq (m/datoms db2 :eavt)))))
            (is (= :person/name (:ident (m/attribute db2 73))) "schema restored")
            (let [r (tx! c2 [{:db/id "cy" :person/name "Cy"}])]
              (is (= next-before (m/basis-t (:db-after r))))
              (is (= (+ (bit-shift-left 4 42) (inc next-before)) (get (:tempids r) "cy"))
                  "ids continue the sequence"))
            (m/release c2))))
      (is (true? (m/delete-database u)))
      (is (false? (m/delete-database u)))
      (finally
        (doseq [f (reverse (file-seq (io/file dir)))] (.delete ^java.io.File f))))))

(deftest stable-old-db-test
  (let [c (fresh)]
    (tx! c schema)
    (tx! c [{:db/id "al" :person/name "Al" :person/age 1}])
    (let [old (m/db c)
          it (m/datoms old :eavt)
          before (vec it)
          writer (future (dotimes [i 200]
                           @(m/transact c [{:db/id "x" :person/name (str "P" i) :person/age i}])))]
      (loop [n 0]
        (when (< n 20)
          (is (= before (vec it)) "iteration on the old db is stable during commits")
          (recur (inc n))))
      @writer
      (is (= before (vec it)))
      (is (= 1401 (m/basis-t (m/db c))))
      (is (= 1001 (m/basis-t old)))
      (is (= 201 (count (seq (m/datoms (m/db c) :avet :person/name))))
          "the live db sees every commit"))))

(deftest many-dbs-do-not-leak-test
  (let [c (fresh)
        before (.get index/open-cursors)]
    (tx! c schema)
    (dotimes [_ 500] (doall (seq (m/datoms (m/db c) :aevt :db/ident))))
    (is (= before (.get index/open-cursors)))))
