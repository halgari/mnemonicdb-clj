(ns mnemonic.query.parser-test
  (:require [clojure.test :refer [deftest is testing]]
            [mnemonic.query.parser :as p]))

(defn- v [s] {:type :var :sym s})
(def ^:private blank {:type :blank})
(defn- c [x] {:type :const :val x})
(defn- src [s] {:type :src :sym s})

(defn- err
  "Run `f` and return the ex-data of the thrown ex-info (or nil)."
  [f]
  (try (f) nil
       (catch clojure.lang.ExceptionInfo e (ex-data e))))

(defn- err-kind [f] (:db/error (err f)))

(defmacro ^:private invalid
  "Asserts that `form` throws :db.error/invalid-query."
  [form]
  `(is (= :db.error/invalid-query (err-kind (fn [] ~form))) (pr-str '~form)))

(defn- where [q] (:where (p/parse-query q)))
(defn- last-clause [q] [(last (where q))])

;; ---------------------------------------------------------------- shapes

(deftest vector-form-basic
  (is (= {:find {:spec :find-rel :elements [(v '?x) (v '?y)]}
          :return-map nil
          :with []
          :in [{:bind :src :sym '$}]
          :where [{:clause :data-pattern :src '$
                   :terms [(v '?x) (c :a) (v '?y)]}]}
         (p/parse-query '[:find ?x ?y :where [?x :a ?y]]))))

(deftest map-form-and-list-form-and-string-form
  (let [expected (p/parse-query '[:find ?x :in $ ?n :where [?x :a ?n]])]
    (is (= expected (p/parse-query '{:find [?x] :in [$ ?n] :where [[?x :a ?n]]})))
    (is (= expected (p/parse-query '(:find ?x :in $ ?n :where [?x :a ?n]))))
    (is (= expected (p/parse-query '(quote [:find ?x :in $ ?n :where [?x :a ?n]]))))
    (is (= expected (p/parse-query "[:find ?x :in $ ?n :where [?x :a ?n]]")))
    (is (= expected (p/parse-query '[:where [?x :a ?n] :in $ ?n :find ?x])))))

(deftest in-only-query
  (is (= [] (:where (p/parse-query '[:find ?x :in $ ?x])))))

;; ----------------------------------------------------------- find specs

(deftest find-specs
  (is (= :find-rel (-> (p/parse-query '[:find ?x ?y :where [?x :a ?y]]) :find :spec)))
  (is (= {:spec :find-coll :elements [(v '?x)]}
         (:find (p/parse-query '[:find [?x ...] :where [?x :a _]]))))
  (is (= {:spec :find-scalar :elements [(v '?x)]}
         (:find (p/parse-query '[:find ?x . :where [?x :a _]]))))
  (is (= {:spec :find-tuple :elements [(v '?x) (v '?y)]}
         (:find (p/parse-query '[:find [?x ?y] :where [?x :a ?y]])))))

(deftest find-elements
  (is (= [{:type :agg :agg 'count :args [(v '?x)]}
          {:type :agg :agg 'min :args [(c 3) (v '?y)]}
          {:type :agg :agg 'my.ns/agg :args [(v '?y)]}]
         (-> (p/parse-query '[:find (count ?x) (min 3 ?y) (my.ns/agg ?y)
                              :where [?x :a ?y]])
             :find :elements)))
  (is (= [{:type :pull :src '$ :var '?x :pattern (c '[*])}
          {:type :pull :src '$2 :var '?x :pattern (c '[:a {:b [:c]}])}
          {:type :pull :src '$ :var '?x :pattern (v '?pat)}]
         (-> (p/parse-query '[:find (pull ?x [*]) (pull $2 ?x [:a {:b [:c]}])
                              (pull ?x ?pat)
                              :in $ $2 ?pat :where [?x :a _]])
             :find :elements)))
  (is (= :find-scalar
         (-> (p/parse-query '[:find (count ?x) . :where [?x :a _]]) :find :spec))))

(deftest return-maps
  (is (= {:type :keys :names '[a b]}
         (:return-map (p/parse-query '[:find ?x ?y :keys a b :where [?x :a ?y]]))))
  (is (= {:type :syms :names '[a]}
         (:return-map (p/parse-query '[:find ?x :syms a :where [?x :a _]]))))
  (is (= {:type :strs :names '[a b]}
         (:return-map (p/parse-query '[:find [?x ?y] :strs a b :where [?x :a ?y]]))))
  (testing "count mismatch / unsupported find specs"
    (invalid (p/parse-query '[:find ?x ?y :keys a :where [?x :a ?y]]))
    (invalid (p/parse-query '[:find ?x . :keys a :where [?x :a _]]))
    (invalid (p/parse-query '[:find ?x :keys a :syms b :where [?x :a _]]))))

(deftest with-clause
  (is (= '[?y] (:with (p/parse-query '[:find ?x :with ?y :where [?x :a ?y]]))))
  (is (= :db.error/unbound-query-variables
         (err-kind #(p/parse-query '[:find ?x :with ?w :where [?x :a _]]))))
  (invalid (p/parse-query '[:find ?x :with ?x :where [?x :a _]])))

;; ------------------------------------------------------------------ :in

(deftest in-bindings
  (is (= [{:bind :src :sym '$}
          {:bind :src :sym '$foo}
          {:bind :rules}
          {:bind :scalar :sym '?a}
          {:bind :blank}
          {:bind :tuple :elems [{:bind :scalar :sym '?b} {:bind :blank}
                                {:bind :scalar :sym '?c}]}
          {:bind :coll :elem {:bind :scalar :sym '?d}}
          {:bind :rel :elems [{:bind :scalar :sym '?e} {:bind :scalar :sym '?f}]}]
         (:in (p/parse-query
               '[:find ?a :in $ $foo % ?a _ [?b _ ?c] [?d ...] [[?e ?f]]
                 :where [?a :x _]]))))
  (invalid (p/parse-query '[:find ?x :in $ 5 :where [?x :a _]]))
  (invalid (p/parse-query '[:find ?x :in $ [?x ?y ...] :where [?x :a _]]))
  (invalid (p/parse-query '[:find ?x :in $ [] :where [?x :a _]])))

;; --------------------------------------------------------------- patterns

(deftest data-patterns
  (is (= [{:clause :data-pattern :src '$ :terms [(v '?e)]}]
         (where '[:find ?e :where [?e]])))
  (is (= [{:clause :data-pattern :src '$2
           :terms [(v '?e) (c :a/b) blank (c "s") (c 42)]}
          {:clause :data-pattern :src '$
           :terms [(c [:user/id 1]) (c 'sym) (c 1.5)]}]
         (where '[:find ?e :in $ $2
                  :where [$2 ?e :a/b _ "s" 42]
                         [[:user/id 1] sym 1.5]])))
  (testing "four-element pattern"
    (is (= [(v '?e) (c :a) (v '?v) (v '?tx)]
           (-> (where '[:find ?e :where [?e :a ?v ?tx]]) first :terms)))))

(deftest data-pattern-errors
  (invalid (p/parse-query '[:find ?x :where []]))
  (invalid (p/parse-query '[:find ?x :where 5]))
  (invalid (p/parse-query '[:find ?x :where [?x :a _ _ _ _]])))

;; ------------------------------------------------------ expressions / fns

(deftest predicate-and-fn-exprs
  (is (= [{:clause :data-pattern :src '$ :terms [(v '?x) (c :a) (v '?y)]}
          {:clause :pred :fn '> :args [(v '?y) (c 1)]}
          {:clause :fn :fn 'inc :args [(v '?y)]
           :binding {:bind :scalar :sym '?z}}
          {:clause :fn :fn 'ground :args [(c [1 2])]
           :binding {:bind :coll :elem {:bind :scalar :sym '?c}}}
          {:clause :fn :fn 'vector :args [(v '?x) (v '?y)]
           :binding {:bind :tuple :elems [{:bind :scalar :sym '?t1} {:bind :blank}]}}
          {:clause :fn :fn 'ground :args [(c [[1 2]])]
           :binding {:bind :rel :elems [{:bind :scalar :sym '?r1}
                                        {:bind :scalar :sym '?r2}]}}
          {:clause :fn :fn 'get-else :args [(src '$) (v '?x) (c :a) (c "d")]
           :binding {:bind :scalar :sym '?g}}
          {:clause :fn :fn 'clojure.string/upper-case :args [(c "x")]
           :binding {:bind :blank}}]
         (where '[:find ?x
                  :where [?x :a ?y]
                  [(> ?y 1)]
                  [(inc ?y) ?z]
                  [(ground [1 2]) [?c ...]]
                  [(vector ?x ?y) [?t1 _]]
                  [(ground [[1 2]]) [[?r1 ?r2]]]
                  [(get-else $ ?x :a "d") ?g]
                  [(clojure.string/upper-case "x") _]]))))

(deftest expr-errors
  (invalid (p/parse-query '[:find ?x :where [?x :a ?y] [(inc ?y) ?z ?w]]))
  (invalid (p/parse-query '[:find ?x :where [?x :a ?y] [(vector ?y _)]]))
  (invalid (p/parse-query '[:find ?x :where [?x :a ?y] [(5 ?y)]]))
  (invalid (p/parse-query '[:find ?x :where [?x :a ?y] [(inc ?y) 5]])))

(deftest insufficient-binding-errors
  (let [e (err #(p/parse-query '[:find ?x :where [?x :a _] [(> ?q 1)]]))]
    (is (= :db.error/insufficient-binding (:db/error e)))
    (is (= '#{?q} (:variables e)))
    (is (= :cognitect.anomalies/incorrect (:cognitect.anomalies/category e))))
  (is (= :db.error/insufficient-binding
         (err-kind #(p/parse-query '[:find ?x :where [?x :a _] [(inc ?q) ?r]]))))
  (testing "Datomic defers predicates: written order is not an error"
    (is (some? (p/parse-query '[:find ?x :where [(> ?y 1)] [?x :a ?y]]))))
  (testing "fn-bound vars satisfy later exprs"
    (is (some? (p/parse-query '[:find ?r :where [?x :a ?y] [(inc ?y) ?z] [(inc ?z) ?r]]))))
  (testing "inputs count as bound"
    (is (some? (p/parse-query '[:find ?x :in $ ?n :where [?x :a ?y] [(> ?y ?n)]])))))

(deftest unbound-query-variables
  (let [e (err #(p/parse-query '[:find ?x ?y :where [?x :a _]]))]
    (is (= :db.error/unbound-query-variables (:db/error e)))
    (is (= '#{?y} (:variables e))))
  (is (= :db.error/unbound-query-variables
         (err-kind #(p/parse-query '[:find (count ?q) :where [?x :a _]]))))
  (is (= :db.error/unbound-query-variables
         (err-kind #(p/parse-query '[:find (pull ?q [*]) :where [?x :a _]]))))
  (is (= :db.error/unbound-query-variables
         (err-kind #(p/parse-query '[:find (pull ?x ?pat) :where [?x :a _]]))))
  (testing "vars bound by fn bindings, rules, or, and inputs are bound"
    (is (some? (p/parse-query '[:find ?r ?s ?o :in $ % ?i
                                :where [?x :a _] [(inc ?i) ?r] (rule ?s)
                                (or [?o :b _] [?o :c _])])))))

;; ------------------------------------------------------------------ rules

(deftest rule-expressions
  (is (= [{:clause :rule :src '$ :name 'ancestor :args [(v '?x) (v '?y)]}
          {:clause :rule :src '$2 :name 'r :args [(c 5) blank (src '$)]}]
         (where '[:find ?x ?y :in $ $2 %
                  :where (ancestor ?x ?y) ($2 r 5 _ $)]))))

(deftest parse-rules-test
  (is (= '{ancestor [{:head [?x ?y] :required []
                      :body [{:clause :data-pattern :src $
                              :terms [{:type :var :sym ?x} {:type :const :val :parent}
                                      {:type :var :sym ?y}]}]}
                     {:head [?x ?y] :required []
                      :body [{:clause :data-pattern :src $
                              :terms [{:type :var :sym ?x} {:type :const :val :parent}
                                      {:type :var :sym ?z}]}
                             {:clause :rule :src $ :name ancestor
                              :args [{:type :var :sym ?z} {:type :var :sym ?y}]}]}]
           sib [{:head [?a ?b ?c] :required [?a]
                 :body [{:clause :pred :fn not= :args [{:type :var :sym ?b}
                                                       {:type :var :sym ?c}]}]}]}
         (p/parse-rules '[[(ancestor ?x ?y) [?x :parent ?y]]
                          [(ancestor ?x ?y) [?x :parent ?z] (ancestor ?z ?y)]
                          [(sib [?a] ?b ?c) [(not= ?b ?c)]]])))
  (is (= {} (p/parse-rules [])))
  (testing "errors"
    (invalid (p/parse-rules '[[r ?x] [?x :a _]]))
    (invalid (p/parse-rules '[[(r ?x)]]))
    (invalid (p/parse-rules '[[(r 5) [?x :a _]]]))
    (invalid (p/parse-rules '[[(r ?x) [?x :a _]] [(r ?x ?y) [?x :a ?y]]]))
    (invalid (p/parse-rules '[[(r ?x) 5]]))
    (invalid (p/parse-rules '{:a 1}))))

;; ------------------------------------------------------------ not and or

(deftest not-clauses
  (is (= [{:clause :data-pattern :src '$ :terms [(v '?x) (c :a) (v '?y)]}
          {:clause :not :src '$ :vars '[?x]
           :body [{:clause :data-pattern :src '$ :terms [(v '?x) (c :b) (c 1)]}]}
          {:clause :not-join :src '$2 :vars '[?x]
           :body [{:clause :data-pattern :src '$2 :terms [(v '?x) (c :b) (v '?q)]}]}]
         (where '[:find ?x :in $ $2
                  :where [?x :a ?y] (not [?x :b 1])
                  ($2 not-join [?x] [$2 ?x :b ?q])]))))

(deftest not-var-rules
  (let [e (err #(p/parse-query '[:find ?x :where [?x :a _] (not [?y :a ?z])]))]
    (is (= :db.error/insufficient-binding (:db/error e)))
    (is (= '#{?y ?z} (:variables e))))
  (is (= :db.error/insufficient-binding
         (err-kind #(p/parse-query '[:find ?x :where [?x :a _]
                                     (not-join [?q] [?x :a _])]))))
  (testing "not-join may have body-local vars"
    (is (some? (p/parse-query '[:find ?x :where [?x :a _]
                                (not-join [?x] [?x :a ?z])]))))
  (testing "plain not may not introduce vars through a fn binding"
    (is (= :db.error/insufficient-binding
           (err-kind #(p/parse-query '[:find ?x :where [?x :a ?y]
                                       (not [(inc ?y) ?z] [(> ?z 5)])]))))))

(deftest or-clauses
  (is (= [{:clause :data-pattern :src '$ :terms [(v '?y) (c :q) blank]}
          {:clause :or :src '$ :vars '[?x]
           :branches [[{:clause :data-pattern :src '$ :terms [(v '?x) (c :a) (c 1)]}]
                      [{:clause :data-pattern :src '$ :terms [(v '?x) (c :b) (c 2)]}
                       {:clause :pred :fn 'odd? :args [(v '?x)]}]]}
          {:clause :or-join :src '$2 :vars '[?y ?x] :required '[?y]
           :branches [[{:clause :data-pattern :src '$2 :terms [(v '?x) (c :a) (v '?y)]}]]}
          {:clause :or-join :src '$ :vars '[?x ?z] :required []
           :branches [[{:clause :data-pattern :src '$ :terms [(v '?x) (c :a) (v '?z)]}]]}]
         (where '[:find ?x ?y ?z :in $ $2
                  :where [?y :q _]
                  (or [?x :a 1] (and [?x :b 2] [(odd? ?x)]))
                  ($2 or-join [[?y] ?x] [$2 ?x :a ?y])
                  (or-join [?x ?z] [?x :a ?z])]))))

(deftest or-var-rules
  (invalid (p/parse-query '[:find ?x :where (or [?x :a _] [?y :a _])]))
  (invalid (p/parse-query '[:find ?x :where (or (and [?x :a ?y]) [?x :a 3])]))
  (invalid (p/parse-query '[:find ?x :where [?x :a _] (or)]))
  (testing "or-join required vars must be bound"
    (is (= :db.error/insufficient-binding
           (err-kind #(p/parse-query '[:find ?x :where
                                       (or-join [[?q] ?x] [?x :a ?q])])))))
  (testing "or-join join vars need not appear in every branch for validity"
    (is (some? (p/parse-query '[:find ?x :where [?x :a ?y]
                                (or-join [?y] [?x :a ?y] [?x :b ?y])])))))

(deftest and-clause
  (is (= [{:clause :and
           :clauses [{:clause :data-pattern :src '$ :terms [(v '?x) (c :a) (v '?y)]}
                     {:clause :pred :fn 'pos? :args [(v '?y)]}]}]
         (where '[:find ?x :where (and [?x :a ?y] [(pos? ?y)])])))
  (is (= [{:clause :not :src '$ :vars '[?x ?y]
           :body [{:clause :and
                   :clauses [{:clause :data-pattern :src '$
                              :terms [(v '?x) (c :a) (v '?y)]}]}]}]
         (->> (where '[:find ?x :where [?x :a ?y] (not (and [?x :a ?y]))])
              rest vec))))

;; ------------------------------------------------------ top-level errors

(deftest malformed-queries
  (doseq [q ['[] '[:where [?x :a _]] '[:find ?x] '[:find :where [?x :a _]]
             '[:find x :where [?x :a _]] '[:find ?x ?x :where [?x :a _]]
             '[:find ?x ?y . :where [?x :a ?y]] '[:find [?x ?y ...] :where [?x :a ?y]]
             '[:find _ :where [?x :a _]] '[:find (count 5) :where [?x :a _]]
             '[:find (pull ?x) :where [?x :a _]] '[:find (pull 5 [*]) :where [?x :a _]]
             '[:find ?x :foo 1 :where [?x :a _]] 5 nil "not a query ["
             '{:find [?x] :where [[?x :a _]] :bogus 1}]]
    (let [e (err #(p/parse-query q))]
      (is (= :db.error/invalid-query (:db/error e)) (pr-str q))
      (is (= :cognitect.anomalies/incorrect (:cognitect.anomalies/category e)) (pr-str q)))))

;; ------------------------------------------------------------------ cache

(deftest memoized
  (let [q '[:find ?x :where [?x :cache/test _]]]
    (is (identical? (p/parse-query q) (p/parse-query q)))
    (is (identical? (p/parse-query q)
                    (p/parse-query '[:find ?x :where [?x :cache/test _]])))
    (is (identical? (p/parse-rules '[[(r ?x) [?x :a _]]])
                    (p/parse-rules '[[(r ?x) [?x :a _]]]))))
  (testing "errors are not cached and are rethrown"
    (is (some? (err #(p/parse-query '[:find ?x]))))
    (is (some? (err #(p/parse-query '[:find ?x]))))))

;; ------------------------------------------------------- src propagation

(deftest source-propagation
  (let [pat (fn [src e a val] {:clause :data-pattern :src src :terms [(v e) (c a) (c val)]})]
    (is (= [{:clause :not :src '$2 :vars '[?e] :body [(pat '$2 '?e :b 1)]}]
           (last-clause '[:find ?e :in $ $2 :where [?e :z _] ($2 not [?e :b 1])])))
    (is (= [{:clause :not-join :src '$2 :vars '[?e]
             :body [(pat '$2 '?e :b 1) (pat '$ '?e :c 1)]}]
           (last-clause '[:find ?e :in $ $2 :where [?e :z _] ($2 not-join [?e] [?e :b 1] [$ ?e :c 1])])))
    (is (= [{:clause :or :src '$2 :vars '[?e]
             :branches [[(pat '$2 '?e :a 2)] [(pat '$2 '?e :b 2)]]}]
           (where '[:find ?e :in $ $2 :where ($2 or [?e :a 2] [?e :b 2])])))
    (is (= [{:clause :or-join :src '$2 :vars '[?e] :required []
             :branches [[(pat '$2 '?e :a 2)]
                        [(pat '$ '?e :b 2)]]}]
           (where '[:find ?e :in $ $2 :where ($2 or-join [?e] [?e :a 2] [$ ?e :b 2])])))
    (testing "nested not inside or inherits through the chain"
      (let [[o] (where '[:find ?e :in $ $2
                         :where ($2 or [?e :a 2]
                                       (and [?e :b 1] (not [?e :c 3])))])]
        (is (= '$2 (-> o :branches second second :src)))
        (is (= '$2 (-> o :branches second second :body first :src)))
        (is (= '$2 (-> o :branches second first :src)))))
    (testing "rule expressions inherit too"
      (is (= '$2 (-> (last-clause '[:find ?e :in $ $2 % :where [?e :z _] ($2 not (r ?e))])
                     first :body first :src))))
    (testing "rule bodies default to $"
      (is (= '$ (-> (p/parse-rules '[[(r ?e) [?e :a _]]]) (get 'r) first :body first :src))))))

(deftest src-term-in-pattern-rejected
  (invalid (p/parse-query '[:find ?e :in $ $x :where [?e :a $x]])))

(deftest duplicate-find-elements
  (is (= 2 (count (-> (p/parse-query '[:find (count ?e) (count ?e) :where [?e :a _]])
                      :find :elements))))
  (invalid (p/parse-query '[:find ?e ?e :where [?e :a _]])))
