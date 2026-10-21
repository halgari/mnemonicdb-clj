# Query AST

Contract between `mnemonic.query.parser` (Task 3) and the query engine
(Tasks 8 and 10). The AST is plain Clojure data (maps, vectors, symbols,
keywords); constants are kept exactly as read.

```clojure
(mnemonic.query.parser/parse-query q)  ;; => query AST
(mnemonic.query.parser/parse-rules rs) ;; => rule map
```

Both functions are pure and cached by their input form (value equality, bounded
at 1024 entries; the cache is cleared when full). Errors are never cached.
Returned structures must be treated as immutable (they are shared).

`parse-query` accepts: the vector form `[:find ... :in ... :where ...]`, a list
form `(:find ...)`, the map form `{:find [...] :in [...] :where [[...]]}`, any
of those wrapped in `(quote ...)`, and a string containing EDN of any of those.
Sections may appear in any order in the vector/list form; a section keyword
repeated in the vector form concatenates its contents.

## Conventions

- **Variable**: unqualified symbol starting with `?`.
- **Blank**: the symbol `_`.
- **Source var (src-var)**: unqualified symbol starting with `$` (`$`, `$2`,
  `$foo`). `$` is the default source.
- **Rules var**: the symbol `%`, only meaningful in `:in`.
- Everything else (keywords, numbers, strings, vectors such as lookup refs
  `[:user/id 1]`, plain symbols without `?`/`$` prefix) is a **constant**.
- `:src` fields hold a bare symbol (`'$`, `'$2`), always present, default `'$`.
- Lists of variables (`:with`, `:vars`, `:required`, `:head`) are vectors of
  bare symbols.

### Terms

A *term* is a position that can hold a variable, blank, source or constant. It
is a map with a `:type` tag:

| Form | Meaning |
|---|---|
| `{:type :var :sym ?x}` | variable |
| `{:type :blank}` | `_` |
| `{:type :src :sym $2}` | source var (only appears as a function/rule argument; the leading source of a pattern/clause is lifted into `:src`) |
| `{:type :const :val v}` | constant, `v` exactly as read |

### Bindings

Used by `:in` and by function-expression result bindings. A *binding* is a map
with a `:bind` tag:

| Form | Syntax |
|---|---|
| `{:bind :scalar :sym ?x}` | `?x` |
| `{:bind :blank}` | `_` |
| `{:bind :tuple :elems [b ...]}` | `[?a _ ?b]` (`b` is `:scalar` or `:blank`) |
| `{:bind :coll :elem b}` | `[?x ...]` (`b` is `:scalar` or `:blank`) |
| `{:bind :rel :elems [b ...]}` | `[[?a ?b]]` (`b` is `:scalar` or `:blank`) |
| `{:bind :src :sym $foo}` | `$`, `$foo` (`:in` only) |
| `{:bind :rules}` | `%` (`:in` only) |

## Query

```clojure
{:find       <find-spec>
 :return-map nil | {:type :keys|:syms|:strs, :names [sym-or-kw-or-str ...]}
 :with       [?var ...]                  ; possibly empty
 :in         [<binding> ...]             ; default [{:bind :src :sym $}]
 :where      [<clause> ...]}             ; possibly empty (written order kept)
```

### `:find`

```clojure
{:spec     :find-rel | :find-coll | :find-scalar | :find-tuple
 :elements [<find-elem> ...]}
```

| `:spec` | Syntax | Elements |
|---|---|---|
| `:find-rel` | `:find ?a ?b` | 1 or more |
| `:find-coll` | `:find [?a ...]` | exactly 1 |
| `:find-scalar` | `:find ?a .` | exactly 1 |
| `:find-tuple` | `:find [?a ?b]` | 1 or more |

Find elements:

```clojure
{:type :var :sym ?x}
{:type :agg :agg count :args [<term> ...]}     ; (count ?x), (min 3 ?y), (my.ns/f ?x)
{:type :pull :src $ :var ?x :pattern <pat>}    ; (pull ?x [...]) / (pull $2 ?x ?pat)
```

- `:agg` is the aggregate symbol as written (may be qualified for custom
  aggregates). `:args` are terms (`:var` or `:const`); the last is always a
  `:var`. So `(min 3 ?y)` is `[{:type :const :val 3} {:type :var :sym ?y}]`.
- `:pattern` of a pull is `{:type :const :val <raw pattern>}` (the raw vector
  or map, unparsed -- Task 7 owns the pull grammar) or `{:type :var :sym ?pat}`
  when the pattern comes from an input.
- The same element cannot appear twice in `:find`.

### `:return-map`

From `:keys`, `:syms` or `:strs`. `:names` are the names exactly as written.
Only allowed with `:find-rel` or `:find-tuple`, and `(count :names)` must equal
the number of find elements. `nil` when absent.

### `:in`

A vector of bindings (see above). When the query has no `:in`, it is
`[{:bind :src :sym $}]`. The engine matches the arguments to `d/q` against this
vector positionally. `{:bind :rules}` marks where the rule set argument goes.

### `:where` clauses

Every clause is a map with a `:clause` tag, listed here in written order of
the source; the parser does **not** reorder.

#### Data pattern -- `[?e :attr ?v]`, `[$2 ?e _ "x"]`

```clojure
{:clause :data-pattern :src $ :terms [<term> ...]}
```
`:terms` holds 1 to 5 terms in positional order e, a, v, tx, added. Missing
trailing positions are absent from the vector (they are wildcards). Terms are
`:var`, `:blank` or `:const` (an attribute such as `:person/name`, an entity
id, a lookup ref, a value...). Attribute existence is not validated.

#### Predicate -- `[(> ?x 10)]`

```clojure
{:clause :pred :fn > :args [<term> ...]}
```
`:fn` is the function symbol as written (resolution is the engine's job).
`:args` are `:var`, `:const` or `:src` terms (`_` is rejected).

#### Function expression -- `[(inc ?x) ?y]`, `[(ground [1 2]) [?v ...]]`

```clojure
{:clause :fn :fn inc :args [<term> ...] :binding <binding>}
```
`:binding` is a binding from the *Bindings* table (`:scalar`, `:blank`,
`:tuple`, `:coll`, `:rel`). Built-ins such as `get-else $ ?e :a "d"`, `ground`,
`missing?`, `tuple`, `untuple` all parse to these two shapes; an `:src` term
appears as an argument where the source is written (`[(get-else $ ?e :a 0) ?v]`
has `:args [{:type :src :sym $} ...]`, and `[(missing? $ ?e :a)]` is a `:pred`).

#### Rule expression -- `(ancestor ?x ?y)`, `($2 ancestor ?x _)`

```clojure
{:clause :rule :src $ :name ancestor :args [<term> ...]}
```
`:args` are any terms (`:var`, `:blank`, `:const`, `:src`).

#### `not` / `not-join`

```clojure
{:clause :not      :src $ :vars [?x ...] :body [<clause> ...]}   ; (not clause+)
{:clause :not-join :src $ :vars [?x ...] :body [<clause> ...]}   ; (not-join [?x ...] clause+)
```
`:vars` are the join variables: for `not-join`, as written; for `not`, every
variable used anywhere in the body, in order of first appearance (so `not` ==
`not-join` over all its variables). Join variables must be bound by the
enclosing scope. The leading source applies to the body (`($2 not ...)`).
Body data patterns carry their own `:src` (defaulting to `$`; the engine
should treat the clause `:src` as the default for body patterns without an
explicit one if that distinction matters to it -- the parser does not rewrite
body `:src`).

#### `or` / `or-join`

```clojure
{:clause :or      :src $ :vars [?x ...] :branches [[<clause> ...] ...]}
{:clause :or-join :src $ :vars [?y ?x ...] :required [?y ...] :branches [[<clause> ...] ...]}
```
- Each branch is a vector of clauses (an `(and c1 c2)` branch is flattened to
  `[c1 c2]`; a bare clause is a one-element branch). At least one branch.
- `or`: `:vars` is the set of vars used (identical across all branches, in order
  of first appearance). `or-join`: `:vars` are the join vars as written
  (with the `[[?req ...] ?other ...]` form, `:required` first, then others);
  `:required` is the subset in the leading rule-vars vector (empty if absent).
- Required variables must be bound before the clause.

#### `and`

```clojure
{:clause :and :clauses [<clause> ...]}
```
`and` inside `or`/`or-join` is flattened into the branch; elsewhere (top
level, inside `not`) it is preserved as above and means the conjunction.

### Binding validation done by the parser

The parser does the analysis that needs no database. Datomic defers predicates
and function expressions until their arguments are bound, so **written order
is not an error**: the check is "can every clause eventually be satisfied".
Variables bound by: inputs; data patterns; rule expressions (all their var
args); function-expression bindings (once their args are bound); `or`/`or-join`
join vars. Predicates, function args, `not`/`not-join` join vars and `or-join`
`:required` vars must be bound by something else in scope. Rule bodies
themselves are not binding-checked (their head variables are bound by callers).

## Rules

```clojure
(parse-rules '[[(ancestor ?x ?y) [?x :parent ?y]]
               [(ancestor ?x ?y) [?x :parent ?z] (ancestor ?z ?y)]
               [(sib [?a] ?b ?c) [(not= ?b ?c)]]])
;; =>
{ancestor [{:head [?x ?y] :required [] :body [<clause> ...]}
           {:head [?x ?y] :required [] :body [<clause> ...]}]
 sib      [{:head [?a ?b ?c] :required [?a] :body [<clause> ...]}]}
```

- Result: map from rule-name symbol to a vector of definitions in written
  order (several definitions of the same name are alternative branches, OR).
- `:head`: all head variables in positional order (required ones first);
  `:required`: the subset in the leading `[?v ...]` vector of the head (must be
  bound by the caller), `[]` if none.
- `:body`: parsed clauses, same shapes as `:where`.
- All definitions of a name must have the same head arity.

## Errors

`ex-info` with data `{:db/error <kw>, :cognitect.anomalies/category
:cognitect.anomalies/incorrect, :cognitect.anomalies/message <msg>, ...}`.

| `:db/error` | When | Extra data |
|---|---|---|
| `:db.error/unbound-query-variables` | `:find`/`:with` (incl. aggregate args and pull var/pattern var) reference vars not bound by `:in` or `:where` (Datomic keyword and message) | `:variables` (set) |
| `:db.error/insufficient-binding` | predicate/function args, `not`/`not-join` vars or `or-join` required vars cannot be bound (Datomic keyword) | `:variables` (set), `:clause` |
| `:db.error/invalid-query` | everything else malformed. Datomic throws plain exceptions without `:db/error` for these (missing `:find`, bad `:find` element, `or` branches with differing var sets, bad rule head, ...), so the keyword is our choice | -- |

Also reported as `:db.error/invalid-query` (Datomic would either ignore or
crash): unknown query sections/keys, duplicate `:find` elements, a `:with`
variable that also appears in `:find`, `:keys`/`:syms`/`:strs` mismatches,
`_` as a function argument, empty `not`/`or`/`and`.

Checks that need the database or call-time arguments (`:db.error/too-few-inputs`,
unknown attributes, function resolution) belong to the engine.
