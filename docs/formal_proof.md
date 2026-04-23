# Formal Proof: Forge is Deterministic and Correct

A formal proof that Forge's BFS decomposition algorithm is **deterministic**, **lossless**, and **terminating**, using first-order logic and structural induction.

---

## 1. Definitions

### 1.1 JSON Domain — Finite Ordered Trees

We model JSON documents as finite ordered labeled trees.

**Definition 1** *(JSON Value).* The domain of JSON values $\mathcal{J}$ is inductively defined:

$$
\mathcal{J} \;::=\; \text{Scalar}(v) \;\mid\; \text{Object}(\langle k_1{:}\,j_1,\;\ldots,\;k_n{:}\,j_n \rangle) \;\mid\; \text{Array}([j_1,\;\ldots,\;j_m])
$$

where $v \in \mathbb{S}$ (strings, numbers, booleans, null), each $k_i \in \mathbb{S}$ is a field name, and each $j_i \in \mathcal{J}$.

**Definition 2** *(JSON Tree).* A JSON value $J \in \mathcal{J}$ defines a finite rooted tree $T(J) = (V, E, \lambda, \tau)$:

| Symbol | Meaning |
|:---|:---|
| $V$ | Finite set of nodes |
| $E \subseteq V \times V$ | Edge relation (parent → child) |
| $\lambda : V \to \mathbb{S}$ | Field-name labeling function |
| $\tau : V \to \lbrace\texttt{SCALAR},\,\texttt{STRUCT},\,\texttt{ARRAY}\rbrace$ | Type assignment function |

**Axiom 1** *(Finiteness).*

$$\forall\, J \in \mathcal{J} :\; \lvert V(T(J))\rvert \;<\; \infty$$

**Axiom 2** *(Acyclicity).*

$$\forall\, J \in \mathcal{J} :\; T(J) \text{ is acyclic (a tree)}$$

**Definition 3** *(Depth).*

$$d(\text{root}) = 0 \qquad d(v) = d\bigl(\text{parent}(v)\bigr) + 1$$

**Definition 4** *(Maximum depth).*

$$D(J) = \max\bigl\lbrace\, d(v) : v \in V(T(J)) \,\bigr\rbrace$$

By Axiom 1, $D(J) < \infty$ for all $J$.

**Axiom 3** *(Type consistency).* For each field $k$ at a given path in the tree, all rows agree on the structural type of $k$:

$$\forall\; r_1, r_2 \in J : \quad \tau(r_1.k) = \tau(r_2.k)$$

> **Note:** When this axiom is violated (a field is scalar in some rows and object/array in others), Forge's type-promotion rule ($\textsf{ARRAY} > \textsf{STRUCT} > \textsf{SCALAR}$) is still deterministic (Theorem 1 holds), but scalar values in rows where the field is not of the promoted type are **coerced to NULL**. Theorem 2 (losslessness) requires this axiom.

**Definition 5a** *(Structural isomorphism).* Two JSON values $J_1 \cong J_2$ iff there exists a bijection $\phi : V(T(J_1)) \to V(T(J_2))$ such that:

1. $\phi$ preserves the root: $\phi(\text{root}_1) = \text{root}_2$
2. $\phi$ preserves edges: $(u, v) \in E_1 \iff (\phi(u), \phi(v)) \in E_2$
3. $\phi$ preserves labels: $\lambda_1(v) = \lambda_2(\phi(v))$ for all $v$
4. $\phi$ preserves types: $\tau_1(v) = \tau_2(\phi(v))$ for all $v$
5. $\phi$ preserves scalar values: if $\tau(v) = \textsf{SCALAR}$, then $\text{val}_1(v) = \text{val}_2(\phi(v))$
6. $\phi$ preserves child ordering: for all $v$, the sequence of children of $v$ in $T_1$ maps to the same-ordered sequence in $T_2$

---

### 1.2 The Decomposition Function

**Definition 5** *(Model).* A relational model $M = (\textit{name},\;\textit{cols},\;R)$ consists of:

| Component | Description |
|:---|:---|
| $\textit{name} \in \mathbb{S}$ | Model identifier (e.g., `frg__root__line1`) |
| $\textit{cols} = \lbrace c_1, \ldots, c_k \rbrace$ | Column names (always includes `ingestion_hash`, `idx`) |
| $R$ | Finite set of tuples over *cols* |

**Definition 6** *(Decompose).* The decomposition function maps a JSON value to a finite set of models:

$$\texttt{decompose} : \mathcal{J} \;\to\; \mathcal{P}(\mathcal{M})$$

Defined by the following BFS algorithm:

```
DECOMPOSE(J):
    Q ← queue containing (root(T(J)), "frg", 0)
    models ← ∅

    WHILE Q ≠ ∅:
        batch ← drain(Q)                              // all items at current level
        FOR EACH (node, path, depth) ∈ batch:
            keys    ← GET_KEYS(node)                   // sorted lexicographically
            types   ← GET_TYPES(node, keys)            // inspects all rows
            scalars ← { k ∈ keys : types(k) = SCALAR }
            nested  ← { k ∈ keys : types(k) ∈ {STRUCT, ARRAY} }

            M ← CREATE_MODEL(path, scalars, depth)
            models ← models ∪ {M}

            FOR EACH k ∈ nested (in sorted order):
                child_path ← path ++ "__" ++ ABBREVIATE(k)
                Q.enqueue( (child_node(node, k), child_path, depth + 1) )

    RETURN models
```

**Definition 7** *(Index function).* For each row $r$ in model $M$ at depth $d$, the index is a positional path from root to the current element:

$$\text{idx}(r) \;=\; i_0 \cdot i_1 \cdot \;\cdots\; \cdot i_d$$

where $i_j$ is the positional index at nesting level $j$, and $\cdot$ represents the underscore delimiter (e.g., `3_7_2`). Each $i_j \in \mathbb{N}^+$ is assigned by `ROW_NUMBER() OVER (ORDER BY insertion)` at level $j$.

**Axiom 4** *(UNNEST semantics).* The `UNNEST` operator on an array of length $m$ produces exactly $m$ rows, one per element, in insertion order. Formally, for array $A = [a_1, \ldots, a_m]$:

$$\text{UNNEST}(A) = \lbrace (a_j, j) : j \in [1, m] \rbrace \quad \text{and} \quad \lvert\text{UNNEST}(A)\rvert = m$$

**Axiom 5** *(Column storage fidelity).* For supported types $T \in \lbrace\textsf{STRING}, \textsf{INT64}, \textsf{FLOAT64}, \textsf{BOOL}, \textsf{TIMESTAMP}\rbrace$, storing value $v$ of type $T$ in a BigQuery column and reading it back yields $v$.

**Lemma 1** *(idx injectivity).* Within any model $M$ at depth $d$, the composite key $(h, \text{idx})$ is injective:

$$\forall\; r_1, r_2 \in M : \quad (h(r_1), \text{idx}(r_1)) = (h(r_2), \text{idx}(r_2)) \implies r_1 = r_2$$

*Proof.* The idx is constructed as $i_0 \cdot i_1 \cdot \ldots \cdot i_d$ where each $i_j$ is a `ROW_NUMBER` — a sequential integer unique within its parent partition. Two rows in $M$ share the same hash $h$ only if they originate from the same source document. Within that document, the tuple $(i_0, i_1, \ldots, i_d)$ is unique because at each level $j$, `ROW_NUMBER` assigns distinct integers to siblings. Since the delimiter $\cdot$ separates integer components and each $i_j \geq 1$, the string representation is injective (no ambiguity: `SPLIT` recovers the original tuple). $\square$

> **Example:** `idx = "3_7_2"` means the 3rd root record → 7th child element → 2nd grandchild element.

**Definition 8** *(Join predicate).* For parent model $M_p$ at depth $d_p$ and child model $M_c$ at depth $d_c = d_p + 1$:

$$\text{Join}(r_p,\, r_c) \;\iff\; r_p.\text{hash} = r_c.\text{hash} \;\wedge\; \bigwedge_{j=0}^{d_p} \text{Split}(r_p.\text{idx},\, j) = \text{Split}(r_c.\text{idx},\, j)$$

---

### 1.3 The Rollup Function

**Definition 9** *(Rollup).* The rollup function reconstructs JSON from the model set:

$$\texttt{rollup} : \mathcal{P}(\mathcal{M}) \;\to\; \mathcal{J}$$

Procedure (bottom-up):

1. For each **leaf** model: wrap scalar columns into a `STRUCT`
2. For each **parent** model: `LEFT JOIN` children using the join predicate
3. For each **array** child: `ARRAY_AGG(child_struct IGNORE NULLS)`
4. For each **struct** child: `ANY_VALUE(child_struct)`
5. Recurse bottom-up until the root model is reached

---

## 2. Theorem 1: Determinism

> **Theorem.** $\;\forall\, J \in \mathcal{J} :\;$ `decompose(J)` produces a unique, fixed set of models. Given identical input $J$, every execution yields the same output.

### Proof

By structural induction on $D(J)$.

---

**Base case:** $D(J) = 0$ *(scalar fields only)*

The input has no nested fields.
- `GET_KEYS(J)` returns a fixed set $K$, determined entirely by the data in $J$.
- `GET_TYPES(J, K)` maps each $k \in K$ to `SCALAR`, determined by the values in $J$.
- One model is produced with columns $K \cup \lbrace h, \; \text{idx}\rbrace$ where $h$ = `ingestion_hash`.

This is a pure function of $J$. $\blacksquare$

---

**Inductive step.** Assume determinism holds for all $J'$ with $D(J') \leq n$. Consider $J$ with $D(J) = n + 1$.

**Step 1 — Key discovery is deterministic:**

$$\forall\; J, J' \in \mathcal{J} : \quad J = J' \implies \text{GetKeys}(J) = \text{GetKeys}(J')$$

`GET_KEYS` queries all rows (not a sample) and returns the union of all keys, sorted lexicographically. The sort establishes a canonical ordering.

**Step 2 — Type inference is deterministic:**

$$\forall\; k \in K : \quad \tau(k) = \begin{cases} \textsf{ARRAY} & \text{if } \exists\, r \in J : \text{isArray}(r.k) \\\\ \textsf{STRUCT} & \text{if } \exists\, r \in J : \text{isObject}(r.k) \wedge \neg\text{isArray}(r.k) \\\\ \textsf{SCALAR} & \text{otherwise} \end{cases}$$

This is a pure function of $J$.

**Step 3 — Partition is deterministic:** The split into *scalars* and *nested* is determined by $\tau$, which is determined by $J$.

**Step 4 — Naming is deterministic:**

$$\text{name}(\textit{path},\;\textit{field}) = \textit{path} \;\|\; \text{Abbrev}(\textit{field}) \;\|\; \textit{counter}$$

where $\|$ denotes string concatenation with the `__` delimiter.

- `ABBREV` takes the first 4 characters — deterministic.
- `counter` resolves collisions in sorted order — deterministic.
- `path` encodes full ancestry — unique per branch.

**Step 5 — BFS order is deterministic:** The queue processes all nodes at depth $d$ before any at depth $d + 1$. Within a level, children are enqueued in sorted key order — canonical traversal.

**Step 6 — Inductive closure:** Each nested child at depth $d + 1$ has $D(\text{child}) \leq n$. By the inductive hypothesis, its decomposition is deterministic.

**Conclusion:** `decompose(J)` is deterministic for $D(J) = n + 1$. $\blacksquare$

---

**Corollary 1** *(Unique naming).* All models in `decompose(J)` have distinct names because:

1. At each parent, child field names are distinct (JSON object key uniqueness).
2. `ABBREV + counter` resolves collisions deterministically within each parent.
3. Path prefixes encode full ancestry, preventing cross-branch collisions.

---

## 3. Theorem 2: Correctness (Losslessness)

> **Theorem.** For all type-consistent $J \in \mathcal{J}$ (satisfying Axiom 3):
>
> $$\text{rollup}\bigl(\text{decompose}(J)\bigr) \;\cong\; J$$
>
> where $\cong$ is structural isomorphism (Definition 5a).

### Proof

By induction on $D(J)$.

---

**Base case:** $D(J) = 0$

$J$ contains only scalar fields. `decompose` produces one model $M_{\text{root}}$ with one column per scalar field plus metadata. `rollup` wraps these back into a `STRUCT`, excluding metadata:

$$\text{rollup}(\lbrace M_{\text{root}} \rbrace) \;\cong\; J \qquad\blacksquare$$

---

**Inductive step.** Assume correctness for all subtrees with depth $\leq n$. Consider $J$ with $D(J) = n + 1$.

Let $J$ have scalar fields $S = \lbrace s_1, \ldots, s_a \rbrace$ and nested fields $N = \lbrace n_1, \ldots, n_b \rbrace$.

**Decomposition produces:**

$$\lbrace M_{\text{root}} \rbrace \;\cup\; \bigcup_{i=1}^{b} \texttt{decompose}(J.n_i)$$

**Rollup reconstructs:**

$$\text{rollup}\bigl(\text{decompose}(J)\bigr) = \text{Struct}\!\left(s_1, \ldots, s_a,\; \underbrace{\text{ArrayAgg}\bigl(\text{rollup}(\text{decompose}(J.n_i))\bigr)}_{\text{for each } n_i \in N}\right)$$

We verify three sub-properties:

---

**3a. Scalar preservation.**
Each $s_i$ is stored as a typed column in $M_{\text{root}}$. By Axiom 3 (type consistency), $s_i$ is scalar in all rows, so no type coercion occurs. By Axiom 5 (column storage fidelity), storing and reading back preserves the value:

$$\forall\; s_i \in S : \quad \text{rollup}(M_{\text{root}}).s_i = J.s_i \qquad \checkmark$$

---

**3b. STRUCT field preservation.**
For a `STRUCT` child $n_i$, we must show exactly one child row matches each parent.

*Proof of unit cardinality.* When $\tau(n_i) = \textsf{STRUCT}$ (by Axiom 3, in all rows), the decomposition does not apply `UNNEST` — it extracts the struct fields directly. Each parent row $r_p$ produces exactly one child row $r_c$ with $\text{idx}(r_c) = \text{idx}(r_p)$ (same depth, no additional position appended). Therefore:

$$\bigl\lvert\lbrace r_c \in M_{n_i} : \text{Join}(r_p, r_c) \rbrace\bigr\rvert = 1$$

`ANY_VALUE` returns this unique row. By the inductive hypothesis, the child's internal structure is correctly reconstructed. $\checkmark$

---

**3c. ARRAY field preservation.**
For an `ARRAY` child $n_i$, the child model contains one row per array element:

$$\text{idx}(r_c) \;=\; \text{idx}(r_p) \cdot j \qquad \text{where } j \text{ is the 1-indexed array position}$$

We verify three properties:

**(i) Completeness** — every element is captured:

$$\forall\; j \in [1,\;\lvert J.n_i \rvert] : \; \exists\; r_c \in M_{n_i} : \; \text{Split}(r_c.\text{idx},\; d_c) = j$$

*Proof.* By Axiom 4, `UNNEST` on array $J.n_i$ of length $m$ produces exactly $m$ pairs $(a_j, j)$. The decomposition assigns $\text{idx}(r_c) = \text{idx}(r_p) \cdot j$ for each. Since $j$ ranges over $[1, m]$, every element has a corresponding row. $\square$

**(ii) No spurious elements** — no extra rows introduced:

$$\lvert M_{n_i} \rvert \;=\; \sum_{r_p \,\in\, M_{\text{parent}}} \lvert J_{r_p}.n_i \rvert$$

*Proof.* By Axiom 4, $\lvert\text{UNNEST}(A)\rvert = \lvert A \rvert$. The decomposition applies UNNEST independently per parent row. Summing: total child rows = sum of array lengths across parents. No rows are added by any other operation. $\square$

**(iii) Order preservation** — array order is maintained:

$$\text{ArrayAgg}(M_{n_i},\; \text{order by idx})[j] \cong J.n_i[j] \qquad \forall\; j$$

*Proof.* `ROW_NUMBER` assigns position $j$ to the $j$-th element of `UNNEST` output (Axiom 4 guarantees insertion order). `ARRAY_AGG ... ORDER BY idx` reconstructs elements in ascending idx order. Since idx encodes position $j$ in its final component, ordering by idx recovers the original array order. By the inductive hypothesis, each element's value is correctly reconstructed. $\square$

---

Combining 3a + 3b + 3c:

$$\text{rollup}\bigl(\text{decompose}(J)\bigr) \cong J \qquad \text{for } D(J) = n + 1. \qquad \blacksquare$$

---

## 4. Theorem 3: Termination

> **Theorem.** $\;\forall\, J \in \mathcal{J} :\;$ `decompose(J)` terminates in finite time.

### Proof

Define the measure function:

$$\mu(Q) \;=\; \sum_{(\textit{node},\,\textit{path},\,\textit{depth})\,\in\, Q} \bigl\lvert V\bigl(\texttt{subtree}(\textit{node})\bigr)\bigr\rvert$$

This counts the total number of tree nodes reachable from all queued items.

At each BFS iteration:

1. A batch of nodes is dequeued (removes their subtree sizes from $\mu$).
2. Only immediate nested children are enqueued.
3. Each child is a **strict** subtree: $\lvert V(\texttt{subtree}(\text{child}))\rvert < \lvert V(\texttt{subtree}(\text{parent}))\rvert$.

Therefore:

$$\forall\, t :\; \mu(Q_{t+1}) \;<\; \mu(Q_t) \quad\text{when any dequeued node has nested children}$$

$$\mu(Q_{t+1}) = 0 \quad\text{when no dequeued node has nested children}$$

Since $\mu(Q_0) = \lvert V(T(J))\rvert < \infty$ (Axiom 1) and $\mu$ is strictly decreasing on $\mathbb{N}$, which is well-ordered, the algorithm terminates in at most $D(J) + 1$ iterations.

**Complexity bound:**

$$\bigl\lvert\texttt{decompose}(J)\bigr\rvert \;\leq\; \bigl\lvert V(T(J))\bigr\rvert \;-\; \bigl\lvert\text{leaves}(T(J))\bigr\rvert \;+\; 1$$

The number of models is bounded by the number of internal (non-leaf) nodes plus the root. $\blacksquare$

---

## 5. Corollaries

**Corollary 2** *(Join-key invertibility).* Follows directly from Lemma 1 (idx injectivity, proved above).

---

**Corollary 3** *(Depth-aware join correctness).* For models $M_p$ at depth $d$ and $M_c$ at depth $d + 1$:

$$\forall\; r_c \in M_c : \; \exists!\; r_p \in M_p : \; \text{Join}(r_p,\, r_c)$$

*Proof.* Let $r_c$ have $\text{idx}(r_c) = i_0 \cdot i_1 \cdot \ldots \cdot i_d \cdot i_{d+1}$. The join predicate matches on the first $d+1$ components: $(h, i_0, \ldots, i_d)$. This is exactly the $(h, \text{idx})$ key of some row in $M_p$ (since $r_c$ was produced by unnesting a value within that parent row). By Lemma 1, this key identifies a unique $r_p \in M_p$. Existence holds because every child row was produced from some parent. Uniqueness holds by Lemma 1. $\square$

---

**Corollary 4** *(Idempotence).*

$$\text{decompose}(J_1) = \text{decompose}(J_2) \quad \text{whenever} \quad J_1 = J_2$$

Follows directly from Theorem 1.

---

## 6. Assumptions and Limitations

The proof relies on the following assumptions:

| # | Assumption | Why It Holds in Forge |
|:---|:---|:---|
| 1 | `GET_KEYS` is total — queries all rows, not a sample | Forge runs `GET_KEYS` against the full table |
| 2 | Type consistency (Axiom 3) — a field has the same structural type in all rows | When violated, Forge promotes to the most complex type; scalar values in non-matching rows are coerced to NULL (losslessness does not hold for those values) |
| 3 | Finite nesting — documents have finite depth | Guaranteed by JSON specification and any real data source |
| 4 | Lossless column storage — warehouse preserves value precision | True for `STRING`, `INT64`, `FLOAT64`, `BOOL`, `TIMESTAMP` in BigQuery |
| 5 | `ARRAY_AGG` ordering — rollup preserves array order | Forge uses `idx`-based ordering, deterministic by construction |

**The proof does NOT cover:**

- **Floating-point edge cases** — JSON numbers with >15 significant digits may lose precision in `FLOAT64` storage.
- **Schema evolution** — `append_new_columns` across multiple runs adds columns but never removes them; the round-trip isomorphism applies to a single run.
- **Thread safety** — the `ThreadPoolExecutor` parallelism in `unnesting.py` operates *within* a BFS level (not across levels), so it affects execution speed but not the model set.
