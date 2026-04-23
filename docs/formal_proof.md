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

$$\texttt{idx}(r) \;=\; i_0\_i_1\_\cdots\_i_d$$

where $i_j$ is the positional index at nesting level $j$.

> **Example:** `idx = "3_7_2"` means the 3rd root record → 7th child element → 2nd grandchild element.

**Definition 8** *(Join predicate).* For parent model $M_p$ at depth $d_p$ and child model $M_c$ at depth $d_c = d_p + 1$:

$$\texttt{JOIN}(r_p, r_c) \;\iff\; r_p.\texttt{hash} = r_c.\texttt{hash} \;\;\wedge\;\; \bigwedge_{j=0}^{d_p} \texttt{SPLIT}(r_p.\texttt{idx},\, j) \;=\; \texttt{SPLIT}(r_c.\texttt{idx},\, j)$$

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
- One model is produced: $\textit{cols} = K \cup \lbrace\texttt{ingestion\_hash},\,\texttt{idx}\rbrace$.

This is a pure function of $J$. $\square$

---

**Inductive step.** Assume determinism holds for all $J'$ with $D(J') \leq n$. Consider $J$ with $D(J) = n + 1$.

**Step 1 — Key discovery is deterministic:**

$$\forall\, J,\, J' \in \mathcal{J} :\; J = J' \;\implies\; \texttt{GET\_KEYS}(J) = \texttt{GET\_KEYS}(J')$$

`GET_KEYS` queries all rows (not a sample) and returns the union of all keys, sorted lexicographically. The sort establishes a canonical ordering.

**Step 2 — Type inference is deterministic:**

$$\forall\, k \in K :\quad \tau(k) = \begin{cases} \texttt{ARRAY} & \text{if } \exists\, r \in J : \texttt{is\_array}(r.k) \\[4pt] \texttt{STRUCT} & \text{if } \exists\, r \in J : \texttt{is\_object}(r.k) \;\wedge\; \neg\texttt{is\_array}(r.k) \\[4pt] \texttt{SCALAR} & \text{otherwise} \end{cases}$$

This is a pure function of $J$.

**Step 3 — Partition is deterministic:** The split into *scalars* and *nested* is determined by $\tau$, which is determined by $J$.

**Step 4 — Naming is deterministic:**

$$\textit{name}(\textit{path},\,\textit{field}) \;=\; \textit{path}\;\texttt{\_\_}\;\texttt{ABBREV}(\textit{field})\;\texttt{+}\;\textit{counter}$$

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

> **Theorem.** $\;\forall\, J \in \mathcal{J}:$
>
> $$\texttt{rollup}\bigl(\texttt{decompose}(J)\bigr) \;\cong\; J$$
>
> where $\cong$ denotes structural isomorphism (same tree shape, same values, same field names, same array ordering).

### Proof

By induction on $D(J)$.

---

**Base case:** $D(J) = 0$

$J$ contains only scalar fields. `decompose` produces one model $M_{\text{root}}$ with one column per scalar field plus metadata. `rollup` wraps these back into a `STRUCT`, excluding metadata:

$$\texttt{rollup}(\lbrace M_{\text{root}} \rbrace) \;\cong\; J \qquad\square$$

---

**Inductive step.** Assume correctness for all subtrees with depth $\leq n$. Consider $J$ with $D(J) = n + 1$.

Let $J$ have scalar fields $S = \lbrace s_1, \ldots, s_a \rbrace$ and nested fields $N = \lbrace n_1, \ldots, n_b \rbrace$.

**Decomposition produces:**

$$\lbrace M_{\text{root}} \rbrace \;\cup\; \bigcup_{i=1}^{b} \texttt{decompose}(J.n_i)$$

**Rollup reconstructs:**

$$\texttt{rollup}\bigl(\texttt{decompose}(J)\bigr) \;=\; \texttt{STRUCT}\Bigl(\, s_1,\;\ldots,\;s_a,\;\underbrace{\texttt{ARRAY\_AGG}\bigl(\texttt{rollup}(\texttt{decompose}(J.n_i))\bigr)}_{\text{for each } n_i \in N}\,\Bigr)$$

We verify three sub-properties:

---

**3a. Scalar preservation.**
Each $s_i$ is stored as a typed column in $M_{\text{root}}$. BigQuery preserves full precision for `STRING`, `INT64`, `FLOAT64`, `BOOL`, `TIMESTAMP`:

$$\forall\, s_i \in S :\; \texttt{rollup}(M_{\text{root}}).s_i \;=\; J.s_i \qquad\checkmark$$

---

**3b. STRUCT field preservation.**
For a `STRUCT` child $n_i$, the child model contains exactly one row per parent row with matching `(hash, idx)`:

$$\bigl\lvert\lbrace\, r_c \in M_{n_i} : \texttt{JOIN}(r_p, r_c) \,\rbrace\bigr\rvert \;=\; 1 \qquad\text{for STRUCT fields}$$

`ANY_VALUE` returns that single row. By the inductive hypothesis, the child's internal structure is correctly reconstructed. $\checkmark$

---

**3c. ARRAY field preservation.**
For an `ARRAY` child $n_i$, the child model contains one row per array element:

$$\texttt{idx}(r_c) \;=\; \texttt{idx}(r_p)\;\texttt{\_}\;j \qquad\text{where } j \text{ is the 1-indexed array position}$$

We verify three properties:

**(i) Completeness** — every element is captured:

$$\forall\, j \in [1,\;\lvert J.n_i \rvert] :\; \exists\, r_c \in M_{n_i} :\; \texttt{SPLIT}(r_c.\texttt{idx},\; d_c) = j$$

Holds because `UNNEST`/`FLATTEN` generates exactly one row per element, with `ROW_NUMBER` assigning sequential positions.

**(ii) No spurious elements** — no extra rows introduced:

$$\lvert M_{n_i} \rvert \;=\; \sum_{r_p \,\in\, M_{\text{parent}}} \lvert J_{r_p}.n_i \rvert$$

Holds because `UNNEST` produces exactly one row per element.

**(iii) Order preservation** — array order is maintained:

$$\texttt{ARRAY\_AGG}(M_{n_i} \;\texttt{ORDER BY idx})[j] \;\cong\; J.n_i[j] \qquad \forall\, j$$

Holds because `ROW_NUMBER` assigns positions in insertion order, and `ARRAY_AGG` reconstructs in `idx` order.

---

Combining 3a + 3b + 3c:

$$\texttt{rollup}\bigl(\texttt{decompose}(J)\bigr) \;\cong\; J \qquad\text{for } D(J) = n + 1. \qquad\blacksquare$$

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

**Corollary 2** *(Join-key invertibility).* The index function is injective within a model:

$$\forall\, M \in \texttt{decompose}(J),\;\;\forall\, r_1, r_2 \in M :\quad (\texttt{hash}(r_1),\;\texttt{idx}(r_1)) = (\texttt{hash}(r_2),\;\texttt{idx}(r_2)) \;\implies\; r_1 = r_2$$

Follows from the `(ingestion_hash, idx)` unique-key constraint enforced by dbt incremental materialization.

---

**Corollary 3** *(Depth-aware join correctness).* For models $M_p$ at depth $d$ and $M_c$ at depth $d + 1$, the join predicate using `SPLIT(idx, '_')[OFFSET(0..d)]` correctly establishes the parent-child relationship:

$$\forall\, r_c \in M_c :\; \exists!\, r_p \in M_p :\; \texttt{JOIN}(r_p, r_c)$$

Every child row has exactly one parent.

---

**Corollary 4** *(Idempotence).*

$$\texttt{decompose}(J_1) = \texttt{decompose}(J_2) \quad\text{whenever}\quad J_1 = J_2$$

Follows directly from Theorem 1.

---

## 6. Assumptions and Limitations

The proof relies on the following assumptions:

| # | Assumption | Why It Holds in Forge |
|:---|:---|:---|
| 1 | `GET_KEYS` is total — queries all rows, not a sample | Forge runs `GET_KEYS` against the full table |
| 2 | Type stability — a field has a consistent type across rows | `GET_TYPES` uses the most complex type present (`ARRAY` > `STRUCT` > `SCALAR`) |
| 3 | Finite nesting — documents have finite depth | Guaranteed by JSON specification and any real data source |
| 4 | Lossless column storage — warehouse preserves value precision | True for `STRING`, `INT64`, `FLOAT64`, `BOOL`, `TIMESTAMP` in BigQuery |
| 5 | `ARRAY_AGG` ordering — rollup preserves array order | Forge uses `idx`-based ordering, deterministic by construction |

**The proof does NOT cover:**

- **Floating-point edge cases** — JSON numbers with >15 significant digits may lose precision in `FLOAT64` storage.
- **Schema evolution** — `append_new_columns` across multiple runs adds columns but never removes them; the round-trip isomorphism applies to a single run.
- **Thread safety** — the `ThreadPoolExecutor` parallelism in `unnesting.py` operates *within* a BFS level (not across levels), so it affects execution speed but not the model set.
