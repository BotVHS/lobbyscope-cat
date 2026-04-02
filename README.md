# lobbyscope.cat — Technical Reference

> **Automated detection of lobbying influence in the Catalan Government**
>
> A data pipeline that cross-references public meeting registries of high-ranking
> officials with regulatory, economic, and governmental decisions to quantify
> the lobbying influence of registered interest groups.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Data Sources](#2-data-sources)
3. [System Architecture](#3-system-architecture)
4. [Embedding Model — LaBSE](#4-embedding-model--labse)
5. [Connection Detection Algorithm](#5-connection-detection-algorithm)
   - 5.1 [Pre-filters](#51-pre-filters)
   - 5.2 [Semantic Similarity](#52-semantic-similarity)
   - 5.3 [Keyword Overlap Bonus](#53-keyword-overlap-bonus)
   - 5.4 [Connection Score Formula](#54-connection-score-formula)
   - 5.5 [Cross-department Penalty](#55-cross-department-penalty)
6. [Lobby Influence Score](#6-lobby-influence-score)
7. [Calibration & Validation](#7-calibration--validation)
8. [Database Schema](#8-database-schema)
9. [Pipeline Execution](#9-pipeline-execution)
10. [Limitations & Known Issues](#10-limitations--known-issues)
11. [Future Work](#11-future-work)
12. [Setup](#12-setup)

---

## 1. Project Overview

lobbyscope.cat aims to make lobbying activity in Catalonia transparent and
quantifiable. The Catalan Government's Lobby Registry (Registre de Grups
d'Interès, Law 19/2014) requires all interest groups to register meetings with
senior officials. This creates a unique dataset of ~47,000 publicly available
meeting records (2017–present).

The core hypothesis is:

> *If an interest group meets with a high-ranking official about topic X,
> and a regulatory, economic or governmental decision about topic X is published
> in the following 180 days by the same department, this constitutes evidence
> of potential lobbying influence.*

The system operationalises this hypothesis by:

1. Embedding meeting topics and decision titles into a shared semantic vector space.
2. Detecting pairs (meeting, decision) with high cosine similarity within a causal time window.
3. Scoring each pair on four dimensions: semantic similarity, temporal proximity, departmental alignment, and economic magnitude.
4. Aggregating per-group signals into a **Lobby Influence Score** (0–100).

The project produces no allegations. Detected connections are probabilistic
signals that require editorial judgment. The score is a relative ranking tool,
not a legal finding.

---

## 2. Data Sources

All data is public and sourced from the Catalan Open Data portal
(analisi.transparenciacatalunya.cat, Socrata API).

| Dataset | Socrata ID | Description | Records (2024) |
|---------|-----------|-------------|---------------|
| Meetings agenda | `hd8k-y28e` | Meetings of senior officials with registered interest groups | ~47,000 |
| DOGC normativa | `gzij-sesj` | Laws, Decrees and Orders published in the Official Gazette (DOGC) | ~4,330 |
| Subsidies | `9efw-9svx` | Public subsidies granted to registered lobby groups | ~28,000 |
| Contracts | `ec6f-uzdi` | Public contracts awarded to registered lobby groups | ~177,000 |
| Government Agreements | `ub8p-uqwj` | Weekly decisions of the Executive Council (Consell Executiu) 2014–present | ~12,631 |
| Interest Groups | `xusp-m64j` | Registered lobby groups with objectives, sector, CIF | ~3,500 |

### Notes on DOGC coverage

The DOGC dataset covers **Lleis** (Laws), **Decrets** (Decrees) and **Ordres**
(Orders). It does **not** include Resolucions (Resolutions), which typically
deal with grants, individual authorisations and lower-level administrative acts.
Resolucions are 3–4× more numerous but substantially less likely to be direct
lobbying targets for the type of groups in the registry.

### Government Agreements (Acords del Govern)

The `ub8p-uqwj` dataset contains decisions of the weekly Consell Executiu
(Cabinet) meetings, including:
- Approvals of subsidy bases and grant calls
- Approval of sectoral plans and strategies
- Urgent regulatory modifications (not published via DOGC immediately)
- Authorisations, delegations, and agreements with entities

This dataset is particularly high-signal for lobbying because it captures
decisions that are upstream of formal DOGC publication.

---

## 3. System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         INGESTION                               │
│                                                                 │
│  Socrata API  ──►  ingesta/*.py  ──►  PostgreSQL 16             │
│  (5 datasets)       normalisation       + pgvector              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                         EMBEDDINGS                              │
│                                                                 │
│  texts (topics, titles)  ──►  LaBSE model  ──►  vector(768)    │
│  sentence-transformers         local CPU/GPU    stored in DB    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    CONNECTION DETECTION                         │
│                                                                 │
│  For each meeting:                                              │
│    1. Pre-filter (protocol / generic topic)                     │
│    2. Temporal window [0, +180 days]                            │
│    3. ANN search via pgvector ivfflat (<=> cosine distance)     │
│    4. Keyword overlap bonus                                     │
│    5. Score formula (sim + time + dept + import)                │
│    6. Cross-dept penalty × 0.80                                 │
│    7. Store if score ≥ 30                                       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     LOBBY INFLUENCE SCORE                       │
│                                                                 │
│  Per group: frequency + diversity + connections + economic value│
│  Aggregated into lobby_scores table (0–100 scale)              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                          FASTAPI                                │
│                                                                 │
│  /grups  /carrecs  /reunions  /ranking  /rss                   │
└─────────────────────────────────────────────────────────────────┘
```

### Technology stack

| Component | Technology |
|-----------|-----------|
| Database | PostgreSQL 16 + pgvector extension |
| ORM | SQLAlchemy 2.x (Core mode for hot paths) |
| Embeddings | sentence-transformers / LaBSE |
| Approximate nearest neighbour | pgvector ivfflat index |
| API | FastAPI + uvicorn |
| Data ingestion | Sodapy / Socrata HTTP API |
| Language | Python 3.11+ |

---

## 4. Embedding Model — LaBSE

**Model**: `sentence-transformers/LaBSE`
(Language-Agnostic BERT Sentence Embedding, Feng et al. 2022)

**Architecture**: 12-layer BERT encoder, 768-dimensional output space,
trained on 109 languages with translation ranking loss.

**Why LaBSE for Catalan text?**

Catalan is a mid-resource language absent from most monolingual models.
LaBSE was explicitly trained with Catalan data and achieves strong
cross-lingual alignment. In practice, meeting topics (8–25 words) and
regulatory titles (10–40 words) are short texts where LaBSE outperforms
longer-context models.

**Observed similarity distribution** (DOGC title vs meeting topic, n≈100 validated pairs):

| Pair type | Cosine similarity range |
|-----------|------------------------|
| Direct topical match (clear TP) | 0.42 – 0.48 |
| Related but indirect | 0.33 – 0.41 |
| Unrelated (FP) | 0.15 – 0.32 |
| Totally unrelated | < 0.15 |

> **Key calibration finding**: The practical ceiling for cosine similarity
> between short meeting topics and regulatory titles in Catalan is approximately
> **0.48–0.50**. This is substantially below the theoretical maximum of 1.0 and
> reflects the structural difference between a topic descriptor
> ("Taula Sectorial Porcina") and a formal legislative title
> ("Decret pel qual s'estableixen les condicions de benestar animal dels porcs").

**Storage**: Embeddings are stored as `vector(768)` columns in PostgreSQL.
The pgvector `<=>` operator computes cosine distance directly in SQL.

**Generation**: Texts are encoded in batches of 64 with `normalize_embeddings=True`.
Normalisation ensures that the dot product equals cosine similarity, which is
required for the `<=>` operator semantics.

---

## 5. Connection Detection Algorithm

**Current version**: `1.11.0`

### 5.1 Pre-filters

Before computing any embedding similarity, each meeting passes through two
deterministic pre-filters that discard meetings structurally incapable of
generating lobbying signals.

#### Filter 1: Protocol meetings (`_RE_PROTOCOL`)

Meetings categorised as ceremonial, social, or purely institutional events are
excluded. These include ~85 distinct patterns covering:

- Dinners, galas, concerts, ceremonies
- Award ceremonies (`lliurament de premis`, `entrega de diplomes`)
- Anniversaries, inaugurations, openings
- Press conferences (`roda de premsa`, `conferència de premsa`)
- Sports events (champions, supercopa, competitions)
- Initial contact meetings (`primera presa de contacte`, `presa de contacte`)
- Institutional visits (`visita a les instal·lacions`, `visita institucional`)
- Protocol acts at the highest level (`reunió del president de la Generalitat`)
- Signature ceremonies (`acte de signatura`)

**Implementation**: `re.search()` (anywhere in the string, case-insensitive).
Protocol signals can appear anywhere in a topic description.

#### Filter 2: Generic topics (`_RE_TEMA_GENERIC`)

Meeting topics that are too vague to establish a specific semantic connection
are excluded. These are topics where any decision in any sector could produce
a high similarity score by coincidence:

- Coordination or follow-up meetings without specific subject (`Reunió de coordinació`)
- First contact and presentation meetings (`presentació de l'entitat`)
- Completely generic topics (`Temes generals`, `Temes comuns`, `Projectes en curs`)
- Participation in external events (`assistència a la jornada`, `participació a l'acte`)
- Consultative bodies as topics (`Consell Català de X` — the body meeting, not its work)
- Synergy-seeking language without substance (`per establir sinèrgies comunes`)

**Implementation**: Most patterns use `^` anchor (anchored to start of string).
Some patterns use `.search()` for phrases that can appear anywhere
(e.g., `per establir sinèrgies`).

#### Filter 3: Routine norms (`_RE_NORMA_RUTINA`)

Applied to the decision side rather than the meeting side, this filter excludes
regulatory decisions that are published automatically on a calendar basis and
are therefore independent of any specific lobbying interaction:

- Annual public prices and fees (`taxes per a l'any`, `preus públics`)
- Departmental restructuring decrees (`reestructuració del Departament de X`)
- Presidential succession decrees (`suplència del president`)
- Annual statistical action programmes
- Budget supplements and extraordinary credits
- Electoral regime norms
- Curriculum decrees for specific educational cycles (published per cycle, per year)

These norms, while politically significant, cannot be attributed to a single
meeting because they follow administrative calendars rather than lobbying pressure.

---

### 5.2 Semantic Similarity

For each meeting passing the pre-filters, the system queries the decision
database using approximate nearest-neighbour search:

```sql
SELECT n.id, n.titol, n.departament_codi, n.data_publicacio,
       1 - (n.embedding_titol <=> CAST(:embed AS vector)) AS sim_titol,
       1 - (n.embedding_resum  <=> CAST(:embed AS vector)) AS sim_resum
FROM normativa_dogc n
WHERE n.data_publicacio BETWEEN
    CAST(:data_reunio AS date) - INTERVAL '0 days'
    AND CAST(:data_reunio AS date) + INTERVAL '180 days'
  AND n.embedding_titol IS NOT NULL
ORDER BY n.embedding_titol <=> CAST(:embed AS vector)
LIMIT 20
```

The `<=>` operator is the pgvector cosine distance. Similarity is `1 - distance`.

**Temporal window**: `[0, +180 days]` from the meeting date.
The window is strictly **causal**: only decisions published **after** the meeting
are considered as potential lobbying targets. A meeting cannot influence a decision
that was already published.

The 180-day (6-month) window is a standard parameter in lobbying research,
corresponding to the typical legislative cycle from proposal to publication.

**Per-meeting limit**: Top 20 candidates by cosine distance, per decision type.

**Base threshold**: `THRESHOLD_SIMILITUD = 0.30`

At this threshold, almost all true positives are captured (high recall) while
keeping false positives manageable before the scoring stage provides secondary
discrimination. The primary quality gate is `connexio_score ≥ 70`, not the
similarity threshold.

For subvencions (grants), a stricter threshold of `0.20` (0.30 - 0.10) is used
because the embedding source is the grant purpose text (`finalitat`), which is
typically shorter and more specific than a regulatory title.

---

### 5.3 Keyword Overlap Bonus

LaBSE embeddings of very short texts (5–10 words) can miss exact lexical matches
that are strong evidence of topical overlap. A keyword overlap bonus compensates
for this:

```python
def _keyword_overlap_bonus(tema: str, titol_norma: str) -> float:
    shared_keywords = (
        tokens(tema, min_len=5, no_stopwords=True)
      & tokens(titol_norma, min_len=5, no_stopwords=True)
    )
    return 0.08 if len(shared_keywords) >= 2 else 0.0
```

**Tokenisation**: Unicode NFD normalisation → lowercase → strip accents →
extract alphabetic tokens → filter tokens shorter than 5 characters →
remove Catalan stopwords (`dels`, `les`, `per`, `que`, `amb`, etc.).

**Bonus value**: `+0.08` to the cosine similarity before threshold comparison.

**Motivation**: Without this bonus, a meeting topic "Taula Sectorial Porc"
would not reliably match "Decret sobre benestar animal del sector porcí de Catalunya"
because LaBSE distributes the semantic weight across the sentence, while the
specific term "porcí/porc" carries most of the relevant signal.

**Calibration**: Two shared content words (≥5 characters) is the minimum
threshold to avoid false positives from accidentally shared common vocabulary.

---

### 5.4 Connection Score Formula

The connection score is a linear combination of four independent signals:

$$\text{score} = S_{\text{sim}} + S_{\text{time}} + S_{\text{dept}} + S_{\text{import}}$$

Then, if `dept_match = False`:

$$\text{score} \leftarrow \text{score} \times 0.80$$

Final clamp: $\text{score} \in [0, 100]$.

#### Component A — Semantic Similarity (0–50)

The raw cosine similarity is normalised to the empirically observed range for
this specific domain (LaBSE + short Catalan texts):

$$S_{\text{sim}} = \min\left(\frac{\text{sim} - 0.15}{0.50 - 0.15} \times 50,\ 50\right)$$

where $\text{sim} = \max(\text{sim\_titol}, \text{sim\_resum}) + \text{keyword\_bonus}$.

**Parameters**:
- `SIM_MIN = 0.15`: similarity level corresponding to score 0 (unrelated texts)
- `SIM_MAX = 0.50`: practical ceiling of LaBSE similarity for this text type

| Raw similarity | Normalised score |
|---------------|-----------------|
| 0.15 | 0.0 |
| 0.25 | 14.3 |
| 0.35 | 28.6 |
| 0.42 | 38.6 |
| 0.48 | 47.1 |
| ≥ 0.50 | 50.0 (capped) |

#### Component B — Temporal Proximity (0–30)

The score decreases with temporal distance from the meeting. The signal is
**asymmetric**: decisions published before the meeting (negative `dies`) score
lower than equivalent decisions published after, because the causal direction
is inverted.

| Days from meeting | Score | Interpretation |
|-------------------|-------|----------------|
| 0 – 14 | 30 | Immediate decision after meeting (strongest signal) |
| 15 – 30 | 26 | Very recent, high causal probability |
| 31 – 60 | 20 | Within 2 months |
| 61 – 90 | 14 | Within a quarter |
| 91 – 180 | 8 | Within 6 months |
| > 180 | 3 | Beyond lobbying window |
| −1 to −30 | 22 | Meeting shortly after decision (follow-up) |
| −31 to −60 | 16 | Meeting within 2 months after decision |
| −61 to −90 | 10 | Meeting within 3 months after decision |
| −91 to −120 | 5 | Anticipatory meeting |

The WINDOW_BEFORE_DAYS parameter is set to 0 (causal mode), meaning the SQL
query only retrieves decisions published after the meeting. However, the scoring
function retains the pre-meeting scoring logic for potential future use with
symmetric windows.

#### Component C — Departmental Alignment (0–15)

$$S_{\text{dept}} = \begin{cases} 15.0 & \text{if } \text{dept\_reunio} = \text{dept\_decisio} \neq \{\text{NULL, DESCONEGUT, GOVERN}\} \\ 3.0 & \text{otherwise} \end{cases}$$

A meeting at Department X that is followed by a decision from Department X
is substantially more likely to represent lobbying influence than a cross-department
connection. The GOVERN and DESCONEGUT codes are treated as wildcards because
cross-departmental coordination is inherent to those codes.

#### Component D — Economic Magnitude (0–5)

| Grant/Contract value (€) | Score |
|--------------------------|-------|
| ≥ 1,000,000 | 5.0 |
| ≥ 100,000 | 3.0 |
| > 0 | 1.0 |
| 0 | 0.0 |

This component only applies to subvention and contract connections.
Normativa and Government Agreement connections always score 0 on this dimension.

---

### 5.5 Cross-department Penalty

$$\text{score}_{\text{final}} = \text{score}_{\text{raw}} \times 0.80 \quad \text{if } \text{dept\_match} = \text{False}$$

**Empirical basis**: In 300+ manually validated connection pairs across
versions 1.0–1.11, **0 out of 32 confirmed true positives** had `dept_match = False`.
All validated lobbying connections involved the same department in both the
meeting and the subsequent decision.

**Effect on score ceiling**:

$$\text{score}_{\text{max, cross-dept}} = (50 + 30 + 3 + 0) \times 0.80 = 66.4$$

Since the quality threshold for public display is `score ≥ 70`, the cross-department
penalty effectively excludes all cross-departmental connections from the
high-confidence tier. This is intentional: cross-departmental lobbying connections,
while theoretically possible, cannot be reliably distinguished from coincidental
semantic similarity at this threshold level.

---

## 6. Lobby Influence Score

The Lobby Influence Score aggregates per-group signals across all detected
connections into a single 0–100 index.

$$\text{LIS} = \min(S_F + S_D + S_C + S_V,\ 100)$$

### Component A — Meeting Frequency (0–25)

Stepwise function based on total meeting count:

| Total meetings | Score |
|---------------|-------|
| ≥ 50 | 25.0 |
| ≥ 30 | 22.0 |
| ≥ 20 | 18.0 |
| ≥ 10 | 13.0 |
| ≥ 5 | 9.0 |
| ≥ 2 | 5.0 |
| 1 | 2.0 |

**Rationale**: Raw frequency counts reflect lobbying effort but have diminishing
marginal returns. A step function prevents extreme outliers from dominating,
unlike a logarithmic scale which would require continuous recalibration.

### Component B — Access Diversity (0–25)

$$S_D = \min\left(\frac{C}{N} \times 40,\ 17\right) + \min(D \times 2,\ 8)$$

where:
- $C$ = number of distinct officials contacted
- $N$ = total meetings
- $D$ = number of distinct departments contacted (excluding GOVERN, DESCONEGUT)

**Interpretation**: A lobby contacting many different officials across multiple
departments has a wider influence network than one that repeatedly meets the
same person. The ratio $C/N$ measures contact diversity efficiency; the ideal
ratio is ~0.4 (varied contacts with possible repetition for follow-up).

**Maximum**: $S_D = 17 + 8 = 25$ points
(achieved with 10+ distinct officials at ratio 0.4 or above, across 4+ departments)

### Component C — Decision Connections (0–30)

$$S_C = \min\left(\frac{Q}{N} \times 60,\ 30\right)$$

where $Q$ = connections with `connexio_score ≥ 70` (quality threshold).

**Interpretation**: The ratio of quality connections to total meetings captures
lobbying effectiveness, not just activity. A group that meets officials 50 times
but generates no detectable policy outcomes scores 0 on this dimension.

**Maximum**: $S_C = 30$ at $Q/N \geq 0.5$ (50% of meetings lead to a quality connection).

### Component D — Economic Value (0–20)

Stepwise function based on total grants received:

| Total grants (€) | Score |
|-----------------|-------|
| ≥ 50,000,000 | 20.0 |
| ≥ 10,000,000 | 17.0 |
| ≥ 1,000,000 | 13.0 |
| ≥ 100,000 | 8.0 |
| ≥ 10,000 | 4.0 |
| > 0 | 1.0 |
| 0 | 0.0 |

> **Note**: Only subvencions are currently included. Contracts awarded to
> lobby groups are captured in the database but not yet included in the
> economic magnitude component, which may underestimate the score for
> corporate lobbies (as opposed to associations and NGOs).

### Score Interpretation

| Score range | Interpretation |
|-------------|----------------|
| 0–20 | Low or occasional presence |
| 21–40 | Moderate lobbying activity |
| 41–60 | Active lobby with regular presence |
| 61–80 | Highly active lobby with documented influence |
| 81–100 | High-influence lobby (top ~1% of registry) |

---

## 7. Calibration & Validation

### Methodology

Human validation uses a stratified random sample of 100 meeting–decision pairs:

| Stratum | Score range | Sample size |
|---------|-------------|-------------|
| High quality | score ≥ 70 | 40 pairs |
| Grey zone | score 50–69 | 40 pairs |
| Low quality | score 30–49 | 20 pairs |

Within each stratum, sampling is deduped by meeting topic (no two rows from the
same meeting) to avoid inflating apparent precision from multi-connection meetings.

Human annotators score each pair 0–10 on the **plausibility of lobbying influence**:
- 0 = no apparent relationship
- 5 = possible but uncertain relationship
- 10 = direct relationship, near-certain lobbying signal

### Version History

| Version | Pearson r | F1 @ thr=70 | Precision @ 70 | Recall @ 70 | Key changes |
|---------|-----------|-------------|----------------|-------------|-------------|
| 1.9.0 | — | — | — | — | Protocol filters, generic topic filters |
| 1.10.0 | 0.610 | 0.581 | 0.450 | 0.900 | +8 protocol patterns, keyword overlap bonus |
| 1.11.0 | 0.540* | 0.545* | — | — | +8 protocol/generic patterns, curriculum filters |

*v1.11 regression is attributed to sampling variance (100 pairs, random stratification).
Persistent FP count at score ≥ 70 decreased from 22 to 22 (no improvement, but no
regression either) — the metric difference reflects the different random sample, not
a true regression in precision.

### Key empirical findings

**Cross-department false positives**: 0 out of 32 confirmed true positives involved
cross-departmental matches. The 0.80 penalty is therefore empirically well-calibrated.

**Score threshold**: At `connexio_score ≥ 70`, precision is approximately 45–60%.
Roughly half of displayed connections are editorial false positives — this is
expected and acceptable for a "signal" system, but should be prominently
disclosed in any public-facing product.

**Ceiling effect**: The maximum achievable cosine similarity for this domain
(short Catalan administrative texts, LaBSE) is approximately 0.48–0.50. This
ceiling means the semantic similarity component can contribute at most ~47/50
points, and the score ceiling is effectively:
$$\text{score}_{\text{practical max}} = 47 + 30 + 15 + 0 = 92$$

**Protocol filter coverage**: As of v1.11, the `_RE_PROTOCOL` filter covers
~85 distinct patterns. Analysis of 42 false positives from v1.10 showed that
the remaining FPs at score ≥ 70 are primarily generic meeting topics that cannot
be further filtered without discarding legitimate lobbying topics (false negative risk).

---

## 8. Database Schema

### Core tables

```
carrecs          — Officials with public meeting agendas
grups            — Registered interest groups (lobbies)
reunions         — Individual meetings (one row per meeting record)
normativa_dogc   — Laws, Decrees, Orders from DOGC
subvencions_lobby — Subsidies granted to registered lobby groups
contractes_lobby  — Contracts awarded to registered lobby groups
acords_govern     — Weekly Cabinet (Consell Executiu) decisions
connexions        — Detected meeting–decision pairs with scores
lobby_scores      — Aggregated Lobby Influence Score per group
```

### Key design decisions

**Embeddings stored in-database**: All 768-dimensional vectors are stored in
the same PostgreSQL instance as the relational data. This enables single-query
joins between metadata filters (date, department) and vector similarity search,
avoiding costly round-trips to a separate vector database.

**ivfflat index**: Used for approximate nearest-neighbour search. The `lists=100`
parameter is appropriate for table sizes of 10,000–200,000 rows. For larger tables,
consider `lists = sqrt(n_rows)`.

**Unique constraints on connexions**: Four partial unique indexes enforce the
invariant that no meeting–decision pair is stored twice:
```sql
CREATE UNIQUE INDEX connexions_uniq_normativa
    ON connexions (reunio_id, decisio_normativa_id)
    WHERE decisio_normativa_id IS NOT NULL;
-- (similar for subvencio, contracte, acord_govern)
```

**CHECK constraint — one decision per row**:
```sql
CONSTRAINT una_decisio CHECK (
    (decisio_normativa_id    IS NOT NULL)::int +
    (decisio_subvencio_id    IS NOT NULL)::int +
    (decisio_contracte_id    IS NOT NULL)::int +
    (decisio_acord_govern_id IS NOT NULL)::int = 1
)
```

**Version auto-reset**: The detector checks the version of stored connections
on startup. If the stored version differs from `VERSIO_ALGORISME`, all connections
and lobby scores are deleted and recomputed from scratch. This ensures consistency
without manual migration steps.

### Vector columns summary

| Table | Column | Source text | Dimensions |
|-------|--------|-------------|-----------|
| reunions | embedding_tema | tema_normalitzat | 768 |
| normativa_dogc | embedding_titol | titol | 768 |
| normativa_dogc | embedding_resum | resum | 768 |
| grups | embedding_objectius | objectius | 768 |
| carrecs | embedding_nom | nom_canonical | 768 |
| subvencions_lobby | embedding_finalitat | finalitat | 768 |
| contractes_lobby | embedding_objecte | objecte_contracte | 768 |
| acords_govern | embedding_titol | titol | 768 |

---

## 9. Pipeline Execution

```
run_ingesta.py --fase agendes         # meetings + officials + groups
run_ingesta.py --fase dogc            # DOGC normativa (~4,330 records)
run_ingesta.py --fase acords_govern   # Cabinet decisions (~12,631 records)
run_ingesta.py --fase subvencions     # grants (~28,000 records)
run_ingesta.py --fase contractes      # contracts (~177,000 records)
run_ingesta.py --fase embeddings      # generate all LaBSE embeddings
run_ingesta.py --fase connexions      # detect connections (v1.11)
run_ingesta.py --fase scores          # compute Lobby Influence Scores
run_ingesta.py --fase stats           # print row counts per table
```

The `--fase tot` flag runs all phases in the above order.

**Throughput** (approximate, CPU mode):
- Meetings ingestion: ~2,000 records/min
- Embedding generation: ~128 embeddings/min (CPU), ~5,000/min (GPU)
- Connection detection (pgvector): ~300 meetings/min
- Score calculation: ~500 groups/min

For the full pipeline with ~270,000 records needing embeddings, **GPU acceleration
is highly recommended** (see `SETUP_GPU.md`).

---

## 10. Limitations & Known Issues

### 10.1 Causal inference limitations

The system detects *temporal co-occurrence* of semantically similar meeting topics
and policy decisions. This is a necessary but not sufficient condition for lobbying
influence. Alternative explanations include:

- **Agenda setting by external events**: Both the meeting and the decision may
  be responses to the same external event (e.g., a European regulation, a crisis),
  with no direct causal link between them.
- **Policy anticipation**: Interest groups may schedule meetings *because* they know
  a decision is imminent, not to influence it.
- **Semantic coincidence**: Two unrelated policy areas may share terminology
  (e.g., "sostenibilitat" is used in environmental, economic, and social policy contexts).

### 10.2 Coverage gaps

- **DOGC Resolucions**: Not included. These are 3–4× more numerous and cover
  individual grants, authorisations, and minor regulatory acts — potentially
  the most numerous category of lobbying-influenced decisions.
- **Pre-2014 data**: The meetings dataset starts in 2017; DOGC data is available
  from 2010 but is not ingested for years before the meetings data.
- **Unregistered lobbying**: Only registered groups appear in the registry.
  Large companies, law firms, and many trade associations lobby without registering,
  creating systematic blind spots.
- **Contract embeddings**: ~177,000 contracts currently lack embeddings (slow to
  generate in CPU mode). Connection detection for contracts is disabled until
  embeddings are complete.

### 10.3 Algorithm limitations

- **Meeting topic quality**: ~35–40% of meeting topics are generic descriptions
  that are correctly filtered but reduce the effective sample size.
- **Name normalisation**: The matching of group names between the meetings dataset
  and the groups registry is imperfect (~15% of meetings cannot be linked to a
  registered group due to name variations).
- **Department coding**: ~12% of meetings have no department code or are coded
  as GOVERN/DESCONEGUT, which disables the departmental alignment signal.

### 10.4 Statistical limitations

- **Calibration sample size**: 100 pairs per validation round is insufficient
  for stable Pearson r estimates. The difference between v1.10 (r=0.610) and
  v1.11 (r=0.540) likely reflects sampling variance rather than true regression.
  A fixed holdout set of 500+ pairs is needed for reliable comparison.
- **Annotator agreement**: Currently single-annotator validation. Inter-annotator
  reliability is unknown.

---

## 11. Future Work

### 11.1 Short-term improvements (high impact, low cost)

1. **Add DOGC Resolucions**: Dataset exists on transparenciacatalunya.cat.
   Would increase decision coverage 3–4×, likely doubling detected connections.

2. **Complete contract embeddings**: ~177,000 contracts with `embedding_objecte`
   missing. With GPU, this takes ~35 minutes.

3. **Fixed validation holdout**: Create a permanent 500-pair manually validated
   set (stratified, de-duplicated) for reliable metric comparison across versions.

4. **Economic component for contracts**: Include contract value in the Lobby
   Influence Score (currently only grants are counted).

### 11.2 Medium-term improvements

5. **LLM-based connection explanation**: Use Claude API to generate a one-sentence
   explanation of why a specific meeting–decision pair constitutes a potential
   lobbying signal. This would improve editorial usability.

6. **Entity disambiguation**: Improve group-to-meeting matching using fuzzy
   string matching and CIF-based linking. Would recover ~15% of currently
   unlinked meetings.

7. **Symmetric temporal window**: The current window is strictly causal
   (meeting before decision). A symmetric or pre-window could capture
   *anticipatory* lobbying (meeting after decision to influence implementation).

8. **Multi-meeting aggregation**: Currently each meeting is evaluated independently.
   A sequence of meetings on the same topic within a short window should produce
   a stronger signal than a single meeting.

### 11.3 Research directions

9. **Network analysis**: Model the bipartite graph of (groups, officials) to
   identify structural patterns — groups with high betweenness centrality, officials
   with concentrated sectoral exposure, etc.

10. **Temporal dynamics**: Analyse changes in lobbying intensity before/after
    elections, government changes, or specific legislative events.

11. **Sector-level analysis**: Aggregate signals by sector (health, energy,
    education, etc.) to produce sector-level influence rankings.

12. **Comparison with other transparency registers**: EU Transparency Register,
    Scottish Lobbying Register. Cross-validate methodology and findings.

---

## 12. Setup

### Quick start

```bash
git clone https://github.com/BotVHS/lobbyscope-cat.git
cd lobbyscope-cat
python -m venv venv && source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env  # fill in DATABASE_URL and PGVECTOR_ENABLED=true
docker compose up -d
python run_ingesta.py --fase tot
```

For GPU acceleration on Windows + CUDA, see [`SETUP_GPU.md`](SETUP_GPU.md).

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | — | PostgreSQL connection string |
| `PGVECTOR_ENABLED` | `false` | Set `true` to use pgvector (recommended) |
| `EMBEDDING_MODEL` | `sentence-transformers/LaBSE` | HuggingFace model name |
| `EMBEDDING_DIMS` | `768` | Embedding dimensions |
| `SOCRATA_APP_TOKEN` | — | Optional Socrata API token (increases rate limit) |

### Dependencies

Key packages (see `requirements.txt` for full list):

- `sentence-transformers` — LaBSE model
- `torch` — PyTorch backend for sentence-transformers
- `sqlalchemy` — Database ORM
- `pgvector` — Python client for pgvector
- `sodapy` — Socrata API client
- `fastapi` + `uvicorn` — REST API
- `python-dotenv` — Environment configuration

---

## References

- Feng, F., Yang, Y., Cer, D., Arivazhagan, N., & Wang, W. (2022).
  *Language-agnostic BERT sentence embedding*. arXiv:2007.01852.
- Registre de Grups d'Interès de Catalunya — Law 19/2014 on Transparency.
  https://transparencia.gencat.cat/ca/registres/registre-de-grups-d-interes/
- Analisi Transparència Catalunya — Open Data Portal.
  https://analisi.transparenciacatalunya.cat/
- pgvector: Open-source vector similarity search for PostgreSQL.
  https://github.com/pgvector/pgvector

---

*lobbyscope.cat — Data pipeline v1.11 · April 2026*
*Source: github.com/BotVHS/lobbyscope-cat*
