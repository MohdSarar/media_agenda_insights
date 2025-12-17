# Media Agenda Insights

**Automated Media Monitoring & Narrative Analysis**
[![Run Media Agenda Pipeline](https://github.com/MohdSarar/media_agenda_insights/actions/workflows/pipeline.yml/badge.svg?branch=main)](https://github.com/MohdSarar/media_agenda_insights/actions/workflows/pipeline.yml)

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10+-blue.svg" />
  <img src="https://img.shields.io/badge/Streamlit-Dashboard-red.svg" />
  <img src="https://img.shields.io/badge/PostgreSQL-Database-blue.svg" />
  <img src="https://img.shields.io/badge/NLP-Stanza%20%7C%20spaCy-green.svg" />
  <img src="https://img.shields.io/badge/Embeddings-Sentence--Transformers-purple.svg" />
  <img src="https://img.shields.io/badge/Clustering-Scikit--learn-orange.svg" />
  <img src="https://img.shields.io/badge/Data%20Engineering-ETL%20Pipelines-yellow.svg" />
</p>

---

## Key Technologies

| Layer | Stack |
|-------|-------|
| **Language** | Python 3.10+ |
| **Database** | PostgreSQL |
| **NLP** | Stanza (lemmatization), spaCy (NER), NLTK (stopwords) |
| **ML/Embeddings** | Sentence-Transformers (multilingual MiniLM), scikit-learn (KMeans, NMF, TF-IDF) |
| **Ingestion** | feedparser, BeautifulSoup4, requests |
| **Dashboard** | Streamlit, Plotly, Altair |
| **Automation** | Bash pipeline (Docker-ready, Airflow-compatible) |

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Installation & Configuration](#installation--configuration)
4. [Running the Pipeline](#running-the-pipeline)
5. [Dataset Sources](#dataset-sources)
6. [NLP Pipeline](#nlp-pipeline)
7. [Dashboard Features](#dashboard-features)
8. [Roadmap](#roadmap)
9. [Author](#author)

---

## Project Overview

**Media Agenda Insights** is a fully automated system that monitors French media (TV channels and press outlets), processes content with industrial-grade NLP, and delivers actionable insights:

- **Top 10 keywords** per media outlet per day
- **Topic clusters** via NMF (Non-negative Matrix Factorization) modeling
- **Narrative clusters** using sentence embeddings and KMeans
- **Cross-media comparisons** and editorial bias visualizations
- **Interactive Streamlit dashboard** for data exploration

Built with real data engineering principles: modular pipelines, clean PostgreSQL schema, idempotent ETL, and cloud-ready architecture.

### Objective

> *"What topics dominate the French media landscape each day, and how do different outlets construct their narratives?"*

---

## Architecture

### System Overview

```mermaid
flowchart TB
    subgraph SOURCES["Data Sources"]
        TV["TV Channels<br/>BFMTV, CNEWS, FranceInfo, LCI"]
        PRESS["Press Outlets<br/>Le Monde, Le Figaro, Libération..."]
    end

    subgraph INGESTION["Ingestion Layer"]
        RSS_TV["ingest_tv.py"]
        RSS_PRESS["ingest_press.py"]
    end

    subgraph STORAGE["Storage Layer — PostgreSQL"]
        RAW[("articles_raw")]
        CLEAN[("articles_clean")]
        KW[("keywords_daily")]
        TOPICS[("topics_daily")]
        NARR[("narratives_clusters")]
    end

    subgraph PROCESSING["Processing Layer"]
        NLP["NLP Pipeline<br/>Stanza + spaCy"]
        KEYWORDS["Keyword Extraction<br/>TF-IDF"]
        TOPIC_MODEL["Topic Modeling<br/>NMF"]
        EMBED["Embeddings<br/>Sentence-Transformers"]
        CLUSTER["Clustering<br/>MiniBatchKMeans"]
    end

    subgraph OUTPUT["Output Layer"]
        DASHBOARD["Streamlit Dashboard"]
    end

    TV --> RSS_TV
    PRESS --> RSS_PRESS
    RSS_TV --> RAW
    RSS_PRESS --> RAW
    RAW --> NLP
    NLP --> CLEAN
    CLEAN --> KEYWORDS
    CLEAN --> TOPIC_MODEL
    CLEAN --> EMBED
    KEYWORDS --> KW
    TOPIC_MODEL --> TOPICS
    EMBED --> CLUSTER
    CLUSTER --> NARR
    KW --> DASHBOARD
    TOPICS --> DASHBOARD
    NARR --> DASHBOARD
```

### Data Flow Pipeline

```mermaid
flowchart LR
    A[RSS Feeds] -->|feedparser| B[Raw Articles]
    B -->|BeautifulSoup| C[Clean Text]
    C -->|Stanza| D[Tokens + Lemmas]
    D -->|spaCy| E[Named Entities]
    E --> F[TF-IDF + NMF]
    E --> G[Sentence-BERT]
    F --> H[Topic Clusters]
    G --> I[KMeans]
    I --> J[Narrative Clusters]
    H --> K[Dashboard]
    J --> K
```

### Database Schema

```mermaid
erDiagram
    articles_raw {
        int id PK
        varchar source
        varchar media_type
        text title
        text summary
        text url UK
        timestamp published_at
    }
    
    articles_clean {
        int id PK
        int article_id FK
        text cleaned_text
        text[] lemmas
        jsonb entities
    }
    
    keywords_daily {
        int id PK
        date date
        varchar source
        varchar word
        int count
        int rank
    }
    
    topics_daily {
        int id PK
        date date
        varchar source
        int topic_id
        text topic_label
        int articles_count
    }
    
    narratives_clusters {
        int id PK
        int cluster_id UK
        text label
        text[] top_keywords
        int size
    }
    
    narratives_assignments {
        int id PK
        int article_id FK
        int cluster_id FK
    }

    articles_raw ||--o| articles_clean : "processed"
    articles_raw ||--o{ narratives_assignments : "assigned"
    narratives_clusters ||--o{ narratives_assignments : "contains"
```

> Full SQL schema: [`infra/schema.sql`](infra/schema.sql)

### Project Structure

```
media_agenda_insights/
│
├── ingestion/
│   ├── tv/
│   │   └── ingest_tv.py              # TV RSS feed ingestion
│   └── presse/
│       └── ingest_press.py           # Press RSS feed ingestion
│
├── processing/
│   ├── nlp/
│   │   └── process_articles.py       # Stanza + spaCy pipeline
│   ├── keywords/
│   │   └── extract_keywords.py       # TF-IDF keyword extraction
│   ├── topics/
│   │   └── extract_topics.py         # NMF topic modeling
│   └── narratives/
│       └── embed_and_cluster.py      # Embeddings + clustering
│
├── dashboard/
│   ├── app.py                        # Streamlit entry point
│   ├── data_access.py                # Database access layer
│   └── views/
│       ├── overview.py
│       ├── compare.py
│       ├── topics.py
│       └── narratives.py
│
├── infra/
│   ├── config/
│   │   ├── feeds_tv.yaml
│   │   └── feeds_press.yaml
│   └── schema.sql                    # PostgreSQL schema
│
├── .env
├── requirements.txt
├── pipeline.sh
└── README.md
```

---

## Installation & Configuration

### Prerequisites

- Python 3.10+
- PostgreSQL 14+
- Git

### Step 1: Clone the Repository

```bash
git clone https://github.com/your-username/media_agenda_insights.git
cd media_agenda_insights
```

### Step 2: Create Virtual Environment

```bash
python -m venv .venv

# Linux / macOS
source .venv/bin/activate

# Windows
.venv\Scripts\activate
```

### Step 3: Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### Step 4: Download NLP Models

```bash
# NLTK French stopwords
python -m nltk.downloader stopwords

# spaCy French model
python -m spacy download fr_core_news_sm

# Stanza French model
python -c "import stanza; stanza.download('fr')"
```

### Step 5: Configure Environment

Create a `.env` file at project root:

```env
DATABASE_URL=postgresql://user:password@localhost:5432/media_agenda
```

### Step 6: Initialize Database

```bash
createdb media_agenda
psql -U user -d media_agenda -f infra/schema.sql
```

---

## Running the Pipeline

### Full ETL Pipeline

```bash
bash pipeline.sh
```

Executes sequentially:
1. TV Ingestion
2. Press Ingestion
3. NLP Processing
4. Keyword Extraction
5. Topic Extraction

### Individual Scripts

```bash
# Ingestion
python ingestion/tv/ingest_tv.py
python ingestion/presse/ingest_press.py

# Processing
python processing/nlp/process_articles.py
python processing/keywords/extract_keywords.py
python processing/topics/extract_topics.py
python processing/narratives/embed_and_cluster.py
```

### Launch Dashboard

```bash
streamlit run dashboard/app.py
```

Access: `http://localhost:8501`

---

## Dataset Sources

### Media Coverage

| Type | Sources | Feed Categories |
|------|---------|-----------------|
| **TV** | BFMTV, CNEWS, FranceInfo, TF1/LCI | General, Politics, Society |
| **Press** | Le Monde, Le Figaro, Libération, 20 Minutes, La Dépêche, HuffPost | Une, Politics, Economy, Society |

Configuration files:
- `infra/config/feeds_tv.yaml`
- `infra/config/feeds_press.yaml`

### Ingested Fields

| Field | Description |
|-------|-------------|
| `title` | Article headline |
| `summary` | Article excerpt |
| `url` | Original URL (unique) |
| `published_at` | Publication timestamp |
| `source` | Media outlet identifier |
| `feed_name` | Category (politique, société...) |
| `media_type` | `tv` or `press` |

---

## NLP Pipeline

### Challenge

Raw RSS content contains HTML artifacts, URLs, stopwords, and noise that pollute keyword extraction and topic modeling — resulting in meaningless outputs like `"le, la, de, du, en"`.

### Solution

Multi-layered cleanup pipeline:

| Step | Tool | Purpose |
|------|------|---------|
| HTML Cleaning | BeautifulSoup + Regex | Remove tags, URLs, artifacts |
| Lemmatization | Stanza (French) | Accurate token-level lemmas |
| NER Extraction | spaCy | Named entities as JSONB |
| Stopword Filtering | NLTK + Custom | Remove grammatical noise |
| Frequency Filtering | Custom logic | Remove tokens >40% frequency |
| Topic Modeling | TF-IDF + NMF | Coherent topic clusters |

### Result

**Before:** `"le, la, de, du, en, qui, que..."`

**After:** `"budget, sécurité sociale, assemblée, vote, réforme..."`

---

## Dashboard Features

| View | Description |
|------|-------------|
| **Overview** | Daily top 10 keywords, topic summaries, article counts |
| **Compare Media** | Cross-outlet heatmaps, timeline comparisons, coverage divergences |
| **Narrative Explorer** | Cluster list, keyword composition, per-source distribution |
| **Media Bias Radar** | Radar chart of semantic focus areas across outlets |

---

## Roadmap

| Feature | Status |
|---------|--------|
| Modular ETL pipeline (TV & press RSS) | ✅ Done |
| PostgreSQL schema with indexed analytics tables | ✅ Done |
| NLP pipeline (Stanza + spaCy, multilingual-ready) | ✅ Done |
| Keyword extraction (TF-IDF + advanced filtering) | ✅ Done |
| Topic modeling (NMF, per-source & per-language) | ✅ Done |
| Narrative clustering (Sentence-BERT + KMeans) | ✅ Done |
| Media bias quantification (topic-level) | ✅ Done |
| Topic spike detection (time-series anomaly) | ✅ Done |
| Keyword / Topic / Theme lifetime analysis | ✅ Done |
| **Multilingual media analysis (France 24 FR / EN / ES / AR)** | ✅ Done |
| **Isolated analytics pipeline for international media** | ✅ Done |
| Streamlit analytical dashboard (modular views) | ✅ Done |
| Airflow / Prefect ready | ✅ Done |
| Full HTML article scraping | 🔜 Planned |
| Social media ingestion (Reddit, Mastodon) | 🔜 Planned |
| LLM-based topic labeling | 🔜 Planned |
| Docker containerization | ✅ Done |


---

## Author

<p align="center">
  <b>Mohammed ABUSARAR</b><br>
  CEO & Founder — <b>Madel Data</b><br>
  <i>Data Engineer · NLP Specialist · AI Systems Architect</i>
</p>

---

<p align="center">
  <i>Built for media transparency and data-driven journalism analysis.</i>
</p>