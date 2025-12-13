#!/usr/bin/env bash
set -e

echo "=== Détection du binaire Python ==="

if command -v python3 >/dev/null 2>&1; then
  PYTHON=python3
elif command -v python >/dev/null 2>&1; then
  PYTHON=python
else
  echo "Erreur : ni 'python3' ni 'python' trouvés dans le PATH."
  exit 1
fi

echo "Utilisation de: $PYTHON"

# 1. Création / activation de l'environnement virtuel
echo "=== Création / activation de l'environnement virtuel .venv ==="

if [ ! -d ".venv" ]; then
  $PYTHON -m venv .venv
fi

# Activation compatible Linux/macOS (bin) et Windows (Scripts) via Git Bash/WSL
if [ -f ".venv/bin/activate" ]; then
  . .venv/bin/activate
elif [ -f ".venv/Scripts/activate" ]; then
  . .venv/Scripts/activate
else
  echo "Impossible de trouver le script d'activation du venv."
  exit 1
fi

echo "=== Mise à jour de pip ==="
pip install --upgrade pip

# 2. Installation des dépendances
if [ -f "requirements.txt" ]; then
  echo "=== Installation des bibliothèques depuis requirements.txt ==="
  pip install -r requirements.txt
else
  echo "Attention : requirements.txt introuvable à la racine du projet."
fi

# 3. Téléchargement des ressources NLP (idempotent)
echo "=== Téléchargement des stopwords NLTK ==="
$PYTHON -m nltk.downloader stopwords || true

echo "=== Téléchargement du modèle spaCy fr_core_news_sm ==="
$PYTHON -m spacy download fr_core_news_sm || true

echo "=== Téléchargement des modèles Stanza (fr) ==="
$PYTHON - <<EOF
try:
  import stanza
  stanza.download("fr")
except Exception:
  pass
EOF

# ------------------------------------------------------------
# Helper: run script if exists
# ------------------------------------------------------------
run_if_exists () {
  local f="$1"
  local label="$2"
  if [ -f "$f" ]; then
    echo "=== $label ==="
    $PYTHON "$f"
  else
    echo "⚠️ $f introuvable, étape ignorée."
  fi
}

# ------------------------------------------------------------
# 4. Exécution du pipeline
# ------------------------------------------------------------

# 4.1 Ingestion TV (RSS)
run_if_exists "ingestion/tv/ingest_tv.py" "Étape 1 : Ingestion TV RSS"

# 4.2 Ingestion Presse (RSS)
run_if_exists "ingestion/presse/ingest_press.py" "Étape 2 : Ingestion Presse RSS"

# 4.2bis Ingestion France24 (isolée)
run_if_exists "ingestion/tv/ingest_france24.py" "Étape 2bis : Ingestion France24 (isolée)"

# 4.2ter Ingestion Social (Circle 1)
run_if_exists "ingestion/social/ingest_reddit.py"  "Étape 2ter : Ingestion Social - Reddit"
run_if_exists "ingestion/social/ingest_mastodon.py" "Étape 2ter : Ingestion Social - Mastodon"
run_if_exists "ingestion/social/ingest_youtube.py"  "Étape 2ter : Ingestion Social - YouTube"
run_if_exists "ingestion/social/ingest_tiktok.py"   "Étape 2ter : Ingestion Social - TikTok"

# 4.3 NLP global
run_if_exists "processing/nlp/process_articles.py" "Étape 3 : NLP global (articles)"

# 4.3bis NLP France24
run_if_exists "processing/nlp/process_france24_articles.py" "Étape 3bis : NLP France24 (isolée)"

# 4.3ter NLP Social
run_if_exists "processing/nlp/process_social_posts.py" "Étape 3ter : NLP Social (posts)"

# 4.4 Keywords global
run_if_exists "processing/keywords/extract_keywords.py" "Étape 4 : Keywords global"

# 4.4bis Keywords France24
run_if_exists "processing/keywords/extract_france24_keywords.py" "Étape 4bis : Keywords France24"

# 4.4ter Keywords Social
run_if_exists "processing/keywords/extract_social_keywords.py" "Étape 4ter : Keywords Social"

# 4.5 Topics global
run_if_exists "processing/topics/extract_topics.py" "Étape 5 : Topics global"

# 4.5bis Topics France24
run_if_exists "processing/topics/extract_france24_topics.py" "Étape 5bis : Topics France24"

# 4.5ter Topics Social
run_if_exists "processing/topics/extract_social_topics.py" "Étape 5ter : Topics Social"

# 4.6 Analyse des biais (nom réel dans ton projet)
run_if_exists "processing/bias/analyze_topic_bias.py" "Analyse des biais médiatiques (topic-level)"

# 4.7 Spikes (nom réel dans ton projet)
run_if_exists "processing/spikes/detect_topic_spikes.py" "Détection des spikes (topic-level)"

# 4.8 Lifetime (noms réels dans ton projet)
run_if_exists "processing/lifetime/keyword_lifetime.py" "Lifetime keywords"
run_if_exists "processing/lifetime/topic_lifetime.py"   "Lifetime topics"
run_if_exists "processing/lifetime/theme_lifetime.py"   "Lifetime themes"

echo "=== Pipeline terminé avec succès ✅ ==="
