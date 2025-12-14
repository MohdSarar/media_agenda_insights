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

# Always run from project root (script directory)
cd "$(dirname "$0")"

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
# Helper: run module if corresponding file exists
# ------------------------------------------------------------
run_module_if_exists () {
  local file_path="$1"
  local module_name="$2"
  local label="$3"

  if [ -f "$file_path" ]; then
    echo "=== $label ==="
    $PYTHON -m "$module_name"
  else
    echo "⚠️ $file_path introuvable, étape ignorée."
  fi
}

# ------------------------------------------------------------
# 4. Exécution du pipeline (MODULAR)
# ------------------------------------------------------------

# 4.1 Ingestion TV (RSS)
run_module_if_exists "ingestion/tv/ingest_tv.py" "ingestion.tv.ingest_tv" "Étape 1 : Ingestion TV RSS"

# 4.2 Ingestion Presse (RSS)
run_module_if_exists "ingestion/presse/ingest_press.py" "ingestion.presse.ingest_press" "Étape 2 : Ingestion Presse RSS"

# 4.2bis Ingestion France24 (isolée)
run_module_if_exists "ingestion/tv/ingest_france24.py" "ingestion.tv.ingest_france24" "Étape 2bis : Ingestion France24 (isolée)"

# 4.2ter Ingestion Social (Circle 1)
run_module_if_exists "ingestion/social/ingest_reddit.py"   "ingestion.social.ingest_reddit"   "Étape 2ter : Ingestion Social - Reddit"
run_module_if_exists "ingestion/social/ingest_mastodon.py" "ingestion.social.ingest_mastodon" "Étape 2ter : Ingestion Social - Mastodon"
run_module_if_exists "ingestion/social/ingest_youtube.py"  "ingestion.social.ingest_youtube"  "Étape 2ter : Ingestion Social - YouTube"
run_module_if_exists "ingestion/social/ingest_tiktok.py"   "ingestion.social.ingest_tiktok"   "Étape 2ter : Ingestion Social - TikTok"

# 4.3 NLP global
run_module_if_exists "processing/nlp/process_articles.py" "processing.nlp.process_articles" "Étape 3 : NLP global (articles)"

# 4.3bis NLP France24
run_module_if_exists "processing/nlp/process_france24_articles.py" "processing.nlp.process_france24_articles" "Étape 3bis : NLP France24 (isolée)"

# 4.3ter NLP Social
run_module_if_exists "processing/nlp/process_social_posts.py" "processing.nlp.process_social_posts" "Étape 3ter : NLP Social (posts)"

# 4.4 Keywords global
run_module_if_exists "processing/keywords/extract_keywords.py" "processing.keywords.extract_keywords" "Étape 4 : Keywords global"

# 4.4bis Keywords France24
run_module_if_exists "processing/keywords/extract_france24_keywords.py" "processing.keywords.extract_france24_keywords" "Étape 4bis : Keywords France24"

# 4.4ter Keywords Social
run_module_if_exists "processing/keywords/extract_social_keywords.py" "processing.keywords.extract_social_keywords" "Étape 4ter : Keywords Social"

# 4.5 Topics global
run_module_if_exists "processing/topics/extract_topics.py" "processing.topics.extract_topics" "Étape 5 : Topics global"

# 4.5bis Topics France24
run_module_if_exists "processing/topics/extract_france24_topics.py" "processing.topics.extract_france24_topics" "Étape 5bis : Topics France24"

# 4.5ter Topics Social
run_module_if_exists "processing/topics/extract_social_topics.py" "processing.topics.extract_social_topics" "Étape 5ter : Topics Social"

# 4.6 Analyse des biais
run_module_if_exists "processing/bias/analyze_topic_bias.py" "processing.bias.analyze_topic_bias" "Analyse des biais médiatiques (topic-level)"

# 4.7 Spikes
run_module_if_exists "processing/spikes/detect_topic_spikes.py" "processing.spikes.detect_topic_spikes" "Détection des spikes (topic-level)"

# 4.8 Lifetime
run_module_if_exists "processing/lifetime/keyword_lifetime.py" "processing.lifetime.keyword_lifetime" "Lifetime keywords"
run_module_if_exists "processing/lifetime/topic_lifetime.py"   "processing.lifetime.topic_lifetime"   "Lifetime topics"
run_module_if_exists "processing/lifetime/theme_lifetime.py"   "processing.lifetime.theme_lifetime"   "Lifetime themes"

echo "=== Pipeline terminé avec succès ✅ ==="
