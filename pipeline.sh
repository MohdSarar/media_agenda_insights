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
  # Linux / macOS / WSL
  . .venv/bin/activate
elif [ -f ".venv/Scripts/activate" ]; then
  # Windows (Git Bash / MSYS2) avec venv créé par Python Windows
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

# 3. Téléchargement des ressources NLP (idempotent : si déjà téléchargées, NLTK/spaCy/Stanza réutilisent)
echo "=== Téléchargement des stopwords NLTK (français) ==="
$PYTHON -m nltk.downloader stopwords

echo "=== Téléchargement du modèle spaCy fr_core_news_sm ==="
$PYTHON -m spacy download fr_core_news_sm

echo "=== Téléchargement des modèles Stanza pour le français ==="
$PYTHON - <<EOF
import stanza
stanza.download("fr")
EOF

# 4. Exécution du pipeline complet

# 4.1 Ingestion des flux TV (RSS)
if [ -f "ingestion/tv/ingest_tv.py" ]; then
  echo "=== Étape 1 : Ingestion TV RSS ==="
  $PYTHON ingestion/tv/ingest_tv.py
else
  echo "⚠️ ingestion/tv/ingest_tv.py introuvable, étape ingestion TV ignorée."
fi

# 4.2 Ingestion des flux de presse (RSS)
if [ -f "ingestion/presse/ingest_press.py" ]; then
  echo "=== Étape 2 : Ingestion Presse RSS ==="
  $PYTHON ingestion/presse/ingest_press.py
else
  echo "⚠️ ingestion/presse/ingest_press.py introuvable, étape ingestion Presse ignorée."
fi

# 4.3 NLP : nettoyage + lemmatisation + entités
if [ -f "processing/nlp/process_articles.py" ]; then
  echo "=== Étape 3 : NLP (Stanza + spaCy) ==="
  $PYTHON processing/nlp/process_articles.py
else
  echo "⚠️ processing/nlp/process_articles.py introuvable, étape NLP ignorée."
fi

# 4.4 Extraction des top 10 mots (keywords_daily)
if [ -f "processing/keywords/extract_keywords.py" ]; then
  echo "=== Étape 4 : Extraction des mots-clés ==="
  $PYTHON processing/keywords/extract_keywords.py
else
  echo "⚠️ processing/keywords/extract_keywords.py introuvable, étape keywords ignorée."
fi

# 4.5 Topic modeling (topics_daily)
if [ -f "processing/topics/extract_topics.py" ]; then
  echo "=== Étape 5 : Extraction des sujets (topics) ==="
  $PYTHON processing/topics/extract_topics.py
else
  echo "⚠️ processing/topics/extract_topics.py introuvable, étape topics ignorée."
fi

echo "=== Pipeline terminé avec succès ✅ ==="
