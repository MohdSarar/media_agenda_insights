#!/usr/bin/env bash
set -euo pipefail

# Toujours exécuter depuis la racine du projet
cd /app

PYTHON=python

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

# 1) Ingestion
run_module_if_exists "ingestion/tv/ingest_tv.py" "ingestion.tv.ingest_tv" "Étape 1 : Ingestion TV RSS"
run_module_if_exists "ingestion/presse/ingest_press.py" "ingestion.presse.ingest_press" "Étape 2 : Ingestion Presse RSS"
run_module_if_exists "ingestion/tv/ingest_france24.py" "ingestion.tv.ingest_france24" "Étape 2bis : Ingestion France24 (isolée)"

# Social (si scripts présents)
run_module_if_exists "ingestion/social/ingest_reddit.py"   "ingestion.social.ingest_reddit"   "Étape 2ter : Ingestion Social - Reddit"
run_module_if_exists "ingestion/social/ingest_mastodon.py" "ingestion.social.ingest_mastodon" "Étape 2ter : Ingestion Social - Mastodon"
run_module_if_exists "ingestion/social/ingest_youtube.py"  "ingestion.social.ingest_youtube"  "Étape 2ter : Ingestion Social - YouTube"
run_module_if_exists "ingestion/social/ingest_tiktok.py"   "ingestion.social.ingest_tiktok"   "Étape 2ter : Ingestion Social - TikTok"

# 2) NLP
run_module_if_exists "processing/nlp/process_articles.py" "processing.nlp.process_articles" "Étape 3 : NLP global (articles)"
run_module_if_exists "processing/nlp/process_france24_articles.py" "processing.nlp.process_france24_articles" "Étape 3bis : NLP France24 (isolée)"
run_module_if_exists "processing/nlp/process_social_posts.py" "processing.nlp.process_social_posts" "Étape 3ter : NLP Social (posts)"

# 3) Keywords
run_module_if_exists "processing/keywords/extract_keywords.py" "processing.keywords.extract_keywords" "Étape 4 : Keywords global"
run_module_if_exists "processing/keywords/extract_france24_keywords.py" "processing.keywords.extract_france24_keywords" "Étape 4bis : Keywords France24"
run_module_if_exists "processing/keywords/extract_social_keywords.py" "processing.keywords.extract_social_keywords" "Étape 4ter : Keywords Social"

# 4) Topics
run_module_if_exists "processing/topics/extract_topics.py" "processing.topics.extract_topics" "Étape 5 : Topics global"
run_module_if_exists "processing/topics/extract_france24_topics.py" "processing.topics.extract_france24_topics" "Étape 5bis : Topics France24"
run_module_if_exists "processing/topics/extract_social_topics.py" "processing.topics.extract_social_topics" "Étape 5ter : Topics Social"

# 5) Analyses avancées
run_module_if_exists "processing/bias/analyze_topic_bias.py" "processing.bias.analyze_topic_bias" "Analyse des biais médiatiques (topic-level)"
run_module_if_exists "processing/spikes/detect_topic_spikes.py" "processing.spikes.detect_topic_spikes" "Détection des spikes (topic-level)"
run_module_if_exists "processing/lifetime/keyword_lifetime.py" "processing.lifetime.keyword_lifetime" "Lifetime keywords"
run_module_if_exists "processing/lifetime/topic_lifetime.py"   "processing.lifetime.topic_lifetime"   "Lifetime topics"
run_module_if_exists "processing/lifetime/theme_lifetime.py"   "processing.lifetime.theme_lifetime"   "Lifetime themes"

echo "=== Pipeline terminé avec succès ✅ ==="
