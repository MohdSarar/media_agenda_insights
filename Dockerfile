FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# deps système (psycopg2 + build)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    postgresql-client \
    curl \
 && rm -rf /var/lib/apt/lists/*


# installer deps python
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -U pip \
 && pip install --no-cache-dir -r requirements.txt

# Install spaCy French model
RUN python -m spacy download fr_core_news_sm

# Install NLTK data (stopwords)
RUN python -c "import nltk; nltk.download('stopwords')"

# STANZA MODELS
ENV STANZA_RESOURCES_DIR=/app/.stanza
RUN python - <<'PY'
import stanza
stanza.download("fr", model_dir="/app/.stanza", verbose=False)
PY

# copier le code
COPY . /app

# rendre les scripts bash exécutables (safe même si déjà ok)
RUN chmod +x /app/scripts/*.sh || true

# port Streamlit
EXPOSE 8501

# par défaut: affiche l'aide
CMD ["python", "-c", "print('Use docker compose profiles: dashboard or pipeline')"]
