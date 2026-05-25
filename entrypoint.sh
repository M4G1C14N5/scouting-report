#!/bin/sh
# Write the GCP service account JSON from the env var to a temp file
echo "$GOOGLE_CREDENTIALS_JSON" > /tmp/gcp-key.json
export GOOGLE_APPLICATION_CREDENTIALS=/tmp/gcp-key.json
exec streamlit run app.py
