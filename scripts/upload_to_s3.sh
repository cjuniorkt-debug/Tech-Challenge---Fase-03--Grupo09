#!/usr/bin/env bash
# Sobe arquivos de suporte + CSVs raw para o S3 do Tech Challenge.
# Uso:
#   export BUCKET=raw-bkt-806188865054
#   ./scripts/upload_to_s3.sh
#
# Requer AWS CLI configurado (credenciais do AWS Academy Lab).

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUCKET="${BUCKET:-raw-bkt-806188865054}"
LOCAL_RAW="${LOCAL_RAW:-$ROOT/../state-of-data-pipeline_1/data/raw}"

echo "==> Bucket: s3://$BUCKET"
echo "==> Projeto AWS: $ROOT"

echo "==> Uploading scripts/pipeline_utils.py"
aws s3 cp "$ROOT/src/pipeline_utils.py" "s3://$BUCKET/scripts/pipeline_utils.py"

echo "==> Uploading dictionary/mapa_campos.csv"
aws s3 cp "$ROOT/data/dictionary/mapa_campos.csv" "s3://$BUCKET/dictionary/mapa_campos.csv"

if [[ -d "$LOCAL_RAW" ]]; then
  echo "==> Uploading raw CSVs from $LOCAL_RAW"
  for ano in 2023 2024 2025; do
    if [[ -f "$LOCAL_RAW/$ano.csv" ]]; then
      aws s3 cp "$LOCAL_RAW/$ano.csv" "s3://$BUCKET/raw/$ano.csv"
    else
      echo "AVISO: $LOCAL_RAW/$ano.csv nao encontrado"
    fi
  done
else
  echo "AVISO: pasta raw local nao encontrada ($LOCAL_RAW)."
  echo "       Suba 2023.csv / 2024.csv / 2025.csv manualmente para s3://$BUCKET/raw/"
fi

echo "==> Estrutura atual do bucket:"
aws s3 ls "s3://$BUCKET/" --recursive | head -50

echo "OK. Proximo passo: criar Glue Notebooks e colar/importar os .ipynb de notebooks/"
