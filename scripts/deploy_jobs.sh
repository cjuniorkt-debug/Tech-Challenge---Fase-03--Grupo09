#!/usr/bin/env bash
# Sobe os scripts dos Glue Jobs e cria/atualiza os 3 jobs na conta.
#
# Uso (Lab ativo + AWS CLI configurado):
#   export BUCKET=raw-bkt-806188865054
#   export GLUE_ROLE_ARN=arn:aws:iam::806188865054:role/LabRole   # ajuste se preciso
#   ./scripts/deploy_jobs.sh
#
# Se GLUE_ROLE_ARN nao for passado, o script tenta descobrir a role LabRole.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUCKET="${BUCKET:-raw-bkt-806188865054}"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"
WORKER_TYPE="${WORKER_TYPE:-G.1X}"
NUMBER_OF_WORKERS="${NUMBER_OF_WORKERS:-2}"
GLUE_VERSION="${GLUE_VERSION:-4.0}"

echo "==> Conta / identidade"
aws sts get-caller-identity --output table

ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"

if [[ -z "${GLUE_ROLE_ARN:-}" ]]; then
  # AWS Academy costuma usar LabRole
  GLUE_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/LabRole"
  echo "==> GLUE_ROLE_ARN nao definido; usando padrao Academy: $GLUE_ROLE_ARN"
fi

echo "==> Bucket: s3://$BUCKET"
echo "==> Role:   $GLUE_ROLE_ARN"
echo "==> Region: $REGION"

echo "==> Upload pipeline_utils + jobs"
aws s3 cp "$ROOT/src/pipeline_utils.py" "s3://$BUCKET/scripts/pipeline_utils.py"
aws s3 cp "$ROOT/data/dictionary/mapa_campos.csv" "s3://$BUCKET/dictionary/mapa_campos.csv"
aws s3 cp "$ROOT/jobs/01_bronze_ingestion.py" "s3://$BUCKET/scripts/jobs/01_bronze_ingestion.py"
aws s3 cp "$ROOT/jobs/02_silver_unificacao.py" "s3://$BUCKET/scripts/jobs/02_silver_unificacao.py"
aws s3 cp "$ROOT/jobs/03_gold_indicadores.py" "s3://$BUCKET/scripts/jobs/03_gold_indicadores.py"

create_or_update_job() {
  local JOB_NAME="$1"
  local SCRIPT_KEY="$2"
  local SCRIPT_S3="s3://${BUCKET}/${SCRIPT_KEY}"
  local EXTRA_PY="s3://${BUCKET}/scripts/pipeline_utils.py"

  local COMMON_ARGS=(
    --name "$JOB_NAME"
    --role "$GLUE_ROLE_ARN"
    --glue-version "$GLUE_VERSION"
    --worker-type "$WORKER_TYPE"
    --number-of-workers "$NUMBER_OF_WORKERS"
    --command "Name=glueetl,ScriptLocation=${SCRIPT_S3},PythonVersion=3"
    --default-arguments "{\"--job-language\":\"python\",\"--extra-py-files\":\"${EXTRA_PY}\",\"--BUCKET_NAME\":\"${BUCKET}\",\"--enable-metrics\":\"true\",\"--enable-continuous-cloudwatch-log\":\"true\"}"
    --region "$REGION"
  )

  if aws glue get-job --job-name "$JOB_NAME" --region "$REGION" >/dev/null 2>&1; then
    echo "==> Atualizando job existente: $JOB_NAME"
    # update-job usa --job-update (estrutura diferente)
    aws glue update-job \
      --job-name "$JOB_NAME" \
      --job-update "{
        \"Role\": \"${GLUE_ROLE_ARN}\",
        \"GlueVersion\": \"${GLUE_VERSION}\",
        \"WorkerType\": \"${WORKER_TYPE}\",
        \"NumberOfWorkers\": ${NUMBER_OF_WORKERS},
        \"Command\": {
          \"Name\": \"glueetl\",
          \"ScriptLocation\": \"${SCRIPT_S3}\",
          \"PythonVersion\": \"3\"
        },
        \"DefaultArguments\": {
          \"--job-language\": \"python\",
          \"--extra-py-files\": \"${EXTRA_PY}\",
          \"--BUCKET_NAME\": \"${BUCKET}\",
          \"--enable-metrics\": \"true\",
          \"--enable-continuous-cloudwatch-log\": \"true\"
        }
      }" \
      --region "$REGION" >/dev/null
  else
    echo "==> Criando job: $JOB_NAME"
    aws glue create-job "${COMMON_ARGS[@]}" >/dev/null
  fi
  echo "    OK: $JOB_NAME -> $SCRIPT_S3"
}

create_or_update_job "sod-01-bronze" "scripts/jobs/01_bronze_ingestion.py"
create_or_update_job "sod-02-silver" "scripts/jobs/02_silver_unificacao.py"
create_or_update_job "sod-03-gold"   "scripts/jobs/03_gold_indicadores.py"

echo
echo "Jobs prontos. Para rodar:"
echo "  ./scripts/run_job.sh sod-01-bronze"
echo "  ./scripts/run_job.sh sod-02-silver"
echo "  ./scripts/run_job.sh sod-03-gold"
