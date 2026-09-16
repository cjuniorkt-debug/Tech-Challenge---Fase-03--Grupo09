#!/usr/bin/env bash
# Dispara um Glue Job e acompanha o status ate terminar.
#
# Uso:
#   ./scripts/run_job.sh sod-01-bronze
#   ./scripts/run_job.sh sod-02-silver
#   ./scripts/run_job.sh sod-03-gold

set -euo pipefail

JOB_NAME="${1:-}"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"

if [[ -z "$JOB_NAME" ]]; then
  echo "Uso: $0 <nome-do-job>"
  echo "Ex.: $0 sod-01-bronze"
  exit 1
fi

echo "==> Iniciando job: $JOB_NAME"
RUN_ID="$(aws glue start-job-run --job-name "$JOB_NAME" --region "$REGION" --query JobRunId --output text)"
echo "    JobRunId: $RUN_ID"

echo "==> Aguardando (pode levar varios minutos)..."
while true; do
  STATE="$(aws glue get-job-run \
    --job-name "$JOB_NAME" \
    --run-id "$RUN_ID" \
    --region "$REGION" \
    --query 'JobRun.JobRunState' \
    --output text)"
  echo "    $(date +%H:%M:%S) status=$STATE"

  case "$STATE" in
    SUCCEEDED)
      echo "OK: $JOB_NAME terminou com sucesso."
      exit 0
      ;;
    FAILED|TIMEOUT|STOPPED|ERROR)
      echo "ERRO: $JOB_NAME finalizou com status $STATE"
      echo "Veja o erro:"
      aws glue get-job-run \
        --job-name "$JOB_NAME" \
        --run-id "$RUN_ID" \
        --region "$REGION" \
        --query 'JobRun.{State:JobRunState,Error:ErrorMessage}' \
        --output table
      echo
      echo "Logs (CloudWatch): /aws-glue/jobs/output  e  /aws-glue/jobs/error"
      exit 1
      ;;
  esac
  sleep 20
done
