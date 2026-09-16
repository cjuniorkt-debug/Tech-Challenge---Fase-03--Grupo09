"""Configuracao central do pipeline na AWS.

Ajuste apenas BUCKET_NAME antes de subir / colar nos notebooks.
"""

# >>> AJUSTE AQUI <<<
BUCKET_NAME = "raw-bkt-806188865054"

BUCKET = f"s3://{BUCKET_NAME}"
RAW_DIR = f"{BUCKET}/raw"
BRONZE_DIR = f"{BUCKET}/bronze"
SILVER_DIR = f"{BUCKET}/silver"
GOLD_DIR = f"{BUCKET}/gold"
DICT_DIR = f"{BUCKET}/dictionary"
SCRIPTS_DIR = f"{BUCKET}/scripts"

# Glue Interactive Session
GLUE_VERSION = "4.0"
WORKER_TYPE = "G.1X"
NUMBER_OF_WORKERS = 2
IDLE_TIMEOUT = 60
