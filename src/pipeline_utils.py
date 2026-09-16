"""Utilitarios compartilhados pelos notebooks Glue (Bronze/Silver/Gold).

Neste projeto AWS, o arquivo sobe para S3 e e carregado no Glue via:

    %extra_py_files s3://<bucket>/scripts/pipeline_utils.py

Funcoes de SparkSession local / Path do projeto original foram removidas:
no Glue a sessao vem do GlueContext e os paths sao S3.
"""
import re
import unicodedata


def sanitize_colname(raw: str) -> str:
    """Converte cabecalho bruto da pesquisa em identificador seguro
    para Spark / Glue Data Catalog / Athena: somente [a-z0-9_].
    """
    if raw is None:
        return raw
    text = raw.lower()
    norm = unicodedata.normalize('NFKD', text)
    ascii_only = norm.encode('ascii', 'ignore').decode('ascii')
    ascii_only = re.sub(r"[^a-z0-9]+", "_", ascii_only)
    ascii_only = ascii_only.strip('_')
    if not ascii_only:
        ascii_only = 'col'
    if ascii_only[0].isdigit():
        ascii_only = f"c_{ascii_only}"
    if len(ascii_only) > 110:
        cut = ascii_only[:110]
        last_us = cut.rfind('_')
        if last_us > 30:
            cut = cut[:last_us]
        ascii_only = cut
    return ascii_only


def dedupe_names(names):
    """Garante unicidade numa lista de nomes ja sanitizados."""
    seen = {}
    result = []
    for n in names:
        if n not in seen:
            seen[n] = 0
            result.append(n)
        else:
            seen[n] += 1
            result.append(f"{n}_{seen[n]}")
    return result


def explode_multiselect(df, coluna, coluna_saida='opcao'):
    """Explode coluna multi-select (opcoes separadas por virgula) em formato longo."""
    from pyspark.sql import functions as F

    return (
        df
        .withColumn(coluna_saida, F.explode(F.split(F.col(coluna), r',\s*')))
        .withColumn(coluna_saida, F.trim(F.col(coluna_saida)))
        .filter((F.col(coluna_saida).isNotNull()) & (F.col(coluna_saida) != ''))
        .drop(coluna)
    )


def descobrir_vocabulario_multiselect(valores_distintos, marcador_fim='.'):
    """Descobre opcoes atomicas quando as opcoes podem conter virgulas internas."""
    candidatas = sorted(
        {v for v in valores_distintos if f'{marcador_fim}, ' not in v},
        key=len, reverse=True,
    )

    def reconstroi(celula):
        resto = celula
        while resto:
            opcao = next((o for o in candidatas if resto.startswith(o)), None)
            if opcao is None:
                return False
            resto = resto[len(opcao):]
            if resto.startswith(', '):
                resto = resto[2:]
            elif resto:
                return False
        return True

    falhas = [v for v in valores_distintos if not reconstroi(v)]
    if falhas:
        raise ValueError(
            f"Nao foi possivel reconstruir {len(falhas)} valor(es) a partir do "
            f"vocabulario descoberto -- exemplo: {falhas[0]!r}."
        )
    return candidatas


def explode_multiselect_por_vocabulario(df, coluna, vocabulario, coluna_saida='opcao'):
    """Explode multi-select usando vocabulario explicito (sem split ingenuo)."""
    from pyspark.sql import functions as F

    def reconstroi(celula):
        resto = celula
        partes = []
        while resto:
            opcao = next((o for o in vocabulario if resto.startswith(o)), None)
            if opcao is None:
                raise ValueError(f"Valor fora do vocabulario: {celula!r}")
            partes.append(opcao)
            resto = resto[len(opcao):]
            resto = resto[2:] if resto.startswith(', ') else resto
        return partes

    valores_distintos = [r[0] for r in df.select(coluna).distinct().collect() if r[0] is not None]
    mapeamento = {v: reconstroi(v) for v in valores_distintos}

    expr = F.lit(None)
    for valor, opcoes in mapeamento.items():
        expr = F.when(F.col(coluna) == valor, F.array(*[F.lit(o) for o in opcoes])).otherwise(expr)

    return (
        df
        .withColumn(coluna_saida, F.explode(expr))
        .drop(coluna)
    )
