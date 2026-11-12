from pyspark.sql import Column, functions as F


def formatar_nome(coluna: Column) -> Column:
    """Padroniza a capitalização e mantém preposições em minúsculas."""
    preposicoes = (
        "a", "ao", "aos", "à", "às",
        "da", "das", "de", "do", "dos",
        "em", "na", "nas", "no", "nos",
        "por", "pela", "pelas", "pelo", "pelos",
        "com", "para", "sem",
    )

    nome = F.initcap(F.lower(F.trim(coluna)))

    for preposicao in preposicoes:
        padrao = rf"(?U)(?i)\b{preposicao}\b"
        nome = F.regexp_replace(nome, padrao, preposicao)

    return nome