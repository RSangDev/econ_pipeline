"""
ingestion/apis.py
Clientes para as APIs públicas brasileiras.

Fontes:
  - IBGE SIDRA: PIB trimestral, população
  - Banco Central (BCB SGS): SELIC, IPCA, câmbio USD/BRL
  - IPEA Data: desemprego (PNAD), índice de Gini
"""

import requests
import pandas as pd
import logging
from datetime import datetime, date
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ── Timeouts e retries ────────────────────────────────────────────
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "econ-pipeline/1.0 (portfolio project)"})
TIMEOUT = 30


# ════════════════════════════════════════════════════════════════
# IBGE SIDRA
# ════════════════════════════════════════════════════════════════
def fetch_ibge_pib(ano_inicio: int = 2015, ano_fim: int = 2023) -> pd.DataFrame:
    """
    PIB trimestral a preços de mercado — Tabela 1620 do SIDRA.
    Variação % em relação ao mesmo trimestre do ano anterior.
    """
    url = (
        f"https://servicodados.ibge.gov.br/api/v3/agregados/1620/periodos/"
        f"{ano_inicio}01-{ano_fim}04/variaveis/584"
        f"?localidades=N1[all]&classificacao=11255[90707]"
    )
    logger.info(f"IBGE PIB: {url}")
    resp = SESSION.get(url, timeout=TIMEOUT)
    resp.raise_for_status()
    data = resp.json()

    rows = []
    for item in data[0]["resultados"][0]["series"][0]["serie"].items():
        periodo, valor = item
        ano  = int(periodo[:4])
        trim = int(periodo[4:])
        rows.append({
            "fonte":    "ibge",
            "indicador":"pib_variacao_pct",
            "periodo":  f"{ano}-T{trim}",
            "ano":      ano,
            "trimestre":trim,
            "valor":    float(valor) if valor != "..." else None,
            "unidade":  "%",
            "dt_carga": datetime.now().isoformat(),
        })
    df = pd.DataFrame(rows)
    logger.info(f"  → IBGE PIB: {len(df)} registros")
    return df


def fetch_ibge_populacao() -> pd.DataFrame:
    """Estimativa de população por UF — Tabela 6579."""
    url = (
        "https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/2022"
        "/variaveis/9324?localidades=N3[all]"
    )
    logger.info(f"IBGE População: {url}")
    resp = SESSION.get(url, timeout=TIMEOUT)
    resp.raise_for_status()
    data = resp.json()

    rows = []
    for resultado in data[0]["resultados"]:
        for serie in resultado["series"]:
            uf    = serie["localidade"]["nome"]
            valor = list(serie["serie"].values())[0]
            rows.append({
                "fonte":     "ibge",
                "indicador": "populacao",
                "uf":        uf,
                "ano":       2022,
                "valor":     float(valor) if valor != "..." else None,
                "unidade":   "pessoas",
                "dt_carga":  datetime.now().isoformat(),
            })
    df = pd.DataFrame(rows)
    logger.info(f"  → IBGE População: {len(df)} UFs")
    return df


# ════════════════════════════════════════════════════════════════
# BANCO CENTRAL — SGS (Sistema Gerenciador de Séries Temporais)
# ════════════════════════════════════════════════════════════════
def _fetch_bcb_serie(codigo: int, nome: str, unidade: str,
                     data_inicio: str = "01/01/2015") -> pd.DataFrame:
    """Busca qualquer série do BCB SGS pelo código."""
    hoje = date.today().strftime("%d/%m/%Y")
    url  = (
        f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"
        f"?formato=json&dataInicial={data_inicio}&dataFinal={hoje}"
    )
    logger.info(f"BCB série {codigo} ({nome})")
    resp = SESSION.get(url, timeout=TIMEOUT)
    resp.raise_for_status()
    data = resp.json()

    rows = []
    for item in data:
        dt = pd.to_datetime(item["data"], format="%d/%m/%Y")
        rows.append({
            "fonte":     "bcb",
            "indicador": nome,
            "data":      dt.date().isoformat(),
            "ano":       dt.year,
            "mes":       dt.month,
            "valor":     float(item["valor"]) if item["valor"] else None,
            "unidade":   unidade,
            "dt_carga":  datetime.now().isoformat(),
        })
    df = pd.DataFrame(rows)
    logger.info(f"  → BCB {nome}: {len(df)} registros")
    return df


def fetch_bcb_selic() -> pd.DataFrame:
    """Taxa SELIC mensal — série 4189."""
    return _fetch_bcb_serie(4189, "selic_mensal", "% a.a.")


def fetch_bcb_ipca() -> pd.DataFrame:
    """IPCA mensal — série 433."""
    return _fetch_bcb_serie(433, "ipca_mensal", "%")


def fetch_bcb_cambio() -> pd.DataFrame:
    """Câmbio USD/BRL mensal — série 3698."""
    return _fetch_bcb_serie(3698, "cambio_usd_brl", "R$/USD")


# ════════════════════════════════════════════════════════════════
# IPEA DATA
# ════════════════════════════════════════════════════════════════
def _fetch_ipea_serie(serie_id: str, nome: str, unidade: str) -> pd.DataFrame:
    """Busca série do IPEA Data API."""
    url = f"http://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='{serie_id}')"
    logger.info(f"IPEA série {serie_id} ({nome})")
    resp = SESSION.get(url, timeout=TIMEOUT)
    resp.raise_for_status()
    data = resp.json().get("value", [])

    rows = []
    for item in data:
        val = item.get("VALVALOR")
        dt_raw = item.get("VALDATA", "")
        if not dt_raw or val is None:
            continue
        try:
            dt = pd.to_datetime(dt_raw[:10])
        except Exception:
            continue
        if dt.year < 2010:
            continue
        rows.append({
            "fonte":     "ipea",
            "indicador": nome,
            "data":      dt.date().isoformat(),
            "ano":       dt.year,
            "mes":       dt.month,
            "valor":     float(val),
            "unidade":   unidade,
            "dt_carga":  datetime.now().isoformat(),
        })
    df = pd.DataFrame(rows)
    logger.info(f"  → IPEA {nome}: {len(df)} registros")
    return df


def fetch_ipea_desemprego() -> pd.DataFrame:
    """Taxa de desemprego PNAD — PNADC12_TDESOC12."""
    return _fetch_ipea_serie("PNADC12_TDESOC12", "desemprego_pct", "%")


def fetch_ipea_gini() -> pd.DataFrame:
    """Índice de Gini — PNADC12_RDPC12 (renda per capita como proxy disponível)."""
    return _fetch_ipea_serie("PNADC12_RDPC12", "renda_media_pc", "R$")


# ════════════════════════════════════════════════════════════════
# Runner de testes
# ════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import os
    os.makedirs("data/raw", exist_ok=True)

    funcs = [
        ("ibge_pib",        fetch_ibge_pib),
        ("ibge_populacao",  fetch_ibge_populacao),
        ("bcb_selic",       fetch_bcb_selic),
        ("bcb_ipca",        fetch_bcb_ipca),
        ("bcb_cambio",      fetch_bcb_cambio),
        ("ipea_desemprego", fetch_ipea_desemprego),
        ("ipea_gini",       fetch_ipea_gini),
    ]

    for nome, func in funcs:
        try:
            df = func()
            path = f"data/raw/{nome}.csv"
            df.to_csv(path, index=False)
            print(f"✅ {nome}: {len(df)} registros → {path}")
        except Exception as e:
            print(f"❌ {nome}: {e}")
