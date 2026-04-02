"""
ingestion/apis.py
Clientes para as APIs públicas brasileiras.

Fontes:
  - IBGE SIDRA: PIB trimestral, população
  - Banco Central (BCB SGS): SELIC, IPCA, câmbio USD/BRL
  - IPEA Data: desemprego (PNAD), renda per capita
"""

import requests
import pandas as pd
import logging
import urllib3
from datetime import datetime, date

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

TIMEOUT = 30

# ── Session com SSL resiliente ────────────────────────────────────
# Alguns servidores brasileiros (IBGE, IPEA) têm problemas de handshake SSL
# em containers Docker com OpenSSL moderno. Tentamos primeiro com SSL normal;
# se falhar com SSLError, tentamos com verify=False.
def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "econ-pipeline/1.0 (portfolio project)"})
    return s

SESSION = _make_session()


def _get(url: str) -> requests.Response:
    """GET com fallback automático para verify=False se houver SSLError."""
    try:
        resp = SESSION.get(url, timeout=TIMEOUT, verify=True)
        resp.raise_for_status()
        return resp
    except requests.exceptions.SSLError:
        logger.warning(f"SSLError com verify=True, tentando verify=False: {url[:80]}...")
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        resp = SESSION.get(url, timeout=TIMEOUT, verify=False)
        resp.raise_for_status()
        return resp


# ════════════════════════════════════════════════════════════════
# IBGE SIDRA
# ════════════════════════════════════════════════════════════════
def fetch_ibge_pib(ano_inicio: int = 2015, ano_fim: int = 2023) -> pd.DataFrame:
    """
    PIB — tenta múltiplos endpoints do SIDRA em ordem.
    Tabela 1620 fica instável; usa 5932 (anual) como fallback principal.
    """
    # Candidatos em ordem de preferência
    candidatos = [
        # Tabela 5932 — PIB acumulado anual, variação % (mais estável)
        {
            "url": (
                f"https://servicodados.ibge.gov.br/api/v3/agregados/5932"
                f"/periodos/{'|'.join(str(a) for a in range(ano_inicio, ano_fim+1))}"
                f"/variaveis/6561?localidades=N1[all]"
            ),
            "tipo": "anual",
        },
        # Tabela 1621 — PIB trimestral índice (sem classificacao)
        {
            "url": (
                f"https://servicodados.ibge.gov.br/api/v3/agregados/1621"
                f"/periodos/{ano_inicio}01-{ano_fim}04/variaveis/583"
                f"?localidades=N1[all]"
            ),
            "tipo": "trimestral",
        },
        # Tabela 1620 original — pode retornar 500
        {
            "url": (
                f"https://servicodados.ibge.gov.br/api/v3/agregados/1620"
                f"/periodos/{ano_inicio}01-{ano_fim}04/variaveis/584"
                f"?localidades=N1[all]&classificacao=11255[90707]"
            ),
            "tipo": "trimestral",
        },
    ]

    for candidato in candidatos:
        url  = candidato["url"]
        tipo = candidato["tipo"]
        logger.info(f"IBGE PIB tentando: {url[:80]}...")
        try:
            resp = _get(url)
            data = resp.json()
            if not data or not isinstance(data, list):
                continue

            rows = []
            resultados = data[0].get("resultados", [])
            if not resultados:
                continue

            serie = resultados[0].get("series", [{}])[0].get("serie", {})
            if not serie:
                continue

            for periodo, valor in serie.items():
                if tipo == "anual":
                    ano, trim = int(periodo), None
                    periodo_str = str(ano)
                else:
                    ano  = int(periodo[:4])
                    trim = int(periodo[4:])
                    periodo_str = f"{ano}-T{trim}"

                rows.append({
                    "fonte":     "ibge",
                    "indicador": "pib_variacao_pct",
                    "periodo":   periodo_str,
                    "ano":       ano,
                    "trimestre": trim,
                    "valor":     float(valor) if valor not in ("...", None, "") else None,
                    "unidade":   "%",
                    "dt_carga":  datetime.now().isoformat(),
                })

            if rows:
                df = pd.DataFrame(rows)
                logger.info(f"  → IBGE PIB ({tipo}): {len(df)} registros")
                return df

        except Exception as e:
            logger.warning(f"  Falha ({url[:60]}...): {e}")
            continue

    logger.error("IBGE PIB: todos os endpoints falharam — retornando vazio")
    return pd.DataFrame(columns=[
        "fonte","indicador","periodo","ano","trimestre","valor","unidade","dt_carga"
    ])


def fetch_ibge_populacao() -> pd.DataFrame:
    """Estimativa de população por UF — tenta múltiplos anos."""
    for ano in [2022, 2021, 2020]:
        url = (
            f"https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/{ano}"
            f"/variaveis/9324?localidades=N3[all]"
        )
        logger.info(f"IBGE População {ano}")
        try:
            data = _get(url).json()
            if not data or not data[0].get("resultados"):
                continue
            rows = []
            for resultado in data[0]["resultados"]:
                for serie in resultado.get("series", []):
                    uf    = serie["localidade"]["nome"]
                    valor = list(serie.get("serie", {}).values() or [None])[0]
                    rows.append({
                        "fonte": "ibge", "indicador": "populacao", "uf": uf,
                        "ano": ano,
                        "valor": float(valor) if valor and valor != "..." else None,
                        "unidade": "pessoas", "dt_carga": datetime.now().isoformat(),
                    })
            if rows:
                df = pd.DataFrame(rows)
                logger.info(f"  → {len(df)} UFs")
                return df
        except Exception as e:
            logger.warning(f"  Falha {ano}: {e}")

    logger.error("IBGE População: todas as tentativas falharam")
    return pd.DataFrame(columns=["fonte","indicador","uf","ano","valor","unidade","dt_carga"])


# ════════════════════════════════════════════════════════════════
# BANCO CENTRAL — SGS
# ════════════════════════════════════════════════════════════════
def _fetch_bcb_serie(codigo: int, nome: str, unidade: str,
                     data_inicio: str = "01/01/2015") -> pd.DataFrame:
    hoje = date.today().strftime("%d/%m/%Y")
    url  = (
        f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"
        f"?formato=json&dataInicial={data_inicio}&dataFinal={hoje}"
    )
    logger.info(f"BCB série {codigo} ({nome})")
    data = _get(url).json()

    rows = []
    for item in data:
        dt = pd.to_datetime(item["data"], format="%d/%m/%Y")
        rows.append({
            "fonte": "bcb", "indicador": nome,
            "data": dt.date().isoformat(), "ano": dt.year, "mes": dt.month,
            "valor": float(item["valor"]) if item["valor"] else None,
            "unidade": unidade, "dt_carga": datetime.now().isoformat(),
        })
    df = pd.DataFrame(rows)
    logger.info(f"  → BCB {nome}: {len(df)} registros")
    return df


def fetch_bcb_selic()  -> pd.DataFrame: return _fetch_bcb_serie(4189, "selic_mensal",   "% a.a.")
def fetch_bcb_ipca()   -> pd.DataFrame: return _fetch_bcb_serie(433,  "ipca_mensal",    "%")
def fetch_bcb_cambio() -> pd.DataFrame: return _fetch_bcb_serie(3698, "cambio_usd_brl", "R$/USD")


# ════════════════════════════════════════════════════════════════
# IPEA DATA
# ════════════════════════════════════════════════════════════════
def _fetch_ipea_serie(serie_id: str, nome: str, unidade: str) -> pd.DataFrame:
    url  = f"http://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='{serie_id}')"
    logger.info(f"IPEA série {serie_id} ({nome})")
    data = _get(url).json().get("value", [])

    rows = []
    for item in data:
        val    = item.get("VALVALOR")
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
            "fonte": "ipea", "indicador": nome,
            "data": dt.date().isoformat(), "ano": dt.year, "mes": dt.month,
            "valor": float(val), "unidade": unidade,
            "dt_carga": datetime.now().isoformat(),
        })
    df = pd.DataFrame(rows)
    logger.info(f"  → IPEA {nome}: {len(df)} registros")
    return df


def fetch_ipea_desemprego() -> pd.DataFrame:
    return _fetch_ipea_serie("PNADC12_TDESOC12", "desemprego_pct", "%")


def fetch_ipea_gini() -> pd.DataFrame:
    for serie_id, nome, unidade in [
        ("PNADC12_RDPCHABR12", "renda_media_pc", "R$"),
        ("PNADC12_RDPC12",     "renda_media_pc", "R$"),
        ("GAP_GINI",           "gini",           "índice"),
    ]:
        try:
            df = _fetch_ipea_serie(serie_id, nome, unidade)
            if len(df) > 0:
                return df
        except Exception as e:
            logger.warning(f"  Série {serie_id} falhou: {e}")
    return pd.DataFrame(columns=["fonte","indicador","data","ano","mes","valor","unidade","dt_carga"])


if __name__ == "__main__":
    import os
    os.makedirs("data/raw", exist_ok=True)
    for nome, func in [
        ("ibge_pib",        fetch_ibge_pib),
        ("ibge_populacao",  fetch_ibge_populacao),
        ("bcb_selic",       fetch_bcb_selic),
        ("bcb_ipca",        fetch_bcb_ipca),
        ("bcb_cambio",      fetch_bcb_cambio),
        ("ipea_desemprego", fetch_ipea_desemprego),
        ("ipea_gini",       fetch_ipea_gini),
    ]:
        try:
            df = func()
            df.to_csv(f"data/raw/{nome}.csv", index=False)
            print(f"✅ {nome}: {len(df)} registros")
        except Exception as e:
            print(f"❌ {nome}: {e}")