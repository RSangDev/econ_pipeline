"""
ingestion/apis.py
Clientes para APIs públicas brasileiras.
BCB é a fonte mais estável — usamos séries BCB como fallback/substituto
para indicadores que o IBGE/IPEA não está entregando.
"""
import requests, pandas as pd, logging, urllib3
from datetime import datetime, date

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
TIMEOUT = 30
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "econ-pipeline/1.0"})


def _get(url: str) -> requests.Response:
    try:
        r = SESSION.get(url, timeout=TIMEOUT, verify=True)
        r.raise_for_status()
        return r
    except requests.exceptions.SSLError:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        r = SESSION.get(url, timeout=TIMEOUT, verify=False)
        r.raise_for_status()
        return r


def _fetch_bcb_serie(codigo: int, nome: str, unidade: str,
                     data_inicio: str = "01/01/2015") -> pd.DataFrame:
    hoje = date.today().strftime("%d/%m/%Y")
    url  = (f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"
            f"?formato=json&dataInicial={data_inicio}&dataFinal={hoje}")
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
    logger.info(f"  → {len(df)} registros")
    return df


# ── IBGE ──────────────────────────────────────────────────────────
def fetch_ibge_pib(ano_inicio: int = 2015, ano_fim: int = 2023) -> pd.DataFrame:
    """
    PIB — tenta IBGE, usa BCB série 4380 como fallback.
    BCB 4380 = PIB acumulado nos últimos 4 trimestres (variação % anual).
    """
    anos_pipe = "|".join(str(a) for a in range(ano_inicio, ano_fim + 1))
    candidatos = [
        f"https://servicodados.ibge.gov.br/api/v3/agregados/5932/periodos/{anos_pipe}/variaveis/6561?localidades=N1[all]",
        f"https://servicodados.ibge.gov.br/api/v3/agregados/5932/periodos/{ano_inicio}-{ano_fim}/variaveis/6561?localidades=N1[all]",
        f"https://servicodados.ibge.gov.br/api/v3/agregados/1621/periodos/{ano_inicio}01-{ano_fim}04/variaveis/583?localidades=N1[all]",
    ]
    for url in candidatos:
        logger.info(f"IBGE PIB: {url[:80]}...")
        try:
            data = _get(url).json()
            if not data or not data[0].get("resultados"):
                continue
            serie = data[0]["resultados"][0]["series"][0].get("serie", {})
            rows = []
            for periodo, valor in serie.items():
                if valor in ("...", None, ""):
                    continue
                ano = int(periodo[:4])
                trim = int(periodo[4:]) if len(periodo) > 4 else None
                rows.append({
                    "fonte": "ibge", "indicador": "pib_variacao_pct",
                    "periodo": f"{ano}-T{trim}" if trim else str(ano),
                    "ano": ano, "trimestre": trim,
                    "valor": float(valor), "unidade": "%",
                    "dt_carga": datetime.now().isoformat(),
                })
            if rows:
                df = pd.DataFrame(rows)
                logger.info(f"  → IBGE PIB: {len(df)} registros")
                return df
        except Exception as e:
            logger.warning(f"  IBGE falhou: {e}")

    # Fallback: BCB série 4380 — PIB acumulado 4 trimestres
    logger.warning("IBGE indisponível → BCB série 4380 (PIB acumulado)")
    df = _fetch_bcb_serie(4380, "pib_variacao_pct", "%")
    return df


def fetch_ibge_populacao() -> pd.DataFrame:
    for ano in [2022, 2021, 2020]:
        url = (f"https://servicodados.ibge.gov.br/api/v3/agregados/6579"
               f"/periodos/{ano}/variaveis/9324?localidades=N3[all]")
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
                        "fonte": "ibge", "indicador": "populacao", "uf": uf, "ano": ano,
                        "valor": float(valor) if valor and valor != "..." else None,
                        "unidade": "pessoas", "dt_carga": datetime.now().isoformat(),
                    })
            if rows:
                df = pd.DataFrame(rows)
                logger.info(f"  → {len(df)} UFs")
                return df
        except Exception as e:
            logger.warning(f"  Falha {ano}: {e}")
    return pd.DataFrame(columns=["fonte","indicador","uf","ano","valor","unidade","dt_carga"])


# ── BCB ───────────────────────────────────────────────────────────
def fetch_bcb_selic()  -> pd.DataFrame: return _fetch_bcb_serie(4189, "selic_mensal",   "% a.a.")
def fetch_bcb_ipca()   -> pd.DataFrame: return _fetch_bcb_serie(433,  "ipca_mensal",    "%")
def fetch_bcb_cambio() -> pd.DataFrame: return _fetch_bcb_serie(3698, "cambio_usd_brl", "R$/USD")


# ── IPEA ──────────────────────────────────────────────────────────
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
    logger.info(f"  → {len(df)} registros")
    return df


def fetch_ipea_desemprego() -> pd.DataFrame:
    return _fetch_ipea_serie("PNADC12_TDESOC12", "desemprego_pct", "%")


def fetch_ipea_gini() -> pd.DataFrame:
    """
    Renda — IPEA frequentemente indisponível.
    Fallback: BCB série 7326 (rendimento médio real do trabalho).
    """
    candidatos_ipea = [
        ("PNADC12_RDPCHABR12", "renda_media_pc", "R$"),
        ("PNADC12_RDPC12",     "renda_media_pc", "R$"),
        ("GAP_GINI",           "gini",           "índice"),
    ]
    for serie_id, nome, unidade in candidatos_ipea:
        try:
            df = _fetch_ipea_serie(serie_id, nome, unidade)
            if len(df) > 0:
                return df
        except Exception as e:
            logger.warning(f"  {serie_id}: {e}")

    # Fallback: BCB série 7326 — rendimento médio real habitual (R$)
    logger.warning("IPEA indisponível → BCB série 7326 (rendimento médio real)")
    try:
        df = _fetch_bcb_serie(7326, "renda_media_pc", "R$")
        if len(df) > 0:
            return df
    except Exception as e:
        logger.warning(f"  BCB 7326 falhou: {e}")

    # Fallback 2: BCB série 28544 — massa salarial real
    logger.warning("BCB 7326 indisponível → BCB série 28544 (massa salarial)")
    try:
        return _fetch_bcb_serie(28544, "renda_media_pc", "R$")
    except Exception as e:
        logger.error(f"Todos os fallbacks de renda falharam: {e}")

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