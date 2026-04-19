"""
╔══════════════════════════════════════════════════════════════════════╗
║   BOT GOLS OVER/UNDER v4.3 — MODO AGRESSIVO TURBINADO              ║
╠══════════════════════════════════════════════════════════════════════╣
║  MELHORIAS v4.3 (em relação à v4.2):                                ║
║  ► Threshold reduzido (1.5 → 1.0) — mais sinais por dia            ║
║  ► SCAN_INTERVAL reduzido (120s → 90s)                             ║
║  ► Removido sleep desnecessário entre análises                     ║
║  ► melhor_mercado() reescrito (lógica mais limpa e correta)        ║
║  ► Re-alerta com break — não dispara duas janelas no mesmo ciclo   ║
║  ► coletar_resultados() roda a cada 10min, não a cada varredura    ║
║  ► Limite de análises por ciclo aumentado (50 → 100)               ║
║  ► Logs de diagnóstico mostram motivo de descarte                  ║
║  ► Tier VIGIAR alinhado com MIN_SCORE_ALERTA                       ║
╚══════════════════════════════════════════════════════════════════════╝

  Controles disponíveis no terminal durante a execução:
  ────────────────────────────────────────────────────
  PAUSAR  → suspende as varreduras (não consome requests da API)
  RETOMAR → retoma as varreduras normalmente
  STATUS  → exibe situação atual do bot
  SAIR    → encerra o bot com segurança
"""

import requests
import time
import json
import os
import csv
import math
import logging
import threading
from datetime import datetime, timedelta
from scipy.stats import poisson

# ─────────────────────────────────────────────────────────────────────
#  CREDENCIAIS
# ─────────────────────────────────────────────────────────────────────
import os

def _load_env(path=".env"):
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            for linha in f:
                linha = linha.strip()
                if linha and not linha.startswith("#") and "=" in linha:
                    k, _, v = linha.partition("=")
                    os.environ.setdefault(k.strip(), v.strip())

_load_env()

# Token específico do bot de gols (variável TELEGRAM_TOKEN_GOLS no Railway)
# Se não existir, usa o token principal como fallback
API_FOOTBALL_KEY = os.environ.get("API_FOOTBALL_KEY", "")
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN_GOLS") or os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# ─────────────────────────────────────────────────────────────────────
#  CONFIGURAÇÕES GERAIS  (v4.3 — ajustadas para mais sinais)
# ─────────────────────────────────────────────────────────────────────
SCAN_INTERVAL       = 90      # ↓ era 120s
MIN_SCORE_ALERTA    = 1.0     # ↓ era 1.5 — modo realmente agressivo
MIN_SCORE_REALERTA  = 2.0     # ↓ era 2.5
MAX_JOGOS_DIA       = 30
MAX_JOGOS_WATCHLIST = 20
MAX_EXPOSICAO_PCT   = 40.0
BANKROLL            = 1000.0
KELLY_FRACTION      = 0.15
STAKE_MINIMA        = 5.0

# Intervalo entre coletas de resultados (não roda em toda varredura)
INTERVALO_COLETA_RESULTADOS = 600  # 10 minutos

# Arquivos de dados
ARQ_LOG             = "bot_gols_v4.log"
ARQ_STATE           = "state_v4.json"
ARQ_HISTORICO       = "historico_v4.csv"
ARQ_PENDENTES       = "pending_v4.json"
ARQ_CACHE_STATS     = "cache_stats_v4.json"
ARQ_CACHE_H2H       = "cache_h2h_v4.json"
ARQ_CACHE_FIX       = "cache_fixtures_v4.json"
ARQ_SINAIS_ENVIADOS = "sinais_enviados_v4.json"

# TTL dos caches
TTL_STATS    = 6  * 3600
TTL_H2H      = 12 * 3600
TTL_FIXTURES = 30 * 60

# Controle de requests da API
API_LIMITE_MINIMO = 50

# Horários das watchlists diárias
WATCHLIST_HORAS = [7, 9, 12, 15, 18]

# Re-alertas antes do jogo (em minutos)
REALERTAS_MIN = [120, 60, 30]

# ─────────────────────────────────────────────────────────────────────
#  TIERS DE CONFIANÇA  (v4.3 — VIGIAR alinhado com MIN_SCORE_ALERTA)
# ─────────────────────────────────────────────────────────────────────
TIER_ELITE_MIN  = 5.0
TIER_QUENTE_MIN = 3.5
TIER_MORNO_MIN  = 2.5
TIER_VIGIAR_MIN = 1.0     # ↓ era 1.5

STAKE_LIMITE = {
    "ELITE":  0.05,
    "QUENTE": 0.035,
    "MORNO":  0.02,
    "VIGIAR": 0.01,
}

# ─────────────────────────────────────────────────────────────────────
#  ODDS DE REFERÊNCIA
# ─────────────────────────────────────────────────────────────────────
ODDS_REF = {
    "over_0_5":  1.12,
    "over_1_5":  1.38,
    "over_2_5":  1.82,
    "over_3_5":  2.45,
    "over_4_5":  3.80,
    "under_1_5": 2.75,
    "under_2_5": 2.05,
    "under_3_5": 1.50,
    "ambas_sim": 1.85,
    "ambas_nao": 1.90,
}

# ─────────────────────────────────────────────────────────────────────
#  CONFIGURAÇÕES POR LIGA
# ─────────────────────────────────────────────────────────────────────
LIGAS_CFG = {
    71:  ("Brasileirão Série A",  1.18, 1.52, 1.08),
    72:  ("Brasileirão Série B",  1.15, 1.38, 1.02),
    73:  ("Brasileirão Série C",  1.15, 1.30, 0.98),
    74:  ("Brasileirão Série D",  1.12, 1.25, 0.95),
    75:  ("Copa do Brasil",       1.10, 1.35, 1.00),
    128: ("Liga Argentina",       1.20, 1.55, 1.12),
    131: ("Copa Argentina",       1.10, 1.40, 1.05),
    13:  ("Libertadores",         1.10, 1.45, 1.05),
    11:  ("Sul-Americana",        1.10, 1.35, 0.98),
    239: ("Chile Primera",        1.15, 1.42, 1.05),
    242: ("Liga Colombiana",      1.18, 1.45, 1.08),
    244: ("Equador Série A",      1.15, 1.40, 1.05),
    246: ("Peru Liga 1",          1.15, 1.35, 1.00),
    268: ("Bolívia Liga",         1.12, 1.30, 0.98),
    140: ("La Liga",              1.12, 1.62, 1.18),
    61:  ("Ligue 1",              1.14, 1.55, 1.15),
    135: ("Série A Italiana",     1.12, 1.48, 1.08),
    39:  ("Premier League",       1.08, 1.58, 1.22),
    78:  ("Bundesliga",           1.10, 1.72, 1.28),
    88:  ("Eredivisie",           1.10, 1.85, 1.35),
    94:  ("Primeira Liga (PT)",   1.15, 1.48, 1.08),
    203: ("Süper Lig",            1.18, 1.52, 1.12),
    144: ("Jupiler Pro",          1.10, 1.65, 1.22),
    119: ("Superliga Dinamarca",  1.10, 1.55, 1.15),
    113: ("Allsvenskan",          1.10, 1.45, 1.08),
    103: ("Eliteserien",          1.10, 1.50, 1.10),
    107: ("Ekstraklasa",          1.12, 1.42, 1.05),
    218: ("Bundesliga Áustria",   1.10, 1.60, 1.18),
    197: ("Super League Suíça",   1.10, 1.45, 1.08),
    210: ("Superliga Sérvia",     1.15, 1.45, 1.05),
    169: ("Premier League UCR",   1.12, 1.42, 1.05),
    179: ("Scottish Premiership", 1.10, 1.55, 1.12),
    172: ("Liga I Romena",        1.12, 1.38, 1.02),
    182: ("Super League GRE",     1.15, 1.40, 1.05),
    2:   ("Liga dos Campeões",    1.08, 1.52, 1.10),
    3:   ("Liga Europa",          1.08, 1.42, 1.05),
    848: ("Conference League",    1.08, 1.38, 1.02),
    307: ("Saudi Pro League",     1.20, 1.65, 1.25),
    292: ("K-League",             1.12, 1.42, 1.05),
    98:  ("J-League",             1.10, 1.45, 1.08),
    253: ("MLS",                  1.12, 1.42, 1.08),
    262: ("Liga MX",              1.18, 1.48, 1.08),
    30:  ("AFC Champions",        1.08, 1.38, 1.02),
}

CLASSICOS = [
    ("flamengo","fluminense"), ("flamengo","vasco"), ("flamengo","botafogo"),
    ("corinthians","palmeiras"), ("corinthians","sao paulo"),
    ("sao paulo","palmeiras"), ("gremio","internacional"),
    ("atletico","cruzeiro"), ("river","boca"),
    ("real madrid","barcelona"), ("milan","inter"),
    ("roma","lazio"), ("psg","marseille"),
    ("liverpool","everton"), ("manchester united","manchester city"),
    ("arsenal","tottenham"), ("bayern","dortmund"),
]

# ─────────────────────────────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(ARQ_LOG, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────
#  CONTROLE DE PAUSA/RETOMADA (thread-safe)
# ─────────────────────────────────────────────────────────────────────
_pausado      = False
_encerrar     = False
_lock_pausa   = threading.Lock()
_ts_pausa     = None

# Contadores de diagnóstico (resetados a cada varredura)
_diag_descartes = {"score_baixo": 0, "tier_frio": 0, "sem_mercado": 0, "sem_dados": 0}

def esta_pausado() -> bool:
    with _lock_pausa:
        return _pausado

def pausar():
    global _pausado, _ts_pausa
    with _lock_pausa:
        if not _pausado:
            _pausado  = True
            _ts_pausa = datetime.now()
            log.info("=" * 52)
            log.info("  BOT PAUSADO — varreduras suspensas.")
            log.info("  Digite RETOMAR para continuar.")
            log.info("=" * 52)
            tg(
                "⏸ BOT PAUSADO\n"
                "As varreduras foram suspensas e a API não será consultada.\n"
                "Digite RETOMAR no terminal para continuar."
            )
        else:
            log.info("Bot já está pausado.")

def retomar():
    global _pausado, _ts_pausa
    with _lock_pausa:
        if _pausado:
            duracao = ""
            if _ts_pausa:
                seg     = int((datetime.now() - _ts_pausa).total_seconds())
                duracao = f" (pausado por {seg // 60}min {seg % 60}s)"
            _pausado  = False
            _ts_pausa = None
            log.info("=" * 52)
            log.info(f"  BOT RETOMADO{duracao}")
            log.info("=" * 52)
            tg(f"▶ BOT RETOMADO{duracao}\nVarreduras ativas normalmente.")
        else:
            log.info("Bot já está em execução.")

def exibir_status():
    estado = "PAUSADO ⏸" if esta_pausado() else "ATIVO ▶"
    log.info("=" * 52)
    log.info(f"  STATUS: {estado}")
    log.info(f"  Requests API restantes: {_api_requests_restantes}")
    log.info(f"  Sinais enviados hoje:   {len(_sinais_enviados_hoje)}")
    log.info(f"  Data/hora:              {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    log.info("=" * 52)

def _thread_terminal():
    """Thread paralela que lê comandos do terminal enquanto o bot roda."""
    global _encerrar
    print("\n" + "=" * 52)
    print("  CONTROLES DO BOT:")
    print("  PAUSAR  → suspende as varreduras")
    print("  RETOMAR → retoma as varreduras")
    print("  STATUS  → exibe situação atual")
    print("  SAIR    → encerra o bot com segurança")
    print("=" * 52 + "\n")

    while not _encerrar:
        try:
            cmd = input().strip().upper()
            if cmd == "PAUSAR":
                pausar()
            elif cmd == "RETOMAR":
                retomar()
            elif cmd == "STATUS":
                exibir_status()
            elif cmd == "SAIR":
                log.info("Encerrando por comando do terminal...")
                _encerrar = True
                break
            elif cmd:
                print("  Comandos: PAUSAR | RETOMAR | STATUS | SAIR")
        except (EOFError, KeyboardInterrupt):
            break

# ─────────────────────────────────────────────────────────────────────
#  CONTROLE GLOBAL DE REQUESTS E CACHE DE SINAIS
# ─────────────────────────────────────────────────────────────────────
_api_requests_restantes = 999
_aviso_api_enviado      = False
_sinais_enviados_hoje   = set()

def _pode_chamar_api(minimo: int = 10) -> bool:
    return _api_requests_restantes > minimo

# ─────────────────────────────────────────────────────────────────────
#  HELPERS DE ARQUIVO
# ─────────────────────────────────────────────────────────────────────
def _load(path: str) -> dict:
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def _save(path: str, data):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.warning(f"Erro ao salvar {path}: {e}")

def _purge(cache: dict, ttl: int) -> dict:
    agora = time.time()
    return {k: v for k, v in cache.items()
            if isinstance(v, dict) and agora - v.get("_ts", 0) < ttl}

def _hoje() -> str:
    return datetime.now().strftime("%Y-%m-%d")

# ─────────────────────────────────────────────────────────────────────
#  ANTI-DUPLICATA PERSISTENTE
# ─────────────────────────────────────────────────────────────────────
def _carregar_sinais_enviados():
    global _sinais_enviados_hoje
    dados = _load(ARQ_SINAIS_ENVIADOS)
    hoje  = _hoje()
    lista_hoje = dados.get(hoje, [])
    _sinais_enviados_hoje = set(lista_hoje)
    _save(ARQ_SINAIS_ENVIADOS, {hoje: lista_hoje})
    log.info(f"Anti-duplicata: {len(_sinais_enviados_hoje)} sinal(is) já registrado(s) hoje.")

def _registrar_sinal(chave: str):
    global _sinais_enviados_hoje
    _sinais_enviados_hoje.add(chave)
    dados = _load(ARQ_SINAIS_ENVIADOS)
    hoje  = _hoje()
    lista = dados.get(hoje, [])
    if chave not in lista:
        lista.append(chave)
    dados[hoje] = lista
    _save(ARQ_SINAIS_ENVIADOS, dados)

def _ja_enviado(chave: str) -> bool:
    return chave in _sinais_enviados_hoje

def _chave_sinal(fid, sufixo: str = "") -> str:
    return f"{fid}{sufixo}_{_hoje()}"

# ─────────────────────────────────────────────────────────────────────
#  TELEGRAM
# ─────────────────────────────────────────────────────────────────────
def tg(msg: str) -> bool:
    if not msg:
        return False
    ok = True
    for trecho in [msg[i:i + 3900] for i in range(0, len(msg), 3900)]:
        try:
            r = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": trecho,
                    "disable_web_page_preview": True,
                },
                timeout=10,
            )
            if r.status_code != 200:
                log.warning(f"Telegram {r.status_code}: {r.text[:120]}")
                ok = False
            time.sleep(0.3)
        except Exception as e:
            log.error(f"Telegram erro: {e}")
            ok = False
    return ok

def testar_telegram() -> bool:
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getMe",
            timeout=10,
        )
        if r.status_code == 200:
            log.info(f"Telegram conectado: @{r.json()['result']['username']}")
            return True
        log.error(f"Telegram falhou: {r.status_code}")
        return False
    except Exception as e:
        log.error(f"Telegram: {e}")
        return False

# ─────────────────────────────────────────────────────────────────────
#  API-FOOTBALL
# ─────────────────────────────────────────────────────────────────────
HOST_API = "v3.football.api-sports.io"

def _api(endpoint: str, params: dict, timeout: int = 15):
    global _api_requests_restantes, _aviso_api_enviado

    if _api_requests_restantes <= 5:
        log.error(f"API esgotada ({_api_requests_restantes} restantes). Usando somente cache.")
        return None

    try:
        r = requests.get(
            f"https://{HOST_API}/{endpoint}",
            headers={"x-apisports-key": API_FOOTBALL_KEY},
            params=params,
            timeout=timeout,
        )
        if r.status_code == 429:
            log.warning("Rate limit atingido — aguardando 70s...")
            time.sleep(70)
            return None
        if r.status_code in (401, 403):
            log.error(f"Erro de autenticação: {r.status_code}")
            return None
        if r.status_code != 200:
            return None

        rem = r.headers.get("x-ratelimit-requests-remaining")
        if rem is not None:
            try:
                _api_requests_restantes = int(rem)
                if _api_requests_restantes < API_LIMITE_MINIMO:
                    log.warning(f"ATENÇÃO: {_api_requests_restantes} requests restantes hoje!")
                    if _api_requests_restantes < 30 and not _aviso_api_enviado:
                        tg(
                            f"⚠️ AVISO DE API\n"
                            f"Restam apenas {_api_requests_restantes} requests hoje.\n"
                            f"O bot operará somente com dados em cache até meia-noite."
                        )
                        _aviso_api_enviado = True
            except Exception:
                pass

        return r.json().get("response")

    except requests.exceptions.Timeout:
        log.warning(f"Timeout na API: {endpoint}")
        return None
    except Exception as e:
        log.debug(f"Erro na API: {e}")
        return None

# ─────────────────────────────────────────────────────────────────────
#  FIXTURES (com cache de 30 minutos)
# ─────────────────────────────────────────────────────────────────────
def buscar_fixtures(data_str: str) -> list:
    cache = _load(ARQ_CACHE_FIX)
    agora = time.time()

    if data_str in cache:
        entrada = cache[data_str]
        idade   = agora - entrada.get("_ts", 0)
        if idade < TTL_FIXTURES:
            log.info(
                f"Fixtures {data_str}: cache válido "
                f"({int(idade / 60)}min atrás, {len(entrada['data'])} partidas)"
            )
            return entrada["data"]

    if not _pode_chamar_api(minimo=20):
        log.warning(f"Fixtures {data_str}: requests insuficientes, usando cache anterior")
        return cache.get(data_str, {}).get("data", [])

    log.info(f"Buscando fixtures de {data_str} na API...")
    dados = _api("fixtures", {"date": data_str}) or []
    log.info(f"  → {len(dados)} partidas encontradas")

    if dados:
        por_liga     = {
            f.get("league", {}).get("id", 0): f.get("league", {}).get("name", "?")
            for f in dados
        }
        mapeadas     = [lid for lid in por_liga if lid in LIGAS_CFG]
        nao_mapeadas = [(lid, nm) for lid, nm in por_liga.items() if lid not in LIGAS_CFG]
        log.info(f"  Ligas mapeadas: {len(mapeadas)} | Não mapeadas: {len(nao_mapeadas)}")

        cache[data_str] = {"data": dados, "_ts": agora}
        cache = {k: v for k, v in cache.items() if agora - v.get("_ts", 0) < 3 * 86400}
        _save(ARQ_CACHE_FIX, cache)

    return dados

def buscar_resultado(fid: int):
    if not _pode_chamar_api(minimo=5):
        return None
    dados = _api("fixtures", {"id": fid})
    return dados[0] if dados else None

# ─────────────────────────────────────────────────────────────────────
#  ESTATÍSTICAS DOS TIMES
# ─────────────────────────────────────────────────────────────────────
def buscar_stats(team_id, liga_id, temporada) -> dict:
    chave = f"{team_id}-{liga_id}-{temporada}"
    cache = _purge(_load(ARQ_CACHE_STATS), TTL_STATS)

    if chave in cache:
        return cache[chave]

    if not _pode_chamar_api(minimo=15):
        log.debug(f"Stats {chave}: sem requests, usando padrão")
        return _stats_padrao()

    dados = _api("teams/statistics", {"team": team_id, "league": liga_id, "season": temporada})
    if not dados:
        return _stats_padrao()

    gols  = dados.get("goals", {}) or {}
    fix   = dados.get("fixtures", {}) or {}
    forma = dados.get("form", "") or ""

    def _g(d, *ks, dv=0):
        for k in ks:
            if not isinstance(d, dict): return dv
            d = d.get(k, dv)
            if d is None: return dv
        return d or dv

    ph = max(_g(fix, "played", "home"), 1)
    pa = max(_g(fix, "played", "away"), 1)
    pt = ph + pa

    gfh = max(_g(gols, "for",     "total", "home"), 0.1 * ph)
    gfa = max(_g(gols, "for",     "total", "away"), 0.1 * pa)
    gah = max(_g(gols, "against", "total", "home"), 0.5 * ph)
    gaa = max(_g(gols, "against", "total", "away"), 0.5 * pa)

    cs  = _g(dados.get("clean_sheet") or {}, "total")
    fts = _g(dados.get("failed_to_score") or {}, "total")

    forma_r = forma[-5:] if forma else ""
    pesos   = [0.10, 0.15, 0.20, 0.25, 0.30]
    fs = sum(
        pesos[i] * (1 if c == "W" else -1 if c == "L" else 0)
        for i, c in enumerate(reversed(forma_r[:5]))
    )

    fmin = (gols.get("for") or {}).get("minute") or {}
    p1 = p2 = 0
    for per, val in fmin.items():
        try:
            g  = (val or {}).get("total") or 0
            mx = int(str(per).split("-")[-1].replace("+", "").strip())
            if mx <= 45: p1 += g
            else:        p2 += g
        except Exception:
            pass
    momentum = round((p2 - p1) / (p1 + p2), 3) if (p1 + p2) > 0 else 0.0

    p_marca = 1 - math.exp(-((gfh + gfa) / pt))
    p_sofre = 1 - math.exp(-((gah + gaa) / pt))

    resultado = {
        "gf_h":     round(gfh / ph, 3),
        "gf_a":     round(gfa / pa, 3),
        "ga_h":     round(gah / ph, 3),
        "ga_a":     round(gaa / pa, 3),
        "gf_t":     round((gfh + gfa) / pt, 3),
        "ga_t":     round((gah + gaa) / pt, 3),
        "ph": ph, "pa": pa, "pt": pt,
        "cs_rate":  round(cs  / pt, 3) if pt else 0.25,
        "fts_rate": round(fts / pt, 3) if pt else 0.25,
        "btts_est": round(p_marca * p_sofre, 3),
        "momentum": momentum,
        "fs":       round(fs, 3),
        "forma":    forma_r,
        "_ts":      time.time(),
    }
    cache[chave] = resultado
    _save(ARQ_CACHE_STATS, cache)
    return resultado

def _stats_padrao() -> dict:
    return {
        "gf_h": 1.40, "gf_a": 1.05, "ga_h": 1.05, "ga_a": 1.40,
        "gf_t": 1.25, "ga_t": 1.25,
        "ph": 0, "pa": 0, "pt": 0,
        "cs_rate": 0.25, "fts_rate": 0.25, "btts_est": 0.52,
        "momentum": 0.0, "fs": 0.0, "forma": "",
        "_ts": time.time(),
    }

# ─────────────────────────────────────────────────────────────────────
#  HEAD-TO-HEAD
# ─────────────────────────────────────────────────────────────────────
def buscar_h2h(hid, aid):
    chave = f"{hid}-{aid}"
    cache = _purge(_load(ARQ_CACHE_H2H), TTL_H2H)

    if chave in cache:
        return cache[chave]

    if not _pode_chamar_api(minimo=10):
        log.debug(f"H2H {chave}: sem requests, pulando")
        return None

    dados = _api("fixtures/headtohead", {"h2h": f"{hid}-{aid}", "last": 10})
    if not dados:
        return None

    lista_gols = []
    o15 = o25 = o35 = ambas = n = 0
    for fx in dados:
        st = (fx.get("fixture", {}).get("status", {}) or {}).get("short", "")
        if st not in ("FT", "AET", "PEN"):
            continue
        g  = fx.get("goals", {}) or {}
        hg = g.get("home") or 0
        ag = g.get("away") or 0
        t  = hg + ag
        lista_gols.append(t)
        n += 1
        if t > 1.5: o15 += 1
        if t > 2.5: o25 += 1
        if t > 3.5: o35 += 1
        if hg > 0 and ag > 0: ambas += 1

    if n == 0:
        return None

    media     = sum(lista_gols) / n
    tendencia = 0.0
    if len(lista_gols) >= 5:
        tendencia = round(
            sum(lista_gols[:3]) / 3 - sum(lista_gols[3:]) / len(lista_gols[3:]), 2
        )

    r = {
        "n":         n,
        "media":     round(media, 2),
        "o15":       round(o15   / n, 2),
        "o25":       round(o25   / n, 2),
        "o35":       round(o35   / n, 2),
        "ambas":     round(ambas / n, 2),
        "tendencia": tendencia,
        "_ts":       time.time(),
    }
    cache[chave] = r
    _save(ARQ_CACHE_H2H, cache)
    return r

# ─────────────────────────────────────────────────────────────────────
#  MODELO POISSON
# ─────────────────────────────────────────────────────────────────────
def calcular_lambdas(hs, as_, lid):
    cfg             = LIGAS_CFG.get(lid, ("?", 1.12, 1.45, 1.05))
    _, vc, mgfc, mgac = cfg
    mgff = mgac
    mgaf = mgfc * 0.88

    ah = hs["gf_h"] / mgfc if mgfc > 0 else 1.0
    aa = as_["gf_a"] / mgff if mgff > 0 else 1.0
    dh = hs["ga_h"] / mgaf  if mgaf > 0 else 1.0
    da = as_["ga_a"] / mgfc  if mgfc > 0 else 1.0

    lh = ah * da * mgfc * vc
    la = aa * dh * mgff

    lh *= (1 + hs.get("momentum", 0) * 0.08)
    la *= (1 + as_.get("momentum", 0) * 0.08)
    lh *= (1 + hs.get("fs", 0) * 0.06)
    la *= (1 + as_.get("fs", 0) * 0.06)

    return round(max(0.30, min(lh, 5.5)), 4), round(max(0.20, min(la, 4.5)), 4)

def calcular_probs(lh, la) -> dict:
    lt = lh + la
    p = {
        "o05":      float(1 - poisson.pmf(0, lt)),
        "o15":      float(1 - poisson.cdf(1, lt)),
        "o25":      float(1 - poisson.cdf(2, lt)),
        "o35":      float(1 - poisson.cdf(3, lt)),
        "o45":      float(1 - poisson.cdf(4, lt)),
        "u15":      float(poisson.cdf(1, lt)),
        "u25":      float(poisson.cdf(2, lt)),
        "u35":      float(poisson.cdf(3, lt)),
        "ambas_sim": float((1 - poisson.pmf(0, lh)) * (1 - poisson.pmf(0, la))),
    }
    p["ambas_nao"] = 1.0 - p["ambas_sim"]
    p["lh"] = lh
    p["la"] = la
    p["lt"] = round(lt, 4)
    return p

def kelly(prob: float, odd: float) -> float:
    if odd <= 1.0 or prob <= 0:
        return 0.0
    b  = odd - 1.0
    ev = b * prob - (1 - prob)
    if ev <= 0:
        return 0.0
    return round(BANKROLL * (ev / b) * KELLY_FRACTION, 2)

# ─────────────────────────────────────────────────────────────────────
#  PONTUAÇÃO DO JOGO
# ─────────────────────────────────────────────────────────────────────
def pontuar_jogo(hs, as_, p, h2h, classico, decisivo):
    s = 0.0
    det = []
    lt  = p["lt"]
    p25 = p["o25"]
    bt  = p["ambas_sim"]

    if   lt >= 3.2: s += 3.0; det.append(f"Lambda={lt:.2f}(+3.0)")
    elif lt >= 2.6: s += 2.5; det.append(f"Lambda={lt:.2f}(+2.5)")
    elif lt >= 2.0: s += 2.0; det.append(f"Lambda={lt:.2f}(+2.0)")
    elif lt >= 1.5: s += 1.5; det.append(f"Lambda={lt:.2f}(+1.5)")
    elif lt >= 1.0: s += 1.0; det.append(f"Lambda={lt:.2f}(+1.0)")
    else:           s += 0.5; det.append(f"Lambda={lt:.2f}(+0.5)")

    if   p25 >= 0.60: s += 2.0; det.append(f"+2.5={p25*100:.0f}%(+2.0)")
    elif p25 >= 0.50: s += 1.5; det.append(f"+2.5={p25*100:.0f}%(+1.5)")
    elif p25 >= 0.40: s += 1.0; det.append(f"+2.5={p25*100:.0f}%(+1.0)")
    elif p25 >= 0.30: s += 0.5; det.append(f"+2.5={p25*100:.0f}%(+0.5)")

    if   bt >= 0.58: s += 1.5; det.append(f"Ambas={bt*100:.0f}%(+1.5)")
    elif bt >= 0.48: s += 1.0; det.append(f"Ambas={bt*100:.0f}%(+1.0)")
    elif bt >= 0.38: s += 0.5; det.append(f"Ambas={bt*100:.0f}%(+0.5)")

    if h2h:
        if   h2h["o25"] >= 0.60: s += 1.5; det.append(f"H2H={h2h['o25']*100:.0f}%(+1.5)")
        elif h2h["o25"] >= 0.45: s += 1.0; det.append(f"H2H={h2h['o25']*100:.0f}%(+1.0)")
        elif h2h["o25"] >= 0.33: s += 0.5; det.append(f"H2H={h2h['o25']*100:.0f}%(+0.5)")
        if h2h["tendencia"] > 0.3: s += 0.5; det.append("Tendência H2H(+0.5)")

    cs_medio = (hs["cs_rate"] + as_["cs_rate"]) / 2
    if   cs_medio < 0.15: s += 1.0; det.append(f"SG={cs_medio*100:.0f}%(+1.0)")
    elif cs_medio < 0.25: s += 0.5; det.append(f"SG={cs_medio*100:.0f}%(+0.5)")

    if classico: s *= 0.92; det.append("Clássico x0.92")
    if decisivo: s += 0.30; det.append("Decisivo +0.3")

    return round(s, 2), det

def obter_tier(pontuacao: float):
    if   pontuacao >= TIER_ELITE_MIN:  return "ELITE",  STAKE_LIMITE["ELITE"]
    elif pontuacao >= TIER_QUENTE_MIN: return "QUENTE", STAKE_LIMITE["QUENTE"]
    elif pontuacao >= TIER_MORNO_MIN:  return "MORNO",  STAKE_LIMITE["MORNO"]
    elif pontuacao >= TIER_VIGIAR_MIN: return "VIGIAR", STAKE_LIMITE["VIGIAR"]
    return "FRIO", 0

# ─────────────────────────────────────────────────────────────────────
#  MELHOR MERCADO  (v4.3 — reescrito, lógica limpa)
# ─────────────────────────────────────────────────────────────────────
def melhor_mercado(p: dict):
    """
    Avalia todos os mercados e retorna o de maior EV positivo.
    Aplica um pequeno bônus para 'Mais de 2.5' (mercado mais líquido
    e fácil de encontrar boa odd nas casas).
    """
    mapa = {
        "over_0_5":  p["o05"], "over_1_5": p["o15"],
        "over_2_5":  p["o25"], "over_3_5": p["o35"], "over_4_5": p["o45"],
        "under_1_5": p["u15"], "under_2_5": p["u25"], "under_3_5": p["u35"],
        "ambas_sim": p["ambas_sim"], "ambas_nao": p["ambas_nao"],
    }

    candidatos = []
    for chave, odd in ODDS_REF.items():
        prob = mapa.get(chave, 0)
        if prob <= 0:
            continue
        ev   = prob * (odd - 1) - (1 - prob)
        if ev <= 0:
            continue
        candidatos.append({
            "chave": chave,
            "nome":  _nome_mercado(chave),
            "prob":  prob,
            "odd":   odd,
            "ev":    round(ev, 4),
            "edge":  round(prob - (1 / odd), 4),
            "stake": kelly(prob, odd),
            # EV ajustado: bônus para over 2.5 (mercado mais líquido)
            "ev_ajustado": ev + (0.02 if chave == "over_2_5" else 0.0),
        })

    if not candidatos:
        return None

    melhor = max(candidatos, key=lambda c: c["ev_ajustado"])
    # Remove a chave auxiliar antes de retornar
    melhor.pop("ev_ajustado", None)
    return melhor

def _nome_mercado(k: str) -> str:
    nomes = {
        "over_0_5":  "Mais de 0.5 gols",
        "over_1_5":  "Mais de 1.5 gols",
        "over_2_5":  "Mais de 2.5 gols",
        "over_3_5":  "Mais de 3.5 gols",
        "over_4_5":  "Mais de 4.5 gols",
        "under_1_5": "Menos de 1.5 gols",
        "under_2_5": "Menos de 2.5 gols",
        "under_3_5": "Menos de 3.5 gols",
        "ambas_sim": "Ambas marcam — Sim",
        "ambas_nao": "Ambas marcam — Não",
    }
    return nomes.get(k, k)

def _linha_mercado(k: str) -> float:
    try:
        partes = k.split("_")
        return float(f"{partes[-2]}.{partes[-1]}")
    except Exception:
        return 2.5

# ─────────────────────────────────────────────────────────────────────
#  ANÁLISE DE UMA PARTIDA  (v4.3 — com diagnóstico de descartes)
# ─────────────────────────────────────────────────────────────────────
def analisar_partida(fx) -> dict | None:
    global _diag_descartes
    try:
        fix = fx.get("fixture", {})
        tm  = fx.get("teams",   {})
        lg  = fx.get("league",  {})

        fid = fix.get("id")
        lid = lg.get("id")
        if not fid or not lid:
            _diag_descartes["sem_dados"] += 1
            return None

        temporada = lg.get("season", datetime.now().year)
        rodada    = lg.get("round", "")
        hid       = tm.get("home", {}).get("id")
        aid       = tm.get("away", {}).get("id")
        hnm       = tm.get("home", {}).get("name", "?")
        anm       = tm.get("away", {}).get("name", "?")
        if not hid or not aid:
            _diag_descartes["sem_dados"] += 1
            return None

        hs  = buscar_stats(hid, lid, temporada)
        as_ = buscar_stats(aid, lid, temporada)
        h2h = buscar_h2h(hid, aid)

        classico = any(
            (t1 in hnm.lower() and t2 in anm.lower()) or
            (t1 in anm.lower() and t2 in hnm.lower())
            for t1, t2 in CLASSICOS
        )
        decisivo = any(
            k in rodada.lower()
            for k in ["final", "semi", "quart", "oitav", "playoff", "decis", "mata"]
        )

        lh, la  = calcular_lambdas(hs, as_, lid)
        p       = calcular_probs(lh, la)
        sc, det = pontuar_jogo(hs, as_, p, h2h, classico, decisivo)

        if sc < MIN_SCORE_ALERTA:
            _diag_descartes["score_baixo"] += 1
            log.debug(f"Descartado [score baixo] {hnm} x {anm}: score={sc:.2f} < {MIN_SCORE_ALERTA}")
            return None

        mkt = melhor_mercado(p)
        if not mkt:
            _diag_descartes["sem_mercado"] += 1
            log.debug(f"Descartado [sem mercado +EV] {hnm} x {anm}: lt={p['lt']:.2f}")
            return None

        tier, scap = obter_tier(sc)
        if tier == "FRIO":
            _diag_descartes["tier_frio"] += 1
            return None

        stake     = max(min(mkt["stake"], BANKROLL * scap), STAKE_MINIMA)
        nome_liga = LIGAS_CFG.get(lid, (lg.get("name", "?"),))[0]

        dt = None
        try:
            dt = datetime.fromisoformat(
                fix.get("date", "").replace("Z", "+00:00")
            ).replace(tzinfo=None)
        except Exception:
            pass

        return {
            "fid":       fid,
            "dt":        dt,
            "dt_iso":    fix.get("date", ""),
            "pais":      lg.get("country", ""),
            "liga":      nome_liga,
            "lid":       lid,
            "rodada":    rodada,
            "casa":      hnm,
            "fora":      anm,
            "hid":       hid,
            "aid":       aid,
            "hgf":       hs["gf_t"], "hga": hs["ga_t"],
            "agf":       as_["gf_t"], "aga": as_["ga_t"],
            "hforma":    hs["forma"], "aforma": as_["forma"],
            "hpt":       hs["pt"],    "apt":    as_["pt"],
            "hcs":       hs["cs_rate"], "acs":  as_["cs_rate"],
            "hmom":      hs["momentum"], "amom": as_["momentum"],
            "h2h":       h2h,
            "lh":        lh, "la": la, "lt": p["lt"],
            "probs":     p,
            "mkt":       mkt,
            "stake":     stake,
            "pontuacao": sc,
            "detalhes":  det,
            "classico":  classico,
            "decisivo":  decisivo,
            "tier":      tier,
            "risco":     "PENDENTE",
        }
    except Exception as e:
        log.debug(f"Erro ao analisar partida: {e}")
        _diag_descartes["sem_dados"] += 1
        return None

# ─────────────────────────────────────────────────────────────────────
#  GESTÃO DE RISCO
# ─────────────────────────────────────────────────────────────────────
def filtrar_risco(jogos: list, state: dict):
    pnl = state.get("pnl_hoje", 0.0)

    if pnl <= -(BANKROLL * 0.15):
        log.warning(f"STOP LOSS ativado — PnL do dia: R$ {pnl:.2f}")
        for j in jogos:
            j["risco"] = "STOP_LOSS"
        return jogos, 0.0

    ordenados    = sorted(jogos, key=lambda j: j.get("pontuacao", 0), reverse=True)
    selecionados = []
    exposicao    = 0.0
    max_exp      = BANKROLL * (MAX_EXPOSICAO_PCT / 100)

    for j in ordenados:
        if len(selecionados) >= MAX_JOGOS_DIA:
            j["risco"] = "LIMITE_DIARIO"
            continue
        s = j.get("stake", 0.0)
        if exposicao + s > max_exp:
            disponivel = max(0.0, max_exp - exposicao)
            if disponivel < STAKE_MINIMA:
                j["risco"] = "SEM_BANCA"
                continue
            j["stake"] = round(disponivel, 2)
            s = j["stake"]
        exposicao += s
        j["risco"] = "OK"
        selecionados.append(j)

    return ordenados, exposicao

# ─────────────────────────────────────────────────────────────────────
#  UTILITÁRIOS DE FORMATAÇÃO
# ─────────────────────────────────────────────────────────────────────
def _barra(v, maximo: float = 7.0, largura: int = 8) -> str:
    f = int(min(max(v, 0) / maximo, 1.0) * largura)
    return "█" * f + "░" * (largura - f)

def _fmt_forma(forma: str) -> str:
    if not forma:
        return "--"
    trad = {"W": "V", "D": "E", "L": "D"}
    return "-".join(trad.get(c, c) for c in forma)

def _seta(m: float) -> str:
    return "↑" if m > 0.12 else "↓" if m < -0.12 else "→"

def _label_tier(tier: str) -> str:
    return {
        "ELITE":  "🔥🔥 ELITE — Máxima Confiança",
        "QUENTE": "🔥 QUENTE — Alta Confiança",
        "MORNO":  "⚡ MORNO — Boa Probabilidade",
        "VIGIAR": "👀 VIGIAR — Monitorar",
    }.get(tier, tier)

# ─────────────────────────────────────────────────────────────────────
#  MENSAGEM DE SINAL (PT-BR, com data e hora do jogo)
# ─────────────────────────────────────────────────────────────────────
def formatar_sinal(j: dict, tipo: str = "NOVO SINAL") -> str:
    p   = j["probs"]
    mkt = j["mkt"]
    h2h = j.get("h2h")

    if j["dt"]:
        data_jogo = j["dt"].strftime("%d/%m/%Y")
        hora_jogo = j["dt"].strftime("%H:%M")
    else:
        data_jogo = "A confirmar"
        hora_jogo = "--:--"

    sep = "━" * 34

    linhas = [
        sep,
        f"⚽  {tipo}",
        _label_tier(j["tier"]),
        sep,
        f"📅  Data do jogo:  {data_jogo}",
        f"🕐  Horário:       {hora_jogo}  (horário de Brasília)",
        f"🌍  País:          {j['pais']}",
        f"🏆  Competição:    {j['liga']}",
        "",
        f"🏠  Casa:   {j['casa']}",
        f"✈️   Fora:   {j['fora']}",
    ]

    if j["classico"]:
        linhas.append("🔥  CLÁSSICO")
    if j["decisivo"]:
        linhas.append("🏆  JOGO DECISIVO / MATA-MATA")

    linhas += [
        "",
        "─── ANÁLISE ESTATÍSTICA ───",
        f"   Gols esperados: {j['lh']:.2f} + {j['la']:.2f} = {j['lt']:.2f}",
        f"   Casa  — GF: {j['hgf']:.2f}  GA: {j['hga']:.2f}  "
        f"SG: {j['hcs']*100:.0f}%  {_seta(j['hmom'])}  Forma: {_fmt_forma(j['hforma'])}",
        f"   Fora  — GF: {j['agf']:.2f}  GA: {j['aga']:.2f}  "
        f"SG: {j['acs']*100:.0f}%  {_seta(j['amom'])}  Forma: {_fmt_forma(j['aforma'])}",
        "",
        "─── PROBABILIDADES ───",
        f"   Mais de 1.5 gols  →  {p['o15']*100:.1f}%",
        f"   Mais de 2.5 gols  →  {p['o25']*100:.1f}%",
        f"   Mais de 3.5 gols  →  {p['o35']*100:.1f}%",
        f"   Ambas marcam      →  {p['ambas_sim']*100:.1f}%",
        f"   Menos de 2.5      →  {p['u25']*100:.1f}%",
        f"   Menos de 1.5      →  {p['u15']*100:.1f}%",
    ]

    if h2h:
        tend_str = "↑ subindo" if h2h["tendencia"] > 0 else ("↓ caindo" if h2h["tendencia"] < 0 else "estável")
        linhas += [
            "",
            f"─── HISTÓRICO DO CONFRONTO ({h2h['n']} jogos) ───",
            f"   Média de gols: {h2h['media']:.1f} gols  ({tend_str})",
            f"   Mais de 2.5: {h2h['o25']*100:.0f}%   Ambas marcam: {h2h['ambas']*100:.0f}%",
        ]

    linhas += [
        "",
        "─── SINAL RECOMENDADO ───",
        f"   Mercado:         {mkt['nome']}",
        f"   Probabilidade:   {mkt['prob']*100:.1f}%",
        f"   Odd de referência: {mkt['odd']:.2f}",
        f"   Valor esperado (EV): {mkt['ev']:+.4f}",
        f"   Vantagem (Edge):     {mkt['edge']*100:+.1f}%",
        "",
        f"📈  Pontuação: {j['pontuacao']:.1f}  {_barra(j['pontuacao'])}",
        f"💰  Stake sugerida: R$ {j['stake']:.2f}  (Kelly {int(KELLY_FRACTION*100)}%)",
        sep,
        "⚠️  Verifique as odds reais antes de apostar.",
    ]
    return "\n".join(linhas)

# ─────────────────────────────────────────────────────────────────────
#  WATCHLIST — resumo do dia em PT-BR
# ─────────────────────────────────────────────────────────────────────
def formatar_watchlist(jogos: list, exposicao: float, titulo: str = "WATCHLIST") -> str | None:
    ok = [j for j in jogos if j.get("risco") == "OK"][:MAX_JOGOS_WATCHLIST]
    if not ok:
        return None

    elite  = [j for j in ok if j["tier"] == "ELITE"]
    quente = [j for j in ok if j["tier"] == "QUENTE"]
    morno  = [j for j in ok if j["tier"] == "MORNO"]
    vigiar = [j for j in ok if j["tier"] == "VIGIAR"]

    agora = datetime.now()
    sep   = "━" * 34

    linhas = [
        sep,
        f"⚽  {titulo}",
        sep,
        f"📅  {agora.strftime('%d/%m/%Y')}   🕐  Gerado às {agora.strftime('%H:%M')}",
        (
            f"Total: {len(ok)} sinais   "
            f"🔥🔥{len(elite)}  🔥{len(quente)}  ⚡{len(morno)}  👀{len(vigiar)}"
        ),
        f"Exposição total: R$ {exposicao:.2f}  ({exposicao/BANKROLL*100:.1f}% da banca)",
        f"API: {_api_requests_restantes} requests restantes hoje",
        sep,
    ]

    def bloco(lista, cabecalho):
        if not lista:
            return
        linhas.append(f"\n{cabecalho}")
        for j in lista:
            data_j = j["dt"].strftime("%d/%m") if j["dt"] else "--/--"
            hora_j = j["dt"].strftime("%H:%M") if j["dt"] else "--:--"
            p      = j["probs"]
            mkt    = j["mkt"]
            h2h    = j.get("h2h")

            linhas.append(
                f"\n📅 {data_j}  🕐 {hora_j}  |  {j['liga']}\n"
                f"   {j['casa']}  x  {j['fora']}\n"
                f"   Pontuação: {j['pontuacao']:.1f}  {_barra(j['pontuacao'])}\n"
                f"   Gols esp.: {j['lh']:.2f}+{j['la']:.2f}={j['lt']:.2f}  "
                f"[{_seta(j['hmom'])}{_seta(j['amom'])}]\n"
                f"   +2.5: {p['o25']*100:.0f}%   +1.5: {p['o15']*100:.0f}%   "
                f"Ambas: {p['ambas_sim']*100:.0f}%"
            )
            if h2h:
                linhas.append(
                    f"   H2H ({h2h['n']} jogos): +2.5={h2h['o25']*100:.0f}%  "
                    f"Média={h2h['media']:.1f} gols"
                )
            linhas.append(
                f"   ▶ {mkt['nome']}\n"
                f"     Odd ~{mkt['odd']:.2f}  |  EV: {mkt['ev']:+.3f}  |  "
                f"Stake: R$ {j['stake']:.0f}"
            )
            tags = []
            if j["classico"]: tags.append("CLÁSSICO")
            if j["decisivo"]: tags.append("DECISIVO")
            if tags:
                linhas.append(f"   [{' | '.join(tags)}]")

    bloco(elite,  "─── 🔥🔥 ELITE — Máxima Confiança ───")
    bloco(quente, "─── 🔥 QUENTE — Alta Confiança ───")
    bloco(morno,  "─── ⚡ MORNO — Boa Probabilidade ───")
    bloco(vigiar, "─── 👀 VIGIAR — Monitorar ───")

    linhas += [
        f"\n{sep}",
        "GF=Gols marcados   GA=Gols sofridos   SG=Jogo sem sofrer gols",
        "EV>0 indica valor na aposta   Stake calculada pelo critério de Kelly (15%)",
        "⚠️  Verifique as odds reais antes de apostar.",
    ]
    return "\n".join(linhas)

# ─────────────────────────────────────────────────────────────────────
#  REGISTRO DE PENDENTES E COLETA DE RESULTADOS
# ─────────────────────────────────────────────────────────────────────
def registrar_pendente(j: dict):
    pendentes = _load(ARQ_PENDENTES)
    pendentes[str(j["fid"])] = {
        "fid":       j["fid"],
        "dt_iso":    j["dt_iso"],
        "casa":      j["casa"],
        "fora":      j["fora"],
        "liga":      j["liga"],
        "tier":      j["tier"],
        "pontuacao": j["pontuacao"],
        "lt":        j["lt"],
        "mkt_nome":  j["mkt"]["nome"],
        "mkt_chave": j["mkt"]["chave"],
        "mkt_linha": _linha_mercado(j["mkt"]["chave"]),
        "mkt_tipo":  (
            "over"  if "over"  in j["mkt"]["chave"] else
            "ambas" if "ambas" in j["mkt"]["chave"] else
            "under"
        ),
        "mkt_prob":  j["mkt"]["prob"],
        "mkt_odd":   j["mkt"]["odd"],
        "stake":     j["stake"],
        "ts":        time.time(),
    }
    _save(ARQ_PENDENTES, pendentes)

def coletar_resultados(state: dict):
    pendentes = _load(ARQ_PENDENTES)
    if not pendentes:
        return

    agora      = datetime.now()
    remover    = []
    resolvidos = []

    for fid, inf in pendentes.items():
        try:
            dt_jogo   = datetime.fromisoformat(
                inf["dt_iso"].replace("Z", "+00:00")
            ).replace(tzinfo=None)
            decorrido = (agora - dt_jogo).total_seconds()

            if decorrido < 2 * 3600:
                continue
            if decorrido > 30 * 3600:
                remover.append(fid)
                continue
            if not _pode_chamar_api(minimo=5):
                continue

            fx = buscar_resultado(int(fid))
            if not fx:
                continue

            status = (fx.get("fixture", {}).get("status", {}) or {}).get("short")
            if status not in ("FT", "AET", "PEN"):
                continue

            g  = fx.get("goals", {}) or {}
            hg = g.get("home") or 0
            ag = g.get("away") or 0
            tt = hg + ag

            tipo   = inf["mkt_tipo"]
            linha  = inf["mkt_linha"]
            if   tipo == "over":  acertou = tt > linha
            elif tipo == "under": acertou = tt < linha
            else:
                acertou = (hg > 0 and ag > 0) if "sim" in inf["mkt_chave"] \
                          else not (hg > 0 and ag > 0)

            pnl = inf["stake"] * (inf["mkt_odd"] - 1) if acertou else -inf["stake"]

            _gravar_historico({
                "data":      dt_jogo.strftime("%d/%m/%Y"),
                "horario":   dt_jogo.strftime("%H:%M"),
                "liga":      inf["liga"],
                "partida":   f"{inf['casa']} x {inf['fora']}",
                "tier":      inf["tier"],
                "pontuacao": inf["pontuacao"],
                "lambda":    inf["lt"],
                "mercado":   inf["mkt_nome"],
                "prob_mod":  round(inf["mkt_prob"] * 100, 1),
                "odd_ref":   inf["mkt_odd"],
                "stake":     inf["stake"],
                "placar":    f"{hg}-{ag}",
                "gols":      tt,
                "resultado": "ACERTO" if acertou else "ERRO",
                "pnl":       round(pnl, 2),
            })
            resolvidos.append((inf, hg, ag, tt, acertou, pnl))
            remover.append(fid)

            if dt_jogo.strftime("%Y-%m-%d") == _hoje():
                state["pnl_hoje"] = state.get("pnl_hoje", 0.0) + pnl
                _save(ARQ_STATE, state)

        except Exception as e:
            log.debug(f"Pendente {fid}: {e}")

    for fid in remover:
        pendentes.pop(fid, None)
    _save(ARQ_PENDENTES, pendentes)

    for inf, hg, ag, gols, acertou, pnl in resolvidos:
        icone = "✅" if acertou else "❌"
        tg(
            f"📋  RESULTADO DA APOSTA\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚽  {inf['casa']}  x  {inf['fora']}\n"
            f"🏆  {inf['liga']}  |  Nível: {inf['tier']}\n"
            f"🎯  Mercado: {inf['mkt_nome']}\n"
            f"📊  Placar final: {hg}-{ag}  ({gols} gols no total)\n"
            f"📐  Gols esperados pelo modelo: {inf['lt']:.2f}\n"
            f"{icone}  Resultado: {'ACERTO' if acertou else 'ERRO'}  "
            f"|  PnL: R$ {pnl:+.2f}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )

def _gravar_historico(linha: dict):
    existe = os.path.exists(ARQ_HISTORICO)
    try:
        with open(ARQ_HISTORICO, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(linha.keys()))
            if not existe:
                w.writeheader()
            w.writerow(linha)
    except Exception as e:
        log.error(f"Histórico CSV: {e}")

# ─────────────────────────────────────────────────────────────────────
#  RELATÓRIO SEMANAL
# ─────────────────────────────────────────────────────────────────────
def relatorio_semanal() -> str | None:
    if not os.path.exists(ARQ_HISTORICO):
        return None

    corte  = datetime.now() - timedelta(days=7)
    linhas = []
    try:
        with open(ARQ_HISTORICO, "r", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                try:
                    if datetime.strptime(r["data"], "%d/%m/%Y") >= corte:
                        linhas.append(r)
                except Exception:
                    continue
    except Exception:
        return None

    if not linhas:
        return "📋  RELATÓRIO SEMANAL\nSem apostas registradas nos últimos 7 dias."

    total   = len(linhas)
    acertos = sum(1 for r in linhas if r["resultado"] == "ACERTO")
    pnl     = sum(float(r["pnl"]) for r in linhas)
    stakes  = sum(float(r["stake"]) for r in linhas)
    taxa    = acertos / total * 100 if total else 0
    roi     = pnl / stakes * 100    if stakes else 0
    m_prob  = sum(float(r["prob_mod"]) for r in linhas) / total

    tiers = {}
    mkts  = {}
    for r in linhas:
        tiers.setdefault(r.get("tier", "?"), []).append(r)
        mkts.setdefault(r.get("mercado", "?"), []).append(r)

    sep = "━" * 34
    ls  = [
        sep,
        "📊  RELATÓRIO SEMANAL — BOT GOLS v4.3",
        f"📅  Últimos 7 dias  |  {datetime.now().strftime('%d/%m/%Y')}",
        sep,
        f"Apostas: {total}   Acertos: {acertos}   Erros: {total - acertos}",
        f"Taxa de acerto: {taxa:.1f}%",
        f"PnL total: R$ {pnl:+.2f}   ROI: {roi:+.1f}%",
        "",
        "Por Nível de Confiança:",
    ]
    for tn in ["ELITE", "QUENTE", "MORNO", "VIGIAR"]:
        tr = tiers.get(tn, [])
        if not tr: continue
        tw = sum(1 for r in tr if r["resultado"] == "ACERTO")
        tt = len(tr)
        tp = sum(float(r["pnl"])   for r in tr)
        ts = sum(float(r["stake"]) for r in tr)
        ls.append(
            f"  {tn}: {tw}/{tt} ({tw/tt*100:.0f}%)   ROI: {tp/ts*100 if ts else 0:+.1f}%"
        )
    ls.append("\nPor Mercado:")
    for mn, mr in sorted(mkts.items(), key=lambda x: -len(x[1])):
        mw = sum(1 for r in mr if r["resultado"] == "ACERTO")
        mt = len(mr)
        mp = sum(float(r["pnl"]) for r in mr)
        ls.append(f"  {mn}: {mw}/{mt} ({mw/mt*100:.0f}%)   PnL: R$ {mp:+.0f}")

    desvio = abs(m_prob - taxa)
    ls.append(f"\nCalibração: prevista={m_prob:.1f}%  real={taxa:.1f}%  desvio={desvio:.1f}%")
    ls.append(
        "✅ Modelo calibrado" if desvio <= 10
        else "⚠️ Recalibrar modelo" if desvio > 15
        else "👁 Monitorar calibração"
    )
    ls.append(sep)
    return "\n".join(ls)

# ─────────────────────────────────────────────────────────────────────
#  CONTROLE DE ESTADO
# ─────────────────────────────────────────────────────────────────────
def watchlist_enviada(state: dict, hora: int) -> bool:
    return state.get(f"wl_{hora}_{_hoje()}", False)

def marcar_watchlist(state: dict, hora: int):
    state[f"wl_{hora}_{_hoje()}"] = True
    _save(ARQ_STATE, state)

def relatorio_enviado(state: dict) -> bool:
    return state.get("rel_semanal") == _hoje()

def marcar_relatorio(state: dict):
    state["rel_semanal"] = _hoje()
    _save(ARQ_STATE, state)

# ─────────────────────────────────────────────────────────────────────
#  LOOP PRINCIPAL  (v4.3 — otimizado)
# ─────────────────────────────────────────────────────────────────────
def main():
    global _encerrar, _aviso_api_enviado, _diag_descartes

    log.info("=" * 60)
    log.info("  BOT GOLS v4.3 — MODO AGRESSIVO TURBINADO")
    log.info(f"  Pontuação mín.: {MIN_SCORE_ALERTA}  |  Scan: {SCAN_INTERVAL}s  |  Máx./dia: {MAX_JOGOS_DIA}")
    log.info(f"  Cache: fixtures={TTL_FIXTURES//60}min  stats={TTL_STATS//3600}h  h2h={TTL_H2H//3600}h")
    log.info("=" * 60)

    if not testar_telegram():
        log.error("Telegram não conectou. Verifique token e chat_id.")
        return

    _carregar_sinais_enviados()

    state = _load(ARQ_STATE)
    if state.get("pnl_data") != _hoje():
        state["pnl_hoje"]    = 0.0
        state["pnl_data"]    = _hoje()
        _aviso_api_enviado   = False
        _save(ARQ_STATE, state)

    thread_cmd = threading.Thread(target=_thread_terminal, daemon=True)
    thread_cmd.start()

    tg(
        f"🤖  BOT GOLS v4.3 INICIADO\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📅  {datetime.now().strftime('%d/%m/%Y às %H:%M')}\n"
        f"⚙️   Pontuação mínima: {MIN_SCORE_ALERTA}  (modo agressivo)\n"
        f"⏱   Varredura a cada: {SCAN_INTERVAL}s\n"
        f"🎯  Máximo de sinais/dia: {MAX_JOGOS_DIA}\n"
        f"📦  Cache fixtures: {TTL_FIXTURES//60} min\n"
        f"🕐  Watchlists: {WATCHLIST_HORAS}h\n"
        f"🔔  Re-alertas: T-{REALERTAS_MIN[0]}min, T-{REALERTAS_MIN[1]}min, T-{REALERTAS_MIN[2]}min\n"
        f"💰  Banca: R$ {BANKROLL:.0f}   Exposição máx.: {MAX_EXPOSICAO_PCT}%\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"✅  Sistema ativo! Aguardando partidas...\n"
        f"(Sinais já enviados hoje: {len(_sinais_enviados_hoje)})"
    )

    # Controle de coleta de resultados (não roda em toda varredura)
    ultima_coleta = 0.0

    while not _encerrar:
        try:
            if esta_pausado():
                time.sleep(5)
                continue

            # Reset diagnóstico do ciclo
            _diag_descartes = {"score_baixo": 0, "tier_frio": 0, "sem_mercado": 0, "sem_dados": 0}

            agora = datetime.now()
            log.info(
                f"\n{'─'*52}\n"
                f"  Varredura: {agora.strftime('%d/%m/%Y %H:%M:%S')}\n"
                f"  API: {_api_requests_restantes} requests restantes\n"
                f"  Sinais hoje: {len(_sinais_enviados_hoje)}\n"
                f"{'─'*52}"
            )

            # ── 0. Coleta de resultados (a cada 10 min, não em toda varredura) ─
            if time.time() - ultima_coleta > INTERVALO_COLETA_RESULTADOS:
                coletar_resultados(state)
                ultima_coleta = time.time()

            # ── 1. Busca de fixtures (cache de 30 min) ──────────────────
            hoje   = agora.strftime("%Y-%m-%d")
            amanha = (agora + timedelta(days=1)).strftime("%Y-%m-%d")
            raw    = buscar_fixtures(hoje) + buscar_fixtures(amanha)
            pre    = [
                f for f in raw
                if f.get("fixture", {}).get("status", {}).get("short") in ("NS", "TBD", "PST")
            ]
            log.info(f"Partidas pré-jogo: {len(pre)}")

            # ── 2. Limita análises conforme saldo de requests (mais generoso) ─
            if _api_requests_restantes < 200:
                pre = sorted(pre, key=lambda f: f.get("fixture", {}).get("date", "") or "")
                max_an = min(100, max(20, _api_requests_restantes // 2))
                pre    = pre[:max_an]
                log.info(
                    f"Requests baixos ({_api_requests_restantes}): "
                    f"analisando os {len(pre)} jogos mais próximos"
                )
            else:
                # Se há requests sobrando, analisa até 100 por ciclo
                pre = pre[:100]

            # ── 3. Análise das partidas (sem sleep entre cada uma) ──────
            analises = []
            for fx in pre:
                if _encerrar or esta_pausado():
                    break
                if not _pode_chamar_api(minimo=10):
                    log.warning("Requests insuficientes, encerrando análise deste ciclo.")
                    break
                a = analisar_partida(fx)
                if a:
                    analises.append(a)
                # ↓ removido o time.sleep(0.2) — economiza ~10s por ciclo

            log.info(
                f"Partidas qualificadas: {len(analises)}  |  "
                f"Descartes: score_baixo={_diag_descartes['score_baixo']}, "
                f"tier_frio={_diag_descartes['tier_frio']}, "
                f"sem_mercado={_diag_descartes['sem_mercado']}, "
                f"sem_dados={_diag_descartes['sem_dados']}"
            )

            # ── 4. Gestão de risco ──────────────────────────────────────
            filtrados, exposicao = filtrar_risco(analises, state)
            aprovados = [j for j in filtrados if j.get("risco") == "OK"]
            log.info(f"Aprovados: {len(aprovados)}  |  Exposição: R$ {exposicao:.2f}")

            # ── 5. ENVIO DE SINAIS (com anti-duplicata) ─────────────────
            for j in aprovados:
                chave = _chave_sinal(j["fid"], "_inicial")
                if not _ja_enviado(chave):
                    msg = formatar_sinal(j, tipo="NOVO SINAL")
                    if tg(msg):
                        _registrar_sinal(chave)
                        registrar_pendente(j)
                        # Registra no site (endpoint /gols)
                        try:
                            _web_registrar = globals().get("_web_registrar")
                            if _web_registrar:
                                _web_registrar(j)
                        except Exception:
                            pass
                        log.info(
                            f"Sinal enviado: {j['casa']} x {j['fora']}  "
                            f"({j['dt'].strftime('%d/%m %H:%M') if j['dt'] else '?'})  "
                            f"| Pontuação: {j['pontuacao']:.1f}  Tier: {j['tier']}"
                        )
                        time.sleep(0.5)
                else:
                    log.debug(
                        f"Duplicata ignorada: {j['casa']} x {j['fora']} "
                        f"(fid={j['fid']}, chave={chave})"
                    )

            # ── 6. Watchlists por horário ───────────────────────────────
            hora_atual = agora.hour
            for h in WATCHLIST_HORAS:
                if hora_atual >= h and not watchlist_enviada(state, h):
                    jogos_hoje = [j for j in filtrados if j.get("dt_iso", "").startswith(hoje)]
                    if jogos_hoje:
                        _, exp_h = filtrar_risco(jogos_hoje, state)
                        titulos  = {
                            7:  "WATCHLIST MANHÃ — 07h",
                            9:  "WATCHLIST MANHÃ — 09h",
                            12: "WATCHLIST ALMOÇO — 12h",
                            15: "WATCHLIST TARDE — 15h",
                            18: "WATCHLIST NOITE — 18h",
                        }
                        titulo = titulos.get(h, f"WATCHLIST {h}h")
                        msg    = formatar_watchlist(jogos_hoje, exp_h, titulo)
                        if msg:
                            tg(msg)
                            log.info(f"Watchlist {h}h enviada ({len(jogos_hoje)} jogos)")
                    marcar_watchlist(state, h)

            # ── 7. Re-alertas pré-jogo (com break — só uma janela por ciclo) ─
            for j in aprovados:
                if j["pontuacao"] < MIN_SCORE_REALERTA or not j["dt"]:
                    continue
                delta_min = (j["dt"] - agora).total_seconds() / 60
                if delta_min < 0 or delta_min > max(REALERTAS_MIN) + 15:
                    continue

                for t_min in REALERTAS_MIN:
                    if (t_min - 8) <= delta_min <= t_min:
                        chave = _chave_sinal(j["fid"], f"_t{t_min}")
                        if not _ja_enviado(chave):
                            tipo = f"RE-ALERTA — {t_min} MIN PARA O JOGO"
                            msg  = formatar_sinal(j, tipo=tipo)
                            if tg(msg):
                                _registrar_sinal(chave)
                                registrar_pendente(j)
                                log.info(
                                    f"Re-alerta T-{t_min}min: "
                                    f"{j['casa']} x {j['fora']}"
                                )
                        break  # ← v4.3: dispara no máximo uma janela por ciclo

            # ── 8. Relatório semanal (segunda-feira às 9h) ──────────────
            if agora.weekday() == 0 and agora.hour == 9 and not relatorio_enviado(state):
                rel = relatorio_semanal()
                if rel:
                    tg(rel)
                marcar_relatorio(state)

        except KeyboardInterrupt:
            log.info("Interrompido pelo usuário (Ctrl+C).")
            break
        except Exception as e:
            log.error(f"Erro no loop principal: {e}", exc_info=True)

        if not _encerrar:
            log.info(
                f"Próxima varredura em {SCAN_INTERVAL}s  "
                f"[Sinais hoje: {len(_sinais_enviados_hoje)}  |  "
                f"API: {_api_requests_restantes} requests restantes]"
            )
            for _ in range(SCAN_INTERVAL // 2):
                if _encerrar:
                    break
                time.sleep(2)

    log.info("Bot encerrado.")
    tg(
        f"🔴  BOT GOLS v4.3 ENCERRADO\n"
        f"📅  {datetime.now().strftime('%d/%m/%Y às %H:%M')}\n"
        f"📊  Sinais enviados hoje: {len(_sinais_enviados_hoje)}"
    )


if __name__ == "__main__":
    main()