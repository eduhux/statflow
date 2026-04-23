"""
╔══════════════════════════════════════════════════════════════════════════╗
║   BOT RESULTADO 1X2 v1.0 — PREVISÃO VENCEDOR DA PARTIDA                ║
╠══════════════════════════════════════════════════════════════════════════╣
║  MERCADOS COBERTOS:                                                      ║
║  ► 1   — Vitória do time da casa                                        ║
║  ► X   — Empate                                                         ║
║  ► 2   — Vitória do time visitante                                      ║
║  ► 1X  — Casa ou Empate (dupla chance)                                  ║
║  ► X2  — Fora ou Empate (dupla chance)                                  ║
║  ► 12  — Casa ou Fora (dupla chance, sem empate)                        ║
║                                                                          ║
║  MOTOR:                                                                  ║
║  ► Modelo Dixon-Coles corrigido para probabilidade conjunta              ║
║  ► Força de ataque/defesa por liga (home advantage calibrado)            ║
║  ► Ajuste de forma recente (últimos 5 jogos, pesos exponenciais)         ║
║  ► Momentum de gols por período (pressão do 2º tempo)                   ║
║  ► Head-to-Head (últimos 10 confrontos)                                  ║
║  ► ELO implícito calculado das estatísticas da temporada                ║
║  ► Kelly fracionado 15% para gestão de banca                            ║
║  ► Anti-duplicata persistente + re-alertas pré-jogo                     ║
╚══════════════════════════════════════════════════════════════════════════╝

  Controles no terminal:
  ────────────────────────────────────────────────────
  PAUSAR  → suspende varreduras
  RETOMAR → retoma varreduras
  STATUS  → exibe situação atual
  SAIR    → encerra com segurança
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
from scipy.optimize import brentq

# ─────────────────────────────────────────────────────────────────────
#  CREDENCIAIS
# ─────────────────────────────────────────────────────────────────────
API_FOOTBALL_KEY = os.environ.get("API_FOOTBALL_KEY", "")
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN_RESULTADO") or os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# ─────────────────────────────────────────────────────────────────────
#  CONFIGURAÇÕES GERAIS
# ─────────────────────────────────────────────────────────────────────
SCAN_INTERVAL        = 90        # segundos entre varreduras
MIN_SCORE_ALERTA     = 1.2       # pontuação mínima para gerar sinal
MIN_SCORE_REALERTA   = 2.0       # pontuação mínima para re-alertas
MIN_EV               = 0.03      # EV mínimo absoluto (3%) para qualificar mercado
MAX_JOGOS_DIA        = 30
MAX_JOGOS_WATCHLIST  = 20
MAX_EXPOSICAO_PCT    = 40.0
BANKROLL             = 1000.0
KELLY_FRACTION       = 0.15
STAKE_MINIMA         = 5.0
HORIZONTE_HORAS      = 36        # busca jogos nas próximas 36 horas

# Coleta de resultados a cada 10 min
INTERVALO_COLETA_RESULTADOS = 600

# Arquivos
ARQ_LOG             = "bot_1x2.log"
ARQ_STATE           = "state_1x2.json"
ARQ_HISTORICO       = "historico_1x2.csv"
ARQ_PENDENTES       = "pending_1x2.json"
ARQ_CACHE_STATS     = "cache_stats_1x2.json"
ARQ_CACHE_H2H       = "cache_h2h_1x2.json"
ARQ_CACHE_FIX       = "cache_fixtures_1x2.json"
ARQ_SINAIS_ENVIADOS = "sinais_enviados_1x2.json"

# TTL caches
TTL_STATS    = 6  * 3600
TTL_H2H      = 12 * 3600
TTL_FIXTURES = 30 * 60

# Limite mínimo de requests API para operar
API_LIMITE_MINIMO = 50

# Horários de watchlist
WATCHLIST_HORAS = [7, 9, 12, 15, 18]

# Re-alertas antes do jogo (minutos)
REALERTAS_MIN = [120, 60, 30]

# ─────────────────────────────────────────────────────────────────────
#  TIERS DE CONFIANÇA
# ─────────────────────────────────────────────────────────────────────
TIER_ELITE_MIN  = 5.0
TIER_QUENTE_MIN = 3.5
TIER_MORNO_MIN  = 2.5
TIER_VIGIAR_MIN = 1.2

STAKE_LIMITE = {
    "ELITE":  0.05,
    "QUENTE": 0.035,
    "MORNO":  0.02,
    "VIGIAR": 0.01,
}

# ─────────────────────────────────────────────────────────────────────
#  ODDS DE REFERÊNCIA 1X2 (benchmarks de mercado)
#  Valores representativos de casas mainstream (Bet365 / Betano baseline)
# ─────────────────────────────────────────────────────────────────────
ODDS_REF_1X2 = {
    # Simples
    "casa":     None,   # calculada dinamicamente pelo modelo
    "empate":   None,
    "fora":     None,
    # Dupla chance
    "dc_1x":    None,
    "dc_x2":    None,
    "dc_12":    None,
}

# Margens típicas por liga (overround da bookie)
# Usamos isso para calibrar a odd justa implícita
MARGEM_BOOKIE_DEFAULT = 0.06   # 6% (Betano/Bet365 padrão)

# ─────────────────────────────────────────────────────────────────────
#  CONFIGURAÇÕES POR LIGA  (id: nome, vantagem_casa, media_gf, media_ga)
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

# Clássicos (reduzem ligeiramente a confiança — maior imprevisibilidade)
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
#  CONTROLE DE PAUSA / ENCERRAMENTO (thread-safe)
# ─────────────────────────────────────────────────────────────────────
_pausado    = False
_encerrar   = False
_lock_pausa = threading.Lock()
_ts_pausa   = None

_api_requests_restantes = 999
_aviso_api_enviado      = False
_sinais_enviados_hoje   = set()
_diag_descartes         = {}

def esta_pausado() -> bool:
    with _lock_pausa:
        return _pausado

def pausar():
    global _pausado, _ts_pausa
    with _lock_pausa:
        if not _pausado:
            _pausado  = True
            _ts_pausa = datetime.now()
            log.info("BOT PAUSADO — varreduras suspensas.")
            tg("⏸ BOT PAUSADO\nDigite RETOMAR para continuar.")
        else:
            log.info("Bot já está pausado.")

def retomar():
    global _pausado, _ts_pausa
    with _lock_pausa:
        if _pausado:
            dur = ""
            if _ts_pausa:
                seg = int((datetime.now() - _ts_pausa).total_seconds())
                dur = f" (pausado por {seg // 60}min {seg % 60}s)"
            _pausado  = False
            _ts_pausa = None
            log.info(f"BOT RETOMADO{dur}")
            tg(f"▶ BOT RETOMADO{dur}")
        else:
            log.info("Bot já está em execução.")

def exibir_status():
    estado = "PAUSADO ⏸" if esta_pausado() else "ATIVO ▶"
    log.info(
        f"STATUS: {estado} | API: {_api_requests_restantes} req "
        f"| Sinais hoje: {len(_sinais_enviados_hoje)} "
        f"| {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"
    )

def _thread_terminal():
    global _encerrar
    print("\nCONTROLES: PAUSAR | RETOMAR | STATUS | SAIR\n")
    while not _encerrar:
        try:
            cmd = input().strip().upper()
            if   cmd == "PAUSAR":  pausar()
            elif cmd == "RETOMAR": retomar()
            elif cmd == "STATUS":  exibir_status()
            elif cmd == "SAIR":
                log.info("Encerrando por comando do terminal...")
                _encerrar = True
                break
            elif cmd:
                print("Comandos: PAUSAR | RETOMAR | STATUS | SAIR")
        except (EOFError, KeyboardInterrupt):
            break

def _pode_chamar_api(minimo: int = 10) -> bool:
    return _api_requests_restantes > minimo

# ─────────────────────────────────────────────────────────────────────
#  HELPERS DE ARQUIVO JSON
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
    lista = dados.get(hoje, [])
    _sinais_enviados_hoje = set(lista)
    _save(ARQ_SINAIS_ENVIADOS, {hoje: lista})
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

def _chave_sinal(fid, mercado: str, sufixo: str = "") -> str:
    return f"{fid}_{mercado}{sufixo}_{_hoje()}"

# ─────────────────────────────────────────────────────────────────────
#  TELEGRAM
# ─────────────────────────────────────────────────────────────────────
def tg(msg: str) -> bool:
    if not msg:
        return False
    ok = True
    for trecho in [msg[i:i+3900] for i in range(0, len(msg), 3900)]:
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
            log.info(f"Telegram OK: @{r.json()['result']['username']}")
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
        log.error(f"API esgotada ({_api_requests_restantes} restantes).")
        return None

    try:
        r = requests.get(
            f"https://{HOST_API}/{endpoint}",
            headers={"x-apisports-key": API_FOOTBALL_KEY},
            params=params,
            timeout=timeout,
        )
        if r.status_code == 429:
            log.warning("Rate limit — aguardando 70s...")
            time.sleep(70)
            return None
        if r.status_code in (401, 403):
            log.error(f"Autenticação falhou: {r.status_code}")
            return None
        if r.status_code != 200:
            return None

        rem = r.headers.get("x-ratelimit-requests-remaining")
        if rem is not None:
            try:
                _api_requests_restantes = int(rem)
                if _api_requests_restantes < API_LIMITE_MINIMO:
                    log.warning(f"ATENÇÃO: {_api_requests_restantes} requests restantes!")
                    if _api_requests_restantes < 30 and not _aviso_api_enviado:
                        tg(
                            f"⚠️ AVISO DE API\n"
                            f"Restam apenas {_api_requests_restantes} requests hoje.\n"
                            f"Bot operará somente com cache até meia-noite."
                        )
                        _aviso_api_enviado = True
            except Exception:
                pass

        return r.json().get("response")

    except requests.exceptions.Timeout:
        log.warning(f"Timeout: {endpoint}")
        return None
    except Exception as e:
        log.debug(f"Erro API: {e}")
        return None

# ─────────────────────────────────────────────────────────────────────
#  FIXTURES
# ─────────────────────────────────────────────────────────────────────
def buscar_fixtures(data_str: str) -> list:
    cache = _load(ARQ_CACHE_FIX)
    agora = time.time()

    if data_str in cache:
        entrada = cache[data_str]
        idade   = agora - entrada.get("_ts", 0)
        if idade < TTL_FIXTURES:
            log.info(f"Fixtures {data_str}: cache ({int(idade/60)}min, {len(entrada['data'])} partidas)")
            return entrada["data"]

    if not _pode_chamar_api(minimo=20):
        log.warning(f"Fixtures {data_str}: requests insuficientes, usando cache anterior")
        return cache.get(data_str, {}).get("data", [])

    log.info(f"Buscando fixtures {data_str} na API...")
    dados = _api("fixtures", {"date": data_str}) or []
    log.info(f"  → {len(dados)} partidas")

    if dados:
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

    # Gols marcados/sofridos casa e fora
    gfh = max(_g(gols, "for",     "total", "home"), 0.1 * ph)
    gfa = max(_g(gols, "for",     "total", "away"), 0.1 * pa)
    gah = max(_g(gols, "against", "total", "home"), 0.5 * ph)
    gaa = max(_g(gols, "against", "total", "away"), 0.5 * pa)

    # Vitórias/empates/derrotas por local
    wh  = _g(fix, "wins",  "home")
    wa  = _g(fix, "wins",  "away")
    dh  = _g(fix, "draws", "home")
    da  = _g(fix, "draws", "away")
    lh  = _g(fix, "loses", "home")
    la  = _g(fix, "loses", "away")

    # Taxas de resultado por local
    tx_vitoria_casa  = wh / ph if ph else 0.40
    tx_empate_casa   = dh / ph if ph else 0.28
    tx_derrota_casa  = lh / ph if ph else 0.32
    tx_vitoria_fora  = wa / pa if pa else 0.28
    tx_empate_fora   = da / pa if pa else 0.28
    tx_derrota_fora  = la / pa if pa else 0.44

    cs  = _g(dados.get("clean_sheet") or {}, "total")
    fts = _g(dados.get("failed_to_score") or {}, "total")

    # Forma recente (últimos 5) com pesos exponenciais
    forma_r = forma[-5:] if forma else ""
    pesos   = [0.10, 0.15, 0.20, 0.25, 0.30]
    fs = sum(
        pesos[i] * (1 if c == "W" else -1 if c == "L" else 0)
        for i, c in enumerate(reversed(forma_r[:5]))
    )

    # Momentum de gols por período (identifica time que cresce no 2º tempo)
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

    # ELO implícito baseado no histórico (escala 1000-2000)
    # Taxa de pontos = (V*3 + E*1) / (jogos * 3)
    pts_totais = (wh + wa) * 3 + (dh + da)
    max_pts    = pt * 3
    elo_norm   = (pts_totais / max_pts) if max_pts > 0 else 0.45
    elo        = round(1000 + elo_norm * 1000, 1)

    resultado = {
        # Médias de gols
        "gf_h": round(gfh / ph, 3),
        "gf_a": round(gfa / pa, 3),
        "ga_h": round(gah / ph, 3),
        "ga_a": round(gaa / pa, 3),
        "gf_t": round((gfh + gfa) / pt, 3),
        "ga_t": round((gah + gaa) / pt, 3),
        # Jogos disputados
        "ph": ph, "pa": pa, "pt": pt,
        # Taxas de resultado por local
        "tx_vitoria_casa":  round(tx_vitoria_casa,  3),
        "tx_empate_casa":   round(tx_empate_casa,   3),
        "tx_derrota_casa":  round(tx_derrota_casa,  3),
        "tx_vitoria_fora":  round(tx_vitoria_fora,  3),
        "tx_empate_fora":   round(tx_empate_fora,   3),
        "tx_derrota_fora":  round(tx_derrota_fora,  3),
        # Eficiência defensiva
        "cs_rate":  round(cs  / pt, 3) if pt else 0.25,
        "fts_rate": round(fts / pt, 3) if pt else 0.25,
        # Características
        "momentum": momentum,
        "fs":       round(fs, 3),
        "forma":    forma_r,
        "elo":      elo,
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
        "tx_vitoria_casa": 0.40, "tx_empate_casa": 0.28, "tx_derrota_casa": 0.32,
        "tx_vitoria_fora": 0.28, "tx_empate_fora": 0.28, "tx_derrota_fora": 0.44,
        "cs_rate": 0.25, "fts_rate": 0.25,
        "momentum": 0.0, "fs": 0.0, "forma": "",
        "elo": 1400.0,
        "_ts": time.time(),
    }

# ─────────────────────────────────────────────────────────────────────
#  HEAD-TO-HEAD (foco em resultados 1X2, não apenas gols)
# ─────────────────────────────────────────────────────────────────────
def buscar_h2h(hid, aid) -> dict | None:
    chave = f"{hid}-{aid}"
    cache = _purge(_load(ARQ_CACHE_H2H), TTL_H2H)

    if chave in cache:
        return cache[chave]

    if not _pode_chamar_api(minimo=10):
        return None

    dados = _api("fixtures/headtohead", {"h2h": f"{hid}-{aid}", "last": 10})
    if not dados:
        return None

    n = vitoria_h = empate = vitoria_a = 0
    gols_lista = []

    for fx in dados:
        st = (fx.get("fixture", {}).get("status", {}) or {}).get("short", "")
        if st not in ("FT", "AET", "PEN"):
            continue
        g  = fx.get("goals", {}) or {}
        hg = g.get("home") or 0
        ag = g.get("away") or 0
        t  = hg + ag
        gols_lista.append(t)
        n += 1
        if   hg > ag: vitoria_h += 1
        elif hg == ag: empate   += 1
        else:          vitoria_a += 1

    if n == 0:
        return None

    # Tendência recente: compara últimos 3 vs anteriores
    tendencia_h = 0.0
    if len(gols_lista) >= 5:
        recentes = [1 if fx.get("goals", {}).get("home", 0) > fx.get("goals", {}).get("away", 0) else 0
                    for fx in dados[:3] if (fx.get("fixture", {}).get("status", {}) or {}).get("short") in ("FT", "AET", "PEN")]
        tendencia_h = sum(recentes) / len(recentes) if recentes else 0.5

    r = {
        "n":           n,
        "p_casa":      round(vitoria_h / n, 3),
        "p_empate":    round(empate    / n, 3),
        "p_fora":      round(vitoria_a / n, 3),
        "media_gols":  round(sum(gols_lista) / n, 2),
        "tendencia_h": round(tendencia_h, 3),
        "_ts":         time.time(),
    }
    cache[chave] = r
    _save(ARQ_CACHE_H2H, cache)
    return r

# ─────────────────────────────────────────────────────────────────────
#  MODELO DIXON-COLES (λ corrigido para correlação de scores baixos)
# ─────────────────────────────────────────────────────────────────────
def _rho_correction(j: int, k: int, lh: float, la: float, rho: float) -> float:
    """Correção Dixon-Coles para resultados 0-0, 1-0, 0-1, 1-1."""
    if j == 0 and k == 0: return 1 - lh * la * rho
    if j == 1 and k == 0: return 1 + la * rho
    if j == 0 and k == 1: return 1 + lh * rho
    if j == 1 and k == 1: return 1 - rho
    return 1.0

def calcular_lambdas(hs: dict, as_: dict, lid: int) -> tuple:
    """
    Calcula λ_casa e λ_fora usando o método de força relativa
    com vantagem de mandante calibrada por liga.
    """
    cfg             = LIGAS_CFG.get(lid, ("?", 1.12, 1.45, 1.05))
    _, vc, mgfc, mgac = cfg
    mgff = mgac          # média de gols fora (usado como referência de defesa)
    mgaf = mgfc * 0.88   # média de gols contra times de fora

    # Forças de ataque e defesa relativas
    ah = hs["gf_h"] / mgfc  if mgfc > 0 else 1.0
    aa = as_["gf_a"] / mgff if mgff > 0 else 1.0
    dh = hs["ga_h"] / mgaf  if mgaf > 0 else 1.0
    da = as_["ga_a"] / mgfc if mgfc > 0 else 1.0

    lh = ah * da * mgfc * vc
    la = aa * dh * mgff

    # Ajuste por momentum (tendência recente de gols por período)
    lh *= (1 + hs.get("momentum", 0) * 0.08)
    la *= (1 + as_.get("momentum", 0) * 0.08)

    # Ajuste por forma recente (FS: -1 a +1)
    lh *= (1 + hs.get("fs", 0) * 0.06)
    la *= (1 + as_.get("fs", 0) * 0.06)

    return (
        round(max(0.30, min(lh, 5.5)), 4),
        round(max(0.20, min(la, 4.5)), 4),
    )

def calcular_probs_1x2(lh: float, la: float, rho: float = -0.13) -> dict:
    """
    Calcula P(Casa), P(Empate), P(Fora) via modelo Dixon-Coles.
    rho=-0.13 é o parâmetro de correlação negativa padrão do modelo original.
    Considera placares de 0 a 10 gols por time.
    """
    MAX_G = 11  # 0..10 gols por time

    p_casa = p_emp = p_fora = 0.0

    for i in range(MAX_G):
        for j in range(MAX_G):
            p_ij = (
                poisson.pmf(i, lh)
                * poisson.pmf(j, la)
                * _rho_correction(i, j, lh, la, rho)
            )
            p_ij = max(p_ij, 0.0)
            if   i > j:  p_casa += p_ij
            elif i == j: p_emp  += p_ij
            else:         p_fora += p_ij

    # Normaliza (soma pode diferir ligeiramente de 1 pela truncagem)
    total = p_casa + p_emp + p_fora
    if total > 0:
        p_casa /= total
        p_emp  /= total
        p_fora /= total

    # Dupla chance
    dc_1x = p_casa + p_emp
    dc_x2 = p_emp  + p_fora
    dc_12 = p_casa + p_fora

    return {
        "p_casa":  round(p_casa, 4),
        "p_emp":   round(p_emp,  4),
        "p_fora":  round(p_fora, 4),
        "dc_1x":   round(dc_1x,  4),
        "dc_x2":   round(dc_x2,  4),
        "dc_12":   round(dc_12,  4),
        "lh": lh, "la": la,
        "lt": round(lh + la, 4),
    }

def odd_justa(prob: float, margem: float = MARGEM_BOOKIE_DEFAULT) -> float:
    """Odd justa sem margem. Para comparar com o mercado."""
    if prob <= 0:
        return 999.0
    return round(1.0 / prob, 3)

def odd_com_margem(prob: float, margem: float = MARGEM_BOOKIE_DEFAULT) -> float:
    """Simula a odd que a bookie estaria cobrando (com margem embutida)."""
    if prob <= 0:
        return 999.0
    return round(1.0 / (prob * (1 + margem)), 3)

def kelly(prob: float, odd: float) -> float:
    if odd <= 1.0 or prob <= 0:
        return 0.0
    b  = odd - 1.0
    ev = b * prob - (1 - prob)
    if ev <= 0:
        return 0.0
    return round(BANKROLL * (ev / b) * KELLY_FRACTION, 2)

# ─────────────────────────────────────────────────────────────────────
#  AJUSTE BAYESIANO COM H2H + TAXAS HISTÓRICAS
# ─────────────────────────────────────────────────────────────────────
def ajustar_com_h2h_e_historico(probs: dict, hs: dict, as_: dict, h2h: dict | None) -> dict:
    """
    Blends as probabilidades do modelo Poisson com:
    1. Taxas históricas observadas (peso 0.25)
    2. Head-to-head (peso 0.15 se disponível)
    Mantém o modelo principal com peso dominante.
    """
    pc = probs["p_casa"]
    pe = probs["p_emp"]
    pf = probs["p_fora"]

    # Probabilidade histórica observada (média casa/fora)
    ph_c = (hs["tx_vitoria_casa"] + (1 - as_["tx_vitoria_fora"])) / 2
    ph_e = (hs["tx_empate_casa"]  + as_["tx_empate_fora"])         / 2
    ph_f = (hs["tx_derrota_casa"] + as_["tx_vitoria_fora"])        / 2

    # Normaliza histórico
    s = ph_c + ph_e + ph_f
    if s > 0:
        ph_c /= s; ph_e /= s; ph_f /= s

    # Blend 1: Poisson (60%) + Histórico (40%)
    w_modelo = 0.60
    w_hist   = 0.40

    if h2h and h2h["n"] >= 5:
        # Com H2H suficiente, usa três fontes
        w_modelo = 0.55
        w_hist   = 0.30
        w_h2h    = 0.15

        h2h_c = h2h["p_casa"]
        h2h_e = h2h["p_empate"]
        h2h_f = h2h["p_fora"]

        pc = w_modelo * pc + w_hist * ph_c + w_h2h * h2h_c
        pe = w_modelo * pe + w_hist * ph_e + w_h2h * h2h_e
        pf = w_modelo * pf + w_hist * ph_f + w_h2h * h2h_f
    else:
        pc = w_modelo * pc + w_hist * ph_c
        pe = w_modelo * pe + w_hist * ph_e
        pf = w_modelo * pf + w_hist * ph_f

    # Renormaliza
    total = pc + pe + pf
    if total > 0:
        pc /= total; pe /= total; pf /= total

    return {
        "p_casa": round(pc, 4),
        "p_emp":  round(pe, 4),
        "p_fora": round(pf, 4),
        "dc_1x":  round(pc + pe, 4),
        "dc_x2":  round(pe + pf, 4),
        "dc_12":  round(pc + pf, 4),
        "lh": probs["lh"],
        "la": probs["la"],
        "lt": probs["lt"],
    }

# ─────────────────────────────────────────────────────────────────────
#  PONTUAÇÃO DO JOGO (orientada a confiança 1X2)
# ─────────────────────────────────────────────────────────────────────
def pontuar_jogo_1x2(hs: dict, as_: dict, p: dict, h2h: dict | None,
                     classico: bool, decisivo: bool, mkt: dict) -> tuple:
    """
    Pontua a confiança do sinal de resultado. Quanto maior a probabilidade
    do resultado esperado e o edge sobre a odd de mercado, maior a pontuação.
    """
    s   = 0.0
    det = []

    prob     = mkt["prob"]
    edge     = mkt["edge"]
    ev       = mkt["ev"]
    chave    = mkt["chave"]

    # ── Probabilidade bruta do desfecho ─────────────────────────
    if   prob >= 0.75: s += 4.0; det.append(f"P={prob*100:.0f}%(+4.0)")
    elif prob >= 0.65: s += 3.0; det.append(f"P={prob*100:.0f}%(+3.0)")
    elif prob >= 0.55: s += 2.0; det.append(f"P={prob*100:.0f}%(+2.0)")
    elif prob >= 0.45: s += 1.5; det.append(f"P={prob*100:.0f}%(+1.5)")
    elif prob >= 0.38: s += 1.0; det.append(f"P={prob*100:.0f}%(+1.0)")
    else:              s += 0.5; det.append(f"P={prob*100:.0f}%(+0.5)")

    # ── Edge sobre a bookie ─────────────────────────────────────
    if   edge >= 0.15: s += 2.5; det.append(f"Edge={edge*100:.1f}%(+2.5)")
    elif edge >= 0.10: s += 2.0; det.append(f"Edge={edge*100:.1f}%(+2.0)")
    elif edge >= 0.06: s += 1.5; det.append(f"Edge={edge*100:.1f}%(+1.5)")
    elif edge >= 0.03: s += 1.0; det.append(f"Edge={edge*100:.1f}%(+1.0)")
    else:              s += 0.3; det.append(f"Edge={edge*100:.1f}%(+0.3)")

    # ── Diferença de qualidade ELO ──────────────────────────────
    diff_elo = hs.get("elo", 1400) - as_.get("elo", 1400)
    if "casa" in chave and diff_elo > 200:
        s += 1.0; det.append(f"ELO+{diff_elo:.0f}(+1.0)")
    elif "fora" in chave and diff_elo < -200:
        s += 1.0; det.append(f"ELO{diff_elo:.0f}(+1.0)")
    elif abs(diff_elo) < 80:
        if "empate" in chave:
            s += 0.8; det.append(f"ELO≈(+0.8)")

    # ── Consistência da forma recente ───────────────────────────
    fs_casa = hs.get("fs", 0)
    fs_fora = as_.get("fs", 0)
    if "casa" in chave and fs_casa > 0.20:
        s += 0.8; det.append(f"FormaCasa={fs_casa:.2f}(+0.8)")
    elif "fora" in chave and fs_fora > 0.20:
        s += 0.8; det.append(f"FormaFora={fs_fora:.2f}(+0.8)")

    # ── H2H como confirmação ────────────────────────────────────
    if h2h and h2h["n"] >= 5:
        if "casa" in chave and h2h["p_casa"] > 0.50:
            s += 1.0; det.append(f"H2HCasa={h2h['p_casa']*100:.0f}%(+1.0)")
        elif "fora" in chave and h2h["p_fora"] > 0.40:
            s += 1.0; det.append(f"H2HFora={h2h['p_fora']*100:.0f}%(+1.0)")
        elif "empate" in chave and h2h["p_empate"] > 0.30:
            s += 0.8; det.append(f"H2HEmp={h2h['p_empate']*100:.0f}%(+0.8)")

    # ── Modificadores situacionais ──────────────────────────────
    if classico:
        s *= 0.90
        det.append("Clássico x0.90")
    if decisivo:
        s += 0.40
        det.append("Decisivo +0.4")

    # Penaliza mercados de dupla chance — menos valor informativo
    if "dc_" in chave:
        s *= 0.85
        det.append("DuplaChance x0.85")

    return round(s, 2), det

# ─────────────────────────────────────────────────────────────────────
#  SELEÇÃO DO MELHOR MERCADO 1X2
# ─────────────────────────────────────────────────────────────────────
def melhor_mercado_1x2(p: dict) -> dict | None:
    """
    Avalia todos os mercados 1X2 e dupla chance.
    Usa odds justas calculadas pelo modelo.
    Seleciona o mercado com maior EV real (positivo).
    """
    mapa = {
        "casa":   p["p_casa"],
        "empate": p["p_emp"],
        "fora":   p["p_fora"],
        "dc_1x":  p["dc_1x"],
        "dc_x2":  p["dc_x2"],
        "dc_12":  p["dc_12"],
    }

    nomes = {
        "casa":   "Vitória Casa (1)",
        "empate": "Empate (X)",
        "fora":   "Vitória Visitante (2)",
        "dc_1x":  "Dupla Chance 1X (Casa ou Empate)",
        "dc_x2":  "Dupla Chance X2 (Fora ou Empate)",
        "dc_12":  "Dupla Chance 12 (Casa ou Fora — sem empate)",
    }

    candidatos = []
    for chave, prob in mapa.items():
        if prob <= 0:
            continue

        odd_j   = odd_justa(prob)          # odd sem margem
        odd_mkt = odd_com_margem(prob)     # odd simulada com margem bookie

        # EV calculado com a odd justa vs probabilidade real
        ev   = prob * (odd_j - 1) - (1 - prob)
        edge = prob - (1.0 / odd_j)

        # Só considera mercados com EV positivo mínimo
        if ev < MIN_EV:
            continue

        candidatos.append({
            "chave":      chave,
            "nome":       nomes[chave],
            "prob":       round(prob, 4),
            "odd_justa":  odd_j,
            "odd":        odd_mkt,      # referência para o apostador
            "ev":         round(ev, 4),
            "edge":       round(edge, 4),
            "stake":      kelly(prob, odd_j),
            # Bônus de liquidez: mercados simples têm prioridade leve
            "ev_adj":     ev + (0.01 if chave in ("casa", "fora") else 0.0),
        })

    if not candidatos:
        return None

    melhor = max(candidatos, key=lambda c: c["ev_adj"])
    melhor.pop("ev_adj", None)
    return melhor

def obter_tier(pontuacao: float) -> tuple:
    if   pontuacao >= TIER_ELITE_MIN:  return "ELITE",  STAKE_LIMITE["ELITE"]
    elif pontuacao >= TIER_QUENTE_MIN: return "QUENTE", STAKE_LIMITE["QUENTE"]
    elif pontuacao >= TIER_MORNO_MIN:  return "MORNO",  STAKE_LIMITE["MORNO"]
    elif pontuacao >= TIER_VIGIAR_MIN: return "VIGIAR", STAKE_LIMITE["VIGIAR"]
    return "FRIO", 0

# ─────────────────────────────────────────────────────────────────────
#  ANÁLISE DE PARTIDA
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

        # ── 1. Modelo Poisson com Dixon-Coles
        lh, la    = calcular_lambdas(hs, as_, lid)
        probs_raw = calcular_probs_1x2(lh, la)

        # ── 2. Ajuste Bayesiano com H2H e histórico
        probs = ajustar_com_h2h_e_historico(probs_raw, hs, as_, h2h)

        # ── 3. Seleciona melhor mercado
        mkt = melhor_mercado_1x2(probs)
        if not mkt:
            _diag_descartes["sem_mercado"] += 1
            log.debug(f"Sem mercado +EV: {hnm} x {anm}")
            return None

        # ── 4. Pontua o sinal
        sc, det = pontuar_jogo_1x2(hs, as_, probs, h2h, classico, decisivo, mkt)

        if sc < MIN_SCORE_ALERTA:
            _diag_descartes["score_baixo"] += 1
            log.debug(f"Score baixo ({sc:.2f}): {hnm} x {anm}")
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
            # Estatísticas
            "hgf":   hs["gf_t"],  "hga":  hs["ga_t"],
            "agf":   as_["gf_t"], "aga":  as_["ga_t"],
            "hforma":hs["forma"], "aforma":as_["forma"],
            "hpt":   hs["pt"],    "apt":   as_["pt"],
            "hcs":   hs["cs_rate"], "acs": as_["cs_rate"],
            "hmom":  hs["momentum"], "amom": as_["momentum"],
            "helo":  hs.get("elo", 1400), "aelo": as_.get("elo", 1400),
            "h_tx_v_casa":  hs["tx_vitoria_casa"],
            "h_tx_e_casa":  hs["tx_empate_casa"],
            "h_tx_d_casa":  hs["tx_derrota_casa"],
            "a_tx_v_fora":  as_["tx_vitoria_fora"],
            "a_tx_e_fora":  as_["tx_empate_fora"],
            "a_tx_d_fora":  as_["tx_derrota_fora"],
            # Modelo
            "lh": lh, "la": la, "lt": probs["lt"],
            "probs_raw": probs_raw,
            "probs":     probs,
            "h2h":       h2h,
            # Sinal
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
def filtrar_risco(jogos: list, state: dict) -> tuple:
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
def _barra(v: float, maximo: float = 7.0, largura: int = 8) -> str:
    f = int(min(max(v, 0) / maximo, 1.0) * largura)
    return "█" * f + "░" * (largura - f)

def _fmt_forma(forma: str) -> str:
    if not forma: return "--"
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

def _icone_mkt(chave: str) -> str:
    return {
        "casa":   "🏠",
        "empate": "🤝",
        "fora":   "✈️",
        "dc_1x":  "🔵",
        "dc_x2":  "🟠",
        "dc_12":  "⚡",
    }.get(chave, "🎯")

def _confianca_visual(prob: float) -> str:
    """Barra visual de confiança."""
    pct  = int(prob * 100)
    barr = _barra(prob * 10, maximo=10, largura=10)
    return f"{barr} {pct}%"

# ─────────────────────────────────────────────────────────────────────
#  MENSAGEM DE SINAL
# ─────────────────────────────────────────────────────────────────────
def formatar_sinal(j: dict, tipo: str = "NOVO SINAL") -> str:
    p    = j["probs"]
    pr   = j["probs_raw"]
    mkt  = j["mkt"]
    h2h  = j.get("h2h")

    data_j = j["dt"].strftime("%d/%m/%Y") if j["dt"] else "A confirmar"
    hora_j = j["dt"].strftime("%H:%M")    if j["dt"] else "--:--"
    sep    = "━" * 34

    # Favoritismo
    fav = "🏠 Casa favorita" if p["p_casa"] > p["p_fora"] + 0.15 else \
          "✈️ Visitante favorito" if p["p_fora"] > p["p_casa"] + 0.15 else \
          "⚖️ Jogo equilibrado"

    diff_elo = j["helo"] - j["aelo"]

    linhas = [
        sep,
        f"⚽  {tipo}",
        _label_tier(j["tier"]),
        sep,
        f"📅  Data:    {data_j}",
        f"🕐  Horário: {hora_j}  (horário de Brasília)",
        f"🌍  País:    {j['pais']}",
        f"🏆  Liga:    {j['liga']}",
        "",
        f"🏠  Casa:  {j['casa']}",
        f"✈️   Fora:  {j['fora']}",
        f"   {fav}",
    ]

    if j["classico"]: linhas.append("🔥  CLÁSSICO")
    if j["decisivo"]: linhas.append("🏆  JOGO DECISIVO / MATA-MATA")

    linhas += [
        "",
        "─── ANÁLISE DO MODELO ───",
        f"   Gols esperados: {j['lh']:.2f} (casa) + {j['la']:.2f} (fora) = {j['lt']:.2f} total",
        f"   ELO implícito: {j['helo']:.0f} vs {j['aelo']:.0f} "
        f"({'casa+'+str(abs(int(diff_elo))) if diff_elo >= 0 else 'fora+'+str(abs(int(diff_elo)))})",
        "",
        f"   Casa  — GF: {j['hgf']:.2f}  GA: {j['hga']:.2f}  "
        f"SG: {j['hcs']*100:.0f}%  {_seta(j['hmom'])}  Forma: {_fmt_forma(j['hforma'])}",
        f"         → V/E/D em casa: {j['h_tx_v_casa']*100:.0f}%/"
        f"{j['h_tx_e_casa']*100:.0f}%/{j['h_tx_d_casa']*100:.0f}%",
        "",
        f"   Fora  — GF: {j['agf']:.2f}  GA: {j['aga']:.2f}  "
        f"SG: {j['acs']*100:.0f}%  {_seta(j['amom'])}  Forma: {_fmt_forma(j['aforma'])}",
        f"         → V/E/D fora:  {j['a_tx_v_fora']*100:.0f}%/"
        f"{j['a_tx_e_fora']*100:.0f}%/{j['a_tx_d_fora']*100:.0f}%",
        "",
        "─── PROBABILIDADES FINAIS (modelo ajustado) ───",
        f"   🏠 Vitória Casa:      {_confianca_visual(p['p_casa'])}",
        f"   🤝 Empate:            {_confianca_visual(p['p_emp'])}",
        f"   ✈️  Vitória Visitante: {_confianca_visual(p['p_fora'])}",
        "",
        f"   Odd justa casa:    {odd_justa(p['p_casa']):.2f}",
        f"   Odd justa empate:  {odd_justa(p['p_emp']):.2f}",
        f"   Odd justa visitante: {odd_justa(p['p_fora']):.2f}",
    ]

    if h2h:
        linhas += [
            "",
            f"─── H2H ({h2h['n']} jogos recentes) ───",
            f"   Casa ganhou:   {h2h['p_casa']*100:.0f}%",
            f"   Empates:       {h2h['p_empate']*100:.0f}%",
            f"   Fora ganhou:   {h2h['p_fora']*100:.0f}%",
            f"   Média de gols: {h2h['media_gols']:.1f} por partida",
        ]

    # Detalhes do sinal principal
    linhas += [
        "",
        "─── SINAL RECOMENDADO ───",
        f"   {_icone_mkt(mkt['chave'])}  Mercado: {mkt['nome']}",
        f"   Probabilidade do modelo:   {mkt['prob']*100:.1f}%",
        f"   Odd justa (sem margem):    {mkt['odd_justa']:.3f}",
        f"   Odd estimada (c/ margem):  {mkt['odd']:.3f}",
        f"   EV (valor esperado):       {mkt['ev']:+.4f}",
        f"   Edge sobre a bookie:       {mkt['edge']*100:+.1f}%",
        "",
        f"   💡 Procure odds ACIMA de {mkt['odd_justa']:.2f} para ter vantagem real.",
        "",
        f"📈  Pontuação de confiança: {j['pontuacao']:.1f}  {_barra(j['pontuacao'])}",
        f"💰  Stake sugerida: R$ {j['stake']:.2f}  (Kelly {int(KELLY_FRACTION*100)}%)",
        "",
        f"📋  Detalhes: {' | '.join(j['detalhes'])}",
        sep,
        "⚠️  Verifique as odds reais antes de apostar.",
        "📌  Odds acima da justa = vantagem. Abaixo = prejuízo esperado.",
    ]
    return "\n".join(linhas)

# ─────────────────────────────────────────────────────────────────────
#  WATCHLIST DIÁRIA
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
        f"📅  {agora.strftime('%d/%m/%Y')}  🕐  {agora.strftime('%H:%M')}",
        (
            f"Total: {len(ok)} sinais  "
            f"🔥🔥{len(elite)}  🔥{len(quente)}  ⚡{len(morno)}  👀{len(vigiar)}"
        ),
        f"Exposição total: R$ {exposicao:.2f}  ({exposicao/BANKROLL*100:.1f}% da banca)",
        f"API: {_api_requests_restantes} requests restantes",
        sep,
    ]

    def bloco(lista, cab):
        if not lista: return
        linhas.append(f"\n{cab}")
        for j in lista:
            data_j  = j["dt"].strftime("%d/%m") if j["dt"] else "--/--"
            hora_j  = j["dt"].strftime("%H:%M") if j["dt"] else "--:--"
            p       = j["probs"]
            mkt     = j["mkt"]
            h2h     = j.get("h2h")
            fav_str = (
                "Casa fav." if p["p_casa"] > p["p_fora"] + 0.10 else
                "Fora fav." if p["p_fora"] > p["p_casa"] + 0.10 else
                "Equilibrado"
            )
            linhas.append(
                f"\n📅 {data_j}  🕐 {hora_j}  |  {j['liga']}\n"
                f"   🏠 {j['casa']}  x  ✈️ {j['fora']}\n"
                f"   [{fav_str}]  ELO: {j['helo']:.0f} vs {j['aelo']:.0f}\n"
                f"   Pontuação: {j['pontuacao']:.1f}  {_barra(j['pontuacao'])}\n"
                f"   Casa: {p['p_casa']*100:.0f}%  Emp: {p['p_emp']*100:.0f}%  "
                f"Fora: {p['p_fora']*100:.0f}%\n"
                f"   {_icone_mkt(mkt['chave'])} {mkt['nome']}\n"
                f"     Odd justa ~{mkt['odd_justa']:.2f}  |  "
                f"EV: {mkt['ev']:+.3f}  |  Stake: R$ {j['stake']:.0f}"
            )
            if h2h:
                linhas.append(
                    f"   H2H ({h2h['n']}j): Casa={h2h['p_casa']*100:.0f}%  "
                    f"Emp={h2h['p_empate']*100:.0f}%  Fora={h2h['p_fora']*100:.0f}%"
                )
            tags = []
            if j["classico"]: tags.append("CLÁSSICO")
            if j["decisivo"]: tags.append("DECISIVO")
            if tags: linhas.append(f"   [{' | '.join(tags)}]")

    bloco(elite,  "─── 🔥🔥 ELITE — Máxima Confiança ───")
    bloco(quente, "─── 🔥 QUENTE — Alta Confiança ───")
    bloco(morno,  "─── ⚡ MORNO — Boa Probabilidade ───")
    bloco(vigiar, "─── 👀 VIGIAR — Monitorar ───")

    linhas += [
        f"\n{sep}",
        "Odd justa = odd sem margem da bookie.",
        "Procure odds ACIMA da justa para ter vantagem real.",
        "⚠️  Verifique sempre as odds reais antes de apostar.",
    ]
    return "\n".join(linhas)

# ─────────────────────────────────────────────────────────────────────
#  REGISTRO E COLETA DE RESULTADOS
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
        "p_casa":    j["probs"]["p_casa"],
        "p_emp":     j["probs"]["p_emp"],
        "p_fora":    j["probs"]["p_fora"],
        "mkt_chave": j["mkt"]["chave"],
        "mkt_nome":  j["mkt"]["nome"],
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

    agora   = datetime.now()
    remover = []
    resolvidos = []

    for fid, inf in pendentes.items():
        try:
            dt_jogo   = datetime.fromisoformat(
                inf["dt_iso"].replace("Z", "+00:00")
            ).replace(tzinfo=None)
            decorrido = (agora - dt_jogo).total_seconds()

            if decorrido < 2 * 3600:  continue
            if decorrido > 30 * 3600: remover.append(fid); continue
            if not _pode_chamar_api(minimo=5): continue

            fx = buscar_resultado(int(fid))
            if not fx: continue

            status = (fx.get("fixture", {}).get("status", {}) or {}).get("short")
            if status not in ("FT", "AET", "PEN"): continue

            g  = fx.get("goals", {}) or {}
            hg = g.get("home") or 0
            ag = g.get("away") or 0

            chave = inf["mkt_chave"]
            if   chave == "casa":   acertou = hg > ag
            elif chave == "empate": acertou = hg == ag
            elif chave == "fora":   acertou = ag > hg
            elif chave == "dc_1x":  acertou = hg >= ag
            elif chave == "dc_x2":  acertou = ag >= hg
            elif chave == "dc_12":  acertou = hg != ag
            else:                   acertou = False

            pnl = inf["stake"] * (inf["mkt_odd"] - 1) if acertou else -inf["stake"]

            _gravar_historico({
                "data":       dt_jogo.strftime("%d/%m/%Y"),
                "horario":    dt_jogo.strftime("%H:%M"),
                "liga":       inf["liga"],
                "partida":    f"{inf['casa']} x {inf['fora']}",
                "tier":       inf["tier"],
                "pontuacao":  inf["pontuacao"],
                "lambda":     inf["lt"],
                "mercado":    inf["mkt_nome"],
                "prob_mod":   round(inf["mkt_prob"] * 100, 1),
                "odd_ref":    inf["mkt_odd"],
                "stake":      inf["stake"],
                "placar":     f"{hg}-{ag}",
                "resultado":  "ACERTO" if acertou else "ERRO",
                "pnl":        round(pnl, 2),
            })
            resolvidos.append((inf, hg, ag, acertou, pnl))
            remover.append(fid)

            if dt_jogo.strftime("%Y-%m-%d") == _hoje():
                state["pnl_hoje"] = state.get("pnl_hoje", 0.0) + pnl
                _save(ARQ_STATE, state)

        except Exception as e:
            log.debug(f"Pendente {fid}: {e}")

    for fid in remover:
        pendentes.pop(fid, None)
    _save(ARQ_PENDENTES, pendentes)

    for inf, hg, ag, acertou, pnl in resolvidos:
        icone = "✅" if acertou else "❌"
        tg(
            f"📋  RESULTADO DA APOSTA\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚽  {inf['casa']}  x  {inf['fora']}\n"
            f"🏆  {inf['liga']}  |  Nível: {inf['tier']}\n"
            f"🎯  Mercado: {inf['mkt_nome']}\n"
            f"📊  Placar: {hg}-{ag}\n"
            f"📐  Probabilidade prevista: {inf['mkt_prob']*100:.1f}%\n"
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
        return "📋  RELATÓRIO SEMANAL\nSem apostas nos últimos 7 dias."

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
        "📊  RELATÓRIO SEMANAL — BOT 1X2 v1.0",
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
            f"  {tn}: {tw}/{tt} ({tw/tt*100:.0f}%)  ROI: {tp/ts*100 if ts else 0:+.1f}%"
        )

    ls.append("\nPor Mercado (1X2):")
    for mn, mr in sorted(mkts.items(), key=lambda x: -len(x[1])):
        mw = sum(1 for r in mr if r["resultado"] == "ACERTO")
        mt = len(mr)
        mp = sum(float(r["pnl"]) for r in mr)
        ls.append(f"  {mn}: {mw}/{mt} ({mw/mt*100:.0f}%)   PnL: R$ {mp:+.0f}")

    desvio = abs(m_prob - taxa)
    ls += [
        f"\nCalibração: prevista={m_prob:.1f}%  real={taxa:.1f}%  desvio={desvio:.1f}%",
        (
            "✅ Modelo calibrado" if desvio <= 10 else
            "⚠️ Recalibrar modelo" if desvio > 15 else
            "👁 Monitorar calibração"
        ),
        sep,
    ]
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
#  LOOP PRINCIPAL
# ─────────────────────────────────────────────────────────────────────
def main():
    global _encerrar, _aviso_api_enviado, _diag_descartes

    log.info("=" * 62)
    log.info("  BOT RESULTADO 1X2 v1.0")
    log.info("  Motor: Dixon-Coles + Ajuste Bayesiano + ELO implícito")
    log.info(f"  Score mín.: {MIN_SCORE_ALERTA}  |  EV mín.: {MIN_EV*100:.0f}%  |  Scan: {SCAN_INTERVAL}s")
    log.info("=" * 62)

    if not testar_telegram():
        log.error("Telegram não conectou. Verifique token e chat_id.")
        return

    _carregar_sinais_enviados()

    state = _load(ARQ_STATE)
    if state.get("pnl_data") != _hoje():
        state["pnl_hoje"]  = 0.0
        state["pnl_data"]  = _hoje()
        _aviso_api_enviado = False
        _save(ARQ_STATE, state)

    thread_cmd = threading.Thread(target=_thread_terminal, daemon=True)
    thread_cmd.start()

    tg(
        f"🤖  BOT RESULTADO 1X2 v1.0 INICIADO\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📅  {datetime.now().strftime('%d/%m/%Y às %H:%M')}\n"
        f"🧠  Motor: Dixon-Coles + Bayesiano + ELO\n"
        f"⚙️   Score mínimo: {MIN_SCORE_ALERTA}  |  EV mín.: {MIN_EV*100:.0f}%\n"
        f"⏱   Varredura a cada: {SCAN_INTERVAL}s\n"
        f"🎯  Máx. sinais/dia: {MAX_JOGOS_DIA}\n"
        f"💰  Banca: R$ {BANKROLL:.0f}  |  Exposição máx.: {MAX_EXPOSICAO_PCT}%\n"
        f"🕐  Watchlists: {WATCHLIST_HORAS}h\n"
        f"🔔  Re-alertas: T-{REALERTAS_MIN[0]}min, T-{REALERTAS_MIN[1]}min, T-{REALERTAS_MIN[2]}min\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"✅  Sistema ativo!\n"
        f"(Sinais já enviados hoje: {len(_sinais_enviados_hoje)})"
    )

    ultima_coleta = 0.0

    while not _encerrar:
        try:
            if esta_pausado():
                time.sleep(5)
                continue

            _diag_descartes = {
                "score_baixo": 0, "tier_frio": 0,
                "sem_mercado": 0, "sem_dados": 0,
            }

            agora = datetime.now()
            log.info(
                f"\n{'─'*54}\n"
                f"  Varredura: {agora.strftime('%d/%m/%Y %H:%M:%S')}\n"
                f"  API: {_api_requests_restantes} req restantes  |  "
                f"Sinais hoje: {len(_sinais_enviados_hoje)}\n"
                f"{'─'*54}"
            )

            # ── 0. Coleta de resultados ─────────────────────────────
            if time.time() - ultima_coleta > INTERVALO_COLETA_RESULTADOS:
                coletar_resultados(state)
                ultima_coleta = time.time()

            # ── 1. Fixtures (hoje + amanhã) ─────────────────────────
            hoje   = agora.strftime("%Y-%m-%d")
            amanha = (agora + timedelta(days=1)).strftime("%Y-%m-%d")
            raw    = buscar_fixtures(hoje) + buscar_fixtures(amanha)
            pre    = [
                f for f in raw
                if f.get("fixture", {}).get("status", {}).get("short") in ("NS", "TBD", "PST")
            ]

            # Filtra apenas ligas mapeadas (modelo calibrado)
            pre = [
                f for f in pre
                if f.get("league", {}).get("id") in LIGAS_CFG
            ]
            log.info(f"Partidas pré-jogo (ligas mapeadas): {len(pre)}")

            # ── 2. Limita por requests disponíveis ──────────────────
            if _api_requests_restantes < 200:
                pre = sorted(pre, key=lambda f: f.get("fixture", {}).get("date", ""))
                max_an = min(80, max(15, _api_requests_restantes // 3))
                pre    = pre[:max_an]
                log.info(f"Requests baixos: analisando {len(pre)} jogos")
            else:
                pre = pre[:100]

            # ── 3. Análise das partidas ─────────────────────────────
            analises = []
            for fx in pre:
                if _encerrar or esta_pausado():
                    break
                if not _pode_chamar_api(minimo=10):
                    log.warning("Requests insuficientes, encerrando ciclo.")
                    break
                a = analisar_partida(fx)
                if a:
                    analises.append(a)

            log.info(
                f"Qualificados: {len(analises)}  |  "
                f"Descartes → score:{_diag_descartes['score_baixo']} "
                f"tier:{_diag_descartes['tier_frio']} "
                f"mercado:{_diag_descartes['sem_mercado']} "
                f"dados:{_diag_descartes['sem_dados']}"
            )

            # ── 4. Gestão de risco ──────────────────────────────────
            filtrados, exposicao = filtrar_risco(analises, state)
            aprovados = [j for j in filtrados if j.get("risco") == "OK"]
            log.info(f"Aprovados: {len(aprovados)}  |  Exposição: R$ {exposicao:.2f}")

            # ── 5. Envio de sinais (anti-duplicata) ─────────────────
            for j in aprovados:
                chave = _chave_sinal(j["fid"], j["mkt"]["chave"], "_inicial")
                if not _ja_enviado(chave):
                    msg = formatar_sinal(j, tipo="NOVO SINAL 1X2")
                    if tg(msg):
                        _registrar_sinal(chave)
                        registrar_pendente(j)
                        log.info(
                            f"Sinal enviado: {j['casa']} x {j['fora']}  "
                            f"→ {j['mkt']['nome']}  "
                            f"({j['mkt']['prob']*100:.0f}% | Edge {j['mkt']['edge']*100:+.1f}%)  "
                            f"Tier: {j['tier']}"
                        )
                        # Registra no site (endpoint /resultados)
                        try:
                            _reg = globals().get("_web_registrar_resultado")
                            if _reg:
                                _reg(j)
                        except Exception:
                            pass
                        time.sleep(0.5)
                else:
                    log.debug(f"Duplicata ignorada: {j['casa']} x {j['fora']}")

            # ── 6. Watchlists por horário ───────────────────────────
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
                        msg = formatar_watchlist(
                            jogos_hoje, exp_h,
                            titulos.get(h, f"WATCHLIST {h}h")
                        )
                        if msg:
                            tg(msg)
                            log.info(f"Watchlist {h}h enviada ({len(jogos_hoje)} jogos)")
                    marcar_watchlist(state, h)

            # ── 7. Re-alertas pré-jogo ──────────────────────────────
            for j in aprovados:
                if j["pontuacao"] < MIN_SCORE_REALERTA or not j["dt"]:
                    continue
                delta_min = (j["dt"] - agora).total_seconds() / 60
                if delta_min < 0 or delta_min > max(REALERTAS_MIN) + 15:
                    continue

                for t_min in REALERTAS_MIN:
                    if (t_min - 8) <= delta_min <= t_min:
                        chave = _chave_sinal(j["fid"], j["mkt"]["chave"], f"_t{t_min}")
                        if not _ja_enviado(chave):
                            msg = formatar_sinal(j, tipo=f"RE-ALERTA 1X2 — {t_min}MIN")
                            if tg(msg):
                                _registrar_sinal(chave)
                                log.info(f"Re-alerta T-{t_min}min: {j['casa']} x {j['fora']}")
                        break

            # ── 8. Relatório semanal (segunda às 9h) ────────────────
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
                f"API: {_api_requests_restantes} req]"
            )
            for _ in range(SCAN_INTERVAL // 2):
                if _encerrar:
                    break
                time.sleep(2)

    log.info("Bot encerrado.")
    tg(
        f"🔴  BOT 1X2 ENCERRADO\n"
        f"📅  {datetime.now().strftime('%d/%m/%Y às %H:%M')}\n"
        f"📊  Sinais enviados hoje: {len(_sinais_enviados_hoje)}"
    )


if __name__ == "__main__":
    main()
