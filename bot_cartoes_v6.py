"""
BOT PRÉ-JOGO — ANÁLISE DE CARTÕES (v6.0)
==========================================
RECONSTRUÇÃO PROFUNDA — Changelog v5.1 → v6.0:

═══════════════════════════════════════════════════════════
 MODELO ESTATÍSTICO (precisão)
═══════════════════════════════════════════════════════════

✓ POISSON DUPLO (Double Poisson)
  Em vez de um λ único, calcula λ_casa e λ_fora separados,
  depois combina via convolução. Captura assimetria real
  entre mandante e visitante.

✓ REGRESSÃO À MÉDIA BAYESIANA
  Usa prior global (média da liga) + dados do time.
  Times com poucos jogos são puxados para a média,
  times com muitos jogos mantêm seus dados reais.
  Substitui a penalidade binária de amostra (0.7x).

✓ HEAD-TO-HEAD (H2H) REAL
  Puxa confrontos diretos via API-Football.
  Média de cartões dos últimos 5 H2H entra como fator
  com peso proporcional à quantidade de jogos disponíveis.

✓ FORM PONDERADA (ÚLTIMOS 5 JOGOS COM DECAIMENTO)
  Jogos mais recentes têm peso maior (1.0, 0.85, 0.72, 0.61, 0.52).
  Vitória → time mais tranquilo (-0.1 cart).
  Derrota → time mais nervoso (+0.15 cart).

✓ IMPORTÂNCIA DO JOGO (CONTEXTUAL)
  Puxa standings via API para saber posição na tabela.
  Rebaixamento, G4, decisão de título → multiplicadores reais.
  Eliminatória com vantagem/desvantagem → ajuste fino.

✓ CALIBRAÇÃO DE SCORE CONTÍNUA (não mais thresholds fixos)
  Score final é Z-score normalizado contra distribuição
  histórica de todos os jogos analisados.

✓ EDGE REAL CONTRA O MERCADO
  Busca odds reais via API-Football (quando disponível)
  em vez de usar odds fixas hardcoded.

═══════════════════════════════════════════════════════════
 INTEGRAÇÃO GEMINI (IA generativa)
═══════════════════════════════════════════════════════════

✓ ANÁLISE CONTEXTUAL PRÉ-JOGO
  Envia dados estruturados ao Gemini para gerar análise
  qualitativa: lesões, suspensões, clima tático, momento.

✓ VALIDAÇÃO CRUZADA DE SCORE
  Gemini recebe os dados e dá sua própria avaliação (1-10).
  Discrepância > 2 pontos → flag de cautela.

═══════════════════════════════════════════════════════════
 INFRAESTRUTURA
═══════════════════════════════════════════════════════════

✓ TIMEZONE ROBUSTO (Windows + Linux)
✓ SEND_TELEGRAM COM FALLBACK (Markdown → texto puro)
✓ HEARTBEAT PERIÓDICO
✓ FILTRO DE RISCO COM LOG DETALHADO
✓ GESTÃO DINÂMICA DE BANCA (Kelly fracionado adaptativo)

Requisitos: pip install requests scipy tzdata google-generativeai
Como rodar: python bot_cartoes_v6.py
"""

import requests
import time
import json
import os
import csv
import logging
import threading
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, time as dtime, timezone
from scipy.stats import poisson
from math import exp, lgamma, log as ln

# ═══════════════════════════════════════════════════════════
#  TIMEZONE ROBUSTO (Windows + Linux + Mac)
# ═══════════════════════════════════════════════════════════
try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    try:
        TZ_LOCAL = ZoneInfo("America/Campo_Grande")
    except (ZoneInfoNotFoundError, KeyError):
        print("⚠️  tzdata não encontrado — usando offset fixo UTC-4.")
        print("   Para melhorar, rode: pip install tzdata")
        TZ_LOCAL = timezone(timedelta(hours=-4))
except ImportError:
    TZ_LOCAL = timezone(timedelta(hours=-4))

# ═══════════════════════════════════════════════════════════
#  GEMINI (IA GENERATIVA)
# ═══════════════════════════════════════════════════════════
GEMINI_API_KEY = "AQ.Ab8RN6Kqa09XYE9K66jnDpMB7EZ1So4zrzWWKlkaTQm01iIFVg"
GEMINI_ATIVO   = True
GEMINI_MODEL   = "gemini-2.0-flash"
GEMINI_MAX_POR_CICLO  = 3          # máximo de chamadas ao Gemini por ciclo de scan
GEMINI_INTERVALO_SEG  = 4          # segundos entre chamadas (respeita rate limit free tier)

_sess_gemini       = None
_gemini_disponivel = True          # desliga automaticamente se cota estourar
_gemini_chamadas_ciclo = 0         # contador por ciclo

def _init_gemini():
    """Inicializa sessão HTTP para Gemini. Chamado uma vez no startup."""
    global _sess_gemini
    _sess_gemini = requests.Session()

def _resetar_gemini_ciclo():
    """Chamado no início de cada ciclo para resetar o contador."""
    global _gemini_chamadas_ciclo
    _gemini_chamadas_ciclo = 0

def consultar_gemini(prompt: str, max_tokens: int = 1024) -> str:
    """
    Consulta a API REST do Gemini com rate limiting embutido.
    Retorna string vazia se não puder consultar (cota, desativado, erro).
    """
    global _gemini_disponivel, _gemini_chamadas_ciclo

    if not GEMINI_ATIVO or not GEMINI_API_KEY or not _gemini_disponivel:
        return ""

    # Limite por ciclo
    if _gemini_chamadas_ciclo >= GEMINI_MAX_POR_CICLO:
        log.debug(f"Gemini: limite de {GEMINI_MAX_POR_CICLO} chamadas/ciclo atingido")
        return ""

    try:
        # Espera entre chamadas para respeitar rate limit
        if _gemini_chamadas_ciclo > 0:
            time.sleep(GEMINI_INTERVALO_SEG)

        url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "maxOutputTokens": max_tokens,
                "temperature": 0.3,
            }
        }
        r = _sess_gemini.post(
            url,
            params={"key": GEMINI_API_KEY},
            json=payload,
            timeout=15
        )

        _gemini_chamadas_ciclo += 1

        if r.status_code == 429:
            log.warning("Gemini: cota excedida (429) — desativando Gemini até próximo reinício")
            _gemini_disponivel = False
            return ""
        if r.status_code != 200:
            log.warning(f"Gemini retornou {r.status_code}: {r.text[:150]}")
            return ""

        data = r.json()
        candidates = data.get("candidates", [])
        if candidates:
            parts = candidates[0].get("content", {}).get("parts", [])
            if parts:
                return parts[0].get("text", "")
        return ""
    except Exception as e:
        log.debug(f"Erro ao consultar Gemini: {e}")
        return ""

def gemini_analise_pre_jogo(dados_jogo: dict) -> dict:
    """
    Envia dados estruturados ao Gemini para análise contextual.
    Retorna dict com 'analise_texto', 'score_gemini' (1-10), 'confianca'.
    """
    if not GEMINI_ATIVO:
        return {"analise_texto": "", "score_gemini": 0, "confianca": "N/A"}

    prompt = f"""Você é um analista de trading esportivo especializado em mercado de cartões.
Analise os seguintes dados de um jogo e forneça:

1. Uma análise breve (3-4 linhas) sobre o potencial de cartões neste jogo
2. Sua avaliação de 1 a 10 (onde 10 = altíssima probabilidade de muitos cartões)
3. Nível de confiança: ALTA, MÉDIA ou BAIXA

DADOS DO JOGO:
- Liga: {dados_jogo.get('liga', '?')}
- Mandante: {dados_jogo.get('casa', '?')} ({dados_jogo.get('cartoes_casa', 0):.2f} cartões/jogo, forma: {dados_jogo.get('forma_casa', '?')})
- Visitante: {dados_jogo.get('fora', '?')} ({dados_jogo.get('cartoes_fora', 0):.2f} cartões/jogo, forma: {dados_jogo.get('forma_fora', '?')})
- Árbitro: {dados_jogo.get('arb_nome', '?')} ({dados_jogo.get('arb_cartoes', 0):.1f} cartões/jogo, dados {'reais' if dados_jogo.get('arb_conhecido') else 'estimados'})
- É clássico/derby: {'Sim' if dados_jogo.get('classico') else 'Não'}
- Fase eliminatória: {'Sim' if dados_jogo.get('decisivo') else 'Não'}
- Média H2H cartões: {dados_jogo.get('h2h_media_cartoes', 'N/D')}
- Score do modelo: {dados_jogo.get('score', 0):.1f}
- Lambda (cartões esperados): {dados_jogo.get('lambda', 0):.2f}
- Contexto tabela: {dados_jogo.get('contexto_tabela', 'N/D')}

Responda EXATAMENTE neste formato JSON (sem markdown, sem crases):
{{"analise": "sua análise aqui", "score": 7.5, "confianca": "ALTA"}}"""

    resposta = consultar_gemini(prompt, max_tokens=500)
    if not resposta:
        return {"analise_texto": "", "score_gemini": 0, "confianca": "N/A"}

    try:
        # Tenta extrair JSON da resposta
        resposta_limpa = resposta.strip()
        # Remove possíveis blocos markdown
        if resposta_limpa.startswith("```"):
            resposta_limpa = resposta_limpa.split("\n", 1)[-1]
            resposta_limpa = resposta_limpa.rsplit("```", 1)[0]
        parsed = json.loads(resposta_limpa.strip())
        return {
            "analise_texto": parsed.get("analise", ""),
            "score_gemini":  float(parsed.get("score", 0)),
            "confianca":     parsed.get("confianca", "N/A"),
        }
    except (json.JSONDecodeError, ValueError) as e:
        log.debug(f"Gemini retornou formato inesperado: {resposta[:200]}")
        return {"analise_texto": resposta[:300], "score_gemini": 0, "confianca": "N/A"}


# ═══════════════════════════════════════════════════════════
#  CREDENCIAIS
# ═══════════════════════════════════════════════════════════
# ⚠️  REVOGAR E REGENERAR — nunca versionar estas chaves
SPORTMONKS_TOKEN  = ""
API_FOOTBALL_KEY  = os.environ.get("API_FOOTBALL_KEY", "")
TELEGRAM_TOKEN    = os.environ.get("TELEGRAM_TOKEN_CARTOES") or os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID  = os.environ.get("TELEGRAM_CHAT_ID", "")

# ═══════════════════════════════════════════════════════════
#  CONFIGURAÇÕES GERAIS
# ═══════════════════════════════════════════════════════════
MIN_SCORE_ALERTA      = 4.5          # ↑ era 4.0 — mais seletivo
INTERVALO_SCAN        = 60           # ↓ era 90 — varredura a cada 1min
JOGOS_FORMA_RECENTE   = 5
ARQUIVO_ALERTAS       = "alertas_cartoes.json"
ARQUIVO_ESTADO        = "estado_bot.json"
ARQUIVO_HISTORICO     = "historico.csv"
ARQUIVO_PENDENTES     = "pendentes.json"
ARQUIVO_LOG           = "bot_cartoes.log"
CACHE_TIMES_ARQUIVO   = "cache_times.json"
CACHE_ARBS_ARQUIVO    = "cache_arbitros.json"
CACHE_H2H_ARQUIVO     = "cache_h2h.json"

TTL_CACHE_TIME        = 6 * 3600
TTL_CACHE_ARBITRO     = 24 * 3600
TTL_CACHE_PARTIDAS    = 600
TTL_CACHE_H2H         = 48 * 3600    # 48h para H2H

# ═══════════════════════════════════════════════════════════
#  WATCHLIST DIÁRIA
# ═══════════════════════════════════════════════════════════
WATCHLIST_ATIVA       = True
WATCHLIST_HORA        = 7
WATCHLIST_MINUTO      = 30
PRE_JOGO_ATIVO        = True

# ═══════════════════════════════════════════════════════════
#  MARCOS DE ALERTA — múltiplos níveis de antecedência
#  Cada marco envia um alerta distinto com propósito específico
# ═══════════════════════════════════════════════════════════
# ANTECIPADOS (pré-live — para planejamento e odds antecipadas):
#   900min = 15h antes (manhã do dia do jogo / noite do dia anterior)
#   600min = 10h antes (primeira janela de odds estáveis)
#   360min = 6h antes (análise consolidada, mercado aquecendo)
# PRÉ-JOGO (perto do kickoff — para entrada efetiva):
#   120min = 2h antes (janela clássica pré-jogo)
#   60min  = 1h antes (última chance antes dos lineups)
#   30min  = 30min antes (alerta final, lineups já divulgados)
PRE_JOGO_MARCOS       = [900, 600, 360, 120, 60, 30]

# Marcos que são considerados "antecipados" (pré-live de longo prazo)
# Usado para diferenciar título/estilo do alerta e o score mínimo exigido
MARCOS_ANTECIPADOS    = [900, 600, 360]

# Score mínimo para alertas ANTECIPADOS (mais exigente — só jogos realmente fortes)
MIN_SCORE_ANTECIPADO  = 6.5          # só tier QUENTE para antecipados
# Score mínimo para alertas PRÉ-JOGO padrão
MIN_SCORE_PRE_JOGO    = 5.5

# Horizonte máximo de busca de jogos (em horas)
HORIZONTE_HORAS       = 20           # cobre os 15h + margem de segurança

MAX_JOGOS_WATCHLIST   = 15

# Compatibilidade retroativa
PRE_JOGO_MINUTOS      = 60

# ═══════════════════════════════════════════════════════════
#  TIERS DE CONFIANÇA
# ═══════════════════════════════════════════════════════════
TIER_QUENTE_MIN       = 7.0          # ↑ era 6.0 — mais exigente
TIER_MORNO_MIN        = 5.5          # ↑ era 5.0
TIER_OBSERVAR_MIN     = 4.5          # ↑ era 4.0

# ═══════════════════════════════════════════════════════════
#  GESTÃO DE RISCO
# ═══════════════════════════════════════════════════════════
BANCA                 = 1000.0
MAX_OPS_DIA           = 6            # ↓ era 8 — mais seletivo
MAX_EXPOSICAO_PCT     = 40.0         # ↑ era 25
KELLY_FRACAO          = 0.15         # ↓ era 0.25 — mais conservador
ODD_PADRAO_35         = 1.50
ODD_PADRAO_45         = 2.00
ODD_PADRAO_55         = 2.80

# ═══════════════════════════════════════════════════════════
#  REGRESSÃO À MÉDIA BAYESIANA
# ═══════════════════════════════════════════════════════════
PRIOR_CARTOES_JOGO    = 4.2          # média global de cartões/jogo (todas as ligas)
PRIOR_FALTAS_JOGO     = 22.0
PRIOR_PESO_JOGOS      = 5            # "5 jogos-fantasma" com a média global
# Fórmula: estimativa = (PRIOR_PESO * PRIOR + jogos_reais * media_real) / (PRIOR_PESO + jogos_reais)

# ═══════════════════════════════════════════════════════════
#  FORM DECAY
# ═══════════════════════════════════════════════════════════
FORM_DECAY_WEIGHTS    = [1.0, 0.85, 0.72, 0.61, 0.52]  # do mais recente ao mais antigo
FORM_VITORIA_AJUSTE   = -0.10        # time vencedor → menos nervoso
FORM_DERROTA_AJUSTE   = +0.15        # time perdedor → mais nervoso
FORM_EMPATE_AJUSTE    = +0.05

# ═══════════════════════════════════════════════════════════
#  CONTEXTO DE TABELA
# ═══════════════════════════════════════════════════════════
CONTEXTO_REBAIXAMENTO_MULT  = 1.12
CONTEXTO_G4_MULT            = 1.06
CONTEXTO_TITULO_MULT        = 1.08
CONTEXTO_MEIO_TABELA_MULT   = 1.00

# ═══════════════════════════════════════════════════════════
#  CLOSING LOOP + RELATÓRIOS
# ═══════════════════════════════════════════════════════════
CLOSING_LOOP_ATIVO    = True
RELATORIO_SEMANAL     = True
RELATORIO_DIA_SEMANA  = 0
RELATORIO_HORA        = 9
RELATORIO_MINUTO      = 30

# ═══════════════════════════════════════════════════════════
#  HEARTBEAT
# ═══════════════════════════════════════════════════════════
HEARTBEAT_ATIVO       = True
HEARTBEAT_CICLOS      = 12

# ═══════════════════════════════════════════════════════════
#  PARALELISMO E PERFORMANCE
# ═══════════════════════════════════════════════════════════
MAX_WORKERS           = 10
API_TIMEOUT           = 10
MAX_TENTATIVAS        = 2

# ═══════════════════════════════════════════════════════════
#  LIGAS MONITORADAS
#  peso = multiplicador de score baseado na agressividade da liga
#  prior_cartoes = média de cartões/jogo específica da liga (bayesian prior)
# ═══════════════════════════════════════════════════════════
LIGAS_MONITORADAS = {
    71:  {"nome": "Brasileirão Série A",         "peso": 1.40, "prior": 5.2},
    72:  {"nome": "Brasileirão Série B",         "peso": 1.25, "prior": 5.0},
    128: {"nome": "Argentina — Liga Profesional","peso": 1.60, "prior": 5.8},
    13:  {"nome": "Copa Libertadores",           "peso": 1.50, "prior": 5.5},
    11:  {"nome": "Copa Sul-Americana",          "peso": 1.35, "prior": 5.0},
    140: {"nome": "La Liga (Espanha)",           "peso": 1.25, "prior": 4.8},
    61:  {"nome": "Ligue 1 (França)",            "peso": 1.15, "prior": 4.2},
    135: {"nome": "Serie A (Itália)",            "peso": 1.30, "prior": 4.9},
    39:  {"nome": "Premier League (Inglaterra)", "peso": 0.85, "prior": 3.6},
    78:  {"nome": "Bundesliga (Alemanha)",       "peso": 0.90, "prior": 3.8},
    2:   {"nome": "Champions League",            "peso": 1.25, "prior": 4.5},
    3:   {"nome": "Europa League",               "peso": 1.15, "prior": 4.2},
}

CLÁSSICOS = [
    ("flamengo", "fluminense"), ("flamengo", "vasco"), ("flamengo", "botafogo"),
    ("corinthians", "palmeiras"), ("corinthians", "são paulo"),
    ("corinthians", "santos"), ("são paulo", "palmeiras"),
    ("santos", "palmeiras"), ("grêmio", "internacional"),
    ("atlético-mg", "cruzeiro"), ("river", "boca"),
    ("real madrid", "barcelona"), ("milan", "inter"),
    ("roma", "lazio"), ("juventus", "torino"),
    ("psg", "marseille"), ("liverpool", "everton"),
    ("manchester united", "manchester city"),
    ("arsenal", "tottenham"), ("bayern", "dortmund"),
    ("fla", "flu"), ("galo", "cruzeiro"),
    ("santa cruz", "sport"), ("bahia", "vitória"),
    ("ceará", "fortaleza"), ("remo", "paysandu"),
]

# ═══════════════════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(ARQUIVO_LOG, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════
#  ESTADO GLOBAL
# ═══════════════════════════════════════════════════════════
_bot_pausado      = False
_ultimo_update_id = 0
_lock_estado      = threading.Lock()

def bot_esta_pausado() -> bool:
    with _lock_estado:
        return _bot_pausado

def pausar_bot():
    global _bot_pausado
    with _lock_estado:
        _bot_pausado = True
    log.info("⏸  Bot PAUSADO pelo operador.")

def retomar_bot():
    global _bot_pausado
    with _lock_estado:
        _bot_pausado = False
    log.info("▶️  Bot RETOMADO pelo operador.")

# ═══════════════════════════════════════════════════════════
#  CACHE EM MEMÓRIA
# ═══════════════════════════════════════════════════════════
_cache_times    = {}
_cache_arbitros = {}
_cache_partidas = {}
_cache_h2h      = {}
_cache_standings = {}   # liga_id-season → (data, ts)
_cache_odds     = {}    # fixture_id → (odds_dict, ts)

# ═══════════════════════════════════════════════════════════
#  HTTP SESSIONS
# ═══════════════════════════════════════════════════════════
_sess_api = requests.Session()
_sess_api.headers.update({"x-apisports-key": API_FOOTBALL_KEY})
_sess_tg  = requests.Session()
APIF_HOST = "v3.football.api-sports.io"

# ═══════════════════════════════════════════════════════════
#  TIMEZONE HELPERS
# ═══════════════════════════════════════════════════════════
def utc_para_local(dt_utc: datetime) -> datetime:
    if dt_utc is None:
        return None
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    return dt_utc.astimezone(TZ_LOCAL).replace(tzinfo=None)

def parse_iso_para_local(iso_str: str):
    if not iso_str:
        return None
    try:
        dt_utc = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return utc_para_local(dt_utc)
    except Exception:
        return None

# ═══════════════════════════════════════════════════════════
#  HELPERS JSON / CACHE
# ═══════════════════════════════════════════════════════════
def _ler_json(path: str) -> dict:
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def _salvar_json(path: str, data: dict):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.warning(f"Falha ao salvar {path}: {e}")

def _limpar_expirados(cache: dict, ttl: int) -> dict:
    agora = time.time()
    return {
        k: v for k, v in cache.items()
        if isinstance(v, dict) and agora - v.get("_ts", 0) < ttl
    }

# ═══════════════════════════════════════════════════════════
#  FORMATAÇÃO PT-BR
# ═══════════════════════════════════════════════════════════
DIAS_SEMANA = {
    0: "Segunda-feira", 1: "Terça-feira", 2: "Quarta-feira",
    3: "Quinta-feira", 4: "Sexta-feira", 5: "Sábado", 6: "Domingo"
}
MESES = {
    1:"janeiro", 2:"fevereiro", 3:"março", 4:"abril",
    5:"maio", 6:"junho", 7:"julho", 8:"agosto",
    9:"setembro", 10:"outubro", 11:"novembro", 12:"dezembro"
}

def formatar_data_ptbr(dt: datetime) -> str:
    return f"{DIAS_SEMANA[dt.weekday()]}, {dt.day} de {MESES[dt.month]} de {dt.year}"

def formatar_hora(dt: datetime) -> str:
    return dt.strftime("%H:%M")

def formatar_datetime_curto(dt: datetime) -> str:
    return dt.strftime("%d/%m") + f" às {dt.strftime('%H:%M')}"

# ═══════════════════════════════════════════════════════════
#  API-FOOTBALL: CHAMADAS BASE
# ═══════════════════════════════════════════════════════════
def _apif_get(path: str, params: dict, tentativas: int = MAX_TENTATIVAS):
    url = f"https://{APIF_HOST}/{path}"
    for tentativa in range(tentativas + 1):
        try:
            r = _sess_api.get(url, params=params, timeout=API_TIMEOUT)
            if r.status_code == 429:
                espera = 60 * (tentativa + 1)
                log.warning(f"Rate limit — aguardando {espera}s")
                time.sleep(espera)
                continue
            if r.status_code in (401, 403):
                log.error(f"Auth falhou ({r.status_code}) — {path}")
                return None
            if r.status_code >= 500:
                if tentativa < tentativas:
                    time.sleep(2 ** tentativa)
                    continue
                return None
            r.raise_for_status()
            return r.json()
        except requests.Timeout:
            if tentativa < tentativas:
                time.sleep(1)
                continue
            return None
        except Exception as e:
            log.debug(f"Erro em {path}: {e}")
            return None
    return None

# ═══════════════════════════════════════════════════════════
#  API-FOOTBALL: FIXTURES
# ═══════════════════════════════════════════════════════════
def buscar_partidas(data_str: str = None) -> list:
    if not data_str:
        data_str = datetime.now().strftime("%Y-%m-%d")
    entrada = _cache_partidas.get(data_str)
    if entrada:
        fixtures, ts = entrada
        if time.time() - ts < TTL_CACHE_PARTIDAS:
            return fixtures
    dados = _apif_get("fixtures", {"date": data_str})
    if not dados:
        return []
    fixtures = dados.get("response", [])
    _cache_partidas[data_str] = (fixtures, time.time())
    return fixtures

def buscar_resultado_fixture(fixture_id: int):
    dados = _apif_get("fixtures", {"id": fixture_id})
    if not dados:
        return None
    resp = dados.get("response", [])
    return resp[0] if resp else None

# ═══════════════════════════════════════════════════════════
#  API-FOOTBALL: STATS DO TIME (com Bayesian shrinkage)
# ═══════════════════════════════════════════════════════════
def _stats_time_padrao() -> dict:
    return {
        "cartoes_por_jogo": 0, "cartoes_por_jogo_raw": 0,
        "faltas_por_jogo": 0, "forma": "",
        "amarelos_total": 0, "vermelhos_total": 0,
        "jogos_disputados": 0, "_ts": time.time()
    }

def _bayesian_estimate(observed_mean: float, n_games: int,
                        prior_mean: float, prior_weight: int) -> float:
    """
    Regressão à média bayesiana.
    Com 0 jogos → retorna prior.
    Com 30+ jogos → praticamente retorna observed_mean.
    """
    return (prior_weight * prior_mean + n_games * observed_mean) / (prior_weight + n_games)

def buscar_stats_time(time_id: int, liga_id: int, temporada: int) -> dict:
    chave = f"{time_id}-{liga_id}-{temporada}"
    entrada = _cache_times.get(chave)
    if entrada and time.time() - entrada.get("_ts", 0) < TTL_CACHE_TIME:
        return entrada
    cache_disco = _limpar_expirados(_ler_json(CACHE_TIMES_ARQUIVO), TTL_CACHE_TIME)
    if chave in cache_disco:
        _cache_times[chave] = cache_disco[chave]
        return cache_disco[chave]

    dados = _apif_get("teams/statistics",
                      {"team": time_id, "league": liga_id, "season": temporada})
    if not dados:
        return _stats_time_padrao()

    resp       = dados.get("response", {})
    cards_data = resp.get("cards", {}) or {}
    amarelos   = sum((v.get("total") or 0) for v in cards_data.get("yellow", {}).values())
    vermelhos  = sum((v.get("total") or 0) for v in cards_data.get("red", {}).values())
    jogados    = (resp.get("fixtures", {}).get("played", {}) or {}).get("total", 0) or 0

    # Média raw
    cartoes_raw = round((amarelos + vermelhos) / max(jogados, 1), 2)

    # Liga info para prior
    liga_info = LIGAS_MONITORADAS.get(liga_id, {})
    prior_cart = liga_info.get("prior", PRIOR_CARTOES_JOGO)

    # Bayesian shrinkage
    cartoes_bayes = round(_bayesian_estimate(cartoes_raw, jogados, prior_cart / 2, PRIOR_PESO_JOGOS), 2)
    # Dividimos prior por 2 porque prior é total do jogo, e aqui é por time

    faltas_jg = round(cartoes_bayes * 7.5, 1)
    forma = resp.get("form", "") or ""

    resultado = {
        "cartoes_por_jogo":     cartoes_bayes,
        "cartoes_por_jogo_raw": cartoes_raw,
        "faltas_por_jogo":      faltas_jg,
        "forma":                forma[-JOGOS_FORMA_RECENTE:] if forma else "",
        "amarelos_total":       amarelos,
        "vermelhos_total":      vermelhos,
        "jogos_disputados":     jogados,
        "_ts":                  time.time()
    }
    _cache_times[chave] = resultado
    cache_disco[chave]  = resultado
    _salvar_json(CACHE_TIMES_ARQUIVO, cache_disco)
    return resultado

# ═══════════════════════════════════════════════════════════
#  API-FOOTBALL: ÁRBITRO
# ═══════════════════════════════════════════════════════════
ARBITROS_CONHECIDOS = {
    "anderson daronco":          5.8,  "raphael claus":             5.4,
    "wilton pereira sampaio":    5.2,  "bruno arleu de araújo":     5.1,
    "felipe fernandes de lima":  6.1,  "savio pereira sampaio":     5.5,
    "ramon abatti abel":         5.3,  "braulio da silva machado":  5.0,
    "flávio rodrigues de souza": 5.1,  "luiz flávio de oliveira":   4.9,
    "paulo roberto alves junior":4.8,  "marcelo de lima henrique":  5.2,
    "antonio mateu lahoz":       6.5,  "felix brych":               4.9,
    "björn kuipers":             5.3,  "michael oliver":            4.5,
    "néstor pitana":             5.7,  "darren england":            4.8,
    "facundo tello":             6.2,  "patricio loustau":          5.6,
    "carlos del cerro grande":   5.4,  "jesús gil manzano":        5.3,
    "anthony taylor":            4.7,  "daniele orsato":            5.0,
    "clement turpin":            5.1,  "slavko vinčić":             4.8,
    "fernando rapallini":        5.5,  "andrés rojas":              5.4,
    "wilmar roldán":             5.9,  "roberto tobar":             5.6,
    "esteban ostojich":          5.3,  "dario herrera":             5.7,
    "fernando echenique":        5.4,  "diego abal":                5.3,
    "mauro vigliano":            5.1,  "silvio trucco":             5.5,
}

def buscar_stats_arbitro(nome_arbitro: str) -> dict:
    if not nome_arbitro:
        return {"nome": "Não informado", "cartoes_por_jogo": PRIOR_CARTOES_JOGO,
                "conhecido": False, "_ts": time.time()}
    chave = nome_arbitro.lower().strip()
    # Remove sufixo de nome (API retorna "Raphael Claus, Brazil" às vezes)
    if "," in chave:
        chave = chave.split(",")[0].strip()

    entrada = _cache_arbitros.get(chave)
    if entrada and time.time() - entrada.get("_ts", 0) < TTL_CACHE_ARBITRO:
        return entrada

    cache_disco = _limpar_expirados(_ler_json(CACHE_ARBS_ARQUIVO), TTL_CACHE_ARBITRO)
    if chave in cache_disco:
        _cache_arbitros[chave] = cache_disco[chave]
        return cache_disco[chave]

    # Tenta match parcial
    media = PRIOR_CARTOES_JOGO
    conhecido = False
    for arb_nome, arb_media in ARBITROS_CONHECIDOS.items():
        if arb_nome in chave or chave in arb_nome:
            media = arb_media
            conhecido = True
            break

    resultado = {
        "nome":             nome_arbitro,
        "cartoes_por_jogo": media,
        "conhecido":        conhecido,
        "_ts":              time.time()
    }
    _cache_arbitros[chave] = resultado
    cache_disco[chave]     = resultado
    _salvar_json(CACHE_ARBS_ARQUIVO, cache_disco)
    return resultado

# ═══════════════════════════════════════════════════════════
#  API-FOOTBALL: HEAD-TO-HEAD (NOVO)
# ═══════════════════════════════════════════════════════════
def buscar_h2h(home_id: int, away_id: int) -> dict:
    """Busca confrontos diretos e retorna média de cartões."""
    chave = f"{min(home_id, away_id)}-{max(home_id, away_id)}"
    entrada = _cache_h2h.get(chave)
    if entrada and time.time() - entrada.get("_ts", 0) < TTL_CACHE_H2H:
        return entrada

    cache_disco = _limpar_expirados(_ler_json(CACHE_H2H_ARQUIVO), TTL_CACHE_H2H)
    if chave in cache_disco:
        _cache_h2h[chave] = cache_disco[chave]
        return cache_disco[chave]

    dados = _apif_get("fixtures/headtohead", {"h2h": f"{home_id}-{away_id}", "last": 5})
    if not dados:
        return {"media_cartoes": None, "jogos": 0, "_ts": time.time()}

    jogos_h2h = dados.get("response", [])
    if not jogos_h2h:
        resultado = {"media_cartoes": None, "jogos": 0, "_ts": time.time()}
    else:
        total_cartoes = 0
        jogos_com_dados = 0
        for jogo in jogos_h2h:
            eventos = jogo.get("events", []) or []
            cartoes = sum(1 for e in eventos if e.get("type") == "Card")
            if cartoes > 0 or eventos:  # tem dados de eventos
                total_cartoes += cartoes
                jogos_com_dados += 1

        if jogos_com_dados > 0:
            media = round(total_cartoes / jogos_com_dados, 2)
        else:
            media = None

        resultado = {
            "media_cartoes": media,
            "jogos": jogos_com_dados,
            "_ts": time.time()
        }

    _cache_h2h[chave] = resultado
    cache_disco[chave] = resultado
    _salvar_json(CACHE_H2H_ARQUIVO, cache_disco)
    return resultado

# ═══════════════════════════════════════════════════════════
#  API-FOOTBALL: STANDINGS (CONTEXTO TABELA) (NOVO)
# ═══════════════════════════════════════════════════════════
def buscar_standings(liga_id: int, temporada: int) -> list:
    chave = f"{liga_id}-{temporada}"
    entrada = _cache_standings.get(chave)
    if entrada:
        data, ts = entrada
        if time.time() - ts < 3600:  # 1h
            return data

    dados = _apif_get("standings", {"league": liga_id, "season": temporada})
    if not dados:
        return []
    resp = dados.get("response", [])
    if not resp:
        return []
    standings_raw = resp[0].get("league", {}).get("standings", [])
    if standings_raw and isinstance(standings_raw[0], list):
        standings = standings_raw[0]
    else:
        standings = standings_raw

    _cache_standings[chave] = (standings, time.time())
    return standings

def contexto_tabela(time_id: int, liga_id: int, temporada: int,
                     total_times: int = 20) -> dict:
    """
    Retorna contexto posicional: zona de rebaixamento, G4, título, meio.
    """
    standings = buscar_standings(liga_id, temporada)
    if not standings:
        return {"posicao": None, "zona": "desconhecida", "mult": 1.0, "descricao": "N/D"}

    posicao = None
    for item in standings:
        if item.get("team", {}).get("id") == time_id:
            posicao = item.get("rank", item.get("position"))
            break

    if posicao is None:
        return {"posicao": None, "zona": "desconhecida", "mult": 1.0, "descricao": "N/D"}

    total = len(standings) if standings else total_times
    zona_rebaixamento = total - 3

    if posicao <= 2:
        return {"posicao": posicao, "zona": "titulo", "mult": CONTEXTO_TITULO_MULT,
                "descricao": f"{posicao}º — briga pelo título"}
    elif posicao <= 4:
        return {"posicao": posicao, "zona": "g4", "mult": CONTEXTO_G4_MULT,
                "descricao": f"{posicao}º — zona de classificação"}
    elif posicao >= zona_rebaixamento:
        return {"posicao": posicao, "zona": "rebaixamento", "mult": CONTEXTO_REBAIXAMENTO_MULT,
                "descricao": f"{posicao}º — zona de rebaixamento"}
    else:
        return {"posicao": posicao, "zona": "meio", "mult": CONTEXTO_MEIO_TABELA_MULT,
                "descricao": f"{posicao}º — meio da tabela"}

# ═══════════════════════════════════════════════════════════
#  API-FOOTBALL: ODDS REAIS + HISTÓRICO DE MOVIMENTAÇÃO
# ═══════════════════════════════════════════════════════════
ARQUIVO_ODDS_HIST = "odds_historico.json"

def buscar_odds_cartoes(fixture_id: int) -> dict:
    """Tenta buscar odds reais de cartões para o jogo."""
    entrada = _cache_odds.get(fixture_id)
    if entrada:
        odds, ts = entrada
        if time.time() - ts < 1800:  # 30min
            return odds

    dados = _apif_get("odds", {"fixture": fixture_id, "bookmaker": 8})  # Bet365
    resultado = {"acima_3_5": None, "acima_4_5": None, "acima_5_5": None}

    if dados:
        try:
            for bookmaker in dados.get("response", [{}])[0].get("bookmakers", []):
                for bet in bookmaker.get("bets", []):
                    bet_name = bet.get("name", "").lower()
                    if "card" in bet_name and "over" in bet_name:
                        for val in bet.get("values", []):
                            v = val.get("value", "")
                            o = val.get("odd")
                            if o:
                                o = float(o)
                                if "3.5" in str(v):
                                    resultado["acima_3_5"] = o
                                elif "4.5" in str(v):
                                    resultado["acima_4_5"] = o
                                elif "5.5" in str(v):
                                    resultado["acima_5_5"] = o
        except Exception:
            pass

    _cache_odds[fixture_id] = (resultado, time.time())

    # Registra histórico de odds para análise de movimentação
    if any(resultado.values()):
        _registrar_historico_odds(fixture_id, resultado)

    return resultado

def _registrar_historico_odds(fixture_id: int, odds: dict):
    """Salva histórico de odds para detectar movimentação (sharp money, steam moves)."""
    try:
        hist = _ler_json(ARQUIVO_ODDS_HIST)
        chave = str(fixture_id)
        if chave not in hist:
            hist[chave] = []

        # Só registra se odds mudaram ou primeira entrada
        snapshot = {
            "ts": time.time(),
            "odds": {k: v for k, v in odds.items() if v is not None}
        }

        # Evita duplicatas: só grava se odds diferentes do último registro
        if not hist[chave] or hist[chave][-1].get("odds") != snapshot["odds"]:
            hist[chave].append(snapshot)
            # Limita a 20 snapshots por jogo (evita bloat)
            hist[chave] = hist[chave][-20:]

        # Limpeza: remove jogos antigos (>3 dias)
        limite = time.time() - (3 * 86400)
        hist = {
            k: v for k, v in hist.items()
            if v and v[-1].get("ts", 0) > limite
        }
        _salvar_json(ARQUIVO_ODDS_HIST, hist)
    except Exception as e:
        log.debug(f"Erro ao registrar histórico odds: {e}")

def analisar_movimento_odds(fixture_id: int, mercado_linha: float) -> dict:
    """
    Analisa movimentação das odds para detectar sharp money.
    Retorna dict com direção, magnitude e sinal.
    """
    try:
        hist = _ler_json(ARQUIVO_ODDS_HIST)
        snapshots = hist.get(str(fixture_id), [])
        if len(snapshots) < 2:
            return {"movimento": "insuficiente", "magnitude": 0, "sinal": "neutro"}

        chave_odd = f"acima_{str(mercado_linha).replace('.', '_')}"
        odds_serie = [s["odds"].get(chave_odd) for s in snapshots if chave_odd in s.get("odds", {})]
        odds_serie = [o for o in odds_serie if o is not None]

        if len(odds_serie) < 2:
            return {"movimento": "insuficiente", "magnitude": 0, "sinal": "neutro"}

        odd_inicial = odds_serie[0]
        odd_atual = odds_serie[-1]
        variacao_pct = ((odd_atual - odd_inicial) / odd_inicial) * 100

        # Interpretação:
        # Odd CAIU (variação negativa) = mercado está apostando MAIS no "acima"
        #   → sinal de CONFIRMAÇÃO (sharp money concorda com modelo)
        # Odd SUBIU = mercado está apostando MENOS no "acima"
        #   → sinal de DIVERGÊNCIA (cautela — mercado sabe algo?)
        if variacao_pct <= -5:
            return {
                "movimento": "caindo_forte",
                "magnitude": abs(variacao_pct),
                "sinal": "confirmacao_forte",
                "descricao": f"Odd caiu {abs(variacao_pct):.1f}% — mercado confirma"
            }
        elif variacao_pct <= -2:
            return {
                "movimento": "caindo",
                "magnitude": abs(variacao_pct),
                "sinal": "confirmacao",
                "descricao": f"Odd caiu {abs(variacao_pct):.1f}% — leve confirmação"
            }
        elif variacao_pct >= 5:
            return {
                "movimento": "subindo_forte",
                "magnitude": variacao_pct,
                "sinal": "divergencia_forte",
                "descricao": f"Odd subiu {variacao_pct:.1f}% — cautela, mercado discorda"
            }
        elif variacao_pct >= 2:
            return {
                "movimento": "subindo",
                "magnitude": variacao_pct,
                "sinal": "divergencia",
                "descricao": f"Odd subiu {variacao_pct:.1f}% — leve divergência"
            }
        else:
            return {
                "movimento": "estavel",
                "magnitude": abs(variacao_pct),
                "sinal": "neutro",
                "descricao": f"Odd estável ({variacao_pct:+.1f}%)"
            }
    except Exception as e:
        log.debug(f"Erro ao analisar movimento odds: {e}")
        return {"movimento": "erro", "magnitude": 0, "sinal": "neutro"}

# ═══════════════════════════════════════════════════════════
#  PREFETCH PARALELO
# ═══════════════════════════════════════════════════════════
def _fetch_stats_time(time_id, liga_id, temporada):
    return f"T-{time_id}", buscar_stats_time(time_id, liga_id, temporada)

def _fetch_stats_arbitro(nome):
    return f"A-{nome}", buscar_stats_arbitro(nome)

def _fetch_h2h(home_id, away_id):
    return f"H-{home_id}-{away_id}", buscar_h2h(home_id, away_id)

def prefetch_todos_stats(fixtures_pre: list):
    temporada    = datetime.now().year
    times_needed = set()
    arbs_needed  = set()
    h2h_needed   = set()
    standings_needed = set()

    for fx in fixtures_pre:
        lid = fx.get("league", {}).get("id")
        if lid not in LIGAS_MONITORADAS:
            continue
        home_id = fx.get("teams", {}).get("home", {}).get("id")
        away_id = fx.get("teams", {}).get("away", {}).get("id")
        arb     = fx.get("fixture", {}).get("referee", "") or ""

        if home_id and f"{home_id}-{lid}-{temporada}" not in _cache_times:
            times_needed.add((home_id, lid, temporada))
        if away_id and f"{away_id}-{lid}-{temporada}" not in _cache_times:
            times_needed.add((away_id, lid, temporada))
        if arb:
            arb_chave = arb.lower().strip()
            if "," in arb_chave:
                arb_chave = arb_chave.split(",")[0].strip()
            if arb_chave not in _cache_arbitros:
                arbs_needed.add(arb)
        if home_id and away_id:
            h2h_key = f"{min(home_id, away_id)}-{max(home_id, away_id)}"
            if h2h_key not in _cache_h2h:
                h2h_needed.add((home_id, away_id))

        # Standings por liga (uma vez só)
        standings_key = f"{lid}-{temporada}"
        if standings_key not in _cache_standings:
            standings_needed.add((lid, temporada))

    total = len(times_needed) + len(arbs_needed) + len(h2h_needed) + len(standings_needed)
    if total == 0:
        log.info("✅ Tudo em cache — nenhuma chamada API necessária")
        return

    log.info(f"⚡ Pré-carregando: {len(times_needed)} times + "
             f"{len(arbs_needed)} árbitros + {len(h2h_needed)} H2H + "
             f"{len(standings_needed)} tabelas ({total} requisições)")
    t0 = time.time()

    def _fetch_standings(lid, temp):
        return f"S-{lid}", buscar_standings(lid, temp)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = (
            [executor.submit(_fetch_stats_time, tid, lid, s) for tid, lid, s in times_needed] +
            [executor.submit(_fetch_stats_arbitro, arb) for arb in arbs_needed] +
            [executor.submit(_fetch_h2h, hid, aid) for hid, aid in h2h_needed] +
            [executor.submit(_fetch_standings, lid, s) for lid, s in standings_needed]
        )
        for i, fut in enumerate(as_completed(futures), 1):
            try:
                fut.result()
            except Exception as e:
                log.debug(f"Prefetch erro: {e}")
            if i % 15 == 0:
                log.info(f"  Progresso: {i}/{total}")

    log.info(f"✅ Pré-carregamento concluído em {time.time() - t0:.1f}s")

# ═══════════════════════════════════════════════════════════
#  DETECÇÕES
# ═══════════════════════════════════════════════════════════
def e_classico(casa: str, fora: str) -> bool:
    h, a = casa.lower(), fora.lower()
    return any(
        (t1 in h and t2 in a) or (t1 in a and t2 in h)
        for t1, t2 in CLÁSSICOS
    )

def e_decisivo(nome_rodada: str) -> bool:
    if not nome_rodada:
        return False
    r = nome_rodada.lower()
    return any(k in r for k in
               ["final", "semi", "quart", "oitav", "playoff", "decis", "knockout",
                "round of 16", "round of 32", "8th", "quarter", "repechage"])

# ═══════════════════════════════════════════════════════════
#  FORM DECAY (NOVO)
# ═══════════════════════════════════════════════════════════
def calcular_ajuste_forma(forma: str) -> float:
    """
    Calcula ajuste de cartões baseado na forma recente com decaimento temporal.
    Retorna valor entre -0.3 e +0.4 que será somado ao λ.
    """
    if not forma:
        return 0.0
    ajuste_total = 0.0
    peso_total   = 0.0
    for i, resultado in enumerate(reversed(forma[-5:])):
        peso = FORM_DECAY_WEIGHTS[i] if i < len(FORM_DECAY_WEIGHTS) else 0.4
        if resultado == "W":
            ajuste_total += FORM_VITORIA_AJUSTE * peso
        elif resultado == "L":
            ajuste_total += FORM_DERROTA_AJUSTE * peso
        elif resultado == "D":
            ajuste_total += FORM_EMPATE_AJUSTE * peso
        peso_total += peso

    return round(ajuste_total / max(peso_total, 1) * len(forma[-5:]), 3)

# ═══════════════════════════════════════════════════════════
#  MODELO POISSON DUPLO (NOVO — substitui o modelo simples)
# ═══════════════════════════════════════════════════════════
def calcular_lambda_duplo(cartoes_casa: float, cartoes_fora: float,
                           cartoes_arb: float, classico: bool, decisivo: bool,
                           peso_liga: float, h2h_media: float | None,
                           h2h_jogos: int, ajuste_forma_casa: float,
                           ajuste_forma_fora: float,
                           contexto_casa: dict, contexto_fora: dict) -> dict:
    """
    Modelo Poisson Duplo: calcula λ_casa e λ_fora separados.
    Retorna dict com lambda_casa, lambda_fora, lambda_total.
    """
    # Base por time (já com bayesian shrinkage)
    lam_casa = max(cartoes_casa, 0.5)
    lam_fora = max(cartoes_fora, 0.5)

    # Fator árbitro (normalizado contra média global)
    fator_arb = cartoes_arb / PRIOR_CARTOES_JOGO
    lam_casa *= fator_arb
    lam_fora *= fator_arb

    # Ajuste de forma
    lam_casa += ajuste_forma_casa
    lam_fora += ajuste_forma_fora

    # Contexto de tabela
    lam_casa *= contexto_casa.get("mult", 1.0)
    lam_fora *= contexto_fora.get("mult", 1.0)

    # Clássico / Eliminatória
    if classico:
        lam_casa *= 1.12
        lam_fora *= 1.12
    if decisivo:
        lam_casa *= 1.08
        lam_fora *= 1.08

    # Peso da liga (suavizado)
    fator_liga = peso_liga ** 0.4  # ↓ era 0.5 — menos distorção
    lam_casa *= fator_liga
    lam_fora *= fator_liga

    # H2H: se tiver dados, faz blend com peso proporcional
    lam_total = lam_casa + lam_fora
    if h2h_media is not None and h2h_jogos >= 2:
        h2h_peso = min(h2h_jogos / 10.0, 0.30)  # max 30% de influência
        lam_total = lam_total * (1 - h2h_peso) + h2h_media * h2h_peso

    # Clamp
    lam_casa = max(round(lam_casa, 2), 0.5)
    lam_fora = max(round(lam_fora, 2), 0.5)
    lam_total = max(round(lam_total, 2), 1.5)

    return {
        "lambda_casa":  lam_casa,
        "lambda_fora":  lam_fora,
        "lambda_total": lam_total,
    }

def prob_acima(lam: float, linha: float) -> float:
    """P(X > linha) usando Poisson CDF."""
    return float(1 - poisson.cdf(int(linha), lam))

def calcular_probabilidades(lam: float) -> dict:
    return {
        "acima_2_5": prob_acima(lam, 2),
        "acima_3_5": prob_acima(lam, 3),
        "acima_4_5": prob_acima(lam, 4),
        "acima_5_5": prob_acima(lam, 5),
        "acima_6_5": prob_acima(lam, 6),
    }

def kelly_stake(prob: float, odd: float, banca: float, fracao: float) -> float:
    if odd <= 1:
        return 0
    b    = odd - 1
    edge = (b * prob - (1 - prob)) / b
    if edge <= 0.02:    # ↑ era 0 — exige pelo menos 2% de edge
        return 0
    return round(banca * edge * fracao, 2)

# ═══════════════════════════════════════════════════════════
#  SCORE (RECONSTRUÍDO — mais granular)
# ═══════════════════════════════════════════════════════════
def calcular_score(cartoes_casa: float, cartoes_fora: float, cartoes_arb: float,
                   faltas_casa: float, faltas_fora: float, classico: bool,
                   decisivo: bool, h2h_media, h2h_jogos: int,
                   contexto_casa: dict, contexto_fora: dict,
                   forma_casa: str, forma_fora: str,
                   arb_conhecido: bool) -> tuple:
    score    = 0.0
    detalhes = []

    # ── Árbitro (0 a 3.0 pts) ──
    if cartoes_arb >= 6.0:
        score += 3.0; detalhes.append(f"Árbitro muito rigoroso ({cartoes_arb:.1f}/jogo +3.0)")
    elif cartoes_arb >= 5.5:
        score += 2.5; detalhes.append(f"Árbitro rigoroso ({cartoes_arb:.1f}/jogo +2.5)")
    elif cartoes_arb >= 5.0:
        score += 2.0; detalhes.append(f"Árbitro exigente ({cartoes_arb:.1f}/jogo +2.0)")
    elif cartoes_arb >= 4.5:
        score += 1.5; detalhes.append(f"Árbitro firme ({cartoes_arb:.1f}/jogo +1.5)")
    elif cartoes_arb >= 4.0:
        score += 1.0; detalhes.append(f"Árbitro moderado ({cartoes_arb:.1f}/jogo +1.0)")

    if not arb_conhecido:
        score -= 0.5
        detalhes.append("Árbitro desconhecido (dados estimados -0.5)")

    # ── Mandante (0 a 2.0 pts) ──
    if cartoes_casa >= 3.0:
        score += 2.0; detalhes.append(f"Mandante muito indisciplinado ({cartoes_casa:.2f}/jogo +2.0)")
    elif cartoes_casa >= 2.5:
        score += 1.5; detalhes.append(f"Mandante indisciplinado ({cartoes_casa:.2f}/jogo +1.5)")
    elif cartoes_casa >= 2.0:
        score += 1.0; detalhes.append(f"Mandante amarelão ({cartoes_casa:.2f}/jogo +1.0)")
    elif cartoes_casa >= 1.5:
        score += 0.5; detalhes.append(f"Mandante moderado ({cartoes_casa:.2f}/jogo +0.5)")

    # ── Visitante (0 a 2.0 pts) ──
    if cartoes_fora >= 3.0:
        score += 2.0; detalhes.append(f"Visitante muito indisciplinado ({cartoes_fora:.2f}/jogo +2.0)")
    elif cartoes_fora >= 2.5:
        score += 1.5; detalhes.append(f"Visitante indisciplinado ({cartoes_fora:.2f}/jogo +1.5)")
    elif cartoes_fora >= 2.0:
        score += 1.0; detalhes.append(f"Visitante amarelão ({cartoes_fora:.2f}/jogo +1.0)")
    elif cartoes_fora >= 1.5:
        score += 0.5; detalhes.append(f"Visitante moderado ({cartoes_fora:.2f}/jogo +0.5)")

    # ── Faltas combinadas (0 a 1.5 pts) ──
    total_faltas = faltas_casa + faltas_fora
    if total_faltas >= 32:
        score += 1.5; detalhes.append(f"Faltas altíssimas ({total_faltas:.0f}/jogo +1.5)")
    elif total_faltas >= 28:
        score += 1.0; detalhes.append(f"Faltas elevadas ({total_faltas:.0f}/jogo +1.0)")
    elif total_faltas >= 24:
        score += 0.5; detalhes.append(f"Faltas moderadas ({total_faltas:.0f}/jogo +0.5)")

    # ── Clássico / Eliminatória (0 a 2.0 pts) ──
    if classico:
        score += 1.5; detalhes.append("Clássico/Derby (+1.5)")
    if decisivo:
        score += 1.0; detalhes.append("Fase eliminatória (+1.0)")

    # ── H2H (0 a 1.5 pts) ──
    if h2h_media is not None and h2h_jogos >= 2:
        if h2h_media >= 6.0:
            score += 1.5; detalhes.append(f"H2H explosivo ({h2h_media:.1f} cart em {h2h_jogos} jogos +1.5)")
        elif h2h_media >= 5.0:
            score += 1.0; detalhes.append(f"H2H quente ({h2h_media:.1f} cart em {h2h_jogos} jogos +1.0)")
        elif h2h_media >= 4.0:
            score += 0.5; detalhes.append(f"H2H moderado ({h2h_media:.1f} cart em {h2h_jogos} jogos +0.5)")

    # ── Contexto tabela (0 a 1.0 pts) ──
    for ctx, rotulo in [(contexto_casa, "Mandante"), (contexto_fora, "Visitante")]:
        zona = ctx.get("zona", "")
        if zona == "rebaixamento":
            score += 0.5; detalhes.append(f"{rotulo} na zona de rebaixamento (+0.5)")
        elif zona == "titulo":
            score += 0.3; detalhes.append(f"{rotulo} brigando pelo título (+0.3)")

    # ── Forma recente (ajuste fino: -0.5 a +0.5) ──
    perdas_casa = forma_casa.count("L") if forma_casa else 0
    perdas_fora = forma_fora.count("L") if forma_fora else 0
    if perdas_casa >= 3 or perdas_fora >= 3:
        score += 0.5; detalhes.append(f"Time em crise de resultados (+0.5)")
    elif perdas_casa + perdas_fora >= 4:
        score += 0.3; detalhes.append(f"Ambos com resultados ruins (+0.3)")

    return round(score, 1), detalhes

def aplicar_peso_liga(score: float, liga_id: int) -> tuple:
    info = LIGAS_MONITORADAS.get(liga_id, {})
    nome = info.get("nome", "Liga não mapeada")
    peso = info.get("peso", 1.0)
    return round(score * (peso ** 0.5), 1), nome, peso  # suavizado

def get_tier(score: float) -> tuple:
    if score >= TIER_QUENTE_MIN:   return ("QUENTE",   "🔥", 5.0)
    if score >= TIER_MORNO_MIN:    return ("MORNO",    "⚡", 3.0)
    if score >= TIER_OBSERVAR_MIN: return ("OBSERVAR", "✅", 1.5)
    return ("FRIO", "⚪", 0)

def escolher_melhor_mercado(probs: dict, odds_reais: dict) -> dict:
    candidatos = [
        ("Acima 3.5", probs["acima_3_5"], odds_reais.get("acima_3_5") or ODD_PADRAO_35, 3.5),
        ("Acima 4.5", probs["acima_4_5"], odds_reais.get("acima_4_5") or ODD_PADRAO_45, 4.5),
        ("Acima 5.5", probs["acima_5_5"], odds_reais.get("acima_5_5") or ODD_PADRAO_55, 5.5),
    ]
    melhor = None
    for nome, prob, odd, linha in candidatos:
        ev = prob * (odd - 1) - (1 - prob)
        edge = prob - (1 / odd) if odd > 0 else 0
        if not melhor or ev > melhor["ev"]:
            melhor = {
                "nome":      nome,
                "prob":      prob,
                "odd":       odd,
                "odd_real":  odd != ODD_PADRAO_35 and odd != ODD_PADRAO_45 and odd != ODD_PADRAO_55,
                "linha":     linha,
                "edge":      edge,
                "ev":        ev,
            }
    return melhor

# ═══════════════════════════════════════════════════════════
#  HELPERS VISUAIS
# ═══════════════════════════════════════════════════════════
def barra_progresso(valor: float, maximo: float = 10, largura: int = 10) -> str:
    preenchido = max(0, min(largura, int((valor / maximo) * largura)))
    return "▓" * preenchido + "░" * (largura - preenchido)

def badge_confianca(score: float) -> str:
    if score >= 8.0: return "▰▰▰▰▰ MÁXIMA"
    if score >= 7.0: return "▰▰▰▰▱ ALTA"
    if score >= 5.5: return "▰▰▰▱▱ BOA"
    if score >= 4.5: return "▰▰▱▱▱ MÉDIA"
    return              "▰▱▱▱▱ BAIXA"

def emoji_forma(forma: str) -> str:
    if not forma:
        return "—"
    mapa = {"W": "✅", "D": "🟡", "L": "❌"}
    return " ".join(mapa.get(c, "❓") for c in forma)

# ═══════════════════════════════════════════════════════════
#  ANÁLISE DE FIXTURE (RECONSTRUÍDA)
# ═══════════════════════════════════════════════════════════
def analisar_fixture(fixture: dict) -> dict | None:
    try:
        fix   = fixture.get("fixture", {})
        times = fixture.get("teams", {})
        liga  = fixture.get("league", {})

        fid = fix.get("id")
        if not fid:
            return None

        liga_id = liga.get("id")
        if liga_id not in LIGAS_MONITORADAS:
            return None

        liga_info  = LIGAS_MONITORADAS[liga_id]
        temporada  = liga.get("season", datetime.now().year)
        rodada     = liga.get("round", "")
        home_id    = times.get("home", {}).get("id")
        away_id    = times.get("away", {}).get("id")
        home_nome  = times.get("home", {}).get("name", "?")
        away_nome  = times.get("away", {}).get("name", "?")

        # Stats base (com Bayesian shrinkage)
        stats_arb  = buscar_stats_arbitro(fix.get("referee", ""))
        stats_casa = buscar_stats_time(home_id, liga_id, temporada) if home_id else _stats_time_padrao()
        stats_fora = buscar_stats_time(away_id, liga_id, temporada) if away_id else _stats_time_padrao()

        # H2H
        h2h_data = buscar_h2h(home_id, away_id) if home_id and away_id else {"media_cartoes": None, "jogos": 0}

        # Contexto tabela
        ctx_casa = contexto_tabela(home_id, liga_id, temporada) if home_id else {}
        ctx_fora = contexto_tabela(away_id, liga_id, temporada) if away_id else {}

        # Detecções
        classico = e_classico(home_nome, away_nome)
        decisivo = e_decisivo(rodada)

        # Form decay
        ajuste_forma_casa = calcular_ajuste_forma(stats_casa["forma"])
        ajuste_forma_fora = calcular_ajuste_forma(stats_fora["forma"])

        # Score composto
        score_base, detalhes = calcular_score(
            stats_casa["cartoes_por_jogo"], stats_fora["cartoes_por_jogo"],
            stats_arb["cartoes_por_jogo"],
            stats_casa["faltas_por_jogo"], stats_fora["faltas_por_jogo"],
            classico, decisivo,
            h2h_data.get("media_cartoes"), h2h_data.get("jogos", 0),
            ctx_casa, ctx_fora,
            stats_casa["forma"], stats_fora["forma"],
            stats_arb.get("conhecido", False)
        )
        score_pesado, liga_nome, peso = aplicar_peso_liga(score_base, liga_id)

        if score_pesado < MIN_SCORE_ALERTA:
            return None

        # Lambda duplo
        lambdas = calcular_lambda_duplo(
            stats_casa["cartoes_por_jogo"], stats_fora["cartoes_por_jogo"],
            stats_arb["cartoes_por_jogo"], classico, decisivo, peso,
            h2h_data.get("media_cartoes"), h2h_data.get("jogos", 0),
            ajuste_forma_casa, ajuste_forma_fora,
            ctx_casa, ctx_fora
        )

        lam_total = lambdas["lambda_total"]
        probs     = calcular_probabilidades(lam_total)

        # Odds reais: NÃO busca aqui (otimização de performance).
        # Será buscado apenas para os jogos aprovados pelo filtro de risco.
        odds_reais = {"acima_3_5": None, "acima_4_5": None, "acima_5_5": None}

        # Mercado recomendado (com odds de referência por enquanto)
        mercado = escolher_melhor_mercado(probs, odds_reais)
        stake   = kelly_stake(mercado["prob"], mercado["odd"], BANCA, KELLY_FRACAO)

        tier_nome, tier_emoji, alocacao = get_tier(score_pesado)

        # Datetime local
        dt_obj = parse_iso_para_local(fix.get("date", ""))

        # Monta resultado
        resultado = {
            "fid":            fid,
            "datetime":       dt_obj,
            "datetime_iso":   fix.get("date", ""),
            "pais":           liga.get("country", ""),
            "liga":           liga_nome,
            "liga_id":        liga_id,
            "peso_liga":      peso,
            "rodada":         rodada,
            "casa":           home_nome,
            "fora":           away_nome,
            "home_id":        home_id,
            "away_id":        away_id,
            "cartoes_casa":   stats_casa["cartoes_por_jogo"],
            "cartoes_fora":   stats_fora["cartoes_por_jogo"],
            "cartoes_casa_raw": stats_casa.get("cartoes_por_jogo_raw", 0),
            "cartoes_fora_raw": stats_fora.get("cartoes_por_jogo_raw", 0),
            "forma_casa":     stats_casa["forma"],
            "forma_fora":     stats_fora["forma"],
            "jogos_casa":     stats_casa["jogos_disputados"],
            "jogos_fora":     stats_fora["jogos_disputados"],
            "arb_nome":       stats_arb["nome"],
            "arb_cartoes":    stats_arb["cartoes_por_jogo"],
            "arb_conhecido":  stats_arb.get("conhecido", False),
            "classico":       classico,
            "decisivo":       decisivo,
            "h2h_media_cartoes": h2h_data.get("media_cartoes"),
            "h2h_jogos":      h2h_data.get("jogos", 0),
            "contexto_casa":  ctx_casa,
            "contexto_fora":  ctx_fora,
            "contexto_tabela": f"{ctx_casa.get('descricao','?')} vs {ctx_fora.get('descricao','?')}",
            "score_base":     score_base,
            "score":          score_pesado,
            "lambdas":        lambdas,
            "lambda":         lam_total,
            "probs":          probs,
            "odds_reais":     odds_reais,
            "mercado":        mercado,
            "stake":          stake,
            "tier":           tier_nome,
            "tier_emoji":     tier_emoji,
            "alocacao_pct":   alocacao,
            "detalhes":       detalhes,
            "gemini":         {},
        }

        # Gemini será chamado DEPOIS do filtro de risco, apenas nos jogos aprovados
        # Isso evita gastar cota com jogos que serão descartados

        return resultado

    except Exception as e:
        log.debug(f"Erro ao analisar fixture: {e}")
        return None

# ═══════════════════════════════════════════════════════════
#  GESTÃO DE RISCO
# ═══════════════════════════════════════════════════════════
def aplicar_filtros_risco(jogos: list) -> tuple:
    ordenados = sorted(jogos,
                       key=lambda j: j["mercado"]["ev"] if j.get("mercado") else 0,
                       reverse=True)
    selecionados    = []
    exposicao_total = 0.0
    max_exposicao   = BANCA * (MAX_EXPOSICAO_PCT / 100)
    desc = {"LIMITE_DE_OPERAÇÕES": 0, "LIMITE_DE_EXPOSIÇÃO": 0, "SEM_EDGE": 0}

    for j in ordenados:
        # Filtro de edge mínimo
        if j.get("mercado", {}).get("edge", 0) <= 0.02:
            j["risco_status"] = "SEM_EDGE"
            desc["SEM_EDGE"] += 1
            continue
        if j.get("stake", 0) <= 0:
            j["risco_status"] = "SEM_EDGE"
            desc["SEM_EDGE"] += 1
            continue
        if len(selecionados) >= MAX_OPS_DIA:
            j["risco_status"] = "LIMITE_DE_OPERAÇÕES"
            desc["LIMITE_DE_OPERAÇÕES"] += 1
            continue
        stake = j.get("stake", 0)
        if exposicao_total + stake > max_exposicao:
            disponivel = max(0.0, max_exposicao - exposicao_total)
            if disponivel < BANCA * 0.005:
                j["risco_status"] = "LIMITE_DE_EXPOSIÇÃO"
                desc["LIMITE_DE_EXPOSIÇÃO"] += 1
                continue
            j["stake"] = round(disponivel, 2)
            stake      = disponivel
        exposicao_total += stake
        j["risco_status"] = "OK"
        selecionados.append(j)

    for motivo, qtd in desc.items():
        if qtd > 0:
            log.info(f"   ⚠️ {qtd} jogos descartados por {motivo}")

    return ordenados, exposicao_total

# ═══════════════════════════════════════════════════════════
#  RENDERIZADORES TELEGRAM
# ═══════════════════════════════════════════════════════════
def _sep(char="━", n=30) -> str:
    return char * n

def _cabecalho_jogo(j: dict) -> str:
    dt = j["datetime"]
    if dt:
        data_str = f"📅 *{formatar_data_ptbr(dt)}*\n🕐 *Horário:* `{formatar_hora(dt)}`"
    else:
        data_str = "📅 Data não disponível"
    tags = []
    if j["classico"]:  tags.append("🔥 CLÁSSICO")
    if j["decisivo"]:  tags.append("🏆 ELIMINATÓRIA")
    tag_l = "  •  ".join(tags) + "\n" if tags else ""
    return (
        f"{_sep()}\n"
        f"🌎 {j['pais']}  •  {j['liga']}\n"
        f"⚽ *{j['casa']}* vs *{j['fora']}*\n"
        f"{tag_l}{data_str}\n{_sep()}"
    )

def render_watchlist(jogos: list, exposicao_total: float):
    if not jogos:
        return None
    jogos_ok = [j for j in jogos if j.get("risco_status") == "OK"][:MAX_JOGOS_WATCHLIST]
    if not jogos_ok:
        return None

    quentes  = [j for j in jogos_ok if j["tier"] == "QUENTE"]
    mornos   = [j for j in jogos_ok if j["tier"] == "MORNO"]
    observar = [j for j in jogos_ok if j["tier"] == "OBSERVAR"]
    filtrados = sum(1 for j in jogos if j.get("risco_status") != "OK")

    agora = datetime.now()
    linhas = [
        f"📋 *WATCHLIST DIÁRIA — ANÁLISE DE CARTÕES v6*", "",
        f"📆 {formatar_data_ptbr(agora)}",
        f"🕐 Gerada às `{formatar_hora(agora)}`", "",
        f"{_sep()}", f"📊 *Resumo do dia*",
        f"  • Jogos selecionados: *{len(jogos_ok)}*",
        f"  • Jogos filtrados (risco): {filtrados}",
        f"  • 🔥 Quentes: {len(quentes)}  ⚡ Mornos: {len(mornos)}  ✅ Observar: {len(observar)}",
        f"  • 💰 Exposição: R$ {exposicao_total:.2f} ({exposicao_total / BANCA * 100:.1f}%)",
        f"{_sep()}",
    ]

    def bloco_tier(titulo, lista, emoji):
        if not lista:
            return []
        b = ["", f"{emoji} *{titulo}*", ""]
        for j in lista:
            dt = j["datetime"]
            mkt = j["mercado"]
            hora = formatar_hora(dt) if dt else "??:??"
            data = formatar_datetime_curto(dt) if dt else "?"
            barra = barra_progresso(j["score"], 12, 8)
            ctx_txt = j.get("contexto_tabela", "N/D")

            b += [
                f"┌─ {j['tier_emoji']} *{j['casa']}* vs *{j['fora']}*",
                f"│  🌎 {j['liga']}",
                f"│  📅 {data}  •  🕐 {hora}",
                f"│  Score: `{j['score']:.1f}` {barra}",
                f"│  λ = `{j['lambda']:.2f}` (casa:{j['lambdas']['lambda_casa']:.1f} fora:{j['lambdas']['lambda_fora']:.1f})",
                f"│  Mercado: *{mkt['nome']}*  Prob: `{mkt['prob'] * 100:.0f}%`  EV: `{mkt['ev']:+.3f}`",
                f"│  Odd: `{mkt['odd']:.2f}`{'✅real' if mkt.get('odd_real') else '📐ref'}  Stake: `R$ {j['stake']:.0f}`",
                f"│  🧑‍⚖️ {j['arb_nome']}: `{j['arb_cartoes']:.1f}`/jogo {'✅' if j['arb_conhecido'] else '⚠️est'}",
                f"│  📊 Tabela: {ctx_txt}",
            ]
            if j.get("h2h_media_cartoes"):
                b.append(f"│  🔄 H2H: `{j['h2h_media_cartoes']:.1f}` cart ({j['h2h_jogos']} jogos)")
            gemini = j.get("gemini", {})
            if gemini.get("analise_texto"):
                b.append(f"│  🤖 Gemini: {gemini.get('confianca','?')} (score IA: {gemini.get('score_gemini',0):.1f})")
            tags = []
            if j["classico"]:  tags.append("CLÁSSICO")
            if j["decisivo"]:  tags.append("ELIMINATÓRIA")
            if tags:
                b.append(f"│  ⚠️ {' · '.join(tags)}")
            b += [f"└{'─' * 28}", ""]
        return b

    linhas += bloco_tier("TIER QUENTE — Entrar com convicção", quentes, "🔥")
    linhas += bloco_tier("TIER MORNO — Alta probabilidade", mornos, "⚡")
    linhas += bloco_tier("TIER OBSERVAR — Monitorar de perto", observar, "✅")

    linhas += [
        _sep(), f"💡 *Legenda*",
        f"  • λ = cartões esperados (modelo Poisson duplo)",
        f"  • Prob = probabilidade do modelo",
        f"  • EV = valor esperado (edge)",
        f"  • ✅real = odd do mercado / 📐ref = odd de referência",
        f"  • Stake = Kelly fracionado {int(KELLY_FRACAO * 100)}%", "",
        f"_Confirme as odds reais antes de operar._",
    ]
    return "\n".join(l for l in linhas if l is not None)

def render_alerta_pre_jogo(j: dict, marco: int = 60) -> str:
    dt = j["datetime"]
    mkt = j["mercado"]
    probs = j["probs"]
    barra = barra_progresso(j["score"], 12, 10)
    conf = badge_confianca(j["score"])
    lams = j["lambdas"]

    # Título e ícone variam conforme o marco
    if marco >= 900:
        titulo_alerta = "🌅 ALERTA ANTECIPADO — 15h antes"
        subtitulo = "Planejamento de entrada com odds iniciais"
        emoji_principal = "🌅"
    elif marco >= 600:
        titulo_alerta = "🌄 ALERTA ANTECIPADO — 10h antes"
        subtitulo = "Primeira janela de odds estáveis"
        emoji_principal = "🌄"
    elif marco >= 360:
        titulo_alerta = "📅 ALERTA PRÉ-LIVE — 6h antes"
        subtitulo = "Mercado aquecendo, análise consolidada"
        emoji_principal = "📅"
    elif marco >= 120:
        titulo_alerta = "⏰ ALERTA PRÉ-JOGO — 2h antes"
        subtitulo = "Janela clássica de entrada"
        emoji_principal = "⏰"
    elif marco >= 60:
        titulo_alerta = "⏰ ALERTA PRÉ-JOGO — 1h antes"
        subtitulo = "Última chance antes dos lineups"
        emoji_principal = "⏰"
    else:
        titulo_alerta = f"🚨 ALERTA FINAL — {marco}min antes"
        subtitulo = "Lineups divulgados, entrada final"
        emoji_principal = "🚨"

    linhas = [
        f"{emoji_principal} *{titulo_alerta}* {j['tier_emoji']}",
        f"_{subtitulo}_", "",
        _cabecalho_jogo(j), "",
        f"🧠 *Modelo Poisson Duplo*",
        f"```",
        f"λ total (esperado)   : {j['lambda']:.2f}",
        f"  λ casa             : {lams['lambda_casa']:.2f}",
        f"  λ fora             : {lams['lambda_fora']:.2f}",
        f"P(Acima 3.5 cartões) : {probs['acima_3_5'] * 100:.1f}%",
        f"P(Acima 4.5 cartões) : {probs['acima_4_5'] * 100:.1f}%",
        f"P(Acima 5.5 cartões) : {probs['acima_5_5'] * 100:.1f}%",
        f"```", "",
        f"📊 *Análise detalhada*",
        f"```",
        f"Score         : {j['score']:.1f}  {barra}",
        f"Confiança     : {conf}",
        f"Mandante      : {j['cartoes_casa']:.2f}/jogo (raw:{j['cartoes_casa_raw']:.2f}) {emoji_forma(j['forma_casa'])}",
        f"Visitante     : {j['cartoes_fora']:.2f}/jogo (raw:{j['cartoes_fora_raw']:.2f}) {emoji_forma(j['forma_fora'])}",
        f"Árbitro       : {j['arb_cartoes']:.1f}/jogo ({'real' if j['arb_conhecido'] else 'est.'})",
        f"Peso liga     : x{j['peso_liga']:.2f}",
        f"Tabela        : {j.get('contexto_tabela', 'N/D')}",
    ]
    if j.get("h2h_media_cartoes"):
        linhas.append(f"H2H           : {j['h2h_media_cartoes']:.1f} cart ({j['h2h_jogos']} jogos)")
    linhas += [f"```", ""]

    # Gemini
    gemini = j.get("gemini", {})
    if gemini.get("analise_texto"):
        linhas += [
            f"🤖 *Análise Gemini* (confiança: {gemini.get('confianca','?')}, score IA: {gemini.get('score_gemini',0):.1f})",
            f"_{gemini['analise_texto']}_", "",
        ]

    linhas += [
        f"🎯 *Mercado recomendado*",
        f"  • Mercado: *{mkt['nome']}*",
        f"  • Probabilidade: `{mkt['prob'] * 100:.1f}%`",
        f"  • Odd: `{mkt['odd']:.2f}` {'(real do mercado)' if mkt.get('odd_real') else '(referência)'}",
        f"  • Edge: `{mkt['edge'] * 100:.1f}%`  EV: `{mkt['ev']:+.3f}`", "",
        f"💰 *Stake (Kelly {int(KELLY_FRACAO * 100)}%):* `R$ {j['stake']:.2f}`",
        f"⚖️ *Tier:* {j['tier_emoji']} {j['tier']}", "",
    ]

    # Análise de movimentação de odds (sharp money detection)
    if mkt.get("odd_real"):
        movimento = analisar_movimento_odds(j["fid"], mkt["linha"])
        if movimento.get("sinal") != "neutro" and movimento.get("movimento") not in ("insuficiente", "erro"):
            emoji_mov = {
                "confirmacao_forte": "🟢", "confirmacao": "🟡",
                "divergencia_forte": "🔴", "divergencia": "🟠",
            }.get(movimento["sinal"], "⚪")
            linhas += [
                f"📈 *Movimentação de Odds* {emoji_mov}",
                f"  {movimento['descricao']}",
                "",
            ]

    if j.get("detalhes"):
        linhas.append(f"📝 *Fatores:*")
        for d in j["detalhes"]:
            linhas.append(f"  • {d}")
        linhas.append("")

    # Aviso específico para alertas antecipados
    if marco >= 600:  # 10h+
        linhas += [
            _sep(),
            f"⚠️ *AVISO — Alerta antecipado*",
            f"As odds ainda vão se movimentar. Vantagens:",
            f"  • Odds iniciais geralmente mais altas",
            f"  • Tempo para avaliar lineups quando saírem",
            f"  • Possibilidade de entrada escalonada",
            f"Recomenda-se aguardar marcos finais (2h/1h/30min)",
            f"antes de entrada completa. Stake sugerido aqui é",
            f"para *entrada parcial* (até 50% da posição).",
            "",
        ]
    elif marco >= 360:  # 6h+
        linhas += [
            _sep(),
            f"⚠️ *Pré-live (6h antes)*",
            f"Odds já mais estáveis, mas lineups ainda não saíram.",
            f"Entrada recomendada: até 70% da posição planejada.",
            "",
        ]

    linhas += [_sep(), f"_Confirme as odds reais antes de entrar._"]
    return "\n".join(linhas)

def render_resultado(info: dict, total_cartoes: int, hit: bool, pnl: float) -> str:
    emoji_res = "✅ ACERTOU" if hit else "❌ ERROU"
    emoji_pnl = "📈" if hit else "📉"
    dt = parse_iso_para_local(info["datetime_iso"])
    dt_str = f"📅 {formatar_data_ptbr(dt)}  •  🕐 {formatar_hora(dt)}\n" if dt else ""
    return (
        f"📊 *RESULTADO REGISTRADO*\n\n{_sep()}\n"
        f"⚽ *{info['casa']}* vs *{info['fora']}*\n"
        f"🌎 {info['liga']}\n{dt_str}{_sep()}\n\n"
        f"🎯 Mercado: *{info['mercado_nome']}*\n"
        f"🟨 Cartões totais: *{total_cartoes}*\n"
        f"   (necessários: acima de {info['mercado_linha']:.0f})\n\n"
        f"{emoji_res}\n"
        f"{emoji_pnl} Resultado: `R$ {pnl:+.2f}`\n\n_Registrado._"
    )

def render_relatorio_semanal(rows: list) -> str:
    total = len(rows)
    acertos = sum(1 for r in rows if r["hit"] == "ACERTO")
    erros = total - acertos
    taxa_ac = (acertos / total * 100) if total else 0
    pnl_total = sum(float(r["pnl"]) for r in rows)
    stake_tot = sum(float(r["stake"]) for r in rows)
    roi = (pnl_total / stake_tot * 100) if stake_tot else 0

    tiers = {"QUENTE": [], "MORNO": [], "OBSERVAR": []}
    mercados = {}
    for r in rows:
        t = r.get("tier", "")
        if t in tiers: tiers[t].append(r)
        m = r.get("mercado", "?")
        mercados.setdefault(m, []).append(r)

    agora = datetime.now()
    linhas = [
        f"📊 *RELATÓRIO SEMANAL v6*", "",
        f"📆 Semana encerrada em {formatar_data_ptbr(agora)}", "",
        _sep(), f"```",
        f"Operações  : {total}", f"Acertos    : {acertos}", f"Erros      : {erros}",
        f"Taxa acerto: {taxa_ac:.1f}%", f"PnL total  : R$ {pnl_total:+.2f}",
        f"Stake total: R$ {stake_tot:.2f}", f"ROI        : {roi:+.1f}%",
        f"```", "", f"*Por Tier*", f"```",
    ]
    for tn, tr in tiers.items():
        if not tr: continue
        ac = sum(1 for r in tr if r["hit"] == "ACERTO")
        tt = len(tr)
        tp = sum(float(r["pnl"]) for r in tr)
        ts = sum(float(r["stake"]) for r in tr)
        troi = (tp / ts * 100) if ts else 0
        linhas.append(f"{tn:<8}: {ac}/{tt} ({ac/tt*100:.0f}%)  ROI {troi:+.1f}%")
    linhas += [f"```", "", f"*Por Mercado*", f"```"]
    for mn, mr in sorted(mercados.items()):
        mw = sum(1 for r in mr if r["hit"] == "ACERTO")
        mt = len(mr)
        mp = sum(float(r["pnl"]) for r in mr)
        linhas.append(f"{mn:<12}: {mw}/{mt} ({mw/mt*100:.0f}%)  PnL R$ {mp:+.0f}")
    linhas.append(f"```")
    prob_media = sum(float(r["prob_modelo"]) for r in rows) / total
    linhas += ["", f"🎯 *Calibração*",
               f"  • Prob média prevista: `{prob_media:.1f}%`",
               f"  • Taxa acerto real: `{taxa_ac:.1f}%`"]
    if abs(prob_media - taxa_ac) > 10:
        linhas.append(f"  ⚠️ _Desvio >10% — recalibrar modelo_")
    return "\n".join(linhas)

# ═══════════════════════════════════════════════════════════
#  MENSAGENS DE SISTEMA
# ═══════════════════════════════════════════════════════════
def render_startup() -> str:
    agora = datetime.now()
    return (
        f"🟢 *BOT DE CARTÕES v6.0 — ONLINE*\n\n"
        f"📅 {formatar_data_ptbr(agora)}\n🕐 `{formatar_hora(agora)}`\n\n{_sep()}\n"
        f"⚡ *Sistema*\n"
        f"  • Modelo: Poisson Duplo + Bayesian\n"
        f"  • IA: Gemini {'✅' if GEMINI_ATIVO else '❌'}\n"
        f"  • Workers: {MAX_WORKERS}\n"
        f"  • Banca: R$ {BANCA:.0f}\n"
        f"  • Max ops/dia: {MAX_OPS_DIA}\n"
        f"  • Exposição máx: {MAX_EXPOSICAO_PCT:.0f}%\n"
        f"  • Kelly: {int(KELLY_FRACAO * 100)}%\n\n"
        f"📋 *Agenda*\n"
        f"  • Watchlist: `{WATCHLIST_HORA:02d}:{WATCHLIST_MINUTO:02d}`\n"
        f"  • Scan: a cada `{INTERVALO_SCAN}s`\n"
        f"  • Horizonte de análise: `{HORIZONTE_HORAS}h` à frente\n"
        f"  • Marcos antecipados: `15h, 10h, 6h` (score ≥{MIN_SCORE_ANTECIPADO})\n"
        f"  • Marcos pré-jogo: `2h, 1h, 30min` (score ≥{MIN_SCORE_PRE_JOGO})\n"
        f"  • Closing loop: {'✅' if CLOSING_LOOP_ATIVO else '❌'}\n"
        f"  • Heartbeat: {'✅ ~1h' if HEARTBEAT_ATIVO else '❌'}\n"
        f"  • Relatório: {'✅ seg 09:30' if RELATORIO_SEMANAL else '❌'}\n\n"
        f"{_sep()}\n⌨️ /pausar  /retomar  /status  /ajuda\n\n_Pronto._"
    )

def render_status() -> str:
    estado = "⏸ PAUSADO" if bot_esta_pausado() else "▶️ ATIVO"
    agora = datetime.now()
    return (
        f"ℹ️ *STATUS v6*\n\n"
        f"🕐 `{formatar_hora(agora)}`  •  {formatar_data_ptbr(agora)}\n\n"
        f"Estado: *{estado}*\n"
        f"Scan: `{INTERVALO_SCAN}s`\nLigas: `{len(LIGAS_MONITORADAS)}`\n"
        f"Cache times: `{len(_cache_times)}`\nCache árbitros: `{len(_cache_arbitros)}`\n"
        f"Cache H2H: `{len(_cache_h2h)}`\nGemini: {'✅' if GEMINI_ATIVO else '❌'}\n"
    )

def render_ajuda() -> str:
    return (
        f"📖 *COMANDOS v6*\n\n{_sep()}\n\n"
        f"⏸ /pausar — para buscas e alertas\n"
        f"▶️ /retomar — retoma\n"
        f"ℹ️ /status — estado do bot\n"
        f"🎯 /proximos — jogos na fila de alerta\n"
        f"📊 /stats — métricas operacionais\n"
        f"❓ /ajuda — esta mensagem\n\n{_sep()}\n_Envie neste chat._"
    )

def render_proximos() -> str:
    """Lista próximos jogos aprovados, com marcos de alerta já disparados."""
    state = _ler_json(ARQUIVO_ESTADO)
    proximos = state.get("proximos_jogos_info", [])
    enviados = state.get("pre_jogo_enviados", {})
    agora = datetime.now()

    if not proximos:
        return (
            f"🎯 *PRÓXIMOS JOGOS NA FILA*\n\n"
            f"🕐 `{formatar_hora(agora)}`\n\n"
            f"_Nenhum jogo aprovado no momento._\n"
            f"O bot está monitorando — alertas chegarão conforme os jogos "
            f"entrarem nos marcos configurados."
        )

    # Contabiliza marcos já disparados por jogo (pelo fid embutido na chave)
    marcos_por_fid = {}
    for chave in enviados.keys():
        # formato: "PG-{fid}-{marco}"
        partes = chave.split("-")
        if len(partes) == 3:
            try:
                fid = partes[1]
                marco = int(partes[2])
                marcos_por_fid.setdefault(fid, []).append(marco)
            except (ValueError, IndexError):
                continue

    linhas = [
        f"🎯 *PRÓXIMOS JOGOS NA FILA*", "",
        f"🕐 `{formatar_hora(agora)}`  •  {formatar_data_ptbr(agora)}", "",
        f"{_sep()}",
    ]

    for idx, info in enumerate(proximos, 1):
        linhas.append(f"\n*{idx}.* {info}")

    linhas += [
        "",
        f"{_sep()}",
        f"📋 *Marcos de alerta*",
        f"  🌅 Antecipados: 15h, 10h, 6h (score ≥ {MIN_SCORE_ANTECIPADO})",
        f"  ⏰ Pré-jogo:    2h, 1h, 30min (score ≥ {MIN_SCORE_PRE_JOGO})",
        f"",
        f"Total de alertas enviados nas últimas 24h: `{len(enviados)}`",
    ]

    return "\n".join(linhas)

def render_stats_operacionais() -> str:
    """Métricas operacionais: alertas, performance por período."""
    state = _ler_json(ARQUIVO_ESTADO)
    alertas_dict = state.get("alertas_hoje", {})
    agora = datetime.now()

    # Calcula alertas por período
    hoje = _data_hoje()
    ontem = (agora - timedelta(days=1)).strftime("%Y-%m-%d")
    semana_corte = (agora - timedelta(days=7)).strftime("%Y-%m-%d")

    alertas_hoje = alertas_dict.get(hoje, 0)
    alertas_ontem = alertas_dict.get(ontem, 0)
    alertas_7d = sum(v for k, v in alertas_dict.items() if k >= semana_corte)
    alertas_30d = sum(alertas_dict.values())

    linhas = [
        f"📊 *ESTATÍSTICAS OPERACIONAIS*", "",
        f"🕐 `{formatar_hora(agora)}`  •  {formatar_data_ptbr(agora)}", "",
        f"{_sep()}",
        f"🚨 *Alertas enviados*",
        f"  • Hoje:           `{alertas_hoje}`",
        f"  • Ontem:          `{alertas_ontem}`",
        f"  • Últimos 7 dias: `{alertas_7d}`",
        f"  • Últimos 30 dias:`{alertas_30d}`",
        f"",
    ]

    # Stats de performance do histórico
    if os.path.exists(ARQUIVO_HISTORICO):
        try:
            periodos = {
                "Últimas 24h":       (agora - timedelta(days=1)),
                "Últimos 7 dias":    (agora - timedelta(days=7)),
                "Últimos 30 dias":   (agora - timedelta(days=30)),
            }

            stats_por_periodo = {nome: {"total": 0, "acertos": 0, "pnl": 0.0, "stake": 0.0}
                                 for nome in periodos}

            with open(ARQUIVO_HISTORICO, "r", encoding="utf-8") as f:
                for r in csv.DictReader(f):
                    try:
                        dt_row = datetime.strptime(r["data"], "%Y-%m-%d")
                        for nome, corte in periodos.items():
                            if dt_row >= corte:
                                s = stats_por_periodo[nome]
                                s["total"] += 1
                                if r["hit"] == "ACERTO":
                                    s["acertos"] += 1
                                s["pnl"] += float(r["pnl"])
                                s["stake"] += float(r["stake"])
                    except Exception:
                        continue

            linhas += [
                f"{_sep()}",
                f"💰 *Performance de operações*",
                f"```",
                f"{'Período':<18} {'Ops':>4} {'Hit%':>6} {'PnL':>10} {'ROI':>7}",
            ]
            for nome, s in stats_por_periodo.items():
                if s["total"] == 0:
                    linhas.append(f"{nome:<18} {'0':>4} {'—':>6} {'R$ 0':>10} {'—':>7}")
                else:
                    hit_pct = (s["acertos"] / s["total"] * 100)
                    roi = (s["pnl"] / s["stake"] * 100) if s["stake"] else 0
                    linhas.append(
                        f"{nome:<18} {s['total']:>4} "
                        f"{hit_pct:>5.0f}% R$ {s['pnl']:>+7.0f} "
                        f"{roi:>+5.1f}%"
                    )
            linhas.append(f"```")
        except Exception as e:
            linhas.append(f"_Erro ao ler histórico: {e}_")
    else:
        linhas += [
            f"{_sep()}",
            f"💰 *Performance de operações*",
            f"_Histórico vazio — aguardando primeiros resultados via closing loop._",
        ]

    # Cache status
    linhas += [
        f"",
        f"{_sep()}",
        f"💾 *Cache em memória*",
        f"  • Times:     `{len(_cache_times)}`",
        f"  • Árbitros:  `{len(_cache_arbitros)}`",
        f"  • H2H:       `{len(_cache_h2h)}`",
        f"  • Standings: `{len(_cache_standings)}`",
        f"  • Odds:      `{len(_cache_odds)}`",
    ]

    return "\n".join(linhas)

def render_heartbeat(ciclos, n_pre, n_qual, n_ok, state: dict = None) -> str:
    agora = datetime.now()
    state = state or {}

    # Métricas operacionais
    alertas_hoje = state.get("alertas_hoje", {}).get(_data_hoje(), 0)
    proximos_jogos = state.get("proximos_jogos_info", [])

    # Stats de acerto dos últimos 7 dias (se houver histórico)
    taxa_acerto_str = ""
    try:
        if os.path.exists(ARQUIVO_HISTORICO):
            corte = datetime.now() - timedelta(days=7)
            total, acertos, pnl_7d, stake_7d = 0, 0, 0.0, 0.0
            with open(ARQUIVO_HISTORICO, "r", encoding="utf-8") as f:
                for r in csv.DictReader(f):
                    try:
                        if datetime.strptime(r["data"], "%Y-%m-%d") >= corte:
                            total += 1
                            if r["hit"] == "ACERTO":
                                acertos += 1
                            pnl_7d += float(r["pnl"])
                            stake_7d += float(r["stake"])
                    except Exception:
                        continue
            if total > 0:
                roi_7d = (pnl_7d / stake_7d * 100) if stake_7d else 0
                taxa_acerto_str = (
                    f"\n📊 *Últimos 7 dias*\n"
                    f"  • Ops: `{total}`  Acertos: `{acertos}/{total}` ({acertos/total*100:.0f}%)\n"
                    f"  • PnL: `R$ {pnl_7d:+.2f}`  ROI: `{roi_7d:+.1f}%`\n"
                )
    except Exception as e:
        log.debug(f"Erro métricas heartbeat: {e}")

    # Próximos jogos na fila de alerta
    proximos_str = ""
    if proximos_jogos:
        proximos_str = "\n🎯 *Próximos jogos na fila*\n"
        for p in proximos_jogos[:3]:
            proximos_str += f"  • {p}\n"

    return (
        f"💤 *HEARTBEAT*\n\n"
        f"🕐 `{formatar_hora(agora)}`  •  {formatar_data_ptbr(agora)}\n\n"
        f"Bot ativo, {ciclos} ciclos sem alertas.\n\n"
        f"📊 *Último ciclo*\n"
        f"  • Pré-jogo: `{n_pre}`\n"
        f"  • Qualificados: `{n_qual}`\n"
        f"  • Aprovados: `{n_ok}`\n"
        f"  • Alertas hoje: `{alertas_hoje}`"
        f"{taxa_acerto_str}"
        f"{proximos_str}"
        f"\n_Sistema operacional._"
    )

# ═══════════════════════════════════════════════════════════
#  TELEGRAM — ENVIO E COMANDOS
# ═══════════════════════════════════════════════════════════
def send_telegram(mensagem: str, parse_mode: str = "Markdown") -> bool:
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

    def _enviar(modo):
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": mensagem,
                   "disable_web_page_preview": True}
        if modo:
            payload["parse_mode"] = modo
        try:
            return _sess_tg.post(url, json=payload, timeout=10)
        except Exception as e:
            log.error(f"Rede Telegram: {e}")
            return None

    r = _enviar(parse_mode)
    if r is None:
        return False
    if r.status_code == 200:
        return True

    log.warning(f"Telegram falhou com {parse_mode} ({r.status_code}) — fallback texto puro")
    r2 = _enviar(None)
    if r2 and r2.status_code == 200:
        return True
    log.error(f"Telegram falhou definitivamente")
    return False

def enviar_fragmentado(mensagem, limite=4000):
    if not mensagem:
        return
    if len(mensagem) <= limite:
        send_telegram(mensagem)
        return
    partes = [mensagem[i:i + limite] for i in range(0, len(mensagem), limite)]
    for parte in partes:
        send_telegram(parte)
        time.sleep(0.8)

def test_telegram() -> bool:
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getMe"
        r = _sess_tg.get(url, timeout=10)
        if r.status_code == 200:
            log.info(f"✅ Telegram: @{r.json()['result']['username']}")
            return True
        log.error(f"❌ Telegram: {r.status_code}")
        return False
    except Exception as e:
        log.error(f"❌ Telegram: {e}")
        return False

def processar_comandos_telegram():
    global _ultimo_update_id
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
        r = _sess_tg.get(url, params={"offset": _ultimo_update_id + 1,
                                       "timeout": 2, "limit": 10}, timeout=8)
        if r.status_code != 200:
            return
        for update in r.json().get("result", []):
            _ultimo_update_id = max(_ultimo_update_id, update["update_id"])
            msg = update.get("message", {})
            if not msg:
                continue
            chat_id = str(msg.get("chat", {}).get("id", ""))
            texto = msg.get("text", "").strip().lower()
            if chat_id != str(TELEGRAM_CHAT_ID):
                continue
            if texto in ("/pausar", "/pause"):
                pausar_bot()
                send_telegram("⏸ *Bot pausado*\nEnvie /retomar para continuar.")
            elif texto in ("/retomar", "/resume", "/continuar"):
                retomar_bot()
                send_telegram(f"▶️ *Bot retomado*\nPróximo scan em até `{INTERVALO_SCAN}s`.")
            elif texto == "/status":
                send_telegram(render_status())
            elif texto in ("/proximos", "/next", "/fila"):
                send_telegram(render_proximos())
            elif texto in ("/stats", "/estatisticas", "/metricas"):
                send_telegram(render_stats_operacionais())
            elif texto in ("/ajuda", "/help", "/start"):
                send_telegram(render_ajuda())
            else:
                send_telegram(f"❓ `{texto}` — Use /ajuda")
    except Exception as e:
        log.debug(f"Erro comandos TG: {e}")

# ═══════════════════════════════════════════════════════════
#  CLOSING LOOP
# ═══════════════════════════════════════════════════════════
def registrar_pendente(j: dict):
    pendentes = _ler_json(ARQUIVO_PENDENTES)
    pendentes[str(j["fid"])] = {
        "fid": j["fid"], "datetime_iso": j["datetime_iso"],
        "casa": j["casa"], "fora": j["fora"], "liga": j["liga"],
        "tier": j["tier"], "score": j["score"], "lambda": j["lambda"],
        "mercado_nome": j["mercado"]["nome"], "mercado_linha": j["mercado"]["linha"],
        "mercado_prob": j["mercado"]["prob"], "mercado_odd": j["mercado"]["odd"],
        "stake": j["stake"], "arb_nome": j["arb_nome"],
        "arb_cartoes": j["arb_cartoes"], "registrado_em": time.time(),
    }
    _salvar_json(ARQUIVO_PENDENTES, pendentes)

def coletar_resultados_pendentes():
    if not CLOSING_LOOP_ATIVO:
        return
    pendentes = _ler_json(ARQUIVO_PENDENTES)
    if not pendentes:
        return
    agora   = datetime.now()
    remover = []
    novos   = []
    for fid, info in pendentes.items():
        try:
            dt_jogo = parse_iso_para_local(info["datetime_iso"])
            if not dt_jogo:
                continue
            decorrido = (agora - dt_jogo).total_seconds()
            if decorrido < 7200:
                continue
            if decorrido > 86400:
                remover.append(fid)
                continue
            fixture = buscar_resultado_fixture(int(fid))
            if not fixture:
                continue
            status = fixture.get("fixture", {}).get("status", {}).get("short")
            if status not in ("FT", "AET", "PEN"):
                continue
            eventos = fixture.get("events", []) or []
            amarelos = sum(1 for e in eventos if e.get("type") == "Card"
                           and e.get("detail") in ("Yellow Card", "Yellowcard"))
            vermelhos = sum(1 for e in eventos if e.get("type") == "Card"
                            and e.get("detail") in ("Red Card", "Second Yellow card"))
            total_c = amarelos + vermelhos
            linha = info["mercado_linha"]
            hit = total_c > linha
            pnl = (info["stake"] * (info["mercado_odd"] - 1)) if hit else -info["stake"]
            _registrar_historico({
                "data": dt_jogo.strftime("%Y-%m-%d"), "hora": dt_jogo.strftime("%H:%M"),
                "liga": info["liga"], "jogo": f"{info['casa']} x {info['fora']}",
                "tier": info["tier"], "score": info["score"],
                "lambda_modelo": info["lambda"], "mercado": info["mercado_nome"],
                "prob_modelo": round(info["mercado_prob"] * 100, 1),
                "odd_ref": info["mercado_odd"], "stake": info["stake"],
                "cartoes_reais": total_c, "amarelos": amarelos, "vermelhos": vermelhos,
                "hit": "ACERTO" if hit else "ERRO", "pnl": round(pnl, 2),
                "arbitro": info["arb_nome"], "arb_estim": info["arb_cartoes"],
            })
            novos.append((info, total_c, hit, pnl))
            remover.append(fid)
        except Exception as e:
            log.debug(f"Erro pendente {fid}: {e}")
    for fid in remover:
        pendentes.pop(fid, None)
    _salvar_json(ARQUIVO_PENDENTES, pendentes)
    for info, tc, hit, pnl in novos:
        send_telegram(render_resultado(info, tc, hit, pnl))
        log.info(f"📊 {info['casa']} x {info['fora']} | {tc} cart | {'OK' if hit else 'ERR'} | R$ {pnl:+.2f}")

def _registrar_historico(linha: dict):
    existe = os.path.exists(ARQUIVO_HISTORICO)
    try:
        with open(ARQUIVO_HISTORICO, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(linha.keys()))
            if not existe:
                w.writeheader()
            w.writerow(linha)
    except Exception as e:
        log.error(f"Erro histórico: {e}")

def gerar_e_enviar_relatorio():
    if not os.path.exists(ARQUIVO_HISTORICO):
        return
    try:
        corte = datetime.now() - timedelta(days=7)
        rows = []
        with open(ARQUIVO_HISTORICO, "r", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                try:
                    if datetime.strptime(r["data"], "%Y-%m-%d") >= corte:
                        rows.append(r)
                except Exception:
                    continue
        if not rows:
            send_telegram("📊 *Relatório semanal*\nNenhuma operação nos últimos 7 dias.")
            return
        enviar_fragmentado(render_relatorio_semanal(rows))
        log.info("📊 Relatório semanal enviado")
    except Exception as e:
        log.error(f"Erro relatório: {e}")

# ═══════════════════════════════════════════════════════════
#  CONTROLE DE ESTADO
# ═══════════════════════════════════════════════════════════
def _data_hoje() -> str:
    return datetime.now().strftime("%Y-%m-%d")

def watchlist_ja_enviada(state): return state.get("watchlist_data") == _data_hoje()
def marcar_watchlist_enviada(state):
    state["watchlist_data"] = _data_hoje(); _salvar_json(ARQUIVO_ESTADO, state)
def hora_watchlist():
    return datetime.now().time() >= dtime(WATCHLIST_HORA, WATCHLIST_MINUTO)
def relatorio_ja_enviado(state): return state.get("relatorio_data") == _data_hoje()
def marcar_relatorio_enviado(state):
    state["relatorio_data"] = _data_hoje(); _salvar_json(ARQUIVO_ESTADO, state)
def hora_relatorio():
    a = datetime.now()
    return a.weekday() == RELATORIO_DIA_SEMANA and a.time() >= dtime(RELATORIO_HORA, RELATORIO_MINUTO)
def pre_jogo_ja_enviado(state, fid): return f"PG-{fid}" in state.get("pre_jogo_enviados", {})
def marcar_pre_jogo_enviado(state, fid):
    state.setdefault("pre_jogo_enviados", {})
    state["pre_jogo_enviados"][f"PG-{fid}"] = time.time()
    state["pre_jogo_enviados"] = {k: v for k, v in state["pre_jogo_enviados"].items() if time.time() - v < 86400}
    _salvar_json(ARQUIVO_ESTADO, state)

# ═══════════════════════════════════════════════════════════
#  LOOP PRINCIPAL
# ═══════════════════════════════════════════════════════════
def main():
    log.info("=" * 60)
    log.info("⚡ BOT DE CARTÕES v6.0 — INICIANDO")
    log.info("=" * 60)

    if not test_telegram():
        log.error("Sem Telegram. Abortando.")
        return

    _init_gemini()
    if GEMINI_ATIVO:
        teste = consultar_gemini("Responda apenas: OK")
        if "OK" in teste.upper() or "ok" in teste.lower():
            log.info("✅ Gemini conectado")
        else:
            log.warning(f"⚠️ Gemini pode não estar funcional: {teste[:100]}")

    log.info("📂 Carregando caches...")
    _cache_times.update(_limpar_expirados(_ler_json(CACHE_TIMES_ARQUIVO), TTL_CACHE_TIME))
    _cache_arbitros.update(_limpar_expirados(_ler_json(CACHE_ARBS_ARQUIVO), TTL_CACHE_ARBITRO))
    _cache_h2h.update(_limpar_expirados(_ler_json(CACHE_H2H_ARQUIVO), TTL_CACHE_H2H))
    log.info(f"   Times: {len(_cache_times)} | Árbitros: {len(_cache_arbitros)} | H2H: {len(_cache_h2h)}")

    send_telegram(render_startup())
    state = _ler_json(ARQUIVO_ESTADO)

    while True:
        t_ciclo = time.time()
        n_pre = n_anal = n_ok = 0

        try:
            processar_comandos_telegram()

            if bot_esta_pausado():
                log.info("⏸  Pausado")
                time.sleep(INTERVALO_SCAN)
                continue

            coletar_resultados_pendentes()

            # Reset do contador de chamadas Gemini por ciclo
            _resetar_gemini_ciclo()

            hoje = _data_hoje()
            amanha = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
            # Busca também depois de amanhã se o horizonte estender além de 24h
            # (ex: scan às 23h quer jogos das 14h do dia seguinte = ~15h à frente)
            depois_amanha = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d")
            todos = buscar_partidas(hoje) + buscar_partidas(amanha) + buscar_partidas(depois_amanha)

            # PRÉ-FILTRO RÁPIDO: rejeita fixtures irrelevantes antes de gastar CPU/API
            # Critérios: (1) está em liga monitorada, (2) status é pré-jogo,
            # (3) jogo acontece dentro do HORIZONTE_HORAS à frente
            agora_ts = datetime.now().timestamp()
            janela_max_ts = agora_ts + (HORIZONTE_HORAS * 3600)  # horizonte configurável
            janela_min_ts = agora_ts - (2 * 3600)                # 2h atrás (tolerância)

            pre_jogo = []
            for f in todos:
                # Filtro 1: liga monitorada
                lid = f.get("league", {}).get("id")
                if lid not in LIGAS_MONITORADAS:
                    continue
                # Filtro 2: status pré-jogo
                status = f.get("fixture", {}).get("status", {}).get("short")
                if status not in ("NS", "TBD", "PST"):
                    continue
                # Filtro 3: janela temporal relevante
                ts = f.get("fixture", {}).get("timestamp")
                if ts and not (janela_min_ts <= ts <= janela_max_ts):
                    continue
                pre_jogo.append(f)

            # Deduplicação (mesmo fixture pode vir em 2 dias se timestamps forem próximos à meia-noite)
            pre_jogo_dedup = {}
            for f in pre_jogo:
                fid = f.get("fixture", {}).get("id")
                if fid and fid not in pre_jogo_dedup:
                    pre_jogo_dedup[fid] = f
            pre_jogo = list(pre_jogo_dedup.values())

            n_pre = len(pre_jogo)
            log.info(f"🔎 {n_pre} jogos na janela de {HORIZONTE_HORAS}h (de {len(todos)} totais)")

            # PRIORIZAÇÃO: jogos mais próximos primeiro.
            # Se o ciclo falhar no meio, os jogos urgentes já foram processados.
            pre_jogo.sort(key=lambda f: f.get("fixture", {}).get("timestamp", float('inf')))

            prefetch_todos_stats(pre_jogo)

            # Análise paralela dos fixtures (cache já quente, paralelismo seguro)
            t_anal = time.time()
            analises = []
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(analisar_fixture, fx) for fx in pre_jogo]
                for fut in as_completed(futures):
                    try:
                        r = fut.result()
                        if r:
                            analises.append(r)
                    except Exception as e:
                        log.debug(f"Erro análise paralela: {e}")
            n_anal = len(analises)
            log.info(f"📊 {n_anal} qualificados (score >= {MIN_SCORE_ALERTA}) em {time.time() - t_anal:.1f}s")

            jogos_filtrados, exposicao = aplicar_filtros_risco(analises)
            jogos_ok = [j for j in jogos_filtrados if j.get("risco_status") == "OK"]
            n_ok = len(jogos_ok)
            log.info(f"🛡 {n_ok} aprovados | Exposição R$ {exposicao:.2f}")

            # ── Odds reais: busca paralela APENAS nos aprovados ──
            if jogos_ok:
                t_odds = time.time()
                with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(jogos_ok))) as executor:
                    fut_map = {executor.submit(buscar_odds_cartoes, j["fid"]): j for j in jogos_ok}
                    for fut in as_completed(fut_map):
                        try:
                            odds = fut.result()
                            j = fut_map[fut]
                            j["odds_reais"] = odds
                            # Recalcula mercado e stake com odds reais
                            j["mercado"] = escolher_melhor_mercado(j["probs"], odds)
                            j["stake"] = kelly_stake(
                                j["mercado"]["prob"], j["mercado"]["odd"],
                                BANCA, KELLY_FRACAO
                            )
                        except Exception as e:
                            log.debug(f"Erro odds: {e}")
                log.debug(f"💱 Odds carregadas em {time.time() - t_odds:.1f}s")

            # ── Gemini: apenas para jogos que vão ser ALERTADOS neste ciclo ──
            # Prioriza jogos PRÓXIMOS (≤2h) sobre antecipados (Gemini é caro em cota).
            # Para antecipados (>=6h antes), Gemini só é chamado se sobrar cota.
            if GEMINI_ATIVO and _gemini_disponivel and jogos_ok:
                agora_check = datetime.now()
                jogos_gemini_urgente = []   # marcos pré-jogo (<360min)
                jogos_gemini_antecip = []   # marcos antecipados (>=360min)

                def _tol_por_marco(m: int) -> float:
                    base = (INTERVALO_SCAN / 60) + 1
                    if m >= 600:  return max(base, 5)
                    elif m >= 360: return max(base, 3)
                    else:          return base

                for j in jogos_ok:
                    if not j["datetime"]:
                        continue
                    delta = (j["datetime"] - agora_check).total_seconds() / 60
                    if delta < 0:
                        continue
                    # Para cada marco, verifica se precisa de Gemini
                    for marco in sorted(PRE_JOGO_MARCOS, reverse=True):
                        score_min = MIN_SCORE_ANTECIPADO if marco in MARCOS_ANTECIPADOS else MIN_SCORE_PRE_JOGO
                        if j["score"] < score_min:
                            continue
                        tol = _tol_por_marco(marco)
                        if (marco - tol) <= delta <= (marco + 1):
                            mk = f"PG-{j['fid']}-{marco}"
                            if mk not in state.get("pre_jogo_enviados", {}):
                                if marco in MARCOS_ANTECIPADOS:
                                    jogos_gemini_antecip.append(j)
                                else:
                                    jogos_gemini_urgente.append(j)
                                break

                # Urgentes primeiro (próximos do kickoff), depois antecipados
                jogos_gemini_urgente.sort(key=lambda x: x["score"], reverse=True)
                jogos_gemini_antecip.sort(key=lambda x: x["score"], reverse=True)
                jogos_para_gemini = jogos_gemini_urgente + jogos_gemini_antecip

                for j in jogos_para_gemini[:GEMINI_MAX_POR_CICLO]:
                    if _gemini_chamadas_ciclo >= GEMINI_MAX_POR_CICLO:
                        break
                    gemini_resp = gemini_analise_pre_jogo(j)
                    j["gemini"] = gemini_resp
                    if gemini_resp.get("score_gemini", 0) > 0:
                        delta_g = abs(j["score"] - gemini_resp["score_gemini"])
                        if delta_g > 2.0:
                            j["detalhes"].append(
                                f"⚠️ Gemini discorda (IA: {gemini_resp['score_gemini']:.1f} vs modelo: {j['score']:.1f})"
                            )
                    log.info(f"🤖 Gemini: {j['casa']} vs {j['fora']} "
                             f"(IA: {gemini_resp.get('score_gemini', 0):.1f})")


            # Watchlist
            if WATCHLIST_ATIVA and hora_watchlist() and not watchlist_ja_enviada(state):
                jogos_hoje = [j for j in jogos_filtrados
                              if j["datetime"] and j["datetime"].strftime("%Y-%m-%d") == hoje]
                jogos_hoje_ok = [j for j in jogos_hoje if j.get("risco_status") == "OK"]
                if jogos_hoje_ok:
                    msg = render_watchlist(jogos_hoje, exposicao)
                    if msg:
                        enviar_fragmentado(msg)
                        log.info(f"📋 Watchlist: {len(jogos_hoje_ok)} jogos")
                        for j in jogos_hoje_ok:
                            registrar_pendente(j)
                        marcar_watchlist_enviada(state)
                else:
                    log.info("Sem jogos aprovados para hoje")
                    marcar_watchlist_enviada(state)

            # Pré-alertas em múltiplos marcos (15h, 10h, 6h, 2h, 1h, 30min)
            # Janela assimétrica: dispara quando delta está entre (marco - scan/60 - 1) e marco.
            # Marcos ANTECIPADOS (>=360min) exigem score mais alto (MIN_SCORE_ANTECIPADO).
            # Marcos PRÉ-JOGO (<360min) usam MIN_SCORE_PRE_JOGO padrão.
            if PRE_JOGO_ATIVO:
                agora_local = datetime.now()
                # Tolerância dinâmica baseada no intervalo de scan (garante captura)
                tolerancia_entrada = (INTERVALO_SCAN / 60) + 1  # ex.: 60s = 2min

                # Para marcos antecipados (muito distantes), aumenta tolerância
                # proporcionalmente para compensar imprecisão de horários (kickoff TBD, etc.)
                def _tolerancia_por_marco(marco: int) -> float:
                    if marco >= 600:    # 10h+
                        return max(tolerancia_entrada, 5)   # mínimo 5min
                    elif marco >= 360:  # 6h+
                        return max(tolerancia_entrada, 3)   # mínimo 3min
                    else:
                        return tolerancia_entrada

                for j in jogos_ok:
                    if not j["datetime"]:
                        continue
                    delta_min = (j["datetime"] - agora_local).total_seconds() / 60

                    # Ignora jogos que já começaram ou muito distantes
                    if delta_min < 0 or delta_min > max(PRE_JOGO_MARCOS) + 10:
                        continue

                    # Testa marcos do mais antigo (maior) ao mais próximo (menor)
                    for marco in sorted(PRE_JOGO_MARCOS, reverse=True):
                        # Score mínimo depende do tipo de marco
                        if marco in MARCOS_ANTECIPADOS:
                            score_min = MIN_SCORE_ANTECIPADO
                        else:
                            score_min = MIN_SCORE_PRE_JOGO

                        if j["score"] < score_min:
                            continue

                        tol = _tolerancia_por_marco(marco)
                        # Janela: [marco - tolerancia, marco + 1]
                        if (marco - tol) <= delta_min <= (marco + 1):
                            marco_key = f"PG-{j['fid']}-{marco}"
                            if marco_key in state.get("pre_jogo_enviados", {}):
                                break  # já enviou este marco, não testa os próximos
                            if send_telegram(render_alerta_pre_jogo(j, marco=marco)):
                                state.setdefault("pre_jogo_enviados", {})
                                state["pre_jogo_enviados"][marco_key] = time.time()
                                # Retenção: antecipados ficam até 24h, pré-jogo 12h
                                retencao = 86400 if marco >= 360 else 43200
                                state["pre_jogo_enviados"] = {
                                    k: v for k, v in state["pre_jogo_enviados"].items()
                                    if time.time() - v < 86400
                                }

                                # Contador de alertas por dia (para métricas)
                                state.setdefault("alertas_hoje", {})
                                hoje_key = _data_hoje()
                                state["alertas_hoje"][hoje_key] = state["alertas_hoje"].get(hoje_key, 0) + 1
                                # Limpeza: mantém só últimos 30 dias
                                corte_dias = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
                                state["alertas_hoje"] = {
                                    k: v for k, v in state["alertas_hoje"].items() if k >= corte_dias
                                }

                                _salvar_json(ARQUIVO_ESTADO, state)
                                registrar_pendente(j)

                                # Registra no site (endpoint /cartoes)
                                try:
                                    _reg = globals().get("_web_registrar_cartao")
                                    if _reg:
                                        _reg({**j, "dt_iso": j.get("datetime").isoformat() if j.get("datetime") else ""})
                                except Exception:
                                    pass

                                # Log diferenciado
                                if marco >= 600:
                                    horas_txt = f"{marco // 60}h"
                                    log.info(f"🌅 Alerta ANTECIPADO T-{horas_txt}: "
                                             f"{j['casa']} vs {j['fora']} (em {delta_min/60:.1f}h)")
                                elif marco >= 360:
                                    horas_txt = f"{marco // 60}h"
                                    log.info(f"📅 Alerta pré-live T-{horas_txt}: "
                                             f"{j['casa']} vs {j['fora']} (em {delta_min/60:.1f}h)")
                                else:
                                    log.info(f"⏰ Alerta T-{marco}min: "
                                             f"{j['casa']} vs {j['fora']} (em {delta_min:.0f}min)")
                            break  # não dispara múltiplos marcos no mesmo ciclo

            # Relatório semanal
            if RELATORIO_SEMANAL and hora_relatorio() and not relatorio_ja_enviado(state):
                gerar_e_enviar_relatorio()
                marcar_relatorio_enviado(state)

            # Heartbeat
            if HEARTBEAT_ATIVO:
                state.setdefault("ciclos_sem_alerta", 0)
                state["ciclos_sem_alerta"] = state["ciclos_sem_alerta"] + 1 if n_ok == 0 else 0

                # Coleta próximos jogos aprovados para mostrar no heartbeat
                agora_hb = datetime.now()
                proximos = []
                for j in sorted(jogos_ok, key=lambda x: x["datetime"] or datetime.max):
                    if not j["datetime"]:
                        continue
                    delta_h = (j["datetime"] - agora_hb).total_seconds() / 3600
                    if 0 < delta_h <= HORIZONTE_HORAS:
                        if delta_h >= 1:
                            tempo_txt = f"em {delta_h:.1f}h"
                        else:
                            tempo_txt = f"em {int(delta_h * 60)}min"
                        proximos.append(
                            f"{j['tier_emoji']} {j['casa']} vs {j['fora']} "
                            f"(score {j['score']:.1f}, {tempo_txt})"
                        )
                state["proximos_jogos_info"] = proximos[:5]

                if state["ciclos_sem_alerta"] >= HEARTBEAT_CICLOS:
                    send_telegram(render_heartbeat(
                        state["ciclos_sem_alerta"], n_pre, n_anal, n_ok, state
                    ))
                    state["ciclos_sem_alerta"] = 0
                _salvar_json(ARQUIVO_ESTADO, state)

        except KeyboardInterrupt:
            log.info("🛑 Encerrado (Ctrl+C)")
            send_telegram(f"🔴 *Bot OFFLINE* — {formatar_hora(datetime.now())}")
            break
        except Exception as e:
            log.error(f"Erro loop: {e}", exc_info=True)

        decorrido = time.time() - t_ciclo
        prox = max(0, INTERVALO_SCAN - decorrido)
        log.info(f"⏱ Ciclo {decorrido:.1f}s — próximo em {prox:.0f}s")
        time.sleep(prox)


if __name__ == "__main__":
    main()