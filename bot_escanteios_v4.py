#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔═══════════════════════════════════════════════════════════════╗
║   BOT DE ESCANTEIOS v4.5 — PRÉ-LIVE + LIVE + SITE WEB       ║
║   Motor completo com servidor web embutido na porta 8765      ║
╚═══════════════════════════════════════════════════════════════╝

Para rodar:
  1. pip install requests
  2. python3 bot_escanteios_v4.py
  3. Abra o arquivo index.html no navegador

O site atualiza os sinais automaticamente a cada 15 segundos.
Comandos: [P] Pausar  [S] Sair  [R] Relatório  [L] Jogos ao vivo
"""

import requests
import time
import json
import threading
import sys
import os
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, HTTPServer

# ═══════════════════ CREDENCIAIS ═══════════════════
# Lê do arquivo .env (local) ou de variáveis de ambiente (Railway)

def _carregar_env(caminho=".env"):
    """Carrega variáveis do arquivo .env sem bibliotecas externas."""
    if os.path.exists(caminho):
        with open(caminho, "r", encoding="utf-8") as f:
            for linha in f:
                linha = linha.strip()
                if not linha or linha.startswith("#"):
                    continue
                if "=" in linha:
                    chave, _, valor = linha.partition("=")
                    os.environ.setdefault(chave.strip(), valor.strip())

_carregar_env()

API_FOOTBALL_KEY = os.environ.get("API_FOOTBALL_KEY", "")
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
GEMINI_API_KEY   = os.environ.get("GEMINI_API_KEY", "")

# Valida credenciais
_faltando = [n for n, v in [
    ("API_FOOTBALL_KEY", API_FOOTBALL_KEY),
    ("TELEGRAM_TOKEN",   TELEGRAM_TOKEN),
    ("TELEGRAM_CHAT_ID", TELEGRAM_CHAT_ID),
    ("GEMINI_API_KEY",   GEMINI_API_KEY),
] if not v]

if _faltando:
    print("\n❌ CREDENCIAIS FALTANDO — configure o arquivo .env ou as variáveis no Railway:")
    for c in _faltando:
        print(f"   • {c}")
    print("\nVeja o arquivo .env.exemplo para instruções.\n")
    exit(1)

# ═══════════════════ CONFIG GERAL ═══════════════════
API_BASE       = "https://v3.football.api-sports.io"
TELEGRAM_API   = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
GEMINI_URL     = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}"
# Gemini desativado temporariamente — chave HTTP 401. Reative quando renovar a chave.
USAR_GEMINI    = False

# ── CICLOS SEPARADOS ──
CICLO_LIVE_SEG     = 90    # ao vivo: a cada 90s (era 45)
CICLO_PRE_SEG      = 300   # pré-live: a cada 5 min
CICLO_AGENDA_SEG   = 1800  # agenda do dia: a cada 30 min

# ── PRÉ-LIVE ──
PRE_LIVE_IMINENTE  = 60    # até 60min — sinal detalhado com H2H
PRE_LIVE_PROXIMO   = 180   # até 3h — sinal com médias dos times
PRE_LIVE_HOJE      = 720   # até 12h — agenda do dia com projeções

# ── CONTROLE ──
MAX_SINAIS_JOGO_CICLO = 3
CONFIANCA_MINIMA      = 60
COOLDOWN_LIVE_MIN     = 10  # mín 10min entre sinais do mesmo tipo no mesmo jogo

# ── SERVIDOR WEB EMBUTIDO ──
PORTA_WEB        = int(os.environ.get("PORT", 8765))  # Railway injeta PORT automaticamente
ARQUIVO_JSON     = "sinais_data.json"
MAX_SINAIS_WEB   = 200

# ═══════════════════════════════════════════════════════
#  SERVIDOR WEB — expõe sinais via HTTP para o site
# ═══════════════════════════════════════════════════════

class _GerenciadorWeb:
    """Armazena sinais em memória e os serve via HTTP."""
    def __init__(self):
        self.sinais   = []
        self.lock     = threading.Lock()
        self.stats    = {"total": 0, "ao_vivo": 0, "pre_live": 0, "agenda": 0,
                         "ultimo_update": None, "bot_ativo": True}
        self._carregar()

    def _carregar(self):
        if os.path.exists(ARQUIVO_JSON):
            try:
                with open(ARQUIVO_JSON, "r", encoding="utf-8") as f:
                    d = json.load(f)
                    self.sinais = d.get("sinais", [])[-MAX_SINAIS_WEB:]
                    self.stats  = d.get("stats", self.stats)
            except Exception:
                pass

    def _salvar(self):
        try:
            with open(ARQUIVO_JSON, "w", encoding="utf-8") as f:
                json.dump({"sinais": self.sinais, "stats": self.stats},
                          f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def _categoria(self, tipo):
        if tipo.startswith("PRE"):           return "pre_live"
        if tipo in ("OVER","UNDER","OVER_1T","UNDER_1T"): return "mercado"
        if tipo in ("MOMENTUM","QUENTE","TEMPO2"):         return "momentum"
        if tipo in ("PRESSAO","DESESPERO","SECA"):         return "pressao"
        if tipo in ("DOMINIO","HANDICAP"):                 return "handicap"
        if tipo in ("BLOQUEIOS","FORA_ALVO"):              return "tecnico"
        if tipo == "AGENDA":                               return "agenda"
        return "outro"

    def adicionar_sinal(self, sinal, ia=None):
        with self.lock:
            agora = datetime.now(timezone.utc) - timedelta(hours=3)
            chave = sinal.get("chave", "")
            if any(s.get("id") == chave for s in self.sinais):
                return  # evita duplicata

            registro = {
                "id":         chave,
                "tipo":       sinal.get("tipo", ""),
                "categoria":  self._categoria(sinal.get("tipo", "")),
                "timestamp":  agora.isoformat(),
                "horario":    agora.strftime("%H:%M"),
                "data":       agora.strftime("%d/%m/%Y"),
                "novo":       True,
                "status":     "ativo",
                # Jogo
                "casa":       sinal.get("nome_casa", ""),
                "fora":       sinal.get("nome_fora", ""),
                "gols_casa":  sinal.get("gols_casa", 0),
                "gols_fora":  sinal.get("gols_fora", 0),
                "minuto":     sinal.get("minuto", 0),
                "liga":       sinal.get("nome_liga", ""),
                "pais":       sinal.get("pais", ""),
                "tier":       sinal.get("tier", "T3"),
                # Escanteios
                "esc_casa":   sinal.get("esc_casa", 0),
                "esc_fora":   sinal.get("esc_fora", 0),
                "total_esc":  sinal.get("total_esc", 0),
                "media_liga": sinal.get("media_liga", 9.5),
                # Sinal
                "confianca":     sinal.get("confianca", 60),
                "linha":         sinal.get("linha", None),
                "projecao":      sinal.get("projecao", None),
                "ritmo":         sinal.get("ritmo", None),
                "ritmo_rec":     sinal.get("ritmo_rec", None),
                "fator_placar":  sinal.get("fator_placar", None),
                "in_janela":     sinal.get("in_janela", False),
                "posse_casa":    sinal.get("posse_casa", 0),
                "posse_fora":    sinal.get("posse_fora", 0),
                "total_chutes":  sinal.get("total_chutes", 0),
                # Extras
                "time_dom":   sinal.get("time_dom", None),
                "time_mom":   sinal.get("time_mom", None),
                "time_pressao": sinal.get("time_pressao", None),
                "time_f":     sinal.get("time_f", None),
                "time_p":     sinal.get("time_p", None),
                "diff_g":     sinal.get("diff_g", None),
                "esc_rec":    sinal.get("esc_rec", None),
                "esc_dom":    sinal.get("esc_dom", None),
                "esc_out":    sinal.get("esc_out", None),
                "chutes_gol": sinal.get("chutes_gol", None),
                "ataques":    sinal.get("ataques", None),
                "score":      sinal.get("score", None),
                "taxa":       sinal.get("taxa", None),
                "total_bl":   sinal.get("total_bl", None),
                "diff":       sinal.get("diff", None),
                "min_sem":    sinal.get("min_sem", None),
                "min_inicio": sinal.get("min_inicio", None),
                "avg_casa":   sinal.get("avg_casa", None),
                "avg_fora":   sinal.get("avg_fora", None),
                "h2h_media":  sinal.get("h2h_media", None),
                "detalhes":   sinal.get("detalhes", []),
                "analise_ia": ia,
            }

            self.sinais.insert(0, registro)
            if len(self.sinais) > MAX_SINAIS_WEB:
                self.sinais = self.sinais[:MAX_SINAIS_WEB]

            # remove flag "novo" dos mais antigos
            for s in self.sinais[1:]:
                s["novo"] = False

            self.stats["total"] += 1
            self.stats["ultimo_update"] = agora.isoformat()
            cat = registro["categoria"]
            if cat == "pre_live":
                self.stats["pre_live"] += 1
            elif cat == "agenda":
                self.stats["agenda"] += 1
            else:
                self.stats["ao_vivo"] += 1

            self._salvar()

    def adicionar_agenda(self, jogos_lista):
        with self.lock:
            agora = datetime.now(timezone.utc) - timedelta(hours=3)
            sid = f"agenda_{agora.strftime('%Y%m%d_%H%M')}"
            if any(s.get("id") == sid for s in self.sinais):
                return
            registro = {
                "id": sid, "tipo": "AGENDA", "categoria": "agenda",
                "timestamp": agora.isoformat(), "horario": agora.strftime("%H:%M"),
                "data": agora.strftime("%d/%m/%Y"), "novo": True, "status": "ativo",
                "jogos": jogos_lista, "confianca": 100,
                "casa": "", "fora": "", "gols_casa": 0, "gols_fora": 0,
                "minuto": 0, "liga": "Agenda", "pais": "", "tier": "T1",
                "esc_casa": 0, "esc_fora": 0, "total_esc": 0,
                "media_liga": 0, "analise_ia": None,
            }
            self.sinais.insert(0, registro)
            self.stats["agenda"] += 1
            self.stats["ultimo_update"] = agora.isoformat()
            self._salvar()

    def get_dados(self):
        with self.lock:
            return json.dumps({
                "sinais": self.sinais[:100],
                "stats":  self.stats,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }, ensure_ascii=False)


class _Handler(BaseHTTPRequestHandler):
    def _headers_cors(self):
        """Envia todos os headers CORS obrigatórios."""
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.send_header("Access-Control-Max-Age", "86400")

    def do_OPTIONS(self):
        """Responde ao preflight CORS que o navegador envia antes do GET."""
        self.send_response(200)
        self._headers_cors()
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        if self.path.startswith("/sinais"):
            body = _web.get_dados().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self._headers_cors()
            self.send_header("Content-Length", len(body))
            self.end_headers()
            self.wfile.write(body)

        elif self.path.startswith("/gols"):
            # Endpoint do bot de gols
            body = _gols.get_dados().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self._headers_cors()
            self.send_header("Content-Length", len(body))
            self.end_headers()
            self.wfile.write(body)

        elif self.path in ("/", "/health"):
            body = b'{"status":"ok","servico":"CornerEdge Bot"}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self._headers_cors()
            self.send_header("Content-Length", len(body))
            self.end_headers()
            self.wfile.write(body)

        elif self.path.startswith("/debug"):
            # Diagnóstico completo — ajuda a ver o que está acontecendo
            import traceback
            try:
                t_ativo = datetime.now() - est.inicio
                h = int(t_ativo.total_seconds() // 3600)
                m = int((t_ativo.total_seconds() % 3600) // 60)
                info = {
                    "versao": "4.6",
                    "status": "pausado" if est.pausado else "ativo",
                    "uptime": f"{h}h {m}min",
                    "ciclos": est.ciclos,
                    "total_sinais_enviados": est.total_sinais,
                    "sinais_no_site": len(_web.sinais),
                    "sinais_gols_no_site": len(_gols.sinais),
                    "jogos_vivos_ultimo_ciclo": est.jogos_vivos,
                    "jogos_pre_ultimo_ciclo": est.jogos_pre,
                    "sinais_por_tipo": dict(est.por_tipo),
                    "ts_live": est.ts_live,
                    "ts_pre": est.ts_pre,
                    "ts_agenda": est.ts_agenda,
                    "porta_web": PORTA_WEB,
                    "telegram_chat_id": TELEGRAM_CHAT_ID,
                    "api_football_ok": bool(API_FOOTBALL_KEY),
                    "gemini_ok": bool(GEMINI_API_KEY),
                    "horario_brt": hora_br_curta(),
                }
                body = json.dumps(info, ensure_ascii=False, indent=2).encode("utf-8")
            except Exception as e:
                body = json.dumps({"erro": str(e)}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self._headers_cors()
            self.send_header("Content-Length", len(body))
            self.end_headers()
            self.wfile.write(body)

        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *a):
        pass  # silencia logs HTTP no terminal


def _iniciar_servidor_web():
    try:
        server = HTTPServer(("0.0.0.0", PORTA_WEB), _Handler)
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
        print(f"[WEB] Servidor HTTP ativo na porta {PORTA_WEB}")
        return True
    except Exception as e:
        print(f"[WEB] Falha ao iniciar servidor: {e}")
        return False


# Instância global do gerenciador web
_web = _GerenciadorWeb()


# ═══════════════════════════════════════════════════════
#  GERENCIADOR DE SINAIS DE GOLS — endpoint /gols
# ═══════════════════════════════════════════════════════

class _GerenciadorGols:
    """Armazena sinais do bot de gols em memória e os serve via /gols."""
    MAX = 150

    def __init__(self):
        self.sinais = []
        self.lock   = threading.Lock()
        self.stats  = {"total": 0, "elite": 0, "quente": 0, "morno": 0, "vigiar": 0,
                       "ultimo_update": None}
        self._carregar()

    def _carregar(self):
        arq = "sinais_gols.json"
        if os.path.exists(arq):
            try:
                with open(arq, "r", encoding="utf-8") as f:
                    d = json.load(f)
                    self.sinais = d.get("sinais", [])[-self.MAX:]
                    self.stats  = d.get("stats", self.stats)
            except Exception:
                pass

    def _salvar(self):
        try:
            with open("sinais_gols.json", "w", encoding="utf-8") as f:
                json.dump({"sinais": self.sinais, "stats": self.stats},
                          f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def adicionar(self, sinal: dict):
        """Recebe um sinal do bot de gols e normaliza para o site."""
        with self.lock:
            agora = datetime.now(timezone.utc) - timedelta(hours=3)
            sid   = f"gol_{sinal.get('fid', '')}_{agora.strftime('%Y%m%d_%H%M')}"

            if any(s.get("id") == sid for s in self.sinais):
                return

            registro = {
                "id":         sid,
                "timestamp":  agora.isoformat(),
                "horario":    agora.strftime("%H:%M"),
                "data":       agora.strftime("%d/%m/%Y"),
                "novo":       True,
                # Jogo
                "fid":        sinal.get("fid"),
                "casa":       sinal.get("casa", ""),
                "fora":       sinal.get("fora", ""),
                "liga":       sinal.get("liga", ""),
                "pais":       sinal.get("pais", ""),
                "dt":         sinal.get("dt_iso", ""),
                # Modelo
                "lh":         sinal.get("lh", 0),
                "la":         sinal.get("la", 0),
                "lt":         sinal.get("lt", 0),
                "probs":      sinal.get("probs", {}),
                "mkt":        sinal.get("mkt", {}),
                "h2h":        sinal.get("h2h"),
                # Times
                "hgf":        sinal.get("hgf", 0),
                "hga":        sinal.get("hga", 0),
                "agf":        sinal.get("agf", 0),
                "aga":        sinal.get("aga", 0),
                "hforma":     sinal.get("hforma", ""),
                "aforma":     sinal.get("aforma", ""),
                "hcs":        sinal.get("hcs", 0),
                "acs":        sinal.get("acs", 0),
                # Score
                "pontuacao":  sinal.get("pontuacao", 0),
                "tier":       sinal.get("tier", "VIGIAR"),
                "stake":      sinal.get("stake", 0),
                "classico":   sinal.get("classico", False),
                "decisivo":   sinal.get("decisivo", False),
            }

            self.sinais.insert(0, registro)
            if len(self.sinais) > self.MAX:
                self.sinais = self.sinais[:self.MAX]

            for s in self.sinais[1:]:
                s["novo"] = False

            self.stats["total"] += 1
            self.stats["ultimo_update"] = agora.isoformat()
            tier = registro["tier"].lower()
            if tier in self.stats:
                self.stats[tier] += 1

            self._salvar()

    def get_dados(self):
        with self.lock:
            return json.dumps({
                "sinais": self.sinais[:80],
                "stats":  self.stats,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }, ensure_ascii=False)


# Instância global do gerenciador de gols
_gols = _GerenciadorGols()

# ── Inicia o servidor HTTP imediatamente ao carregar o módulo ──
# O Railway detecta a porta assim que o processo começa — não pode esperar até o main()
_servidor_web_ativo = _iniciar_servidor_web()

# ═══════════════════ LIGAS ═══════════════════
LIGAS_T1 = {
    39: "Premier League", 140: "La Liga", 135: "Serie A",
    78: "Bundesliga", 61: "Ligue 1", 2: "Champions League",
    3: "Europa League", 71: "Brasileirão A", 13: "Libertadores",
}
LIGAS_T2 = {
    72: "Brasileirão B", 73: "Copa do Brasil", 848: "Conference League",
    94: "Liga Portugal", 88: "Eredivisie", 203: "Süper Lig",
    144: "Jupiler Pro", 253: "MLS", 11: "Sul-Americana",
    307: "Saudi Pro League", 40: "Championship", 79: "2. Bundesliga",
    141: "La Liga 2", 136: "Serie B Itália", 62: "Ligue 2",
    235: "Liga MX", 128: "Liga Argentina", 218: "J-League",
    292: "K-League",
}
LIGAS_T3 = {
    1: "Copa do Mundo", 4: "Euro", 5: "CAN", 9: "Copa América",
    30: "Eliminatórias", 113: "Allsvenskan", 103: "Eliteserien",
    119: "Superligaen", 345: "Liga Tcheca", 106: "Ekstraklasa",
    179: "Scottish Prem", 169: "Super League Grécia",
}
TODAS_LIGAS = {**LIGAS_T1, **LIGAS_T2, **LIGAS_T3}

# Média de escanteios por jogo por liga (calibragem)
MEDIA_ESC = {
    39: 10.7, 78: 10.2, 135: 10.0, 140: 9.8, 61: 9.5,
    71: 9.6, 2: 10.5, 3: 10.0, 13: 9.8, 94: 9.4, 88: 10.3,
    203: 9.2, 72: 9.3, 73: 9.5, 40: 10.4, 253: 9.1,
    235: 9.0, 128: 9.4, 218: 9.7, 144: 9.6, 307: 8.8,
}
MEDIA_PADRAO = 9.5

# Tradução de países
PAISES_BR = {
    "World": "Mundial", "Brazil": "Brasil", "England": "Inglaterra",
    "Spain": "Espanha", "Italy": "Itália", "Germany": "Alemanha",
    "France": "França", "Portugal": "Portugal", "Netherlands": "Holanda",
    "Belgium": "Bélgica", "Turkey": "Turquia", "Argentina": "Argentina",
    "USA": "EUA", "Mexico": "México", "Japan": "Japão",
    "South-Korea": "Coreia do Sul", "Saudi-Arabia": "Arábia Saudita",
    "Scotland": "Escócia", "Greece": "Grécia", "Poland": "Polônia",
    "Czech-Republic": "Tchéquia", "Denmark": "Dinamarca",
    "Sweden": "Suécia", "Norway": "Noruega", "Colombia": "Colômbia",
    "Chile": "Chile", "Paraguay": "Paraguai", "Uruguay": "Uruguai",
    "Peru": "Peru", "Ecuador": "Equador", "Bolivia": "Bolívia",
    "Venezuela": "Venezuela", "China": "China", "Australia": "Austrália",
    "Russia": "Rússia", "Ukraine": "Ucrânia", "Austria": "Áustria",
    "Switzerland": "Suíça", "Croatia": "Croácia", "Serbia": "Sérvia",
    "Romania": "Romênia", "Ireland": "Irlanda", "South-Africa": "África do Sul",
    "Egypt": "Egito", "Morocco": "Marrocos", "Canada": "Canadá",
    "Costa-Rica": "Costa Rica", "Honduras": "Honduras", "India": "Índia",
}

# ═══════════════════ ESTADO DO BOT ═══════════════════

class Estado:
    def __init__(self):
        self.pausado = False
        self.rodando = True
        self.sinais = {}             # {chave: {ts, tipo}}
        self.total_sinais = 0
        self.ciclos = 0
        self.inicio = datetime.now()
        self.jogos_vivos = 0
        self.jogos_pre = 0
        self.lock = threading.Lock()
        self.snapshots = defaultdict(list)
        self.por_tipo = defaultdict(int)
        self.cache_stats = {}        # cache de stats de times
        self.cache_h2h = {}
        self.sinais_jogo_ciclo = defaultdict(int)  # fixture_id -> count no ciclo
        # Timestamps para ciclos separados
        self.ts_live = 0
        self.ts_pre = 0
        self.ts_agenda = 0
        self.agenda_enviada_hoje = None
        self.jogos_agenda = 0

    def registrar(self, chave, tipo):
        with self.lock:
            self.sinais[chave] = {"ts": datetime.now(), "tipo": tipo}
            self.total_sinais += 1
            self.por_tipo[tipo] += 1

    def ja_enviado(self, chave):
        with self.lock:
            return chave in self.sinais

    def pode_enviar_jogo(self, fixture_id):
        """Anti-spam: máx N sinais por jogo por ciclo."""
        with self.lock:
            return self.sinais_jogo_ciclo[fixture_id] < MAX_SINAIS_JOGO_CICLO

    def contar_jogo(self, fixture_id):
        with self.lock:
            self.sinais_jogo_ciclo[fixture_id] += 1

    def reset_ciclo(self):
        with self.lock:
            self.sinais_jogo_ciclo.clear()

    def salvar_snap(self, fid, minuto, ec, ef):
        with self.lock:
            snaps = self.snapshots[fid]
            if snaps and snaps[-1]["m"] == minuto:
                return
            snaps.append({"m": minuto, "c": ec, "f": ef, "t": ec + ef, "ts": time.time()})
            if len(snaps) > 30:
                self.snapshots[fid] = snaps[-30:]

    def momentum(self, fid, janela=12):
        with self.lock:
            snaps = self.snapshots.get(fid, [])
            if len(snaps) < 2:
                return 0, 0, 0
            atual = snaps[-1]
            antigo = None
            for s in snaps:
                if atual["m"] - s["m"] >= janela:
                    antigo = s
                elif antigo:
                    break
            if not antigo:
                antigo = snaps[0]
                if antigo == atual:
                    return 0, 0, 0
            return (max(0, atual["t"] - antigo["t"]),
                    max(0, atual["c"] - antigo["c"]),
                    max(0, atual["f"] - antigo["f"]))

    def limpar(self, horas=5):
        with self.lock:
            agora = datetime.now()
            self.sinais = {
                k: v for k, v in self.sinais.items()
                if (agora - v["ts"]).total_seconds() < horas * 3600
            }
            ts_now = time.time()
            self.snapshots = defaultdict(list, {
                k: v for k, v in self.snapshots.items()
                if v and (ts_now - v[-1]["ts"]) < horas * 3600
            })

    def relatorio(self):
        t = datetime.now() - self.inicio
        h, m = int(t.total_seconds() // 3600), int((t.total_seconds() % 3600) // 60)
        tipos = "\n".join(f"    {t}: {c}" for t, c in
                          sorted(self.por_tipo.items(), key=lambda x: -x[1])
                          ) or "    Nenhum"
        return (
            f"\n{'═'*50}\n"
            f"  📊 RELATÓRIO v4.0\n{'═'*50}\n"
            f"  ⏱  Ativo: {h}h {m}min\n"
            f"  📡 Ciclos: {self.ciclos}\n"
            f"  🔔 Sinais: {self.total_sinais}\n"
            f"  ⚽ Ao vivo: {self.jogos_vivos} │ Pré-live: {self.jogos_pre}\n"
            f"  ⏸  {'PAUSADO' if self.pausado else 'ATIVO'}\n"
            f"{'─'*50}\n  📈 Por tipo:\n{tipos}\n{'═'*50}\n"
        )

est = Estado()

# ═══════════════════ UTILITÁRIOS ═══════════════════

def hora_br():
    return (datetime.now(timezone.utc) - timedelta(hours=3)).strftime("%H:%M:%S")

def hora_br_curta():
    return (datetime.now(timezone.utc) - timedelta(hours=3)).strftime("%H:%M")

def log(msg, tp="INFO"):
    cores = {"INFO":"\033[36m","SINAL":"\033[32m","ERRO":"\033[31m",
             "AVISO":"\033[33m","OK":"\033[92m","PRE":"\033[95m"}
    print(f"{cores.get(tp,'')}\033[0m[{hora_br()}] [{tp}] {msg}\033[0m")

def si(v, d=0):
    try: return int(v) if v is not None else d
    except: return d

def sf(v, d=0.0):
    if v is None: return d
    if isinstance(v, str): v = v.replace("%","").strip()
    try: return float(v)
    except: return d

def traduzir_pais(p):
    return PAISES_BR.get(p, p) if p else ""

# ═══════════════════ API ═══════════════════

def api(endpoint, params=None):
    headers = {"x-apisports-key": API_FOOTBALL_KEY}
    url = f"{API_BASE}/{endpoint}"
    for t in range(3):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=30)
            if r.status_code == 200:
                d = r.json()
                errs = d.get("errors")
                if errs and ((isinstance(errs, dict) and errs) or (isinstance(errs, list) and errs)):
                    if isinstance(errs, dict):
                        for v in errs.values():
                            if "rate" in str(v).lower() or "limit" in str(v).lower():
                                log("Rate limit. Pausa 60s.", "AVISO")
                                time.sleep(60)
                                continue
                    log(f"Erro API: {errs}", "ERRO")
                    return None
                rem = r.headers.get("x-ratelimit-requests-remaining")
                if rem and int(rem) < 5:
                    time.sleep(10)
                return d.get("response", [])
            elif r.status_code == 429:
                time.sleep(60 * (t + 1))
            else:
                log(f"HTTP {r.status_code}: {endpoint}", "ERRO")
                return None
        except requests.exceptions.Timeout:
            time.sleep(5)
        except requests.exceptions.ConnectionError:
            time.sleep(10)
        except Exception as e:
            log(f"Erro: {e}", "ERRO")
            return None
    return None

def telegram(msg):
    try:
        r = requests.post(f"{TELEGRAM_API}/sendMessage",
                          json={"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                                "parse_mode": "HTML", "disable_web_page_preview": True},
                          timeout=15)
        if r.status_code == 200:
            return True
        # Log detalhado do erro para ajudar no diagnóstico
        erro = r.json().get('description', r.text[:100])
        log(f"Telegram ERRO — chat_id usado: '{TELEGRAM_CHAT_ID}' — resposta: {erro}", "ERRO")
        return False
    except Exception as e:
        log(f"Telegram exceção: {e}", "ERRO")
        return False


# ═══════════════════ GEMINI — CO-PILOTO DE IA ═══════════════════

_GEMINI_PROMPT = (
    "Você é um analista profissional de trading esportivo especializado em escanteios. "
    "Recebe dados de um sinal detectado e retorna uma análise CURTA (máx 2 frases) em PT-BR. "
    "Regras: seja direto como trader falando com trader. Use termos: entrada, odd, valor, linha, over, under. "
    "Diga o que o trader deve OBSERVAR ou FAZER. Se há risco, avise. "
    "NUNCA use emoji. NUNCA repita dados que já estão no sinal. Foque em INSIGHT que os números não dizem."
)

def gemini_analisar(sinal):
    """Envia dados do sinal para Gemini e retorna análise curta."""
    if not USAR_GEMINI:
        return None

    partes = [
        "Tipo: " + sinal.get("tipo", ""),
        sinal.get("nome_casa", "") + " x " + sinal.get("nome_fora", ""),
        "Liga: " + sinal.get("nome_liga", ""),
        "Min: " + str(sinal.get("minuto", 0)),
        "Placar: " + str(sinal.get("gols_casa", 0)) + "x" + str(sinal.get("gols_fora", 0)),
        "Esc: " + str(sinal.get("esc_casa", 0)) + "x" + str(sinal.get("esc_fora", 0)),
        "Confiança: " + str(sinal.get("confianca", 0)) + "%",
    ]
    for k in ["projecao", "linha", "ritmo", "fator_placar", "media_liga",
              "esc_rec", "time_pressao", "time_dom", "time_p", "diff_g",
              "h2h_media", "avg_casa", "avg_fora", "min_inicio",
              "posse_casa", "posse_fora", "total_chutes", "score"]:
        v = sinal.get(k)
        if v is not None and v != 0 and v != "":
            partes.append(f"{k}: {v}")
    if sinal.get("detalhes") and isinstance(sinal["detalhes"], list):
        partes.append("Extra: " + "; ".join(str(d) for d in sinal["detalhes"]))

    dados = "\n".join(partes)

    try:
        r = requests.post(GEMINI_URL, json={
            "contents": [{"parts": [{"text": _GEMINI_PROMPT + "\n\nDADOS:\n" + dados + "\n\nAnálise (2 frases):"}]}],
            "generationConfig": {"temperature": 0.7, "maxOutputTokens": 150, "topP": 0.9},
        }, timeout=20)

        if r.status_code == 200:
            cands = r.json().get("candidates", [])
            if cands:
                txt = cands[0].get("content", {}).get("parts", [{}])[0].get("text", "").strip()
                txt = txt.replace('"', '').replace('<', '').replace('>', '').strip()
                if len(txt) > 280:
                    txt = txt[:277] + "..."
                if txt:
                    log(f"    🧠 Gemini OK", "OK")
                    return txt
        else:
            log(f"    Gemini: HTTP {r.status_code}", "AVISO")
    except Exception as e:
        log(f"    Gemini: {e}", "AVISO")

    return None

def xstat(obj, nome):
    if not obj: return 0
    for s in obj.get("statistics", []):
        if s.get("type","").lower() == nome.lower():
            return sf(s.get("value"), 0)
    return 0

# ═══════════════════ BUSCA DE DADOS ═══════════════════

def buscar_ao_vivo():
    jogos = api("fixtures", {"live": "all"})
    if not jogos: return []
    resultado = []
    for j in jogos:
        lid = j.get("league",{}).get("id")
        st = j.get("fixture",{}).get("status",{}).get("short","")
        if st in ("HT","FT","AET","PEN","NS","PST","CANC","ABD","AWD","WO"):
            continue
        if lid in TODAS_LIGAS:
            resultado.append(j)
    if not resultado and jogos:
        log(f"Sem ligas mapeadas. Usando todos {len(jogos)} jogos.", "AVISO")
        return [j for j in jogos if j.get("fixture",{}).get("status",{}).get("short","")
                not in ("HT","FT","AET","PEN","NS","PST","CANC","ABD","AWD","WO")]
    return resultado

def buscar_pre_live(janela_min=180):
    """Busca jogos que começam nas próximas janela_min minutos."""
    agora_utc = datetime.now(timezone.utc)
    from_ts = agora_utc.strftime("%Y-%m-%d")

    jogos = api("fixtures", {"date": from_ts, "status": "NS"})
    if not jogos:
        return []

    resultado = []
    for j in jogos:
        lid = j.get("league",{}).get("id")
        if lid not in TODAS_LIGAS:
            continue
        ts_str = j.get("fixture",{}).get("date","")
        if not ts_str:
            continue
        try:
            ts_str_clean = ts_str.replace("Z", "+00:00")
            jogo_dt = datetime.fromisoformat(ts_str_clean)
            if jogo_dt.tzinfo is None:
                jogo_dt = jogo_dt.replace(tzinfo=timezone.utc)
            diff_min = (jogo_dt - agora_utc).total_seconds() / 60
            if 0 < diff_min <= janela_min:
                j["_minutos_para_inicio"] = int(diff_min)
                # Classificar faixa
                if diff_min <= PRE_LIVE_IMINENTE:
                    j["_faixa"] = "IMINENTE"
                elif diff_min <= PRE_LIVE_PROXIMO:
                    j["_faixa"] = "PROXIMO"
                else:
                    j["_faixa"] = "HOJE"
                resultado.append(j)
        except Exception:
            continue

    # Ordena: iminentes primeiro
    resultado.sort(key=lambda x: x.get("_minutos_para_inicio", 999))
    return resultado


def buscar_agenda_dia():
    """Busca TODOS os jogos do dia para montar agenda com projeções."""
    agora_utc = datetime.now(timezone.utc)
    hoje = agora_utc.strftime("%Y-%m-%d")

    jogos = api("fixtures", {"date": hoje, "status": "NS"})
    if not jogos:
        return []

    resultado = []
    for j in jogos:
        lid = j.get("league",{}).get("id")
        if lid not in TODAS_LIGAS:
            continue
        ts_str = j.get("fixture",{}).get("date","")
        if not ts_str:
            continue
        try:
            ts_str_clean = ts_str.replace("Z", "+00:00")
            jogo_dt = datetime.fromisoformat(ts_str_clean)
            if jogo_dt.tzinfo is None:
                jogo_dt = jogo_dt.replace(tzinfo=timezone.utc)
            # Só jogos que ainda não começaram
            if jogo_dt > agora_utc:
                brt = jogo_dt - timedelta(hours=3)
                j["_horario_brt"] = brt.strftime("%H:%M")
                j["_minutos_para_inicio"] = int((jogo_dt - agora_utc).total_seconds() / 60)
                resultado.append(j)
        except Exception:
            continue

    resultado.sort(key=lambda x: x.get("_minutos_para_inicio", 999))
    return resultado

def buscar_stats(fixture_id):
    s = api("fixtures/statistics", {"fixture": fixture_id})
    if not s or len(s) < 2: return None, None
    return s[0], s[1]

def buscar_stats_time(team_id, liga_id, season=None):
    """Busca estatísticas do time na temporada para análise pré-live."""
    cache_key = f"{team_id}_{liga_id}"
    if cache_key in est.cache_stats:
        c = est.cache_stats[cache_key]
        if time.time() - c["ts"] < 3600:  # cache 1h
            return c["data"]

    if season is None:
        season = datetime.now().year
        if datetime.now().month < 7:
            season -= 1

    data = api("teams/statistics", {"team": team_id, "league": liga_id, "season": season})
    if data:
        est.cache_stats[cache_key] = {"data": data, "ts": time.time()}
    return data

def buscar_h2h(id_casa, id_fora, n=6):
    ck = f"{id_casa}_{id_fora}"
    if ck in est.cache_h2h:
        c = est.cache_h2h[ck]
        if time.time() - c["ts"] < 7200:
            return c["data"]
    data = api("fixtures/headtohead", {"h2h": f"{id_casa}-{id_fora}", "last": n})
    if data:
        est.cache_h2h[ck] = {"data": data, "ts": time.time()}
    return data

def buscar_ultimos_jogos(team_id, n=5):
    """Busca últimos N jogos finalizados do time."""
    cache_key = f"last_{team_id}"
    if cache_key in est.cache_stats:
        c = est.cache_stats[cache_key]
        if time.time() - c["ts"] < 3600:
            return c["data"]

    data = api("fixtures", {"team": team_id, "last": n, "status": "FT"})
    if data:
        est.cache_stats[cache_key] = {"data": data, "ts": time.time()}
    return data


# ═══════════════════════════════════════════════════════
#  MÓDULO PRÉ-LIVE — ANÁLISE ANTES DO JOGO COMEÇAR
# ═══════════════════════════════════════════════════════

def calcular_media_corners_time(team_id, n=5):
    """Calcula média de escanteios a favor e contra nos últimos jogos."""
    jogos = buscar_ultimos_jogos(team_id, n)
    if not jogos:
        return None, None

    total_favor = 0
    total_contra = 0
    count = 0

    for j in jogos:
        fid = j.get("fixture",{}).get("id")
        if not fid:
            continue
        sc, sf_stats = buscar_stats(fid)
        if not sc or not sf_stats:
            continue

        is_home = j.get("teams",{}).get("home",{}).get("id") == team_id
        if is_home:
            total_favor += xstat(sc, "Corner Kicks")
            total_contra += xstat(sf_stats, "Corner Kicks")
        else:
            total_favor += xstat(sf_stats, "Corner Kicks")
            total_contra += xstat(sc, "Corner Kicks")
        count += 1

        time.sleep(0.3)  # rate limit

    if count == 0:
        return None, None

    return round(total_favor / count, 1), round(total_contra / count, 1)


def analisar_h2h_corners(id_casa, id_fora):
    """Analisa média de escanteios totais nos confrontos diretos."""
    h2h = buscar_h2h(id_casa, id_fora, 6)
    if not h2h:
        return None, 0

    total_esc = 0
    count = 0

    for j in h2h:
        fid = j.get("fixture",{}).get("id")
        if not fid:
            continue
        sc, sf_stats = buscar_stats(fid)
        if sc and sf_stats:
            e1 = xstat(sc, "Corner Kicks")
            e2 = xstat(sf_stats, "Corner Kicks")
            total_esc += e1 + e2
            count += 1
        time.sleep(0.3)

    if count == 0:
        return None, 0
    return round(total_esc / count, 1), count


def gerar_sinais_pre_live(jogo):
    """
    Gera sinais pré-live baseado em:
    1. Média de escanteios dos dois times (últimos 5 jogos)
    2. H2H de escanteios
    3. Perfil da liga (média de corners)
    4. Combinação dos 3 fatores
    """
    sinais = []

    fix = jogo.get("fixture", {})
    fixture_id = fix.get("id", 0)
    teams = jogo.get("teams", {})
    casa = teams.get("home", {})
    fora_t = teams.get("away", {})
    nome_casa = casa.get("name", "Casa")
    nome_fora = fora_t.get("name", "Fora")
    id_casa = casa.get("id", 0)
    id_fora = fora_t.get("id", 0)

    liga = jogo.get("league", {})
    liga_id = liga.get("id", 0)
    nome_liga = liga.get("name", "Liga")
    pais = liga.get("country", "")
    season = liga.get("season", datetime.now().year)
    tier = "T1" if liga_id in LIGAS_T1 else ("T2" if liga_id in LIGAS_T2 else "T3")
    media_liga = MEDIA_ESC.get(liga_id, MEDIA_PADRAO)
    min_inicio = jogo.get("_minutos_para_inicio", 0)

    base = {
        "fixture_id": fixture_id, "nome_casa": nome_casa, "nome_fora": nome_fora,
        "gols_casa": 0, "gols_fora": 0, "minuto": 0,
        "nome_liga": nome_liga, "pais": pais, "tier": tier,
        "esc_casa": 0, "esc_fora": 0, "total_esc": 0,
        "posse_casa": 0, "posse_fora": 0,
        "total_chutes": 0, "total_gol": 0,
        "media_liga": media_liga, "min_inicio": min_inicio,
    }

    # ── BUSCAR DADOS DOS TIMES ──
    log(f"    📊 Coletando dados pré-live: {nome_casa} x {nome_fora}...", "PRE")

    # Médias de corners dos times (usa cache para economizar API)
    media_casa_favor, media_casa_contra = None, None
    media_fora_favor, media_fora_contra = None, None

    # Tenta buscar dos últimos jogos (econômico em API calls)
    ultimos_casa = buscar_ultimos_jogos(id_casa, 5)
    ultimos_fora = buscar_ultimos_jogos(id_fora, 5)

    corners_casa_total = []
    corners_fora_total = []

    # Analisa últimos jogos do time da casa
    if ultimos_casa:
        for uj in ultimos_casa[:3]:  # limita a 3 para economia de API
            uf_id = uj.get("fixture",{}).get("id")
            if not uf_id:
                continue
            s1, s2 = buscar_stats(uf_id)
            if s1 and s2:
                c1 = xstat(s1, "Corner Kicks")
                c2 = xstat(s2, "Corner Kicks")
                corners_casa_total.append(c1 + c2)
            time.sleep(0.3)

    # Analisa últimos jogos do time de fora
    if ultimos_fora:
        for uj in ultimos_fora[:3]:
            uf_id = uj.get("fixture",{}).get("id")
            if not uf_id:
                continue
            s1, s2 = buscar_stats(uf_id)
            if s1 and s2:
                c1 = xstat(s1, "Corner Kicks")
                c2 = xstat(s2, "Corner Kicks")
                corners_fora_total.append(c1 + c2)
            time.sleep(0.3)

    # H2H
    h2h_media, h2h_count = None, 0
    h2h_data = buscar_h2h(id_casa, id_fora, 5)
    h2h_corners = []
    if h2h_data:
        for hj in h2h_data[:3]:
            hf_id = hj.get("fixture",{}).get("id")
            if not hf_id:
                continue
            s1, s2 = buscar_stats(hf_id)
            if s1 and s2:
                c1 = xstat(s1, "Corner Kicks")
                c2 = xstat(s2, "Corner Kicks")
                h2h_corners.append(c1 + c2)
            time.sleep(0.3)

    # ── CALCULAR PROJEÇÃO PRÉ-LIVE ──
    fatores = []
    detalhes = []

    # Fator 1: Média de corners nos jogos do time da casa
    if corners_casa_total:
        avg_casa = sum(corners_casa_total) / len(corners_casa_total)
        fatores.append(avg_casa)
        detalhes.append(f"Média jogos {nome_casa}: {avg_casa:.1f}")
    else:
        avg_casa = None

    # Fator 2: Média de corners nos jogos do time de fora
    if corners_fora_total:
        avg_fora = sum(corners_fora_total) / len(corners_fora_total)
        fatores.append(avg_fora)
        detalhes.append(f"Média jogos {nome_fora}: {avg_fora:.1f}")
    else:
        avg_fora = None

    # Fator 3: H2H
    if h2h_corners:
        avg_h2h = sum(h2h_corners) / len(h2h_corners)
        fatores.append(avg_h2h)
        detalhes.append(f"H2H ({len(h2h_corners)} jogos): {avg_h2h:.1f}")
        h2h_media = avg_h2h
        h2h_count = len(h2h_corners)
    else:
        avg_h2h = None

    # Fator 4: Média da liga (sempre disponível)
    fatores.append(media_liga)

    if not fatores:
        return sinais

    # Projeção = média ponderada dos fatores disponíveis
    # Peso: dados do time (35% cada), H2H (20%), liga (10%)
    if len(fatores) >= 3:
        # Tem dados dos times + H2H ou liga
        pesos = []
        vals = []
        if avg_casa is not None:
            pesos.append(0.30)
            vals.append(avg_casa)
        if avg_fora is not None:
            pesos.append(0.30)
            vals.append(avg_fora)
        if avg_h2h is not None:
            pesos.append(0.25)
            vals.append(avg_h2h)
        pesos.append(0.15)
        vals.append(media_liga)

        # Normaliza pesos
        total_peso = sum(pesos)
        projecao = sum(v * (p / total_peso) for v, p in zip(vals, pesos))
    else:
        projecao = sum(fatores) / len(fatores)

    projecao = round(projecao, 1)

    # ── GERAR SINAIS PRÉ-LIVE ──
    linhas = [7.5, 8.5, 9.5, 10.5, 11.5, 12.5]

    # OVER PRÉ-LIVE
    for linha in linhas:
        if projecao >= linha * 1.10:  # 10% de margem
            conf = 55
            margem = (projecao - linha) / linha
            conf += min(25, int(margem * 100))
            if avg_h2h and avg_h2h >= linha:
                conf += 8
            if avg_casa and avg_fora and min(avg_casa, avg_fora) >= linha * 0.9:
                conf += 7  # ambos os times têm média alta
            if tier == "T1":
                conf += 3
            conf = max(CONFIANCA_MINIMA, min(92, conf))

            chave = f"{fixture_id}_pre_over_{linha}"
            if not est.ja_enviado(chave) and conf >= CONFIANCA_MINIMA:
                sinais.append({
                    **base, "tipo": "PRE_OVER",
                    "chave": chave, "linha": linha,
                    "projecao": projecao, "confianca": conf,
                    "detalhes": detalhes,
                    "avg_casa": avg_casa, "avg_fora": avg_fora,
                    "h2h_media": h2h_media, "h2h_count": h2h_count,
                })
            break

    # UNDER PRÉ-LIVE
    for linha in sorted(linhas, reverse=True):
        if projecao <= linha * 0.80:  # 20% abaixo
            conf = 55
            margem = (linha - projecao) / linha
            conf += min(25, int(margem * 100))
            if avg_h2h and avg_h2h <= linha * 0.85:
                conf += 8
            if avg_casa and avg_fora and max(avg_casa, avg_fora) <= linha:
                conf += 7
            conf = max(CONFIANCA_MINIMA, min(88, conf))

            chave = f"{fixture_id}_pre_under_{linha}"
            if not est.ja_enviado(chave) and conf >= CONFIANCA_MINIMA:
                sinais.append({
                    **base, "tipo": "PRE_UNDER",
                    "chave": chave, "linha": linha,
                    "projecao": projecao, "confianca": conf,
                    "detalhes": detalhes,
                    "avg_casa": avg_casa, "avg_fora": avg_fora,
                    "h2h_media": h2h_media, "h2h_count": h2h_count,
                })
            break

    # AMBOS TIMES COM MÉDIA ALTA → Sinal especial
    if avg_casa and avg_fora:
        if avg_casa >= 5.0 and avg_fora >= 5.0:
            # Ambos os times participam de jogos com muitos corners
            conf = max(CONFIANCA_MINIMA, min(85, 58 + int((avg_casa + avg_fora - 10) * 5)))
            chave = f"{fixture_id}_pre_ambos_alto"
            if not est.ja_enviado(chave) and conf >= CONFIANCA_MINIMA:
                sinais.append({
                    **base, "tipo": "PRE_AMBOS_ALTO",
                    "chave": chave, "projecao": projecao,
                    "confianca": conf, "detalhes": detalhes,
                    "avg_casa": avg_casa, "avg_fora": avg_fora,
                    "h2h_media": h2h_media, "h2h_count": h2h_count,
                })

    return sinais


# ═══════════════════════════════════════════════════════
#  MOTOR AO VIVO — SINAIS COM PRECISÃO AUMENTADA
# ═══════════════════════════════════════════════════════

def fator_placar(gc, gf, minuto):
    diff = abs(gc - gf)
    if gc == 0 and gf == 0:
        return 1.35 if minuto >= 55 else 1.25
    if diff == 0:
        return 1.15
    if diff == 1:
        return 1.30 if minuto >= 60 else 1.20
    if diff == 2:
        return 1.30 if minuto >= 55 else 1.15
    return 0.85 if minuto <= 35 else 1.10

def time_perdendo(gc, gf, nc, nf):
    if gc < gf: return nc, gf - gc
    if gf < gc: return nf, gc - gf
    return None, 0

def analisar_ao_vivo(jogo, sc, sf_s):
    sinais = []
    fix = jogo.get("fixture",{})
    fid = fix.get("id", 0)
    minuto = si(fix.get("status",{}).get("elapsed"), 0)
    if minuto <= 0: return sinais

    teams = jogo.get("teams",{})
    nc = teams.get("home",{}).get("name","Casa")
    nf = teams.get("away",{}).get("name","Fora")
    gc = si(jogo.get("goals",{}).get("home"), 0)
    gf = si(jogo.get("goals",{}).get("away"), 0)

    liga = jogo.get("league",{})
    lid = liga.get("id", 0)
    nl = liga.get("name","Liga")
    pais = liga.get("country","")
    tier = "T1" if lid in LIGAS_T1 else ("T2" if lid in LIGAS_T2 else "T3")
    ml = MEDIA_ESC.get(lid, MEDIA_PADRAO)

    # Stats
    ec = xstat(sc, "Corner Kicks"); ef = xstat(sf_s, "Corner Kicks"); te = ec + ef
    chc = xstat(sc, "Total Shots"); chf = xstat(sf_s, "Total Shots"); tch = chc + chf
    gc_s = xstat(sc, "Shots on Goal"); gf_s = xstat(sf_s, "Shots on Goal"); tsg = gc_s + gf_s
    fc = xstat(sc, "Shots off Goal"); ff = xstat(sf_s, "Shots off Goal"); tfo = fc + ff
    pc = xstat(sc, "Ball Possession"); pf = xstat(sf_s, "Ball Possession")
    ac = xstat(sc, "Dangerous Attacks"); af = xstat(sf_s, "Dangerous Attacks")
    if ac == 0 and af == 0:
        ac = xstat(sc, "Attacks"); af = xstat(sf_s, "Attacks")
    bc = xstat(sc, "Blocked Shots"); bf = xstat(sf_s, "Blocked Shots"); tbl = bc + bf

    est.salvar_snap(fid, minuto, int(ec), int(ef))
    er, erc, erf = est.momentum(fid, 12)
    fp = fator_placar(gc, gf, minuto)
    rg = te / minuto if minuto > 0 else 0

    if minuto >= 20:
        rr = er / 12 if er > 0 else rg * 0.5
        rp = rg * 0.35 + rr * 0.65
    else:
        rp = rg

    proj = rp * 90

    base = {
        "fixture_id": fid, "nome_casa": nc, "nome_fora": nf,
        "gols_casa": gc, "gols_fora": gf, "minuto": minuto,
        "nome_liga": nl, "pais": pais, "tier": tier,
        "esc_casa": int(ec), "esc_fora": int(ef), "total_esc": int(te),
        "posse_casa": pc, "posse_fora": pf,
        "total_chutes": int(tch), "total_gol": int(tsg), "media_liga": ml,
    }

    # ══ SINAL 1: OVER AO VIVO ══
    if minuto >= 8 and te >= 2:
        pa = proj * fp
        for linha in [7.5, 8.5, 9.5, 10.5, 11.5, 12.5]:
            if pa >= linha * 1.20:
                c = 50
                c += min(20, int((pa - linha) / linha * 80))
                c += 8 if er >= 2 else 0
                jn = any(a <= minuto <= b for a, b in [(20,42),(50,75)])
                c += 10 if jn else 0
                c += 5 if ml >= 10 else 0
                c += 5 if abs(gc - gf) <= 1 else 0
                c -= 12 if minuto >= 80 else 0
                c -= 5 if minuto >= 75 else 0
                c = max(CONFIANCA_MINIMA, min(95, c))
                ch = f"{fid}_ov_{linha}"
                if not est.ja_enviado(ch) and c >= CONFIANCA_MINIMA:
                    sinais.append({**base, "tipo":"OVER", "chave":ch,
                        "linha":linha, "projecao":round(pa,1), "ritmo":round(rp,3),
                        "ritmo_rec":round(er/12,3) if er>0 else 0, "confianca":c,
                        "in_janela":jn, "fator_placar":round(fp,2)})
                break

    # ══ SINAL 2: UNDER AO VIVO ══
    if minuto >= 55 and rg <= 0.09:
        for linha in [7.5, 8.5, 9.5, 10.5]:
            p90 = rg * 90
            if p90 <= linha * 0.70:
                c = 55
                c += min(25, int((1 - p90/linha) * 50))
                c += 10 if 60 <= minuto <= 80 else 0
                c += 8 if er == 0 else 0
                c -= 8 if abs(gc-gf) <= 1 and minuto < 75 else 0
                c = max(CONFIANCA_MINIMA, min(92, c))
                ch = f"{fid}_un_{linha}"
                if not est.ja_enviado(ch) and c >= CONFIANCA_MINIMA:
                    sinais.append({**base, "tipo":"UNDER", "chave":ch,
                        "linha":linha, "projecao":round(p90,1), "ritmo":round(rg,3),
                        "confianca":c, "esc_rec":er})
                break

    # ══ SINAL 3: MOMENTUM ══
    if er >= 2 and minuto >= 15:
        c = 72 + (er - 2) * 8
        c += 5 if fp >= 1.15 else 0
        if erc > erf: tm, em = nc, erc
        elif erf > erc: tm, em = nf, erf
        else: tm, em = "Ambos", er
        c = max(CONFIANCA_MINIMA, min(92, c))
        ch = f"{fid}_mom_{minuto//10}"
        if not est.ja_enviado(ch) and c >= CONFIANCA_MINIMA:
            sinais.append({**base, "tipo":"MOMENTUM", "chave":ch,
                "time_mom":tm, "esc_rec":er, "esc_mom":em,
                "confianca":c, "janela":12})

    # ══ SINAL 4: PRESSÃO OFENSIVA ══
    if minuto >= 12:
        for nm, cg, at, po, et, cf_t in [
            (nc, gc_s, ac, pc, ec, fc), (nf, gf_s, af, pf, ef, ff)]:
            sc_p = 0
            det = []
            if cg >= 4: sc_p += 25; det.append(f"Gol:{int(cg)}")
            if at >= 45: sc_p += 25; det.append(f"Atq:{int(at)}")
            if po >= 58: sc_p += 15; det.append(f"Posse:{po:.0f}%")
            if et >= 3: sc_p += 20
            if cf_t >= 3: sc_p += 15; det.append(f"Fora:{int(cf_t)}")
            if sc_p >= 55:
                c = max(CONFIANCA_MINIMA, min(88, 45 + sc_p))
                ch = f"{fid}_pr_{nm}_{minuto//12}"
                if not est.ja_enviado(ch) and c >= CONFIANCA_MINIMA:
                    sinais.append({**base, "tipo":"PRESSAO", "chave":ch,
                        "time_pressao":nm, "score":sc_p, "confianca":c,
                        "chutes_gol":int(cg), "ataques":int(at),
                        "chutes_fora":int(cf_t), "detalhes":det})

    # ══ SINAL 5: JOGO QUENTE ══
    if minuto <= 30 and te >= 5:
        rq = te / minuto; pq = rq * 90
        c = 75 + (te - 5) * 5 + (5 if ml >= 10 else 0)
        c = max(CONFIANCA_MINIMA, min(94, c))
        ch = f"{fid}_hot"
        if not est.ja_enviado(ch) and c >= CONFIANCA_MINIMA:
            sinais.append({**base, "tipo":"QUENTE", "chave":ch,
                "projecao":round(pq,1), "ritmo":round(rq,3), "confianca":c})

    # ══ SINAL 6: DOMINÂNCIA ══
    if te >= 4 and abs(ec - ef) >= 3:
        d = abs(ec - ef)
        dom = nc if ec > ef else nf
        c = max(CONFIANCA_MINIMA, min(88, 50 + int(d) * 8))
        ch = f"{fid}_dom_{minuto//18}"
        if not est.ja_enviado(ch) and c >= CONFIANCA_MINIMA:
            sinais.append({**base, "tipo":"DOMINIO", "chave":ch,
                "time_dom":dom, "esc_dom":int(max(ec,ef)),
                "esc_out":int(min(ec,ef)), "confianca":c})

    # ══ SINAL 7: SECA ══
    if minuto >= 25 and te == 0:
        ch = f"{fid}_seca"
        if not est.ja_enviado(ch):
            sinais.append({**base, "tipo":"SECA", "chave":ch,
                "confianca":70, "min_sem":minuto})

    # ══ SINAL 8: BLOQUEIOS ══
    if minuto >= 20 and tbl >= 5 and tch > 0:
        tx = tbl / tch
        if tx >= 0.25:
            c = max(CONFIANCA_MINIMA, min(82, 50 + int(tbl) * 3))
            ch = f"{fid}_bl_{minuto//15}"
            if not est.ja_enviado(ch) and c >= CONFIANCA_MINIMA:
                sinais.append({**base, "tipo":"BLOQUEIOS", "chave":ch,
                    "total_bl":int(tbl), "taxa":round(tx*100,1), "confianca":c})

    # ══ SINAL 9: HANDICAP ══
    if te >= 6 and minuto >= 30 and abs(ec - ef) >= 4:
        d = abs(ec - ef)
        forte = nc if ec > ef else nf
        fraco = nf if ec > ef else nc
        c = max(CONFIANCA_MINIMA, min(85, 55 + int(d) * 5))
        ch = f"{fid}_hc_{minuto//20}"
        if not est.ja_enviado(ch) and c >= CONFIANCA_MINIMA:
            sinais.append({**base, "tipo":"HANDICAP", "chave":ch,
                "time_f":forte, "time_fr":fraco,
                "esc_f":int(max(ec,ef)), "esc_fr":int(min(ec,ef)),
                "diff":int(d), "confianca":c})

    # ══ SINAL 10: 2º TEMPO FORTE ══
    if 50 <= minuto <= 78 and te >= 4 and rp > rg * 1.3:
        c = max(CONFIANCA_MINIMA, min(85, 55 + int(er * 8)))
        ch = f"{fid}_2t"
        if not est.ja_enviado(ch) and c >= CONFIANCA_MINIMA:
            sinais.append({**base, "tipo":"TEMPO2", "chave":ch,
                "projecao":round(rp*90,1), "ritmo_g":round(rg,3),
                "ritmo_r":round(rp,3), "confianca":c})

    # ══ SINAL 11: DESESPERO ══
    tp, dg = time_perdendo(gc, gf, nc, nf)
    if tp and dg >= 2 and minuto >= 60:
        c = max(CONFIANCA_MINIMA, min(88, 60 + dg * 5 + (minuto - 60) // 5 * 3))
        ch = f"{fid}_desp_{minuto//10}"
        if not est.ja_enviado(ch) and c >= CONFIANCA_MINIMA:
            sinais.append({**base, "tipo":"DESESPERO", "chave":ch,
                "time_p":tp, "diff_g":dg, "confianca":c})

    # ══ SINAL 12: CHUTES FORA ══
    if minuto >= 20 and tfo >= 6 and tch > 0:
        tx = tfo / tch
        if tx >= 0.35:
            c = max(CONFIANCA_MINIMA, min(80, 50 + int(tfo) * 3))
            ch = f"{fid}_fo_{minuto//15}"
            if not est.ja_enviado(ch) and c >= CONFIANCA_MINIMA:
                sinais.append({**base, "tipo":"FORA_ALVO", "chave":ch,
                    "total_fo":int(tfo), "taxa":round(tx*100,1), "confianca":c})

    # ══ SINAL 13: OVER 1º TEMPO (antes do intervalo) ══
    if 15 <= minuto <= 40 and te >= 3:
        ritmo_1t = te / minuto
        proj_45 = ritmo_1t * 45
        for linha_1t in [3.5, 4.5, 5.5]:
            if proj_45 >= linha_1t * 1.15:
                c = 55
                c += min(20, int((proj_45 - linha_1t) / linha_1t * 80))
                c += 5 if er >= 2 else 0
                c = max(CONFIANCA_MINIMA, min(88, c))
                ch = f"{fid}_ov1t_{linha_1t}"
                if not est.ja_enviado(ch) and c >= CONFIANCA_MINIMA:
                    sinais.append({**base, "tipo":"OVER_1T", "chave":ch,
                        "linha":linha_1t, "projecao":round(proj_45,1),
                        "ritmo":round(ritmo_1t,3), "confianca":c})
                break

    # ══ SINAL 14: UNDER 1º TEMPO ══
    if 30 <= minuto <= 43 and te <= 1:
        ritmo_1t = te / minuto
        proj_45 = ritmo_1t * 45
        for linha_1t in [4.5, 3.5]:
            if proj_45 <= linha_1t * 0.65:
                c = 60
                c += 10 if te == 0 else 0
                c += 5 if er == 0 else 0
                c = max(CONFIANCA_MINIMA, min(85, c))
                ch = f"{fid}_un1t_{linha_1t}"
                if not est.ja_enviado(ch) and c >= CONFIANCA_MINIMA:
                    sinais.append({**base, "tipo":"UNDER_1T", "chave":ch,
                        "linha":linha_1t, "projecao":round(proj_45,1),
                        "ritmo":round(ritmo_1t,3), "confianca":c})
                break

    return sinais


# ═══════════════════════════════════════════════════════
#  FORMATAÇÃO — LAYOUT LIMPO PT-BR
# ═══════════════════════════════════════════════════════

def barra(v):
    c = round(v / 20)
    return "▓" * c + "░" * (5 - c)

def nivel(v):
    if v >= 85: return "🔴 ALTA"
    if v >= 70: return "🟠 MÉDIA+"
    if v >= 55: return "🟡 MÉDIA"
    return "⚪ MOD"

def fmt(s, ia=None):
    t = s["tipo"]
    c = s.get("confianca", 50)
    p = traduzir_pais(s.get("pais",""))
    mi = s.get("min_inicio", 0)

    # ── Cabeçalho ──
    if t.startswith("PRE"):
        h = (
            f"⚽ <b>{s['nome_casa']}</b> vs <b>{s['nome_fora']}</b>\n"
            f"🏟 {s['nome_liga']}{' · ' + p if p else ''}\n"
            f"⏳ Começa em <b>{mi} min</b>"
        )
    else:
        h = (
            f"⚽ <b>{s['nome_casa']}</b> {s['gols_casa']}x{s['gols_fora']} <b>{s['nome_fora']}</b>\n"
            f"🏟 {s['nome_liga']}{' · ' + p if p else ''} · {s['minuto']}'\n"
            f"📐 Escanteios: <b>{s['esc_casa']}</b> x <b>{s['esc_fora']}</b> (Total: <b>{s['total_esc']}</b>)"
        )

    # ── Corpo ──
    if t == "PRE_OVER":
        det = "\n".join(f"  • {d}" for d in s.get("detalhes",[]))
        corpo = (
            f"\n\n📋 <b>PRÉ-LIVE: OVER {s['linha']} ESCANTEIOS</b>\n"
            f"┌ Projeção estatística: <b>{s['projecao']}</b> escanteios\n"
            f"│ Média liga: <b>{s['media_liga']}</b>\n"
            f"│ <b>Fontes de dados:</b>\n{det}\n"
            f"└ Confiança: <b>{c}%</b> {nivel(c)} [{barra(c)}]\n"
            f"\n💡 <i>Análise pré-jogo indica Over {s['linha']}. Entre antes do início para melhor odd.</i>"
        )
    elif t == "PRE_UNDER":
        det = "\n".join(f"  • {d}" for d in s.get("detalhes",[]))
        corpo = (
            f"\n\n📋 <b>PRÉ-LIVE: UNDER {s['linha']} ESCANTEIOS</b>\n"
            f"┌ Projeção estatística: <b>{s['projecao']}</b> escanteios\n"
            f"│ Média liga: <b>{s['media_liga']}</b>\n"
            f"│ <b>Fontes de dados:</b>\n{det}\n"
            f"└ Confiança: <b>{c}%</b> {nivel(c)} [{barra(c)}]\n"
            f"\n💡 <i>Dados indicam Under {s['linha']}. Odds pré-jogo costumam ser melhores.</i>"
        )
    elif t == "PRE_AMBOS_ALTO":
        det = "\n".join(f"  • {d}" for d in s.get("detalhes",[]))
        corpo = (
            f"\n\n🔥 <b>PRÉ-LIVE: AMBOS TIMES GERAM MUITOS CORNERS</b>\n"
            f"┌ Projeção: <b>{s['projecao']}</b> escanteios\n"
            f"│ Média liga: <b>{s['media_liga']}</b>\n"
            f"│ <b>Fontes:</b>\n{det}\n"
            f"└ Confiança: <b>{c}%</b> {nivel(c)} [{barra(c)}]\n"
            f"\n💡 <i>Ambos os times participam de jogos com muitos escanteios. Cenário ideal para Over.</i>"
        )
    elif t == "OVER":
        jn = " ✅ Janela ideal" if s.get("in_janela") else ""
        corpo = (
            f"\n\n🔔 <b>OVER {s['linha']} ESCANTEIOS</b>{jn}\n"
            f"┌ Projeção 90': <b>{s['projecao']}</b>\n"
            f"│ Ritmo: <b>{s['ritmo']}</b> · Recente(12'): <b>{s.get('ritmo_rec',0)}</b>\n"
            f"│ Média liga: <b>{s['media_liga']}</b> · Fator placar: <b>x{s.get('fator_placar',1.0)}</b>\n"
            f"└ Confiança: <b>{c}%</b> {nivel(c)} [{barra(c)}]\n"
            f"\n💡 <i>Projeção ponderada acima da linha. Entrada: Over {s['linha']}.</i>"
        )
    elif t == "UNDER":
        corpo = (
            f"\n\n🔕 <b>UNDER {s['linha']} ESCANTEIOS</b>\n"
            f"┌ Projeção 90': <b>{s['projecao']}</b>\n"
            f"│ Ritmo: <b>{s['ritmo']}</b> · Últimos 12': <b>{s.get('esc_rec',0)}</b> esc\n"
            f"└ Confiança: <b>{c}%</b> {nivel(c)} [{barra(c)}]\n"
            f"\n💡 <i>Ritmo muito baixo. Entrada: Under {s['linha']}.</i>"
        )
    elif t == "MOMENTUM":
        corpo = (
            f"\n\n🌊 <b>ONDA DE ESCANTEIOS</b>\n"
            f"┌ {s['esc_rec']} escanteios nos últimos {s['janela']}'\n"
            f"│ Momentum: <b>{s['time_mom']}</b> ({s['esc_mom']} esc)\n"
            f"└ Confiança: <b>{c}%</b> {nivel(c)} [{barra(c)}]\n"
            f"\n💡 <i>Escanteios em onda. Próximo corner provável.</i>"
        )
    elif t == "PRESSAO":
        corpo = (
            f"\n\n🔥 <b>PRESSÃO: {s['time_pressao']}</b>\n"
            f"┌ Chutes gol: <b>{s['chutes_gol']}</b> · Ataques: <b>{s['ataques']}</b>\n"
            f"│ Chutes fora: <b>{s.get('chutes_fora',0)}</b> · Score: <b>{s['score']}</b>/100\n"
            f"└ Confiança: <b>{c}%</b> {nivel(c)} [{barra(c)}]\n"
            f"\n💡 <i>{s['time_pressao']} pressiona. Próximo escanteio provável a seu favor.</i>"
        )
    elif t == "QUENTE":
        corpo = (
            f"\n\n🔥🔥 <b>JOGO QUENTE</b>\n"
            f"┌ Projeção 90': <b>{s['projecao']}</b> · Ritmo: <b>{s['ritmo']}</b> esc/min\n"
            f"└ Confiança: <b>{c}%</b> {nivel(c)} [{barra(c)}]\n"
            f"\n💡 <i>Ritmo altíssimo! Over alto é a entrada.</i>"
        )
    elif t == "DOMINIO":
        corpo = (
            f"\n\n👑 <b>DOMÍNIO: {s['time_dom']}</b>\n"
            f"┌ {s['time_dom']}: <b>{s['esc_dom']}</b> · Adversário: <b>{s['esc_out']}</b>\n"
            f"└ Confiança: <b>{c}%</b> {nivel(c)} [{barra(c)}]\n"
            f"\n💡 <i>Over individual ou Handicap de escanteios.</i>"
        )
    elif t == "SECA":
        corpo = (
            f"\n\n🏜️ <b>SECA DE ESCANTEIOS</b>\n"
            f"┌ {s['min_sem']}' sem nenhum escanteio\n"
            f"└ Confiança: <b>{c}%</b> {nivel(c)} [{barra(c)}]\n"
            f"\n💡 <i>Pode ser Under lucrativo ou explosão iminente.</i>"
        )
    elif t == "BLOQUEIOS":
        corpo = (
            f"\n\n🛡️ <b>CHUTES BLOQUEADOS</b>\n"
            f"┌ Bloqueados: <b>{s['total_bl']}</b> · Taxa: <b>{s['taxa']}%</b>\n"
            f"└ Confiança: <b>{c}%</b> {nivel(c)} [{barra(c)}]\n"
            f"\n💡 <i>Bolas bloqueadas = desvio = escanteio iminente.</i>"
        )
    elif t == "HANDICAP":
        corpo = (
            f"\n\n📊 <b>HANDICAP: {s['time_f']}</b>\n"
            f"┌ {s['time_f']}: <b>{s['esc_f']}</b> · {s['time_fr']}: <b>{s['esc_fr']}</b>\n"
            f"│ Diferença: <b>{s['diff']}</b>\n"
            f"└ Confiança: <b>{c}%</b> {nivel(c)} [{barra(c)}]\n"
            f"\n💡 <i>Handicap {s['time_f']} -1.5 ou -2.5.</i>"
        )
    elif t == "TEMPO2":
        corpo = (
            f"\n\n⚡ <b>2º TEMPO ACELERANDO</b>\n"
            f"┌ Projeção: <b>{s['projecao']}</b> · Geral: <b>{s['ritmo_g']}</b> → Recente: <b>{s['ritmo_r']}</b>\n"
            f"└ Confiança: <b>{c}%</b> {nivel(c)} [{barra(c)}]\n"
            f"\n💡 <i>2T mais rápido. Over ao vivo.</i>"
        )
    elif t == "DESESPERO":
        corpo = (
            f"\n\n😤 <b>TIME DESESPERADO</b>\n"
            f"┌ {s['time_p']} perdendo por <b>{s['diff_g']}</b> gol(s) · Min {s['minuto']}'\n"
            f"└ Confiança: <b>{c}%</b> {nivel(c)} [{barra(c)}]\n"
            f"\n💡 <i>Pressão nos minutos finais gera escanteios. Over parcial.</i>"
        )
    elif t == "FORA_ALVO":
        corpo = (
            f"\n\n🎯 <b>CHUTES FORA DO ALVO</b>\n"
            f"┌ Total fora: <b>{s['total_fo']}</b> · Taxa: <b>{s['taxa']}%</b>\n"
            f"└ Confiança: <b>{c}%</b> {nivel(c)} [{barra(c)}]\n"
            f"\n💡 <i>Chutes errando = bola desviada = escanteio.</i>"
        )
    elif t == "OVER_1T":
        corpo = (
            f"\n\n🔔 <b>OVER {s['linha']} ESCANTEIOS — 1º TEMPO</b>\n"
            f"┌ Projeção 45': <b>{s['projecao']}</b>\n"
            f"│ Ritmo: <b>{s['ritmo']}</b> esc/min\n"
            f"└ Confiança: <b>{c}%</b> {nivel(c)} [{barra(c)}]\n"
            f"\n💡 <i>Ritmo alto no 1T. Over {s['linha']} escanteios no 1º tempo.</i>"
        )
    elif t == "UNDER_1T":
        corpo = (
            f"\n\n🔕 <b>UNDER {s['linha']} ESCANTEIOS — 1º TEMPO</b>\n"
            f"┌ Projeção 45': <b>{s['projecao']}</b>\n"
            f"│ Ritmo: <b>{s['ritmo']}</b> esc/min\n"
            f"└ Confiança: <b>{c}%</b> {nivel(c)} [{barra(c)}]\n"
            f"\n💡 <i>Jogo parado no 1T. Under {s['linha']} antes do intervalo.</i>"
        )
    else:
        corpo = f"\n\n📢 <b>{t}</b>\n"

    tag = "📋 PRÉ-LIVE" if t.startswith("PRE") else "🔴 AO VIVO"

    # Se tem análise do Gemini, adiciona como insight inteligente
    if ia:
        insight = f"\n\n🧠 <i>{ia}</i>"
    else:
        insight = ""

    rodape = f"{insight}\n\n{tag} · 🕐 {hora_br_curta()} (Brasília)\n⚠️ <i>Máx 3% da banca por entrada.</i>"
    return h + corpo + rodape


# ═══════════════════════════════════════════════════════
#  AGENDA DO DIA — Jogos que vão acontecer com projeção
# ═══════════════════════════════════════════════════════

def gerar_agenda():
    """Envia agenda com todos os jogos do dia e projeção de escanteios."""
    hoje_str = (datetime.now(timezone.utc) - timedelta(hours=3)).strftime("%Y-%m-%d")

    # Não envia a mesma agenda duas vezes no dia
    if est.agenda_enviada_hoje == hoje_str:
        return
    if time.time() - est.ts_agenda < CICLO_AGENDA_SEG:
        return

    est.ts_agenda = time.time()
    log("📅 Montando agenda do dia...", "PRE")

    jogos = buscar_agenda_dia()
    if not jogos:
        log("Nenhum jogo agendado para hoje.", "PRE")
        return

    est.jogos_agenda = len(jogos)

    # Agrupa por liga
    por_liga = defaultdict(list)
    for j in jogos:
        nl = j.get("league",{}).get("name","Liga")
        por_liga[nl].append(j)

    # Monta mensagem (limita a 30 jogos para não estourar Telegram)
    linhas = [f"📅 <b>AGENDA DO DIA — {hoje_str}</b>\n"]
    linhas.append(f"⚽ <b>{len(jogos)}</b> jogos mapeados nas ligas monitoradas\n")

    count = 0
    for liga_nome in sorted(por_liga.keys()):
        js = por_liga[liga_nome]
        pais_raw = js[0].get("league",{}).get("country","")
        pais = traduzir_pais(pais_raw)
        lid = js[0].get("league",{}).get("id",0)
        ml = MEDIA_ESC.get(lid, MEDIA_PADRAO)

        linhas.append(f"\n🏟 <b>{liga_nome}</b>{' · ' + pais if pais else ''} (média: {ml})")

        for j in js:
            if count >= 30:
                break
            nc = j.get("teams",{}).get("home",{}).get("name","?")
            nf = j.get("teams",{}).get("away",{}).get("name","?")
            hr = j.get("_horario_brt", "??:??")
            mi = j.get("_minutos_para_inicio", 0)

            # Indicador de tempo
            if mi <= 60:
                tempo = "🔴 IMINENTE"
            elif mi <= 180:
                tempo = "🟡 Em breve"
            else:
                tempo = f"⏰ {hr}"

            linhas.append(f"  {tempo} — {nc} x {nf}")
            count += 1

    if count >= 30:
        linhas.append(f"\n... e mais {len(jogos) - 30} jogos")

    linhas.append(f"\n\n📋 Sinais pré-live serão enviados conforme os jogos se aproximam.")
    linhas.append(f"🕐 {hora_br_curta()} (Brasília)")

    msg = "\n".join(linhas)
    if telegram(msg):
        est.agenda_enviada_hoje = hoje_str
        log(f"📅 Agenda enviada: {len(jogos)} jogos.", "SINAL")
        # Registra no site
        jogos_web = []
        for j in jogos:
            nc  = j.get("teams",{}).get("home",{}).get("name","?")
            nf  = j.get("teams",{}).get("away",{}).get("name","?")
            hr  = j.get("_horario_brt", "??:??")
            mi  = j.get("_minutos_para_inicio", 999)
            nl  = j.get("league",{}).get("name","Liga")
            urg = "iminente" if mi <= 60 else "breve" if mi <= 180 else "normal"
            jogos_web.append({"casa": nc, "fora": nf, "horario": hr,
                              "liga": nl, "urgencia": urg})
        _web.adicionar_agenda(jogos_web)


# ═══════════════════════════════════════════════════════
#  CICLO PRÉ-LIVE — Jogos próximos com análise
# ═══════════════════════════════════════════════════════

def ciclo_pre_live():
    """Roda a cada CICLO_PRE_SEG — analisa jogos próximos."""
    if time.time() - est.ts_pre < CICLO_PRE_SEG:
        return
    est.ts_pre = time.time()
    est.reset_ciclo()

    log("📋 Buscando jogos pré-live...", "PRE")
    pre = buscar_pre_live(PRE_LIVE_PROXIMO)  # até 3h
    est.jogos_pre = len(pre)
    sinais_pre = 0

    if not pre:
        log("Nenhum jogo pré-live no momento.", "PRE")
        return

    log(f"📋 {len(pre)} jogo(s) pré-live.", "PRE")

    # Iminentes (até 60min): análise completa com H2H
    # Próximos (60-180min): análise com médias dos times
    for j in pre[:8]:
        if not est.rodando:
            break
        nc = j.get("teams",{}).get("home",{}).get("name","?")
        nf = j.get("teams",{}).get("away",{}).get("name","?")
        mi = j.get("_minutos_para_inicio", 0)
        faixa = j.get("_faixa", "HOJE")

        log(f"  📋 {nc} x {nf} (em {mi}min — {faixa})", "PRE")

        sinais = gerar_sinais_pre_live(j)
        for s in sorted(sinais, key=lambda x: -x.get("confianca",0)):
            if not est.rodando:
                break
            fid = s.get("fixture_id")
            if not est.pode_enviar_jogo(fid):
                break

            ia = gemini_analisar(s)
            msg = fmt(s, ia)
            if telegram(msg):
                est.registrar(s["chave"], s["tipo"])
                est.contar_jogo(fid)
                sinais_pre += 1
                _web.adicionar_sinal(s, ia)
                log(f"    ✅ {s['tipo']} ({s.get('confianca','?')}%)", "SINAL")
            time.sleep(1.5)
        time.sleep(0.5)

    if sinais_pre > 0:
        log(f"Pré-live: {sinais_pre} sinal(is).", "SINAL")


# ═══════════════════════════════════════════════════════
#  CICLO AO VIVO — Menos frequente, mais preciso
# ═══════════════════════════════════════════════════════

def ciclo_live():
    """Roda a cada CICLO_LIVE_SEG — analisa jogos ao vivo."""
    if time.time() - est.ts_live < CICLO_LIVE_SEG:
        return
    est.ts_live = time.time()
    est.reset_ciclo()

    log("Buscando jogos ao vivo...", "INFO")
    vivos = buscar_ao_vivo()
    est.jogos_vivos = len(vivos)
    sinais_live = 0

    if not vivos:
        log("Nenhum jogo ao vivo.", "AVISO")
        return

    log(f"📡 {len(vivos)} jogo(s) ao vivo.", "INFO")

    for j in vivos:
        if not est.rodando:
            break
        fix = j.get("fixture",{})
        fid = fix.get("id")
        nc = j.get("teams",{}).get("home",{}).get("name","?")
        nf = j.get("teams",{}).get("away",{}).get("name","?")
        m = si(fix.get("status",{}).get("elapsed"), 0)
        if m <= 0:
            continue

        log(f"  ⚽ {nc} x {nf} ({m}')", "INFO")
        sc, sf_s = buscar_stats(fid)
        if not sc or not sf_s:
            log(f"    Sem stats.", "AVISO")
            continue

        time.sleep(0.4)
        sinais = analisar_ao_vivo(j, sc, sf_s)
        log(f"    Sinais detectados: {len(sinais)}", "INFO")
        sinais.sort(key=lambda x: -x.get("confianca",0))

        for s in sinais:
            if not est.rodando:
                break
            if not est.pode_enviar_jogo(fid):
                log(f"    Limite de sinais por jogo atingido.", "AVISO")
                break

            ia = gemini_analisar(s)
            msg = fmt(s, ia)
            ok_tg = telegram(msg)
            log(f"    Telegram envio: {'✅' if ok_tg else '❌'} — {s['tipo']} ({s.get('confianca','?')}%)", "INFO")
            if ok_tg:
                est.registrar(s["chave"], s["tipo"])
                est.contar_jogo(fid)
                sinais_live += 1
                _web.adicionar_sinal(s, ia)
                log(f"    Site registrado: {len(_web.sinais)} sinais no buffer", "INFO")
            time.sleep(1.2)
        time.sleep(0.3)

    est.ciclos += 1
    est.limpar()

    log(f"Ao vivo: {sinais_live} sinal(is) | Total no site: {len(_web.sinais)}", "SINAL" if sinais_live > 0 else "INFO")


# ═══════════════════════════════════════════════════════
#  CICLO MESTRE — Orquestra os 3 ciclos
# ═══════════════════════════════════════════════════════

def ciclo_mestre():
    """Loop de 10s que verifica qual ciclo precisa rodar."""
    # 1. Agenda do dia (a cada 30min)
    try:
        gerar_agenda()
    except Exception as e:
        log(f"Erro agenda: {e}", "ERRO")

    # 2. Pré-live (a cada 5min)
    try:
        ciclo_pre_live()
    except Exception as e:
        log(f"Erro pré-live: {e}", "ERRO")

    # 3. Ao vivo (a cada 90s)
    try:
        ciclo_live()
    except Exception as e:
        log(f"Erro live: {e}", "ERRO")


# ═══════════════════════════════════════════════════════
#  CONTROLE VIA TELEGRAM — substitui o teclado no Railway
#  Envie comandos direto no chat do bot:
#  /pausar  /retomar  /status  /jogos  /ajuda
# ═══════════════════════════════════════════════════════

_ultimo_update_id = 0  # controla quais mensagens já foram processadas

def telegram_get_updates():
    """Busca novas mensagens enviadas ao bot."""
    global _ultimo_update_id
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
            params={"offset": _ultimo_update_id + 1, "timeout": 5, "limit": 10},
            timeout=10,
        )
        if r.status_code == 200:
            return r.json().get("result", [])
    except Exception:
        pass
    return []

def processar_comando_telegram(texto, chat_id):
    """Processa um comando recebido via Telegram e responde."""
    cmd = texto.strip().lower().split()[0] if texto.strip() else ""

    if cmd in ("/pausar", "/pause"):
        est.pausado = True
        telegram("⏸ <b>Bot pausado.</b>\nEnvie /retomar para continuar.")
        log("Bot pausado via Telegram.", "AVISO")

    elif cmd in ("/retomar", "/resume", "/continuar"):
        est.pausado = False
        telegram("▶️ <b>Bot retomado.</b> Monitorando jogos normalmente.")
        log("Bot retomado via Telegram.", "OK")

    elif cmd in ("/status", "/relatorio", "/r"):
        t = datetime.now() - est.inicio
        h, m = int(t.total_seconds()//3600), int((t.total_seconds()%3600)//60)
        tipos = "\n".join(
            f"  • {tp}: {ct}" for tp, ct in
            sorted(est.por_tipo.items(), key=lambda x: -x[1])[:8]
        ) or "  Nenhum ainda"
        msg = (
            f"📊 <b>Status do Bot</b>\n\n"
            f"⏱ Ativo há: <b>{h}h {m}min</b>\n"
            f"📡 Ciclos: <b>{est.ciclos}</b>\n"
            f"🔔 Sinais enviados: <b>{est.total_sinais}</b>\n"
            f"⚽ Jogos vivos: <b>{est.jogos_vivos}</b>\n"
            f"📋 Jogos pré-live: <b>{est.jogos_pre}</b>\n"
            f"⏸ Status: <b>{'PAUSADO' if est.pausado else 'ATIVO'}</b>\n\n"
            f"📈 <b>Sinais por tipo:</b>\n{tipos}\n\n"
            f"🕐 {hora_br_curta()} (Brasília)"
        )
        telegram(msg)

    elif cmd in ("/jogos", "/live", "/l"):
        jogos = buscar_ao_vivo()
        if jogos:
            linhas = [f"⚽ <b>Jogos ao vivo agora ({len(jogos)})</b>\n"]
            for j in jogos[:15]:
                nc = j.get("teams",{}).get("home",{}).get("name","?")
                nf = j.get("teams",{}).get("away",{}).get("name","?")
                m  = si(j.get("fixture",{}).get("status",{}).get("elapsed"),0)
                gc = si(j.get("goals",{}).get("home"),0)
                gf = si(j.get("goals",{}).get("away"),0)
                nl = j.get("league",{}).get("name","?")
                linhas.append(f"• {nc} {gc}x{gf} {nf} ({m}') — {nl}")
            telegram("\n".join(linhas))
        else:
            telegram("⚽ Nenhum jogo ao vivo no momento.")

    elif cmd in ("/ajuda", "/help", "/comandos"):
        telegram(
            "🤖 <b>Comandos disponíveis</b>\n\n"
            "/status — Relatório completo do bot\n"
            "/jogos — Lista jogos ao vivo agora\n"
            "/pausar — Pausa o envio de sinais\n"
            "/retomar — Retoma o envio de sinais\n"
            "/ajuda — Mostra esta mensagem"
        )

def listener_telegram():
    """Thread que escuta comandos enviados ao bot via Telegram."""
    global _ultimo_update_id
    log("Listener de comandos Telegram ativo.", "OK")
    while est.rodando:
        try:
            updates = telegram_get_updates()
            for upd in updates:
                _ultimo_update_id = upd.get("update_id", _ultimo_update_id)
                msg = upd.get("message", {})
                texto = msg.get("text", "")
                chat_id = msg.get("chat", {}).get("id")

                # Só processa comandos do chat autorizado
                if texto.startswith("/") and str(chat_id) == str(TELEGRAM_CHAT_ID):
                    log(f"Comando recebido: {texto}", "INFO")
                    processar_comando_telegram(texto, chat_id)

            time.sleep(3)  # verifica a cada 3 segundos
        except Exception as e:
            log(f"Listener Telegram: {e}", "ERRO")
            time.sleep(10)


# ═══════════════════ MAIN ═══════════════════

def main():
    print("""
╔═══════════════════════════════════════════════════════════════╗
║   🤖 BOT DE ESCANTEIOS v4.6 — RAILWAY EDITION                ║
╠═══════════════════════════════════════════════════════════════╣
║  Controle via Telegram:                                       ║
║    /status   /jogos   /pausar   /retomar   /ajuda             ║
╠═══════════════════════════════════════════════════════════════╣
║  📅 Agenda do dia (a cada 30min)                              ║
║  📋 Pré-live até 3h antes (a cada 5min)                       ║
║  🔴 Ao vivo com precisão (a cada 90s)                         ║
║  🧠 Gemini IA — análise contextualizada por sinal             ║
╚═══════════════════════════════════════════════════════════════╝
    """)

    log("Iniciando v4.6...", "OK")
    log(f"Ligas: {len(TODAS_LIGAS)} (T1={len(LIGAS_T1)} T2={len(LIGAS_T2)} T3={len(LIGAS_T3)})", "INFO")
    log(f"Gemini IA: {'ATIVO' if USAR_GEMINI else 'DESATIVADO'}", "OK" if USAR_GEMINI else "AVISO")
    log(f"Ciclos: Agenda {CICLO_AGENDA_SEG}s · Pré-live {CICLO_PRE_SEG}s · Live {CICLO_LIVE_SEG}s", "INFO")

    # ── Verifica se o token do Telegram é válido ──
    try:
        r_tg = requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getMe", timeout=10)
        if r_tg.status_code == 200 and r_tg.json().get("ok"):
            nome_bot = r_tg.json()["result"].get("username","?")
            log(f"✅ Telegram OK — bot: @{nome_bot}", "OK")
        else:
            log(f"❌ TOKEN TELEGRAM INVÁLIDO! Resposta: {r_tg.text[:120]}", "ERRO")
            log("Atualize o TELEGRAM_TOKEN nas variáveis do Railway.", "ERRO")
    except Exception as e:
        log(f"Erro ao verificar Telegram: {e}", "ERRO")

    # ── Tenta descobrir o chat_id correto via getUpdates ──
    log(f"TELEGRAM_CHAT_ID configurado: '{TELEGRAM_CHAT_ID}'", "INFO")
    try:
        r_upd = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
            params={"limit": 5}, timeout=10
        )
        if r_upd.status_code == 200:
            updates = r_upd.json().get("result", [])
            if updates:
                for upd in updates:
                    chat = upd.get("message", {}).get("chat", {})
                    cid = chat.get("id")
                    ctype = chat.get("type", "")
                    cname = chat.get("first_name") or chat.get("title") or "?"
                    if cid:
                        log(f"Chat detectado: id={cid} tipo={ctype} nome={cname}", "INFO")
            else:
                log("getUpdates vazio — envie uma mensagem para o bot no Telegram para detectar o chat_id", "AVISO")
    except Exception as e:
        log(f"getUpdates erro: {e}", "AVISO")

    if _servidor_web_ativo:
        log(f"🌐 Servidor web ativo na porta {PORTA_WEB}", "OK")
    else:
        log(f"⚠️  Servidor web não iniciou na porta {PORTA_WEB}", "AVISO")

    msg_inicio = (
        "🤖 <b>Bot de Escanteios v4.6</b>\n"
        "AGENDA + PRÉ-LIVE + AO VIVO + 🧠 IA\n\n"
        f"📡 {len(TODAS_LIGAS)} ligas monitoradas\n"
        f"🧠 Gemini: <b>{'Ativo' if USAR_GEMINI else 'Desativado'}</b>\n\n"
        "<b>Frequência:</b>\n"
        f"📅 Agenda: a cada {CICLO_AGENDA_SEG // 60}min\n"
        f"📋 Pré-live: a cada {CICLO_PRE_SEG // 60}min\n"
        f"🔴 Ao vivo: a cada {CICLO_LIVE_SEG}s\n\n"
        "<b>Comandos via Telegram:</b>\n"
        "/status · /jogos · /pausar · /retomar · /ajuda\n\n"
        f"🕐 {hora_br_curta()} (Brasília)\n"
        "⚠️ <i>Máx 3% da banca por entrada.</i>"
    )
    if telegram(msg_inicio):
        log("Telegram ✓", "OK")
    else:
        log("Telegram falhou. Continuando...", "ERRO")

    # Inicia listener de comandos Telegram (substitui o teclado no Railway)
    threading.Thread(target=listener_telegram, daemon=True).start()

    # ── Inicia o bot de gols em thread separada ──
    def _iniciar_bot_gols():
        """Roda o bot_gols_v4_3.py como thread dentro do mesmo processo."""
        try:
            import importlib.util, sys
            arq = os.path.join(os.path.dirname(__file__), "bot_gols_v4_3.py")
            if not os.path.exists(arq):
                log("bot_gols_v4_3.py não encontrado — bot de gols não iniciado.", "AVISO")
                return

            # Injeta o _gols para o bot de gols registrar sinais no site
            spec   = importlib.util.spec_from_file_location("bot_gols", arq)
            modulo = importlib.util.module_from_spec(spec)

            # Sobrescreve token do Telegram com o token específico dos gols
            token_gols = os.environ.get("TELEGRAM_TOKEN_GOLS", "")
            if token_gols:
                modulo.TELEGRAM_TOKEN   = token_gols  # type: ignore
                modulo.TELEGRAM_CHAT_ID = TELEGRAM_CHAT_ID  # mesmo chat

            spec.loader.exec_module(modulo)

            # Monkey-patch: após cada sinal enviado, registra no site
            tg_original = modulo.tg  # type: ignore

            def tg_com_web(msg):
                ok = tg_original(msg)
                return ok

            modulo.tg = tg_com_web  # type: ignore

            # Callback para registrar sinal no endpoint /gols
            modulo._web_registrar = lambda sinal: _gols.adicionar(sinal)  # type: ignore

            log("🥅 Bot de Gols v4.3 iniciando...", "OK")
            modulo.main()  # type: ignore — roda o loop principal do bot de gols
        except Exception as e:
            log(f"Bot de Gols — erro ao iniciar: {e}", "ERRO")

    threading.Thread(target=_iniciar_bot_gols, daemon=True).start()
    log("🥅 Thread do bot de gols iniciada.", "OK")

    # Loop mestre: tick a cada 10s, cada ciclo checa seu próprio timer
    while est.rodando:
        try:
            if not est.pausado:
                ciclo_mestre()
            else:
                log("Pausado. Envie /retomar no Telegram.", "AVISO")

            for _ in range(2):
                if not est.rodando:
                    break
                time.sleep(5)

        except KeyboardInterrupt:
            est.rodando = False
        except Exception as e:
            log(f"Erro geral: {e}", "ERRO")
            time.sleep(30)

    print(est.relatorio())
    log("Bot encerrado. 👋", "OK")

if __name__ == "__main__":
    main()