# CornerEdge — Bot de Sinais de Escanteios

Sistema completo de sinais de escanteios com Telegram + Site ao vivo.

## Estrutura do projeto

```
corneredge/
├── bot_escanteios_v4.py   # Bot principal (roda no Railway)
├── index.html             # Site com linha do tempo (hospeda no Vercel)
├── requirements.txt       # Dependências Python
├── Procfile               # Comando de inicialização do Railway
├── runtime.txt            # Versão do Python
├── railway.toml           # Configuração do Railway
├── vercel.json            # Configuração do Vercel
├── .gitignore             # Arquivos ignorados pelo Git
├── .env.exemplo           # Modelo de credenciais (copie para .env)
└── README.md              # Este arquivo
```

## Como fazer o deploy

### Passo 1 — Railway (bot Python)

1. Acesse [railway.app](https://railway.app) e crie uma conta gratuita
2. Clique em **New Project → Deploy from GitHub repo**
3. Selecione este repositório
4. Vá em **Settings → Variables** e adicione:
   - `API_FOOTBALL_KEY` = sua chave
   - `TELEGRAM_TOKEN` = seu token
   - `TELEGRAM_CHAT_ID` = seu chat id
   - `GEMINI_API_KEY` = sua chave Gemini
5. Vá em **Settings → Networking → Add a Public URL**
6. Copie a URL gerada (ex: `https://corneredge.railway.app`)

### Passo 2 — Atualizar a URL no index.html

No arquivo `index.html`, encontre esta linha:
```
const API_URL_RAILWAY = 'COLOQUE_URL_DO_RAILWAY_AQUI';
```
Substitua pela URL do Railway:
```
const API_URL_RAILWAY = 'https://corneredge.railway.app/sinais';
```

### Passo 3 — Vercel (site estático)

1. Acesse [vercel.com](https://vercel.com) e crie uma conta gratuita
2. Clique em **Add New → Project**
3. Importe este repositório do GitHub
4. Clique em **Deploy** (sem configuração extra)
5. Seu site estará em: `https://seusite.vercel.app`

## Variáveis de ambiente

Nunca coloque as chaves diretamente no código.
Configure-as no painel do Railway em Settings → Variables.

| Variável | Onde obter |
|---|---|
| API_FOOTBALL_KEY | api-football.com |
| TELEGRAM_TOKEN | @BotFather no Telegram |
| TELEGRAM_CHAT_ID | Seu ID de usuário |
| GEMINI_API_KEY | aistudio.google.com |

## Licença

Uso pessoal.
