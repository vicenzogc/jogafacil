import asyncio
import google.generativeai as genai
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import os
from dotenv import load_dotenv
import httpx
import pandas as pd

PER_PAGE = 50

# Configuração da sua API Key (idealmente via variáveis de ambiente)
load_dotenv()
api_key = os.environ.get("GOOGLE_API_KEY")
genai.configure(api_key=api_key)

app = FastAPI(title="Hackathon Sports Analytics API")

# ---------------------------------------------------------
# 1. A "Ferramenta" (A lógica de dados que substitui a alucinação)
# ---------------------------------------------------------
def get_match_statistics(team_a: str, team_b: str) -> dict:
    """
    Busca e processa as estatísticas vitais para o confronto direto.
    No mundo real, aqui você usaria Pandas para filtrar seu DataFrame
    dos últimos 10 jogos e retornar os dados agregados.
    """
    # Dados mockados representando o que seu Pandas retornaria
    return {
        "team_a": team_a,
        "team_b": team_b,
        "team_a_momentum": "Vem de 4 vitórias seguidas. Ataque marcando em média 2.1 gols por jogo.",
        "team_b_momentum": "Defesa instável, sofreu gols nos últimos 5 jogos. Faltas cometidas subiram 15%.",
        "key_injury": f"O principal armador do {team_b} está fora por lesão.",
        "penalty_advantage": team_a
    }  

# ---------------------------------------------------------
# 2. Configuração do Modelo e Injeção de Contexto
# ---------------------------------------------------------
# Instanciamos o modelo. O gemini-2.5-flash é perfeito para respostas rápidas de texto.
model = genai.GenerativeModel(
    model_name='gemini-2.5-flash',
    system_instruction=(
        "Você é um cientista de dados e analista esportivo sênior. "
        "Sua função é ler estatísticas brutas fornecidas pelo sistema e traduzi-las em "
        "um resumo analítico, direto e envolvente de no máximo 4 frases. "
        "Não invente dados. Baseie-se APENAS nas informações fornecidas."
    )
)

# ---------------------------------------------------------
# 3. Rota FastAPI (Onde a mágica acontece)
# ---------------------------------------------------------
class MatchRequest(BaseModel):
    team_a: str
    team_b: str

@app.post("/api/generate-summary")
async def generate_static_summary(request: MatchRequest):
    try:
        # 1. Executamos nossa função de agregação de dados
        stats_data = get_match_statistics(request.team_a, request.team_b)
        
        # 2. Montamos o prompt injetando os dados reais
        prompt = f"""
        Gere o resumo estático para a partida entre {request.team_a} e {request.team_b}.
        Aqui estão os dados calculados pelo nosso backend:
        {stats_data}
        
        Crie um insight focado no momento dos times e no ponto de atenção principal.
        """
        
        # 3. Chamamos o LLM
        response = model.generate_content(prompt)
        
        return {"summary": response.text}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/media_gols_por_time_liga")
async def media_gols_por_time_liga(league_id: int, team_name: str):
    async with httpx.AsyncClient() as client:
        response = await client.get("https://api.b365api.com/v3/events/ended", params={"token": os.environ.get("ESPORTE_API_KEY"), "league_id": league_id, "sport_id": 1})
        dados = response.json()

        df = pd.DataFrame(dados['results'])
        # calcula a media de gols por jogo de cada time
        df['total_goals'] = df['home_score'] + df['away_score']
        gols_por_jogo = df['total_goals'].mean()

    return dados

# Função interna (não é endpoint) — renomeada para evitar conflito
async def _fetch_page_internal(client: httpx.AsyncClient, league_id: int, page: int) -> dict:
    response = await client.get(
        "https://api.b365api.com/v3/events/ended",
        params={
            "token": os.environ.get("ESPORTE_API_KEY"),
            "league_id": league_id,
            "sport_id": 1,
            "page": page,
        }
    )
    response.raise_for_status()
    return response.json()

@app.get("/matches/by-team")
async def get_matches_by_team(
    league_id: str = Query(..., description="ID da liga"),
    team_name: str = Query(..., description="Nome (ou parte) do time"),
    time_status: str | None = Query(None, description="Filtro opcional de status (ex: '3' = finalizado)"),
):
    async with httpx.AsyncClient(timeout=30) as client:
        # Busca a primeira página para saber o total
        first_page = await _fetch_page_internal(client, league_id, page=1)

        pager = first_page.get("pager", {})
        total = pager.get("total", 0)
        per_page = pager.get("per_page", PER_PAGE)
        total_pages = -(-total // per_page)  # ceil division

        # Busca todas as páginas restantes em paralelo
        tasks = [_fetch_page_internal(client, league_id, p) for p in range(2, total_pages + 1)]
        remaining_pages = await asyncio.gather(*tasks, return_exceptions=True)

    # Consolida todos os resultados
    all_matches = list(first_page.get("results", []))
    for page_data in remaining_pages:
        if isinstance(page_data, Exception):
            continue  # ignora páginas que falharam
        all_matches.extend(page_data.get("results", []))

    # Filtra por time
    filtered = [m for m in all_matches if match_contains_team(m, team_name)]

    # Filtro opcional de status
    if time_status is not None:
        filtered = [m for m in filtered if m.get("time_status") == time_status]

    if not filtered:
        raise HTTPException(status_code=404, detail=f"Nenhuma partida encontrada para '{team_name}'")

    return {
        "team": team_name,
        "total": len(filtered),
        "matches": filtered,
    }

def match_contains_team(match: dict, team_name: str) -> bool:
    name = team_name.lower()
    home = match.get("home", {}).get("name", "").lower()
    away = match.get("away", {}).get("name", "").lower()
    return name in home or name in away

# Para rodar: uvicorn main:app --reload