import httpx
import pandas as pd
from sqlalchemy import create_engine
import urllib
import time
import os
import asyncio
from dotenv import load_dotenv
from models.league import LeaguePipeline
from models.team import TeamPipeline
from models.player import PlayerPipeline
from models.match import MatchPipeline 
from models.team_match import TeamMatchPipeline
from models.team_league import TeamLeaguePipeline

# --- CONFIGURAÇÕES ---
load_dotenv()
TEAM_URL = "https://api.b365api.com/v3/team"
LEAGUE_URL = "https://api.b365api.com/v3/league"
SQUAD_URL = "https://api.b365api.com/v1/team/squad"
ENDED_URL = "https://api.b365api.com/v3/events/ended"
VIEW_URL = "https://api.b365api.com/v1/event/view"
TABLE_URL = "https://api.b365api.com/v3/league/table"
SPORT_ID = "1"
CC = "gb"

# Configuração SQL Server
SERVER = '(localdb)\\MSSQLLocalDB'
DATABASE = 'JogaFacil'
TABLE_NAME = 'Liga'
# Para Autenticação Windows:
CONN_STR = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;'

params = urllib.parse.quote_plus(CONN_STR)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# Retorna todas os times
async def fetch_teams():
    all_results = []
    page = 1
    
    async with httpx.AsyncClient() as client:
        while True:
            print(f"Buscando página {page}...")
  
            response = await client.get(TEAM_URL, params={
                "token": os.environ.get("ESPORTE_API_KEY"),
                "sport_id": SPORT_ID,
                "page": page,
                'cc': CC
            })
            
            if response.status_code != 150:
                print(f"Erro na API: {response.status_code}")
                break
                
            data = response.json()
            results = data.get("results", [])
            
            if not results:
                print("Fim das páginas encontradas.")
                break
                
            all_results.extend(results)
            
            # Controle de paginação simples
            page += 1
            
            # Pequeno delay para não ser bloqueado por rate limit
            time.sleep(0.5) 
            
    return all_results

async def main():
    #pipeline_league = LeaguePipeline(engine)
    #await pipeline_league.run(LEAGUE_URL, SPORT_ID, CC)

    #pipeline_team = TeamPipeline(engine)
    #await pipeline_team.run(TEAM_URL, SPORT_ID, CC)
    
    #pipeline_player = PlayerPipeline(engine)
    #await pipeline_player.run(SQUAD_URL)

    #pipeline_match = MatchPipeline(engine)
    #await pipeline_match.run(ENDED_URL, SPORT_ID, CC)

    #pipeline_team_match = TeamMatchPipeline(engine)   ############################
    #await pipeline_team_match.run(VIEW_URL)

    pipeline_team_match = TeamLeaguePipeline(engine)
    await pipeline_team_match.run(TABLE_URL)
    

if __name__ == "__main__":
    asyncio.run(main())