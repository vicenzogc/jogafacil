import httpx
import asyncio
import pandas as pd
import os
from .base import Base
import time
import random

class PlayerPipeline(Base):
    a = 0
    TABLE_NAME = "Player"
    COLS_MAP = {
        'id': 'player_id',
        'name': 'name',
        'cc': 'cc',
        'birthdate': 'birthdate',
        'position': 'position',
        'team_id': 'team_id'
    }

    def __init__(self, engine):
        self.engine = engine

    def get_all_teams(self) -> pd.DataFrame:
        return pd.read_sql("SELECT * FROM Team", self.engine)

    async def run(self, url: str) -> None:
        raw = await self.fetch(url)
        if not raw:
            print("Nenhum dado extraído.")
            return
        df = self.transform(raw)
        self.load(df)

    async def fetch_team(
        self,
        client: httpx.AsyncClient,
        player_url: str,
        team_id: int,
        semaphore: asyncio.Semaphore,
    ) -> list:
        async with semaphore:

            response = await client.get(player_url, params={
                "token": os.environ.get("ESPORTE_API_KEY"),
                "team_id": team_id,
            })

            if response.status_code != 200:
                print(f"[team {team_id}] Erro: {response.status_code}")
                return []

            page = response.json().get("results", [])

            for player in page:
                player["team_id"] = team_id

            return page

    async def fetch(self, player_url: str) -> list:
        teams = self.get_all_teams()

        # Semáforo: no máximo 10 requests simultâneas
        semaphore = asyncio.Semaphore(200)

        CHUNKS = 1

        async with httpx.AsyncClient(timeout=15) as client:
            team_ids = teams["team_id"].tolist()
            chunks = [team_ids[i::CHUNKS] for i in range(CHUNKS)]

            results_per_team = []
            for i, chunk in enumerate(chunks):
                tasks = [
                    self.fetch_team(client, player_url, team_id, semaphore)
                    for team_id in chunk
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                results_per_team.extend(results)

                if i < len(chunks) - 1:  # não espera depois do último chunk
                    print(f"⏳ Chunk {i + 1}/{CHUNKS} concluído. Aguardando 5 seegundos...")
                    await asyncio.sleep(5)

        # Achata a lista de listas e descarta erros
        all_results = []
        for team_id, result in zip(teams["team_id"], results_per_team):
            if isinstance(result, Exception):
                print(f"[team {team_id}] Falhou: {result}")
            else:
                all_results.extend(result)

        return all_results

    def transform(self, raw_data: list) -> pd.DataFrame:
        df = pd.DataFrame(raw_data)
        df = df[list(self.COLS_MAP.keys())]
        return df.rename(columns=self.COLS_MAP)

    def load(self, df: pd.DataFrame) -> None:
        try:
            id_col = df.columns[0]
            existentes_df = pd.read_sql(f"SELECT {id_col} FROM {self.TABLE_NAME}", self.engine)
            ids_no_banco = existentes_df[id_col].tolist()
        except Exception:
            ids_no_banco = []

        df_novo = df[~df[id_col].isin(ids_no_banco)]

        if not df_novo.empty:
            df_novo.to_sql(self.TABLE_NAME, self.engine, if_exists='append', index=False)
            print(f"✅ {len(df_novo)} novos registros adicionados em '{self.TABLE_NAME}'.")
        else:
            print(f"ℹ️ Nenhum dado novo para '{self.TABLE_NAME}'.")