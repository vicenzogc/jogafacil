import httpx
import asyncio
import pandas as pd
import os
from .base import Base

class TeamLeaguePipeline(Base):
    TABLE_NAME = "TeamLeague"

    def __init__(self, engine):
        self.engine = engine

    def get_all_leagues(self) -> pd.DataFrame:
        return pd.read_sql("SELECT league_id FROM League", self.engine)

    async def run(self, url: str) -> None:
        raw = await self.fetch(url)
        if not raw:
            print("Nenhum dado extraído.")
            return
        df = self.transform(raw)
        self.load(df)

    async def fetch_league(
        self,
        client: httpx.AsyncClient,
        url: str,
        semaphore: asyncio.Semaphore,
        league_id: int,
    ) -> list:
        async with semaphore:
            try:
                response = await client.get(url, params={
                    "token": os.environ.get("ESPORTE_API_KEY"),
                    "league_id": league_id,
                })
            except httpx.TimeoutException:
                print(f"[league {league_id}] Timeout")
                return []
            except httpx.RequestError as e:
                print(f"[league {league_id}] Erro de conexão: {e}")
                return []

            if response.status_code != 200:
                print(f"[league {league_id}] Erro: {response.status_code}")
                return []

            results = response.json().get("results") or []

            for item in results:
                item["league_id"] = league_id

            return results

    async def fetch(self, url: str) -> list:
        leagues = self.get_all_leagues()
        semaphore = asyncio.Semaphore(200)
        CHUNKS = 1

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(connect=10, read=30, write=10, pool=10)
        ) as client:
            league_ids = leagues["league_id"].tolist()
            chunks = [league_ids[i::CHUNKS] for i in range(CHUNKS)]

            results_per_league = []
            for i, chunk in enumerate(chunks):
                tasks = [
                    self.fetch_league(client, url, semaphore, league_id)
                    for league_id in chunk
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                results_per_league.extend(results)

                if i < len(chunks) - 1:
                    print(f"⏳ Chunk {i + 1}/{CHUNKS} concluído. Aguardando 5 segundos...")
                    await asyncio.sleep(5)

        all_results = []
        for league_id, result in zip(leagues["league_id"], results_per_league):
            if isinstance(result, Exception):
                print(f"[league {league_id}] Falhou: {result}")
            else:
                all_results.extend(result)

        return all_results

    def transform(self, raw_data: list) -> pd.DataFrame:
        df = pd.DataFrame(raw_data)
        return df[["team_id", "league_id"]]

    def load(self, df: pd.DataFrame) -> None:
        try:
            existentes_df = pd.read_sql(f"SELECT team_id, league_id FROM {self.TABLE_NAME}", self.engine)
            ids_no_banco = set(zip(existentes_df["team_id"], existentes_df["league_id"]))
        except Exception:
            ids_no_banco = set()

        df_novo = df[~df.apply(lambda r: (r["team_id"], r["league_id"]) in ids_no_banco, axis=1)]

        if not df_novo.empty:
            df_novo.to_sql(self.TABLE_NAME, self.engine, if_exists='append', index=False)
            print(f"✅ {len(df_novo)} novos registros adicionados em '{self.TABLE_NAME}'.")
        else:
            print(f"ℹ️ Nenhum dado novo para '{self.TABLE_NAME}'.")