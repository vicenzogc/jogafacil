import httpx
import asyncio
import pandas as pd
import os
from .base import Base

class MatchPipeline(Base):
    TABLE_NAME = "Match"
    COLS_MAP = {
        'id': 'match_id',
        'time': 'time',
        'league_id': 'league_id',
    }

    def __init__(self, engine):
        self.engine = engine

    def get_all_leagues(self) -> pd.DataFrame:
        return pd.read_sql("SELECT * FROM League", self.engine)

    async def fetch_league(
        self,
        client: httpx.AsyncClient,
        url: str,
        semaphore: asyncio.Semaphore,
        sport_id: str,
        league_id: int,
        cc: str = None,
        skip_esports: bool = True
    ) -> list:
        async with semaphore:
            all_results = []
            page = 1  # ← definida antes do loop

            while True:
                try:
                    response = await client.get(url, params={
                        "token": os.environ.get("ESPORTE_API_KEY"),
                        "sport_id": sport_id,
                        "league_id": league_id,
                        "page": page, 
                        "cc": cc,
                        "skip_esports": skip_esports
                    })
                except httpx.TimeoutException:
                    print(f"[league {league_id}] Timeout na página {page} — pulando")
                    break
                except httpx.RequestError as e:
                    print(f"[league {league_id}] Erro de conexão: {e}")
                    break

                if response.status_code != 200:
                    print(f"[league {league_id}] Erro: {response.status_code}")
                    break

                results = response.json().get("results", [])
                if not results:
                    break  # ← página vazia = acabou

                for match in results:
                    match["league_id"] = league_id

                all_results.extend(results)
                page += 1
                await asyncio.sleep(0.1)
 
            return all_results

    async def fetch(self, url: str, sport_id: str, cc: str = None, skip_esports: bool = True) -> list:
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
                    self.fetch_league(client, url, semaphore, sport_id, league_id, cc, skip_esports)
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
        df = df[list(self.COLS_MAP.keys())]
        df = df.rename(columns=self.COLS_MAP)

        df['time'] = pd.to_datetime(df['time'].astype(int), unit='s')

        return df

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