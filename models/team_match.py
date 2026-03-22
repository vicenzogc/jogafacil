import httpx
import asyncio
import pandas as pd
import os
from .base import Base

class TeamMatchPipeline(Base):
    TABLE_NAME = "TeamMatch"
    COLS_MAP = {
        'team_id': 'team_id',
        'ss': 'ss',
        'score_1': 'score_1',
        'score_2': 'score_2',
        'attacks': 'attacks',
        'dangerous_attacks': 'dangerous_attacks',
        'yellowcards': 'yellowcards',
        'redcards': 'redcards',
        'possession_rt': 'possession_rt',
        'penalties': 'penalties'
    }
    a = 0

    def __init__(self, engine):
        self.engine = engine

    async def run(self, url: str) -> None:
        raw = await self.fetch(url)
        if not raw:
            print("Nenhum dado extraído.")
            return
        df = self.transform(raw)
        self.load(df)

    def get_all_match(self) -> pd.DataFrame:
        return pd.read_sql("SELECT match_id FROM Match", self.engine)

    async def fetch_event_view(
        self,
        client: httpx.AsyncClient,
        url: str,
        semaphore: asyncio.Semaphore,
        match_id: int,
    ) -> dict:
        async with semaphore:
            try:
                response = await client.get(url, params={
                    "token": os.environ.get("ESPORTE_API_KEY"),
                    "event_id": match_id,
                })
            except httpx.TimeoutException:
                print(f"[{match_id}] Timeout")
                return {}
            except httpx.RequestError as e:
                print(f"[{match_id}] Erro de conexão: {e}")
                return {}

            if response.status_code != 200:
                print(f"[{match_id}] Erro: {response.status_code}")
                return {}

            results = response.json().get("results", [])

            a+= 1
            print(self.a)
            return results[0] if results else {}

    async def fetch(self, url: str) -> list:
        matches = self.get_all_match()
        semaphore = asyncio.Semaphore(300)
        CHUNKS = 1

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(connect=15, read=30, write=10, pool=10)
        ) as client:
            match_ids = matches["match_id"].tolist()
            chunks = [match_ids[i::CHUNKS] for i in range(CHUNKS)]

            results_per_match = []
            for i, chunk in enumerate(chunks):
                tasks = [
                    self.fetch_event_view(client, url, semaphore, match_id)
                    for match_id in chunk
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                results_per_match.extend(results)

                if i < len(chunks) - 1:
                    print(f"⏳ Chunk {i + 1}/{CHUNKS} concluído. Aguardando 5 segundos...")
                    await asyncio.sleep(5)

        all_results = []
        for match_id, result in zip(matches["match_id"], results_per_match):
            if isinstance(result, Exception):
                print(f"[match {match_id}] Falhou: {result}")
            elif result:
                all_results.append(result)

        return all_results

    def transform(self, raw_data: list) -> pd.DataFrame:
        rows = []

        for match in raw_data:
            stats = match.get("stats", {})
            scores = match.get("scores", {})
            ss = match.get("ss", "")

            # ss vem como "2-1", separa por time
            ss_parts = ss.split("-") if ss else [None, None]

            base = {
                "match_id": match["id"],
            }

            for i, (side, team_key) in enumerate([("home", "home"), ("away", "away")]):
                row = {
                    **base,
                    "team_id":          match[team_key]["id"],
                    "ss":               ss_parts[i] if len(ss_parts) > i else None,
                    "score_1":          scores.get("1", {}).get(side),
                    "score_2":          scores.get("2", {}).get(side),
                    "attacks":          stats.get("attacks", [None, None])[i],
                    "dangerous_attacks":stats.get("dangerous_attacks", [None, None])[i],
                    "yellowcards":      stats.get("yellowcards", [None, None])[i],
                    "redcards":         stats.get("redcards", [None, None])[i],
                    "possession_rt":    stats.get("possession_rt", [None, None])[i],
                    "penalties":        stats.get("penalties", [None, None])[i],
                }
                rows.append(row)

        return pd.DataFrame(rows)

    def load(self, df: pd.DataFrame) -> None:
        try:
            existentes_df = pd.read_sql(f"SELECT match_id, team_id FROM {self.TABLE_NAME}", self.engine)
            ids_no_banco = set(zip(existentes_df["match_id"], existentes_df["team_id"]))
        except Exception:
            ids_no_banco = set()

        # chave composta match_id + team_id
        df_novo = df[~df.apply(lambda r: (r["match_id"], r["team_id"]) in ids_no_banco, axis=1)]

        if not df_novo.empty:
            df_novo.to_sql(self.TABLE_NAME, self.engine, if_exists='append', index=False)
            print(f"✅ {len(df_novo)} novos registros adicionados em '{self.TABLE_NAME}'.")
        else:
            print(f"ℹ️ Nenhum dado novo para '{self.TABLE_NAME}'.")