class Base:

    # Orquestra o ETL
    async def run(self, url: str, sport_id, cc: str = None) -> None:
        raw = await self.fetch(url, sport_id, cc)
        if not raw:
            print("Nenhum dado extraído.")
            return
        df = self.transform(raw)
        self.load(df)