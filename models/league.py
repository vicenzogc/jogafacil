import httpx
import pandas as pd
import time
import os
from sqlalchemy import Unicode, Integer, String, Boolean
from .base import Base

class LeaguePipeline(Base):
    TABLE_NAME = "League"
    COLS_MAP = {
        'id': 'league_id',
        'name':'name',
        'cc': 'cc',
        'has_toplist': 'toplist',
        'has_leaguetable': 'leaguetable'
    }

    def __init__(self, engine):
        self.engine = engine

    # Busca dados paginados
    async def fetch(self, league_url: str, sport_id: str, cc: str = None) -> list:
        all_results = []
        page = 1
        async with httpx.AsyncClient(timeout=30) as client:
            while True:
                response = await client.get(league_url, params={
                    "token": os.environ.get("ESPORTE_API_KEY"),
                    "sport_id": sport_id,
                    "page": page,
                    "cc": cc
                })
                if response.status_code != 200:
                    print(f"Erro na API: {response.status_code}")
                    break
                results = response.json().get("results", [])
                if not results:
                    break
                all_results.extend(results)
                page += 1
                time.sleep(0.5)
        return all_results

    # Transforma os dados brutos em DataFrame formatado
    def transform(self, raw_data: list) -> pd.DataFrame:
        df = pd.DataFrame(raw_data)
        df = df[list(self.COLS_MAP.keys())]
        return df.rename(columns=self.COLS_MAP)

    # Carrega os dados no banco de dados
    def load(self, df: pd.DataFrame) -> None:
        # 1. Tenta buscar os IDs que já existem no banco
        try:
            # Pega a coluna de ID 
            id_col = df.columns[0] 
            existentes_df = pd.read_sql(f"SELECT {id_col} FROM {self.TABLE_NAME}", self.engine)
            ids_no_banco = existentes_df[id_col].tolist()
        except Exception:
            # Se a tabela não existir ainda, a lista de existentes é vazia
            ids_no_banco = []

        # 2. Filtra o DataFrame: "Mantém apenas as linhas cujo ID NÃO está no banco"
        df_novo = df[~df[id_col].isin(ids_no_banco)]

        # 3. Faz o append apenas do que sobrou
        if not df_novo.empty:
            # Usamos 'append' para não apagar nada
            df_novo.to_sql(self.TABLE_NAME, self.engine, if_exists='append', index=False)
            print(f"✅ {len(df_novo)} novos registros adicionados em '{self.TABLE_NAME}'.")
        else:
            print(f"ℹ️ Nenhum dado novo para '{self.TABLE_NAME}'. Tudo já estava atualizado.")