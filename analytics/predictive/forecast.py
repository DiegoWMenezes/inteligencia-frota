"""
Analytics Preditiva - Previsão de custos e consumo da frota.
Modelos: Regressão linear e séries temporais (ARIMA).
Previsão para os próximos 6 meses.
"""

import os
import sys
import warnings
import pandas as pd
import numpy as np
from datetime import datetime

warnings.filterwarnings("ignore")

BASE_DIR = os.path.join(os.path.dirname(__file__), "..", "..")
DATA_DIR = os.path.join(BASE_DIR, "data")

# Tentar importar bibliotecas de ML
try:
    from sklearn.linear_model import LinearRegression
    from sklearn.metrics import mean_absolute_error, r2_score
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False
    print("[PREDITIVA] scikit-learn não instalado. Usando regressão manual.")

try:
    from statsmodels.tsa.arima.model import ARIMA
    HAS_STATSMODELS = True
except ImportError:
    HAS_STATSMODELS = False
    print("[PREDITIVA] statsmodels não instalado. Usando modelo de tendência simples.")


class CustoKmForecaster:
    """Previsão de custo por km usando regressão linear."""

    def __init__(self):
        self.model = None
        self.r2 = 0
        self.mae = 0

    def fit(self, df: pd.DataFrame):
        """Treina o modelo com dados históricos."""
        # Agregar por mês
        df_monthly = (
            df.groupby(["ano", "mes"])
            .agg(
                custo_por_km=("custo_por_km", "mean"),
                custo_combustivel=("custo_combustivel", "sum"),
                custo_manutencao=("custo_manutencao", "sum"),
                km_rodado=("km_rodado", "sum"),
            )
            .reset_index()
        )

        # Criar índice temporal
        df_monthly["periodo_idx"] = (df_monthly["ano"] - df_monthly["ano"].min()) * 12 + df_monthly["mes"]
        df_monthly = df_monthly.sort_values("periodo_idx")

        X = df_monthly[["periodo_idx", "km_rodado", "custo_combustivel"]].values
        y = df_monthly["custo_por_km"].values

        if HAS_SKLEARN:
            self.model = LinearRegression()
            self.model.fit(X, y)
            y_pred = self.model.predict(X)
            self.r2 = r2_score(y, y_pred)
            self.mae = mean_absolute_error(y, y_pred)
        else:
            # Regressão linear manual (mínimos quadrados)
            self.model = self._manual_regression(X, y)
            y_pred = X @ self.model["coefficients"] + self.model["intercept"]
            ss_res = np.sum((y - y_pred) ** 2)
            ss_tot = np.sum((y - np.mean(y)) ** 2)
            self.r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0
            self.mae = np.mean(np.abs(y - y_pred))

        print(f"[PREDITIVA] Modelo treinado: R²={self.r2:.4f}, MAE={self.mae:.4f}")
        return self

    def _manual_regression(self, X, y):
        """Regressão linear manual via mínimos quadrados."""
        X_b = np.c_[np.ones(X.shape[0]), X]
        theta = np.linalg.pinv(X_b.T @ X_b) @ X_b.T @ y
        return {
            "intercept": theta[0],
            "coefficients": theta[1:],
        }

    def predict(self, periods_ahead: int = 6, last_ano: int = 2024, last_mes: int = 12) -> pd.DataFrame:
        """Gera previsões para os próximos N meses."""
        predictions = []

        for i in range(1, periods_ahead + 1):
            mes = (last_mes + i - 1) % 12 + 1
            ano = last_ano + (last_mes + i - 1) // 12
            periodo_idx = (ano - last_ano) * 12 + mes + 24  # continuar índice

            # Estimativas simples para variáveis exógenas
            km_estimado = 500000  # km total estimado
            custo_comb_estimado = 1800000  # custo combustível estimado

            if HAS_SKLEARN and hasattr(self.model, "predict"):
                x_input = [[periodo_idx, km_estimado, custo_comb_estimado]]
                pred = self.model.predict(x_input)[0]
            else:
                X_input = np.array([[periodo_idx, km_estimado, custo_comb_estimado]])
                pred = X_input @ self.model["coefficients"] + self.model["intercept"]

            predictions.append({
                "ano": ano,
                "mes": mes,
                "custo_por_km_previsto": round(float(pred), 4),
                "modelo": "regressao_linear",
            })

        return pd.DataFrame(predictions)


class ARIMAForecaster:
    """Previsão usando ARIMA para séries temporais."""

    def __init__(self, order=(1, 1, 1)):
        self.order = order
        self.model = None
        self.fitted = None

    def fit(self, series: pd.Series):
        """Treina modelo ARIMA."""
        if HAS_STATSMODELS:
            try:
                self.model = ARIMA(series, order=self.order)
                self.fitted = self.model.fit()
                print(f"[PREDITIVA-ARIMA] AIC={self.fitted.aic:.2f}")
            except Exception as e:
                print(f"[PREDITIVA-ARIMA] Erro no treino: {e}. Usando média móvel.")
                self.fitted = None
        else:
            self.fitted = None

    def predict(self, periods: int = 6) -> pd.DataFrame:
        """Gera previsão ARIMA."""
        if self.fitted is not None:
            forecast = self.fitted.forecast(steps=periods)
            return pd.DataFrame({
                "periodo": range(1, periods + 1),
                "valor_previsto": forecast.values,
                "modelo": "ARIMA",
            })
        else:
            # Fallback: média dos últimos 3 meses
            return None


class FleetPredictor:
    """Orquestra todos os modelos preditivos."""

    def __init__(self, db_path: str = None, engine: str = "postgresql"):
        self.db_path = db_path or os.path.join(DATA_DIR, "inteligencia_frota.db")
        self.engine = engine

    def load_data(self) -> pd.DataFrame:
        """Carrega dados do DW."""
        if self.engine == "postgresql":
            from sqlalchemy import create_engine
            PG_CONFIG = {
                "host": "localhost", "port": 5432,
                "database": "inteligencia_frota",
                "user": "postgres", "password": "adm123",
            }
            url = (
                f"postgresql+psycopg2://{PG_CONFIG['user']}:{PG_CONFIG['password']}"
                f"@{PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['database']}"
            )
            sa_engine = create_engine(url)
            conn = sa_engine.connect()

        query = """
        SELECT
            f.custo_por_km,
            f.custo_combustivel,
            f.custo_manutencao,
            f.km_rodado,
            f.preco_combustivel,
            f.idade_veiculo,
            f.margem_operacional,
            v.tipo_veiculo,
            t.ano, t.mes
        FROM fato_frota f
        JOIN dim_tempo t ON f.sk_tempo = t.sk_tempo
        JOIN dim_veiculo v ON f.sk_veiculo = v.sk_veiculo
        """

        df = pd.read_sql(query, conn)
        conn.close()
        print(f"[PREDITIVA] Dados carregados: {len(df)} registros")
        return df

    def _get_sqlite_connection(self):
        """Retorna conexao SQLite (fallback)."""
        import sqlite3
        return sqlite3.connect(self.db_path)

    def run_all_forecasts(self, periods_ahead: int = 6) -> dict:
        """Executa todas as previsões."""
        print("=" * 50)
        print("ANALYTICS PREDITIVA - FORECAST")
        print("=" * 50)

        df = self.load_data()

        if df.empty:
            print("[PREDITIVA] Sem dados para previsão.")
            return {}

        results = {}

        # 1. Regressão Linear - Custo por km
        print("\n[1/3] Regressão Linear - Custo por km...")
        forecaster_lr = CustoKmForecaster()
        forecaster_lr.fit(df)

        last_ano = int(df["ano"].max())
        last_mes = int(df[df["ano"] == last_ano]["mes"].max())

        results["regressao_custo_km"] = forecaster_lr.predict(periods_ahead, last_ano, last_mes)

        # 2. ARIMA - Série temporal do custo por km
        print("\n[2/3] ARIMA - Série temporal custo por km...")
        df_monthly = df.groupby(["ano", "mes"]).agg(custo_por_km=("custo_por_km", "mean")).reset_index()
        df_monthly = df_monthly.sort_values(["ano", "mes"])
        series = df_monthly["custo_por_km"]

        forecaster_arima = ARIMAForecaster(order=(1, 1, 1))
        forecaster_arima.fit(series)

        if forecaster_arima.fitted is not None:
            results["arima_custo_km"] = forecaster_arima.predict(periods_ahead)
        else:
            # Fallback: média móvel
            ma_values = series.rolling(window=3).mean().iloc[-periods_ahead:]
            results["media_movel"] = pd.DataFrame({
                "periodo": range(1, periods_ahead + 1),
                "valor_previsto": ma_values.values if len(ma_values) >= periods_ahead
                    else [series.mean()] * periods_ahead,
                "modelo": "media_movel_3m",
            })

        # 3. Tendência linear simples por tipo de veículo
        print("\n[3/3] Tendência por tipo de veículo...")
        df_tipo = df.groupby(["tipo_veiculo", "ano", "mes"]).agg(
            custo_por_km=("custo_por_km", "mean")
        ).reset_index()

        tendencias = []
        for tipo in df_tipo["tipo_veiculo"].unique():
            df_t = df_tipo[df_tipo["tipo_veiculo"] == tipo].sort_values(["ano", "mes"])
            if len(df_t) >= 3:
                x = np.arange(len(df_t))
                y = df_t["custo_por_km"].values
                coef = np.polyfit(x, y, 1)
                tendencia = "CRESCENTE" if coef[0] > 0 else "DECRESCENTE"
                tendencias.append({
                    "tipo_veiculo": tipo,
                    "tendencia": tendencia,
                    "inclinação": round(float(coef[0]), 6),
                    "custo_atual": round(float(y[-1]), 4),
                    "custo_projecao_6m": round(float(coef[0] * (len(y) + 6) + coef[1]), 4),
                })

        results["tendencia_tipo"] = pd.DataFrame(tendencias)

        # Salvar resultados
        output_dir = os.path.join(BASE_DIR, "analytics", "predictive")
        timestamp = datetime.now().strftime("%Y%m%d")

        for name, df_result in results.items():
            if isinstance(df_result, pd.DataFrame) and not df_result.empty:
                path = os.path.join(output_dir, f"forecast_{name}_{timestamp}.csv")
                df_result.to_csv(path, index=False, encoding="utf-8")
                print(f"  -> {name}: salvo em {path}")

        print("\n" + "=" * 50)
        print("PREVISOES CONCLUIDAS")
        print("=" * 50)

        return results


if __name__ == "__main__":
    sys.path.insert(0, BASE_DIR)
    predictor = FleetPredictor()
    results = predictor.run_all_forecasts()

    for name, df in results.items():
        if isinstance(df, pd.DataFrame):
            print(f"\n--- {name} ---")
            print(df.to_string())