from __future__ import annotations

try:
    from airflow.datasets import Dataset
except ModuleNotFoundError:
    from airflow import Dataset  # type: ignore


LANDING_DATASET = Dataset("blockchain-exchange://landing")
STAGING_DATASET = Dataset("blockchain-exchange://staging")
DW_DATASET = Dataset("blockchain-exchange://dw")

