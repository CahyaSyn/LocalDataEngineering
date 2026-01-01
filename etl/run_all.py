from ingest_raw import ingest_raw
from bronze_transform import transform_bronze
from silver_transform import transform_silver
from gold_transform import transform_gold
from data_quality_check import run_data_quality_checks

def run_pipeline():
    print("=== PIPELINE STARTED ===")

    ingest_raw()
    transform_bronze()
    transform_silver()

    # STOP pipeline if data not valid
    run_data_quality_checks()

    transform_gold()

    print("=== PIPELINE COMPLETED ===")

if __name__ == "__main__":
    run_pipeline()