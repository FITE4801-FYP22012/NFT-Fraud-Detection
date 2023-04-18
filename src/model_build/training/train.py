import os
import json
import joblib
import logging
import argparse
import pandas as pd
import mlflow
import mlflow.sklearn
import glob
from pycaret.classification import *

logging.basicConfig(level=logging.INFO)


def train(args):

    logging.info("READING DATA")
    parquet_files = glob.glob(os.path.join(args.input_folder, '*.parquet'))
    if not parquet_files:
        raise Exception("No parquet files found in the input_data_path")
 
    # Read the input parquet file
    # Assume only one parquet file in the input folder
    df = pd.read_parquet(parquet_files[0])

    hyperparameters = json.loads(args.hyperparameters)

    # SET REMOTE MLFLOW SERVER
    mlflow.set_tracking_uri(hyperparameters["tracking_uri"])
    # mlflow.set_experiment(hyperparameters["experiment_name"])

    exp = setup(
        data=df,
        train_size=hyperparameters['train_size'],
        target=hyperparameters['target'],
        session_id=123,
        fold_shuffle=True,
        imputation_type="iterative",
        remove_multicollinearity=hyperparameters['remove_multicollinearity'],
        log_experiment=True,
        experiment_name=hyperparameters["experiment_name"],
    )

    if hyperparameters["save_model_in_registry"]:
        experiment = mlflow.get_experiment_by_name(hyperparameters["experiment_name"])
        runs = mlflow.search_runs(experiment.experiment_id)
        best_run = runs.loc[runs["metrics.AUC"].idxmax()]
        # Get the run ID and artifact URI of the best run
        # best_run_id = best_run.run_id
        # best_run_name = best_run["tags.mlflow.runName"].replace(" ", "_")
        artifact_uri = best_run.artifact_uri
        
        # Define the path to the model artifact (logged as 'model' in the example)
        model_path = os.path.join(artifact_uri.replace(r"file://", ""), "model").lstrip("/")
        mlflow.register_model(
            model_uri=model_path,
            name=hyperparameters['model_name'],
            tags=dict(best_run),
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-folder", type=str, default=os.environ["SM_CHANNEL_INPUT"]
    )
    parser.add_argument("--hyperparameters", type=str, default=os.environ["SM_HPS"])
    parser.add_argument("--output-folder", type=str, default=os.environ["SM_MODEL_DIR"])
    args, _ = parser.parse_known_args()

    # train model
    model = train(args)
