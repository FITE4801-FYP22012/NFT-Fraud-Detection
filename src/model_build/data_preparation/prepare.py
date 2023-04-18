
# from sklearn import datasets
import os
import pandas as pd
import logging
from .parse import read_dynamo_json
from .schema import INT_COLS, BOOL_COLS, FLOAT_COLS, STRING_COLS

log_fmt = "%(asctime)s - data - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=log_fmt)

# def read_json_s3(bucket, key):
#     s3 = boto3.client("s3")
#     obj = s3.get_object(Bucket=bucket, Key=key)
#     return obj["Body"].read()

# def write_parquet_s3(df, bucket, key):
#     buffer = io.BytesIO()
#     df.to_parquet(buffer, index=False)
#     buffer.seek(0)
#     s3 = boto3.client("s3")
#     s3.put_object(Body=buffer.getvalue(), Bucket=bucket, Key=key)

def process_data():
    """Runs data processing scripts to turn raw json data into
    cleaned data ready to be analyzed (in parquet format).
    """
    input_filepath = os.path.join(os.environ['input_folder'], os.environ['file_name'])
    file_name_root = os.path.splitext(os.environ['file_name'])[0]
    output_filepath = os.path.join(os.environ['output_folder'], f"{file_name_root}.parquet")

    logging.info(f'parsing {input_filepath}')
    logging.info("converting dynamo json to dict")
    data = read_dynamo_json(input_filepath, mode="quick")
    
    logging.info("converting raw data to dataframe")
    df = pd.json_normalize(data)

    logging.info("Target distribution:")
    logging.info(df["display_state"].value_counts(dropna=False))
    
    logging.info("making final data set from raw data")
    original_size = df.shape[0]
    # Validate cols and drop duplicate rows/cols
    # Doesn't work on the lists
    # df = df.drop_duplicates() 

    # remove "deleted" collections, and did_self_destruct
    df = df[~df["display_state"].isin(["deleted"])]
    # 'deleted' iff did_self_destruct 
    df.drop(columns=['flags.did_self_destruct'], inplace=True)

    # Remove collections with fewer than MIN_HOLDER_THRESHOLD
    df = df[df["display_stats.holders"] >= os.environ['MIN_HOLDER_THRESHOLD']]

    # df = df.replace({np.nan: None})
    # df.fillna("", inplace=True)

    # Modify the types of the columns
    bool_cols = set(BOOL_COLS) & set(df.columns)

    df[STRING_COLS] = df[STRING_COLS].astype("string")
    df[FLOAT_COLS] = df[FLOAT_COLS].astype("float")
    df[INT_COLS] = df[INT_COLS].astype("int")
    # replace empty strings with None
    df[list(bool_cols)] = df[list(bool_cols)].replace({"": None}).astype("boolean")

    # fillna
    df[INT_COLS] = df[INT_COLS].fillna(0)
    df[FLOAT_COLS] = df[FLOAT_COLS].fillna(0)

    # Encode target 
    df["display_state"].replace(
        ["safe", "normal", "hidden", "caution", "suspicious"],
        [0, 1, 2, 3, 4],
        inplace=True,
    )
    df["display_state"] = df["display_state"].astype("int")

    # df.sort_values(by=['display_stats.holders'], ascending=False)
    final_size = df.shape[0]
    logging.info(f"Original size: {original_size} rows")
    logging.info(f"Final size: {final_size} rows")
    logging.info(f"dropped {original_size - final_size} rows")

    # save as parquet
    logging.info("saving processed data to parquet")
    df.to_parquet(output_filepath, index=False)
    logging.info("processed data saved to parquet")


if __name__ == "__main__":
    process_data()

