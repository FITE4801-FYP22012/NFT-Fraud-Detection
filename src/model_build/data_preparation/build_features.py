import pandas as pd
# -*- coding: utf-8 -*-
import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv

import pandas as pd
import numpy as np


@click.command()
@click.argument("input_filepath", type=click.Path(exists=True))
@click.argument("output_filepath", type=click.Path(), required=False)
def main(input_filepath:str, output_filepath:str=None):
    """Runs feature building scripts to turn processed data from (../processed) into
    training data (saved in ../final).
    """
    if not output_filepath:
        file_name = input_filepath.replace("/processed/", "/final/").rstrip(".parquet").split("/")[-1]
        output_filepath = f"data/final/{file_name}.parquet"
    
    logger = logging.getLogger(__name__)
    logger.info(f'building features from {input_filepath.split("/")[-1]}')
    
    df = pd.read_parquet(input_filepath)

    # get chain_id from pk
    df["chain_id"] = df["pk"].apply(lambda x: x.lstrip("chn#").split(':')[0])
    
    # Convert the lists of external urls to counts
    df["external.url_count"] = df["external.urls"].apply(lambda x: len(x))
    df.drop("external.urls", axis=1, inplace=True)

    # Social features
    for platform in ["discord", "twitter", "instagram"]:
        df[f"external.has_{platform}"] = df[f"external.{platform}"].str.len() > 0
        # df.drop(f"external.{platform}", axis=1, inplace=True)

    flag_cols = ['banner_image_url', 'description', 'collection_image_url', 'display_name']
    for col in flag_cols:
        df[f"has_{col}"] = df[col].str.len() > 0



    df.to_parquet(output_filepath, index=False)
    return df

if __name__ == "__main__":
    log_fmt = "%(asctime)s - data - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()