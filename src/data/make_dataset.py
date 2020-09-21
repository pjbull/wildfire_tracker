# -*- coding: utf-8 -*-
import click
import logging
from multiprocessing import cpu_count
from pathlib import Path
import re

import bs4
from dotenv import find_dotenv, load_dotenv
import pandas as pd
from pqdm.processes import pqdm

def page_to_data_series(snapshot_path):
    page_content = snapshot_path.read_text()
    soup = bs4.BeautifulSoup(page_content, features='lxml')
    
    tables = []
    for t in soup.select("table.table-incident"):
        tables += pd.read_html(str(t), index_col=0)
    
    all_data_series = pd.concat(tables).loc[:, 1]
    all_data_series.name = soup.select_one("H1").text
    
    all_data_series.index = all_data_series.index.str.lower().str.replace(' ', '_')
    
    all_data_series.index.name = 'properties'
    
    # ============ Per column cleanup
    all_data_series.current_as_of = pd.to_datetime(
        re.findall('\"(.*)\"', all_data_series.current_as_of)[0]
    )
    
    if 'date_of_origin' in all_data_series.index:
        all_data_series.date_of_origin = pd.to_datetime(
            all_data_series.date_of_origin.replace("approx. ", "")
        )
    
    if 'estimated_containment_date' in all_data_series.index:
        all_data_series.estimated_containment_date = pd.to_datetime(
            all_data_series.estimated_containment_date.replace("approx. ", "")
        )

    if 'size' in all_data_series.index:
        all_data_series['size_acres'] = float(all_data_series['size'].replace(" Acres", "").replace(",", ""))
        all_data_series.drop('size', inplace=True)
    
    if 'percent_of_perimeter_contained' in all_data_series.index:
        all_data_series.percent_of_perimeter_contained = (
            float(all_data_series.percent_of_perimeter_contained.strip("%")) / 100
        )
    
    all_data_series['lat'] = float(all_data_series.coordinates.split(" ")[0])
    all_data_series['lon'] = float(all_data_series.coordinates.split(" ")[2])
    all_data_series.drop('coordinates', inplace=True)
    
    return all_data_series


@click.command()
@click.argument('input_filepath', type=click.Path(exists=True))
@click.argument('output_filepath', type=click.Path())
def main(input_filepath, output_filepath):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw data')

    input_filepath = Path(input_filepath)
    output_filepath = Path(output_filepath) / "all_incidents.feather"

    all_snapshots = list(input_filepath.glob("**/*.snapshot"))
    
    all_incidents = pqdm(all_snapshots, page_to_data_series, n_jobs=cpu_count() - 1)

    all_incidents = pd.DataFrame(all_incidents)
    
    logger.info(f"Dropping any duplicates (snapshot content is the same) from original rows {all_incidents.shape[0]}")
    all_incidents.drop_duplicates(inplace=True)
    logger.info(f"Dropped duplicates results in rows {all_incidents.shape[0]}")
    
    logger.info(f"Saving data to: {str(output_filepath)}")
    all_incidents.reset_index().rename(columns={'index': 'incident_name'}).to_feather(output_filepath)
    
    logger.info("Finished successfully.")
    

if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
