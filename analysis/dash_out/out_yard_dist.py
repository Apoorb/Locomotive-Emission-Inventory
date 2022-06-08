"""
Output Yard Emission Distribution
Created by: Apoorb
Created on: 06/03/2022
"""
from pathlib import Path
import pandas as pd
import geopandas as gpd
import numpy as np
import psycopg2
from sqlalchemy import create_engine

from locoei.utilis import PATH_RAW, PATH_INTERIM, PATH_PROCESSED

path_fuel_consump = Path.joinpath(
    Path(PATH_INTERIM), r"fuelconsump_2019_tx_2021-10-09.csv"
)
path_cntr_emis = Path.joinpath(Path(PATH_PROCESSED), r"cntr_emis_quant_2021-12-20.csv")
cntr_emis = pd.read_csv(path_cntr_emis, index_col=0)
cntr_emis_19_20 = cntr_emis.loc[cntr_emis.year.isin([2019, 2020])]
cntr_emis_19_20_yrds = (
    cntr_emis_19_20.loc[lambda df: df.scc_description_level_4 == "Yard Locomotives"]
    .filter(
        items=[
            "year",
            "stcntyfips",
            "county_name",
            "scc",
            "scc_description_level_4",
            "eis_facility_id",
            "yardname_v1",
            "pol_type",
            "pollutant",
            "pol_desc",
            "em_fac",
            "site_latitude",
            "site_longitude",
            "controlled_em_quant_ton",
        ]
    )
    .rename(
        columns={
            "year": "Year",
            "county_name": "County",
            "stcntyfips": "State-County FIPS",
            "scc": "SCC",
            "scc_description_level_4": "SCC Description Level 4",
            "eis_facility_id": "EIS Facility ID",
            "yardname_v1": "Yard Name",
            "pol_type": "Pol Type",
            "pollutant": "Pollutant",
            "pol_desc": "Pol Desc",
            "em_fac": "Emission Factor (grams/gallon)",
            "controlled_em_quant_ton": "Emission (U.S. Tons)",
            "site_latitude": "Site Latitude",
            "site_longitude": "Site Longitude",
        }
    )
    .loc[
        lambda df: df.Pollutant.isin(
            ["7439921", "CO", "NH3", "NOX", "PM10-PRI", "PM25-PRI", "SO2", "VOC", "CO2"]
        )
    ]
)
cntr_emis_19_20_yrds["State-County FIPS"] = cntr_emis_19_20_yrds[
    "State-County FIPS"
].astype(int)
geometry = gpd.points_from_xy(
    cntr_emis_19_20_yrds["Site Longitude"], cntr_emis_19_20_yrds["Site Latitude"]
)
cntr_emis_19_20_yrds_gpd = gpd.GeoDataFrame(
    cntr_emis_19_20_yrds, geometry=geometry, crs="EPSG:4326"
)
conn = psycopg2.connect(f"dbname=locoei_lh_emis user=postgres " f"password=civil123")
conn.set_session(autocommit=True)
cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS public.yard_point_emis;")
conn.close()


engine = create_engine("postgresql://postgres:civil123@localhost:5432/locoei_lh_emis")
cntr_emis_19_20_yrds_gpd.to_postgis("yard_point_emis", con=engine)
