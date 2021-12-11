import os
import glob
import pandas as pd
import pyodbc
from locoerlt.utilis import PATH_RAW, PATH_INTERIM, PATH_PROCESSED, get_snake_case_dict

path_county_nm = os.path.join(PATH_RAW, "Texas_County_Boundaries.csv")
path_cntr_emisquant = glob.glob(
    os.path.join(PATH_PROCESSED, "cntr_emis_quant_[0-9]*-*-*.csv")
)[0]
path_narl_yards = glob.glob(os.path.join(PATH_INTERIM, "fuelconsump_2019_tx_*.csv"))[0]


tx_counties = pd.read_csv(path_county_nm)

tx_counties_fil = (
    tx_counties.rename(columns=get_snake_case_dict(tx_counties))
    .assign(stcntyfips=lambda df: df.fips_st_cnty_cd.astype(float))
    .filter(items=["stcntyfips", "cnty_nm"])
)


cntr_emisquant = pd.read_csv(path_cntr_emisquant)
ertac_yards_2020 = (
    cntr_emisquant.loc[
        lambda df: (df.scc_description_level_4 == "Yard Locomotives")
        & (df.year == 2020)
    ]
    .drop_duplicates(["stcntyfips", "county_name", "eis_facility_id", "yardname_v1"])
    .assign(
        yardname=lambda df: df.yardname_v1.str.lower(),
        cnty_nm=lambda df: df.county_name,
        data="yes",
    )
    .rename(columns={"site_latitude": "lat", "site_longitude": "long"})
    .filter(items=["stcntyfips", "eis_facility_id", "yardname", "data", "lat", "long"])
    .reset_index(drop=True)
)

narl_yards_2020 = (
    pd.read_csv(path_narl_yards)
    .loc[lambda df: (df.friylab == "IYcat") & (~df.yardname.isna())]
    .drop_duplicates("yardname")
    .filter(items=["yardname", "stcntyfips", "start_lat", "start_long"])
    .assign(yardname=lambda df: df.yardname.str.lower(), data="yes")
    .reset_index(drop=True)
    .sort_values(by="yardname")
    .rename(columns={"start_lat": "lat", "start_long": "long"})
)


out_rename = {
    "stcntyfips": "FIPS",
    "cnty_nm": "County",
    "eis_facility_id": "EIS Facility ID",
    "yardname": "Yard Name",
    "data_ertac": "Found in ERTAC?",
    "data_narl": "Found in NARL?",
    "lat_ertac": "Lat (ERTAC)",
    "long_ertac": "Long (ERTAC)",
    "lat_narl": "Lat (NARL)",
    "long_narl": "Long (NARL)",
}


narl_ertac_yards_2020 = (
    ertac_yards_2020.merge(
        narl_yards_2020,
        on=["stcntyfips", "yardname"],
        how="outer",
        suffixes=["_ertac", "_narl"],
    )
    .assign(
        data_ertac=lambda df: df.data_ertac.fillna("no"),
        data_narl=lambda df: df.data_narl.fillna("no"),
        eis_facility_id=lambda df: df.eis_facility_id.fillna("-"),
        lat_ertac=lambda df: df.lat_ertac.fillna("-"),
        long_ertac=lambda df: df.long_ertac.fillna("-"),
        lat_narl=lambda df: df.lat_narl.fillna("-"),
        long_narl=lambda df: df.long_narl.fillna("-"),
    )
    .merge(tx_counties_fil, on="stcntyfips")
    .filter(items=out_rename.keys())
    .sort_values(["stcntyfips", "yardname"])
    .rename(columns=out_rename)
)


narl_ertac_yards_2020_common = narl_ertac_yards_2020.loc[
    lambda df: (df["Found in ERTAC?"] == "yes") & (df["Found in NARL?"] == "yes")
]


path_out_proposal = os.path.join(
    r"C:\Users\a-bibeka\OneDrive - Texas A&M Transportation "
    r"Institute\Documents\Proposals",
    "ertac_narl_yards.csv",
)
narl_ertac_yards_2020.to_csv(path_out_proposal)
