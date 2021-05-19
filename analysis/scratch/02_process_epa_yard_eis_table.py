import os
import glob
import pandas as pd
import pyodbc
from locoerlt.utilis import PATH_RAW, PATH_PROCESSED, get_snake_case_dict

path_epa_yard_info = os.path.join(
    PATH_RAW, "eis_report_16580", "fac_conf_proc_unit_16580.csv"
)
path_cntr_emisquant = glob.glob(
    os.path.join(PATH_PROCESSED, "cntr_emis_quant_[0-9]*-*-*.csv")
)[0]
path_yard_prcs = os.path.join(
    PATH_PROCESSED, "imputed_ertac_yard_2017_with_eis_unit_prc.xlsx"
)

cntr_emisquant = pd.read_csv(path_cntr_emisquant)
epa_yard_info = pd.read_csv(path_epa_yard_info)
yards_2020 = (
    cntr_emisquant.loc[
        lambda df: (df.scc_description_level_4 == "Yard Locomotives")
        & (df.year == 2020)
    ]
    .drop_duplicates(["stcntyfips", "eis_facility_id", "yardname_v1"])
    .filter(items=["stcntyfips", "eis_facility_id", "yardname_v1"])
    .assign(
        ertac_yards=1,
    )
    .reset_index(drop=True)
)

epa_yard_info_fil = (
    epa_yard_info.loc[lambda df: df.state == "TX"]
    .rename(columns=get_snake_case_dict(epa_yard_info.columns))
    .assign(epa_yards=1)
)

assert (
    epa_yard_info_fil.groupby("eis_facility_id")["county"].count().max() == 1
), "eis_facility_id is not unique."


yards_2020_epa_info = (
    yards_2020.merge(
        epa_yard_info_fil,
        left_on="eis_facility_id",
        right_on="eis_facility_id",
        how="left"
    )
    .filter(items=[
        "stcntyfips",
        "county",
        "eis_facility_id",
        "yardname_v1",
        "ertac_yards",
        "site_latitude",
        "site_longitude"
    ])
)

yards_2020_epa_info.to_excel(path_yard_prcs, index=False)
