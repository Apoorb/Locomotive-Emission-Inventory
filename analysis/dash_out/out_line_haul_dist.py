"""
Output Line-Haul Fuel Consumption and Emission Distribution
Created by: Apoorb
Created on: 06/03/2022
"""
from pathlib import Path
import pandas as pd
import numpy as np
from locoei.utilis import PATH_INTERIM

path_fuel_consump = Path.joinpath(
    Path(PATH_INTERIM), r"fuelconsump_2019_tx_2021-10-09.csv"
)
fuel_cnsp = pd.read_csv(path_fuel_consump)
fuel_cnsp_filt = fuel_cnsp.filter(items=[
    "fraarcid", "frfranode", "tofranode", "stcntyfips", "fradistrct", "subdiv","net",
    "miles", "timezone", "carrier",
    "friylab", "rr_group", "totnetmiles", "milemx", "st_fuel_consmp",
    "st_fuel_consmp_all_cls1", "county_pct", "cnty_cls1_all_fuel_consmp","link_fuel_consmp",
    "year"
])

fuel_cnsp_cls1 = fuel_cnsp_filt.loc[lambda df: (df.rr_group == "Class I")
                                               & (df.friylab == "Fcat")]
