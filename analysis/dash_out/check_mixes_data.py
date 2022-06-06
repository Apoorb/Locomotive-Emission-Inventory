"""
Check mile mixes and other underlying data.
"""


from pathlib import Path
import pandas as pd
import numpy as np
from locoei.utilis import PATH_INTERIM
path_fuel_consump = Path.joinpath(
    Path(PATH_INTERIM), r"fuelconsump_2019_tx_2021-10-09.csv"
)
path_cntr_emis = Path.joinpath(
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

fuel_cnsp_cls1_test = fuel_cnsp_cls1.groupby(["stcntyfips", "rr_group"]).agg(
    miles=("miles", "sum"),
    totnetmiles=("totnetmiles", "first"),
    milemx=("milemx", "sum"),
    cnty_sum_link_fuel_consmp=("link_fuel_consmp", "sum"),
    cnty_cls1_all_fuel_consmp=("cnty_cls1_all_fuel_consmp", "first")
)
assert np.allclose(fuel_cnsp_cls1_test.cnty_sum_link_fuel_consmp, fuel_cnsp_cls1_test.cnty_cls1_all_fuel_consmp)
assert np.allclose(fuel_cnsp_cls1_test.miles, fuel_cnsp_cls1_test.totnetmiles)
assert np.allclose(fuel_cnsp_cls1_test.milemx, 1)


fuel_cnsp_cls3_pass_comu = fuel_cnsp_filt.loc[lambda df: (df.rr_group.isin(["Class III", "Passenger", "Commuter"]))
                                               & (df.friylab == "Fcat")]
fuel_cnsp_cls3_pass_comu_test = fuel_cnsp_cls3_pass_comu.groupby(["rr_group", "carrier"]).agg(
    miles=("miles", "sum"),
    totnetmiles=("totnetmiles", "first"),
    milemx=("milemx", "sum"),
    cnty_sum_link_fuel_consmp=("link_fuel_consmp", "sum"),
    cnty_cls1_all_fuel_consmp=("st_fuel_consmp", "first")
)
assert np.allclose(fuel_cnsp_cls3_pass_comu_test.cnty_sum_link_fuel_consmp, fuel_cnsp_cls3_pass_comu_test.cnty_cls1_all_fuel_consmp)
assert np.allclose(fuel_cnsp_cls3_pass_comu_test.miles, fuel_cnsp_cls3_pass_comu_test.totnetmiles)
assert np.allclose(fuel_cnsp_cls3_pass_comu_test.milemx, 1)


fuel_cnsp_yards = fuel_cnsp_filt.loc[lambda df: (df.friylab == "IYcat")]
fuel_cnsp_yards.rr_group.unique()
fuel_cnsp_yards.groupby("friylab").link_fuel_consmp.sum()