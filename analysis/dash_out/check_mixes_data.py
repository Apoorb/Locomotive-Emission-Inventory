"""
Check mile mixes and other underlying data.
"""


from pathlib import Path
import pandas as pd
import numpy as np
from locoei.utilis import PATH_INTERIM, PATH_PROCESSED

path_fuel_consump = Path.joinpath(
    Path(PATH_INTERIM), r"fuelconsump_2019_tx_2021-10-09.csv"
)
path_emis = Path.joinpath(Path(PATH_PROCESSED), r"emis_quant_loco_agg_2021-10-09.csv")
fuel_cnsp = pd.read_csv(path_fuel_consump)
emis = pd.read_csv(path_emis, index_col=0)
fuel_cnsp_filt = fuel_cnsp.filter(
    items=[
        "fraarcid",
        "frfranode",
        "tofranode",
        "stcntyfips",
        "fradistrct",
        "subdiv",
        "net",
        "miles",
        "timezone",
        "carrier",
        "friylab",
        "rr_group",
        "totnetmiles",
        "milemx",
        "st_fuel_consmp",
        "st_fuel_consmp_all_cls1",
        "county_pct",
        "cnty_cls1_all_fuel_consmp",
        "link_fuel_consmp",
        "year",
    ]
)
emis.columns
emis_19 = emis.loc[emis.year == 2019]


fuel_cnsp_cls1 = fuel_cnsp_filt.loc[
    lambda df: (df.rr_group == "Class I") & (df.friylab == "Fcat")
]

fuel_cnsp_cls1_test = fuel_cnsp_cls1.groupby(["stcntyfips", "rr_group"]).agg(
    miles=("miles", "sum"),
    totnetmiles=("totnetmiles", "first"),
    milemx=("milemx", "sum"),
    cnty_sum_link_fuel_consmp=("link_fuel_consmp", "sum"),
    cnty_cls1_all_fuel_consmp=("cnty_cls1_all_fuel_consmp", "first"),
)
assert np.allclose(
    fuel_cnsp_cls1_test.cnty_sum_link_fuel_consmp,
    fuel_cnsp_cls1_test.cnty_cls1_all_fuel_consmp,
)
assert np.allclose(fuel_cnsp_cls1_test.miles, fuel_cnsp_cls1_test.totnetmiles)
assert np.allclose(fuel_cnsp_cls1_test.milemx, 1)


fuel_cnsp_cls3_pass_comu = fuel_cnsp_filt.loc[
    lambda df: (df.rr_group.isin(["Class III", "Passenger", "Commuter"]))
    & (df.friylab == "Fcat")
]
fuel_cnsp_cls3_pass_comu_test = fuel_cnsp_cls3_pass_comu.groupby(
    ["rr_group", "carrier"]
).agg(
    miles=("miles", "sum"),
    totnetmiles=("totnetmiles", "first"),
    milemx=("milemx", "sum"),
    cnty_sum_link_fuel_consmp=("link_fuel_consmp", "sum"),
    cnty_cls1_all_fuel_consmp=("st_fuel_consmp", "first"),
)
assert np.allclose(
    fuel_cnsp_cls3_pass_comu_test.cnty_sum_link_fuel_consmp,
    fuel_cnsp_cls3_pass_comu_test.cnty_cls1_all_fuel_consmp,
)
assert np.allclose(
    fuel_cnsp_cls3_pass_comu_test.miles, fuel_cnsp_cls3_pass_comu_test.totnetmiles
)
assert np.allclose(fuel_cnsp_cls3_pass_comu_test.milemx, 1)
fuel_cnsp_cls3_pass_comu_cnty = fuel_cnsp_cls3_pass_comu.groupby(
    ["stcntyfips", "rr_group"]
).agg(miles=("miles", "sum"), cnty_sum_link_fuel_consmp=("link_fuel_consmp", "sum"))


fuel_cnsp_yards = fuel_cnsp_filt.loc[lambda df: (df.friylab == "IYcat")]
fuel_cnsp_yards.rr_group.unique()
fuel_cnsp_yards.groupby("friylab").link_fuel_consmp.sum()

emis_19.columns
cntr_emis_19_yards = emis_19.loc[
    lambda df: df.scc_description_level_4 == "Yard Locomotives"
]
cntr_emis_19_yards_co2 = cntr_emis_19_yards.loc[lambda df: df.pollutant == "CO2"]

cntr_emis_19_yards_co2.em_quant.sum() / cntr_emis_19_yards_co2.county_carr_friy_yardnm_fuel_consmp_by_yr.sum()
cntr_emis_19_yards_co2.em_fac.mean()
