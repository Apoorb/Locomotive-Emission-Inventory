"""

"""
import os
import pytest
import inflection
import pandas as pd
import numpy as np
from locoerlt.utilis import PATH_RAW, PATH_INTERIM
from locoerlt.fuelcsmp import preprc_link

fuel_consump_from_data = (
    "BNSF",
    "KCS",
    "UP",
    "ANR",
    "AWRR",
    "CTXR",
    "MCSA",
    "PTRA",
    "TCT",
    "TN",
    "TXGN",
    "TXOR",
    "TXPF",
)
map_rrgrp = {
    "M": "Freight",  # Main sub network
    "I": "Freight",  # Major Industrial Lead
    "S": "Freight",  # Passing sidings over 4000 feet long
    "O": "Freight",  # Initially was Industrial, but, we are changing
    # it to freight, as industrial does not have yardname. Also, a lot of
    # the minor industrial leads function differently then a switching
    # yard, so they need to be treated differently.
    "Y": "Yard",  # Yard Switching
    "Z": "Transit",  # Transit-only rail line or museum/tourist operation
    "R": "Other",  # Abandoned line that has been physically removed
    "A": "Other",  # Abandoned rail line
    "X": "Other",  # Out of service line
    "F": "Other",  # Rail ferry connection
    "T": "Other",  # Trail on former rail right-of-way
}

path_natrail2020_csv = os.path.join(PATH_INTERIM, "North_American_Rail_Lines.csv")
path_cls1_cntpct = os.path.join(PATH_RAW, "2019CountyPct.csv")
path_fueluserail2019 = os.path.join(PATH_RAW, "RR_2019FuelUsage.csv")
path_rail_carrier_grp = os.path.join(PATH_RAW, "rail_carrier_grp2020.csv")
path_fill_missing_yardnames = os.path.join(
    PATH_INTERIM,
    "gis_debugging",
    "north_america_rail_2021",
    "filled_missing_yards.xlsx",
)
path_fuel_from_data = os.path.join(PATH_RAW, "collected_fuel_tx_cls3_data.csv")
fuel_collected_df = pd.read_csv(path_fuel_from_data)
txrail_2020_preprs = preprc_link(
    path_natrail2020_=path_natrail2020_csv,
    path_rail_carrier_grp_=path_rail_carrier_grp,
    path_fill_missing_yardnames_=path_fill_missing_yardnames,
    map_rrgrp_=map_rrgrp,
)


txrail_2020_preprs_agg_st = txrail_2020_preprs.groupby(["carrier", "net"]).agg(
    totnetmiles=("miles", "sum"),
)

txrail_2020_preprs_agg_st_fuel = txrail_2020_preprs_agg_st.merge(
    fuel_collected_df, on=["carrier", "friylab"]
)


txrail_2020_preprs_agg_st_fuel_rate = (
    txrail_2020_preprs_agg_st_fuel.groupby("friylab")["totnetmiles", "fuel_consump"]
    .sum()
    .reset_index()
    .assign(fuel_rate_per_mile=lambda df: df.fuel_consump / df.totnetmiles)
)
