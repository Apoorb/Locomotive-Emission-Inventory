"""

"""
import os
import pytest
import inflection
import pandas as pd
import numpy as np
from locoerlt.utilis import PATH_RAW, PATH_INTERIM
from locoerlt.fuelcsmp import get_fuel_consmp_by_cnty_carrier

cls1_carriers = ("BNSF", "KCS", "UP")
map_rrgrp = {
    "M": "Freight",  # Main sub network
    "I": "Freight",  # Major Industrial Lead
    "S": "Freight",  # Passing sidings over 4000 feet long
    "O": "Industrial",  # Other track (minor industrial leads)
    "Y": "Yard",  # Yard Switching
    "Z": "Transit",  # Transit-only rail line or museum/tourist operation
    "R": "Other",  # Abandoned line that has been physically removed
    "A": "Other",  # Abandoned rail line
    "X": "Other",  # Out of service line
    "F": "Other",  # Rail ferry connection
    "T": "Other",  # Trail on former rail right-of-way
}
path_natrail2020_csv = os.path.join(PATH_INTERIM, "North_American_Rail_Lines.csv")
path_natrail2020_csv = os.path.join(PATH_RAW,
                                    "archive",
                                    "North_American_Rail_Lines_4_16_2021.csv")

path_natrail2020_old = os.path.join(PATH_RAW, "archive", "NatRail_2020.csv")
path_cls1_cntpct = os.path.join(PATH_RAW, "2019CountyPct.csv")
path_fueluserail2019 = os.path.join(PATH_RAW, "RR_2019FuelUsage.csv")
path_rail_carrier_grp = os.path.join(PATH_RAW, "rail_carrier_grp2020.csv")
path_sql_df = os.path.join(PATH_INTERIM, "testing", "fuelCompOutMar5.csv")

tx_cls_1_3_comut_pasng_19_latest = get_fuel_consmp_by_cnty_carrier(
    path_natrail2020_=path_natrail2020_csv,
    path_rail_carrier_grp_=path_rail_carrier_grp,
    path_fueluserail2019_=path_fueluserail2019,
    path_cls1_cntpct_=path_cls1_cntpct,
    map_rrgrp_=map_rrgrp,
    filter_st=("TX",),
)

tx_cls_1_3_comut_pasng_19_old = get_fuel_consmp_by_cnty_carrier(
    path_natrail2020_=path_natrail2020_old,
    path_rail_carrier_grp_=path_rail_carrier_grp,
    path_fueluserail2019_=path_fueluserail2019,
    path_cls1_cntpct_=path_cls1_cntpct,
    map_rrgrp_=map_rrgrp,
    filter_st=("TX",),
)


tx_cls_1_3_comut_pasng_19_latest_agg_st = (
    tx_cls_1_3_comut_pasng_19_latest
    .groupby(['carrier', 'friylab'])
    .agg(
        st_fuel_consmp=("st_fuel_consmp", "mean"),
        totnetmiles=("totnetmiles", "mean"),
        milemx_st=("milemx", "sum"),
    )
)

tx_cls_1_3_comut_pasng_19_old_agg_st = (
    tx_cls_1_3_comut_pasng_19_old
    .groupby(['carrier', 'friylab'])
    .agg(
        st_fuel_consmp=("st_fuel_consmp", "mean"),
        totnetmiles=("totnetmiles", "mean"),
        milemx_st=("milemx", "sum"),
    )
)


tx_cls_3_comut_pasng_19_latest_old_comp =(
    tx_cls_1_3_comut_pasng_19_latest_agg_st
    .merge(tx_cls_1_3_comut_pasng_19_old_agg_st,
           left_index=True, right_index=True, how="outer",
           suffixes=["_latest", "_old"]
           )
    .assign(
        latest_min_old_mi=lambda df: df.milemx_st_latest - df.milemx_st_old,
        is_latest_old_eq=lambda df: abs(df.latest_min_old_mi) <= 0.2
    )
)