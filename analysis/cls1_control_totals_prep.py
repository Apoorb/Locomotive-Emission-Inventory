"""
Debug control total. And recreate balanced control totals accounting for
missing counties present in control total spreadsheet but absent in natrail
dataset.
"""
import os
import inflection
import pandas as pd
import numpy as np
from locoerlt.utilis import (
    PATH_RAW,
    PATH_INTERIM,
    read_shapefile,
)
from locoerlt.fuelcsmp import preprc_link, preprc_fuelusg

path_natrail2020 = os.path.join(
    PATH_RAW, "North_American_Rail_Lines", "North_American_Rail_Lines.shp"
)
path_cls1_cntpct = os.path.join(PATH_RAW, "2019CountyPct.csv")
path_rail_carrier_grp = os.path.join(PATH_RAW, "rail_carrier_grp2020.csv")
path_fueluserail2019 = os.path.join(PATH_RAW, "RR_2019FuelUsage.csv")

fueluse2019_preprc = preprc_fuelusg(path_fueluserail2019_=path_fueluserail2019)
natrail2020 = read_shapefile(path_natrail2020)
cls1_cntpct = pd.read_csv(path_cls1_cntpct)
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
txrail_2020_preprs = preprc_link(
    path_natrail2020_=path_natrail2020,
    path_rail_carrier_grp_=path_rail_carrier_grp,
    map_rrgrp_=map_rrgrp,
)

txrail_2020_preprs_cls1 = (
    txrail_2020_preprs.loc[lambda df: (
        (df.carrier.isin(["BNSF", "KCS", "UP"])) & (df.friylab == "Fcat"))]
    .groupby(["carrier", "stcntyfips"]).agg(Natrail2020=("carrier", "first"))
    .reset_index()
    .assign(Natrail2020=1,
            stcntyfips=lambda df: df.stcntyfips.astype("int64"))
    .rename(columns={"stcntyfips": "FIPS_natrail"})
)

txrail_2020_preprs_cls1_pivot = (
    txrail_2020_preprs_cls1
    .pivot_table(index="FIPS_natrail", columns="carrier").reset_index()
    .droplevel(level=0, axis=1)
    .fillna(0)
    .rename(columns={"": "FIPS"})
)

cls1_carriers=["BNSF", "KCS", "UP"]

fueluse2019_preprc_fr = (
    fueluse2019_preprc.loc[lambda df: (df.friylab == "Fcat")
                                      & (df.carrier.isin(cls1_carriers))
    ]
    .assign(
        st_fuel_consmp_all_cls1=lambda df: df.st_fuel_consmp.sum(),
        cls1_st_fuel_ratio=lambda df: df.st_fuel_consmp / df.st_fuel_consmp_all_cls1
    )
)

cls1_fuel_freight_state = (fueluse2019_preprc_fr[["carrier", "st_fuel_consmp"]]
 .set_index("carrier").T
 .reset_index(drop=True)
 )

st_fuel_consmp_all_cls1 = (
    fueluse2019_preprc_fr.st_fuel_consmp_all_cls1.values[0])


test_miss_nat_cnttot_cls1_fr =(
    txrail_2020_preprs_cls1_pivot
    .merge(cls1_cntpct, on=["FIPS"], how="outer")
    .assign(st_fuel_consmp_all_cls1=st_fuel_consmp_all_cls1)
    .assign(cnty_fuel_consmp_all_cls1=lambda df: st_fuel_consmp_all_cls1
                                                 * df.CountyPCT)
)

test_miss_nat_cnttot_cls1_fr_1 = pd.concat(
    [test_miss_nat_cnttot_cls1_fr, cls1_fuel_freight_state]
)

path_out_debug = os.path.join(PATH_INTERIM, "debug",
                              "txrail_2020_preprs_cls1_pivot_fuel.xlsx")
test_miss_nat_cnttot_cls1_fr_1.to_excel(path_out_debug)


