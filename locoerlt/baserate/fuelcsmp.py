"""
Get the fuel consumption by Class 1, 3, Amtrak, DART, and TREX.
"""
import os
import inflection
import pandas as pd

from locoerlt.utilis import PATH_RAW, PATH_INTERIM


if __name__ == "__main__":
    path_to_natrail2020 = os.path.join(PATH_RAW, 'NatRail_2020.csv')
    path_to_fueluserail2020 = os.path.join(PATH_RAW, 'RR_2019FuelUsage.csv')
    path_rail_carrier_grp = os.path.join(PATH_RAW,"rail_carrier_grp2020.csv")
    path_sql_df = os.path.join(PATH_INTERIM, "fuelCompOutMar5.csv")

    natrail2020 = pd.read_csv(path_to_natrail2020)
    fueluserail2020 = pd.read_csv(path_to_fueluserail2020)
    rail_carrier_grp = pd.read_csv(path_rail_carrier_grp, index_col=0)
    fuelconsump_sql_mar5 = pd.read_csv(path_sql_df)
    # https://proceedings.esri.com/library/userconf/proc15/papers/402_174.pdf
    map_rrgrp={
        "M":"Freight", # Main sub network
        "I":"Freight", # Major Industrial Lead
        "S":"Freight", # Passing sidings over 4000 feet long
        "O":"Industrial", # Other track (minor industrial leads)
        "Y":"Yard", # Yard Switching
        "Z":"Transit", # Transit-only rail line or museum/tourist operation
        "R":"Other", # Abandoned line that has been physically removed
        "A":"Other", # Abandoned rail line
        "X":"Other", # Out of service line
        "F":"Other", # Rail ferry connection
        "T":"Other", # Trail on former rail right-of-way
    }
    natrail2020_1 = (
        natrail2020
        .rename(
            columns={
                col: inflection.underscore(col) for col in natrail2020.columns}
        )
        .assign(
            rr_netgrp=lambda df: df.net.map(map_rrgrp)
        )
    )
    natrail2020_1.info()

    trk_right_cols = ['rrowner1', 'rrowner2', 'rrowner3', 'trkrghts1',
                      'trkrghts2', 'trkrghts3', 'trkrghts4', 'trkrghts5',
                      'trkrghts6', 'trkrghts7', 'trkrghts8', 'trkrghts9']


    txrail_2020 = (
        natrail2020_1
        .loc[lambda df: (df.stateab == "TX")
                        & (df.rr_netgrp.isin(("Freight", "Industrial", "Yard")))
        ]
        .assign(
            all_oper = lambda df: df[trk_right_cols].apply(set, axis=1))
    )


    fueluse_col_map = {
        "RRCarrier":"carrier",
        "LineHaul":"totfuelconsump_Fcat",
        "Yard":"totfuelconsump_IYcat",
    }
    fueluserail2020_1 = pd.wide_to_long(
        df=(fueluserail2020.rename(columns=fueluse_col_map)
            .filter(fueluse_col_map.values())),
        stubnames="totfuelconsump",
        i='carrier',
        j="friylab",
        sep="_",
        suffix=r'\w+'
    ).reset_index()

    txrail_2020_preprocess = (
        txrail_2020.explode("all_oper")
        .dropna(subset=["all_oper"])
        .drop(columns=trk_right_cols)
        .loc[lambda df: df.all_oper != "NS"] # Based on Madhu's SQL code
        .rename(columns={"all_oper": "carrier"})
        .assign(
            carrier=lambda df: df.carrier.replace(regex=r"^TRE$", value="TREX"),
            friylab=lambda df: df.rr_netgrp.map(
                {"Freight": "Fcat", "Industrial": "IYcat", "Yard":"IYcat"}),
        )
        .loc[lambda df: (~ df.carrier.isin(["DART", "AMTK"]))
                        | ((df.carrier == "DART") & (df.stcntyfips == 48121))
                        | ((df.carrier == "AMTK") & (df.rr_netgrp == "Freight"))
        ] # DART uses fuel only in Denton county. It uses electric engine in
        # Dallas
        # Only use freight network for Amtrak
        .merge(rail_carrier_grp, on="carrier", how="left")
        .merge(fueluserail2020_1, on=["carrier", "friylab"], how="left")
    )


    txrail_2020_milemx = (
        txrail_2020_preprocess
        .loc[lambda df: ~ df.totfuelconsump.isna()] # Removes DART's industrial
        # and Yard rows. They have null fuel consumption.
        .assign(
            totnetmiles=lambda df: df.groupby(
                ["friylab", "carrier"]).miles.transform(sum),
            milemx=lambda df: df.miles / df.totnetmiles,
            netfuelconsump=lambda df: df.milemx * df.totfuelconsump
        )
        .reset_index(drop=True)
    )

    txrail_2020_milemx_comp = (
        txrail_2020_milemx
        .filter(items=['objectid', 'miles', 'rr_netgrp', 'carrier',
                       'rr_group', 'totfuelconsump', 'totnetmiles', 'milemx',
                       'netfuelconsump'])
        .sort_values(by=["carrier", "rr_netgrp", "objectid"])
        .reset_index(drop=True)
    )

    fuelconsump_sql_mar5_comp = (
        fuelconsump_sql_mar5
        .rename(
            columns={
                col: inflection.underscore(col)
                for col in fuelconsump_sql_mar5.columns}
        )
        .rename(
            columns={
                "rr_netgroup":"rr_netgrp",
                "rr_carrier":"carrier",
                "rr_tot_netmiles": "totnetmiles",
                "rr_net_milemix":"milemx",
                "rr_tot_fuel_consump":"totfuelconsump",
                "rr_net_fuel_consump":"netfuelconsump",
                "shape_leng": "shape_st_len"
            }
        )
        .loc[lambda df: (df.carrier != "DART")
                        | ((df.carrier == "DART") & (df.stcntyfips == 48121))
        ]
        .assign(
            totfuelconsump=lambda df: df.totfuelconsump.astype(float),
            totnetmiles=lambda df: df.totnetmiles.astype(float),
            milemx=lambda df: df.milemx.astype(float),
            netfuelconsump=lambda df: df.netfuelconsump.astype(float),
        )
        .filter(items=['objectid', 'miles', 'rr_netgrp', 'carrier',
                       'rr_group', 'totfuelconsump', 'totnetmiles', 'milemx',
                       'netfuelconsump'])
        .sort_values(by=["carrier", "rr_netgrp", "objectid"])
        .reset_index(drop=True)
    )

    pd.testing.assert_frame_equal(
        txrail_2020_milemx_comp, fuelconsump_sql_mar5_comp)
