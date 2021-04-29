"""
Get the fuel consumption by Class 1, 3, Amtrak, DART, and TREX.
"""

import os
import inflection
import pandas as pd
import numpy as np
from locoerlt.utilis import (
    PATH_RAW,
    PATH_INTERIM,
    get_out_file_tsmp,
    cleanup_prev_output,
    read_shapefile,
)


def preprc_link(
    path_natrail2020_: str,
    path_rail_carrier_grp_: str,
    path_fill_missing_yardnames_: str,
    map_rrgrp_: dict,
    trk_right_cols=(
        "rrowner1",
        "rrowner2",
        "rrowner3",
        "trkrghts1",
        "trkrghts2",
        "trkrghts3",
        "trkrghts4",
        "trkrghts5",
        "trkrghts6",
        "trkrghts7",
        "trkrghts8",
        "trkrghts9",
    ),
    filter_st=("TX",),
    filter_rrgrp=("Freight", "Industrial", "Yard"),
    map_friylab={"Freight": "Fcat", "Industrial": "IYcat", "Yard": "IYcat"},
) -> pd.DataFrame:
    """
    Pre-process national rail link data.

    path_natrail2020_:
        Path to national rail dataset.
    path_rail_carrier_grp_:
        Rail carrier with tag: Class 1, 3, Commuter, or Passenger
    path_fill_missing_yardnames_:
        Path to excel file with manually filled yardnames.
    map_rrgrp_:
        xwalk b/w national rail link classifiers and rail groups:
        freight, industrial, and yard
    filter_st:
        Filter state: Texas for this study.
    filter_rrgrp:
        Rail road groups that need to be included in this study.
    map_friylab:
        Line haul fuel consumption  uses freight and industrial networks.
        and yard switching fuel consumption uses yard network.
    Returns
    -------
    pd.DataFrame
        Processed national rail dataset.
    """
    natrail2020 = pd.read_csv(path_natrail2020_)
    missing_yardnames = pd.read_excel(path_fill_missing_yardnames_)
    rail_carrier_grp = pd.read_csv(path_rail_carrier_grp_, index_col=0)
    natrail2020_1 = natrail2020.rename(
        columns={col: inflection.underscore(col) for col in natrail2020.columns}
    ).assign(rr_netgrp=lambda df: df.net.map(map_rrgrp_))

    strail_2020 = natrail2020_1.loc[
        lambda df: (df.stateab.isin(filter_st)) & (df.rr_netgrp.isin(filter_rrgrp))
    ].assign(all_oper=lambda df: df[list(trk_right_cols)].apply(set, axis=1))
    strail_2020_preprocess = (
        strail_2020.explode("all_oper")
        .dropna(subset=["all_oper"])
        .drop(columns=list(trk_right_cols))
        .loc[lambda df: df.all_oper != "NS"]  # Based on Madhu's SQL code
        .rename(columns={"all_oper": "carrier"})
        .merge(missing_yardnames, on=["fraarcid", "net"], how="left")
        .assign(
            yardname=lambda df: np.select(
                [~df.yardname_filled_arcmap.isna(), df.yardname_filled_arcmap.isna()],
                [df.yardname_filled_arcmap, df.yardname],
                np.nan,
            ),
            carrier=lambda df: df.carrier.replace(regex=r"^TRE$", value="TREX"),
            friylab=lambda df: df.rr_netgrp.map(map_friylab),
        )
        .loc[
            lambda df: (~df.carrier.isin(["DART", "AMTK"]))
            | ((df.carrier == "DART") & (df.stcntyfips == 48121))
            | ((df.carrier == "AMTK") & (df.rr_netgrp == "Freight"))
        ]  # 1. DART uses fuel only in Denton county. It uses electric engine in
        # Dallas. 48121: Denton County FIPS code
        # 2. Only use freight network for Amtrak
        .merge(rail_carrier_grp, on="carrier", how="left")
    )
    assert (
        len(
            strail_2020_preprocess.loc[lambda df: (df.net == "Y") & (df.yardname == "")]
        )
        == 0
    ), "Check why there are missing yardnames after imputation."
    return strail_2020_preprocess


def preprc_fuelusg(path_fueluserail2019_: str) -> pd.DataFrame:
    """
    Function to process 2019 statewide fuel usage data.

    path_fueluserail2019_:
        Path to fuel use in 2019 for 3 class 1, 2 commuter, 1 passenger, and 55
        class 3 carriers.
    Returns
    -------
    pandas.DataFrame
        Fuel use data in long format.
    """
    fueluserail2019 = pd.read_csv(path_fueluserail2019_)
    fueluse_col_map = {
        "RRCarrier": "carrier",
        "LineHaul": "st_fuel_consmp_Fcat",
        "Yard": "st_fuel_consmp_IYcat",
    }
    fueluserail2019_preprc = pd.wide_to_long(
        df=(
            fueluserail2019.rename(columns=fueluse_col_map).filter(
                fueluse_col_map.values()
            )
        ),
        stubnames="st_fuel_consmp",
        i="carrier",
        j="friylab",
        sep="_",
        suffix=r"\w+",
    ).reset_index()
    return fueluserail2019_preprc


def get_class_1_freight_fuel_consump(
    fueluse2019_preprc_: pd.DataFrame,
    strail_2020_preprocess_: pd.DataFrame,
    path_cls1_cntpct_: str,
    cls1_carriers_=("BNSF", "KCS", "UP"),
) -> pd.DataFrame:
    """
    Split the 2019 statewide class 1 fuel usage for freight to county by county
    fuel usage.

    fueluse2019_preprc_:
        2019 pre-processed fuel usage data in long format.
    strail_2020_preprocess_
        Filtered rail network data with additional fields.
    path_cls1_cntpct_:
        Path to class 1 county fuel usage distributions.
    cls1_carriers:
        Class 1 carriers.
    Returns
    -------
    pd.DataFrame
        Class 1 (BNSF, KCS, and UP) fuel usage by county and individual link.
    """
    cls1_cntpct = pd.read_csv(path_cls1_cntpct_)
    cls1_cntpct["carrier"] = [cls1_carriers_] * len(cls1_cntpct)
    cls1_cntpct_prc = cls1_cntpct.explode("carrier")
    fueluse2019_preprc_cls1_freight = (
        fueluse2019_preprc_.loc[
            lambda df: (
                (df.carrier.isin(list(cls1_carriers_))) & (df.friylab.isin(["Fcat"]))
            )
        ]
        .assign(
            st_fuel_consmp_all_cls1=lambda df: (
                df.groupby("friylab").st_fuel_consmp.transform(sum)
            ),
        )
        .merge(cls1_cntpct_prc, on="carrier", how="inner")
        .rename(
            columns={col: inflection.underscore(col) for col in cls1_cntpct_prc.columns}
        )
        .assign(
            # TODO: There are some missing counties in NatRail data that are
            #  getting implicitly removed. This cause the total Texas wide
            #  fuel consumption to become lower than what it actually is.
            #  Need to use TransCAD or some other software to recomute county
            #  mix. We are not reporting by individual carrier for TCEQ
            #  reporting, so we can keep this bug in the code till June 1, 2021.
            cnty_cls1_all_fuel_consmp=lambda df: df.st_fuel_consmp_all_cls1
            * df.county_pct,
            stcntyfips=lambda df: df.fips.astype(int),
        )
        .drop(columns="fips")
    )

    def get_county_milemix(strail_2020_preprocess__=strail_2020_preprocess_):
        """Get the county level milemix by carrier."""
        strail_2020_preprocess__1 = strail_2020_preprocess__.loc[
            lambda df: (
                (df.carrier.isin(list(cls1_carriers_))) & (df.friylab.isin(["Fcat"]))
            )
        ].assign(
            stcntyfips=lambda df: df.stcntyfips.astype(int),
            totnetmiles=lambda df: df.groupby(
                ["stcntyfips", "friylab"]
            ).miles.transform(sum),
            milemx=lambda df: df.miles / df.totnetmiles,
        )
        assert (
            strail_2020_preprocess__1.groupby(["stcntyfips", "friylab"])
            .milemx.sum()
            .mean()
            == 1
        )
        return strail_2020_preprocess__1

    strail_2020_preprocess_1 = get_county_milemix()

    fueluse2019_preprc_cls1_freight_milemx = strail_2020_preprocess_1.merge(
        fueluse2019_preprc_cls1_freight,
        on=["stcntyfips", "carrier", "friylab"],
        how="left",
    ).assign(link_fuel_consmp=lambda df: df.milemx * df.cnty_cls1_all_fuel_consmp)

    assert fueluse2019_preprc_cls1_freight_milemx.link_fuel_consmp.isna().sum() == 0, (
        "County fuel distribution data has counties with a distribution "
        "value, whereas the national rail data has no rail miles in that "
        "county. Do more testing on the two dataset to figure out "
        "discrepancies between the two. "
    )
    return fueluse2019_preprc_cls1_freight_milemx


def get_cls1_yard_cls1_indus_cls3_passenger_commuter_fuel_consump(
    fueluse2019_preprc_: pd.DataFrame,
    strail_2020_preprocess_: pd.DataFrame,
    cls1_carriers_=("BNSF", "KCS", "UP"),
) -> pd.DataFrame:
    """
    Use the national rail data to distribute the fuel consumption across
    different counties.

    fueluse2019_preprc_:
        2019 pre-processed fuel usage data in long format.
    strail_2020_preprocess_
        Filtered rail network data with additional fields.
    cls1_carriers:
        Class 1 carriers.
    Returns
    -------
    pd.DataFrame
        Class 3, commuter, and passenger train fuel usage by county  and
        individual link.
    """

    fueluse2019_preprc_cls1_yi_cls3_comut_pasng_milemx = (
        strail_2020_preprocess_.loc[
            lambda df: (
                (~df.carrier.isin(list(cls1_carriers_)))
                | (
                    (df.carrier.isin(list(cls1_carriers_)))
                    & (df.friylab.isin(["IYcat"]))
                )
            )
        ]
        .merge(fueluse2019_preprc_, on=["carrier", "friylab"], how="left")
        .loc[lambda df: ~df.st_fuel_consmp.isna()]
        # Removes DART's industrial and Yard rows. They have null fuel
        # consumption.
        .assign(
            totnetmiles=lambda df: df.groupby(["friylab", "carrier"]).miles.transform(
                sum
            ),
            milemx=lambda df: df.miles / df.totnetmiles,
            link_fuel_consmp=lambda df: df.milemx * df.st_fuel_consmp,
        )
        .reset_index(drop=True)
    )
    return fueluse2019_preprc_cls1_yi_cls3_comut_pasng_milemx


def get_fuel_consmp_by_cnty_carrier(
    path_natrail2020_: str,
    path_rail_carrier_grp_: str,
    path_fill_missing_yardnames_: str,
    path_fueluserail2019_: str,
    path_cls1_cntpct_: str,
    map_rrgrp_: dict,
    cls1_carriers_=("BNSF", "KCS", "UP"),
    filter_st=("TX",),
) -> pd.DataFrame:
    """
    Use statewide fuel usage, proportion of fuel usage by county and national
    rail network data to allocate fuel usage by counties.
    Will use the county percentage data for class 1 carriers + freight to
    allocate county percentages.
    Will use mile mix to allocate the fuel consumption for class 1 yards and
    class 3 carriers,
    passenger, and commuter trains.

    Parameters
    ----------
    path_fill_missing_yardnames_:
        Path to excel file with manually filled yardnames.
    map_rrgrp_
        xwalk b/w national rail link classifiers and rail groups:
        freight, industrial, and yard
    path_cls1_cntpct_
        Path to data with county mix to distribute statewide fuel usage for
        class 1 carriers to individual county.
    path_fueluserail2019_
        Path to statewide fuel usage by carriers.
    path_rail_carrier_grp_
        Rail carrier with tag: Class 1, 3, Commuter, or Passenger
    path_natrail2020_
        Path to national rail dataset.
    cls1_carriers_
        Class 1 carriers in Texas.
    filter_st:
        Filter state: Texas for this study.

    Returns
    -------
    pd.DataFrame
        Comibined class 1, 3, commuter, and passenger dataframe.
    """
    # Test Functions
    txrail_2020_preprs = preprc_link(
        path_natrail2020_=path_natrail2020_,
        path_rail_carrier_grp_=path_rail_carrier_grp_,
        path_fill_missing_yardnames_=path_fill_missing_yardnames_,
        map_rrgrp_=map_rrgrp_,
        filter_st=filter_st,
    )
    fueluse2019_preprc = preprc_fuelusg(path_fueluserail2019_=path_fueluserail2019_)
    txrail_milemx_cls1_19 = get_class_1_freight_fuel_consump(
        fueluse2019_preprc_=fueluse2019_preprc,
        strail_2020_preprocess_=txrail_2020_preprs,
        path_cls1_cntpct_=path_cls1_cntpct_,
        cls1_carriers_=cls1_carriers_,
    )
    txrail_milemx_cls3_comut_pasng_19 = (
        get_cls1_yard_cls1_indus_cls3_passenger_commuter_fuel_consump(
            fueluse2019_preprc_=fueluse2019_preprc,
            strail_2020_preprocess_=txrail_2020_preprs,
            cls1_carriers_=cls1_carriers_,
        )
    )
    txrail_milemx_cls_1_3_comut_pasng_19 = pd.concat(
        [
            txrail_milemx_cls1_19,
            txrail_milemx_cls3_comut_pasng_19.assign(county_pct=np.nan),
        ]
    )
    txrail_milemx_cls_1_3_comut_pasng_19

    return txrail_milemx_cls_1_3_comut_pasng_19


if __name__ == "__main__":
    # Define common variables
    cls1_carriers = ("BNSF", "KCS", "UP")
    st = get_out_file_tsmp()
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

    # Define paths
    # path_natrail2020 = os.path.join(
    #     PATH_RAW, "North_American_Rail_Lines", "North_American_Rail_Lines.shp"
    # )
    # natrail_shp = read_shapefile(path_natrail2020)
    path_natrail2020_csv = os.path.join(PATH_INTERIM, "North_American_Rail_Lines.csv")
    # natrail_shp.to_csv(path_natrail2020_csv)
    path_fill_missing_yardnames = os.path.join(
        PATH_INTERIM,
        "gis_debugging",
        "north_america_rail_2021",
        "filled_missing_yards.xlsx",
    )
    path_cls1_cntpct = os.path.join(PATH_RAW, "2019CountyPct.csv")
    path_fueluserail2019 = os.path.join(PATH_RAW, "RR_2019FuelUsage.csv")
    path_rail_carrier_grp = os.path.join(PATH_RAW, "rail_carrier_grp2020.csv")
    path_sql_df = os.path.join(PATH_INTERIM, "testing", "fuelCompOutMar5.csv")
    path_out_fuel_consump = os.path.join(PATH_INTERIM, f"fuelconsump_2019_tx_{st}.csv")
    path_out_fuel_consump_pat = os.path.join(
        PATH_INTERIM, r"fuelconsump_2019_tx_*-*-*.csv"
    )
    cleanup_prev_output(path_out_fuel_consump_pat)
    # Read Datasets
    fuelconsump_sql_mar5 = pd.read_csv(path_sql_df)
    # https://proceedings.esri.com/library/userconf/proc15/papers/402_174.pdf

    # Test Functions
    txrail_2020_preprs = preprc_link(
        path_natrail2020_=path_natrail2020_csv,
        path_rail_carrier_grp_=path_rail_carrier_grp,
        path_fill_missing_yardnames_=path_fill_missing_yardnames,
        map_rrgrp_=map_rrgrp,
    )

    fueluse2019_preprc = preprc_fuelusg(path_fueluserail2019_=path_fueluserail2019)

    txrail_milemx_cls1_19 = get_class_1_freight_fuel_consump(
        fueluse2019_preprc_=fueluse2019_preprc,
        strail_2020_preprocess_=txrail_2020_preprs,
        path_cls1_cntpct_=path_cls1_cntpct,
        cls1_carriers_=cls1_carriers,
    )

    txrail_milemx_cls3_comut_pasng_19 = (
        get_cls1_yard_cls1_indus_cls3_passenger_commuter_fuel_consump(
            fueluse2019_preprc_=fueluse2019_preprc,
            strail_2020_preprocess_=txrail_2020_preprs,
            cls1_carriers_=cls1_carriers,
        )
    )

    txrail_milemx_cls_1_3_comut_pasng_19 = get_fuel_consmp_by_cnty_carrier(
        path_natrail2020_=path_natrail2020_csv,
        path_rail_carrier_grp_=path_rail_carrier_grp,
        path_fueluserail2019_=path_fueluserail2019,
        path_cls1_cntpct_=path_cls1_cntpct,
        path_fill_missing_yardnames_=path_fill_missing_yardnames,
        map_rrgrp_=map_rrgrp,
    )

    txrail_milemx_cls_1_3_comut_pasng_19["year"] = 2019
    txrail_milemx_cls_1_3_comut_pasng_19.to_csv(path_or_buf=path_out_fuel_consump)
