"""
Tests fuelcsmp module.
"""
import os
import pytest
import inflection
import pandas as pd
import numpy as np
from locoerlt.utilis import PATH_RAW, PATH_INTERIM
from locoerlt.fuelcsmp import (
    get_fuel_consmp_by_cnty_carrier,
    preprc_fuelusg
)


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
path_natrail2020 = os.path.join(
    PATH_RAW, "North_American_Rail_Lines", "North_American_Rail_Lines.shp"
)
path_cls1_cntpct = os.path.join(PATH_RAW, "2019CountyPct.csv")
path_fueluserail2019 = os.path.join(PATH_RAW, "RR_2019FuelUsage.csv")
path_rail_carrier_grp = os.path.join(PATH_RAW, "rail_carrier_grp2020.csv")
path_sql_df = os.path.join(PATH_INTERIM, "testing", "fuelCompOutMar5.csv")


@pytest.fixture()
def fueluserail2019_input_df():
    return preprc_fuelusg(path_fueluserail2019)


@pytest.fixture()
def get_cls1_cls3_comm_passg_py_df():
    txrail_milemx_cls_1_3_comut_pasng_19 = get_fuel_consmp_by_cnty_carrier(
        path_natrail2020_=path_natrail2020,
        path_rail_carrier_grp_=path_rail_carrier_grp,
        path_fueluserail2019_=path_fueluserail2019,
        path_cls1_cntpct_=path_cls1_cntpct,
        map_rrgrp_=map_rrgrp,
        filter_st=("TX",),
    )
    return txrail_milemx_cls_1_3_comut_pasng_19


@pytest.fixture()
def get_county_cls1_prop_py_cd(get_cls1_cls3_comm_passg_py_df):
    county_cls1_prop_py_cd = (
        get_cls1_cls3_comm_passg_py_df.loc[lambda df: df.rr_group == "Class I"]
        .groupby(["stcntyfips", "carrier", "friylab"])
        .agg(
            totnetmiles=("totnetmiles", "mean"),
            milemx=("milemx", "sum"),
            st_fuel_consmp=("st_fuel_consmp", "mean"), # FixMe: This does get
            # the actual value of the state fuel consumption from the data.
            cnty_cls1_fuel_consmp=("cnty_cls1_fuel_consmp", "mean"),
            link_fuel_consmp=("link_fuel_consmp", "sum"),
            county_pct=("county_pct", "mean"),
        )
        .reset_index()
    )
    return county_cls1_prop_py_cd


@pytest.fixture()
def get_county_cls1_prop_input():
    cls1_cntpct = pd.read_csv(path_cls1_cntpct)
    return cls1_cntpct


@pytest.fixture()
def get_cls3_comm_passg_py_df(get_cls1_cls3_comm_passg_py_df):
    txrail_milemx_cls_3_comut_pasng_19_comp = (
        get_cls1_cls3_comm_passg_py_df.loc[lambda df: df.rr_group != "Class I"]
        .rename(
            columns={
                "st_fuel_consmp": "totfuelconsump",
                "link_fuel_consmp": "netfuelconsump",
            }
        )
        .filter(
            items=[
                "objectid",
                "miles",
                "rr_netgrp",
                "carrier",
                "rr_group",
                "totfuelconsump",
                "totnetmiles",
                "milemx",
                "netfuelconsump",
            ]
        )
        .sort_values(by=["carrier", "rr_netgrp", "objectid"])
        .reset_index(drop=True)
    )
    return txrail_milemx_cls_3_comut_pasng_19_comp


@pytest.fixture()
def get_cls3_comm_passg_sql_df():
    fuelconsump_sql_mar5 = pd.read_csv(path_sql_df)
    fuelconsump_sql_mar5_comp = (
        fuelconsump_sql_mar5.rename(
            columns={
                col: inflection.underscore(col) for col in fuelconsump_sql_mar5.columns
            }
        )
        .rename(
            columns={
                "rr_netgroup": "rr_netgrp",
                "rr_carrier": "carrier",
                "rr_tot_netmiles": "totnetmiles",
                "rr_net_milemix": "milemx",
                "rr_tot_fuel_consump": "totfuelconsump",
                "rr_net_fuel_consump": "netfuelconsump",
                "shape_leng": "shape_st_len",
            }
        )
        .loc[lambda df: ~df.carrier.isin(list(cls1_carriers))]
        .loc[
            lambda df: (df.carrier != "DART")
            | ((df.carrier == "DART") & (df.stcntyfips == 48121))
        ]
        .assign(
            totfuelconsump=lambda df: df.totfuelconsump.astype(float),
            totnetmiles=lambda df: df.totnetmiles.astype(float),
            milemx=lambda df: df.milemx.astype(float),
            netfuelconsump=lambda df: df.netfuelconsump.astype(float),
        )
        .filter(
            items=[
                "objectid",
                "miles",
                "rr_netgrp",
                "carrier",
                "rr_group",
                "totfuelconsump",
                "totnetmiles",
                "milemx",
                "netfuelconsump",
            ]
        )
        .sort_values(by=["carrier", "rr_netgrp", "objectid"])
        .reset_index(drop=True)
    )
    return fuelconsump_sql_mar5_comp


@pytest.fixture()
def get_carrier_df():
    rail_carrier_grp = pd.read_csv(path_rail_carrier_grp, index_col=0)
    return rail_carrier_grp


def test_py_and_sql_data_eq_for_cls3_pass_commut(
    get_cls3_comm_passg_py_df, get_cls3_comm_passg_sql_df
):
    pd.testing.assert_frame_equal(get_cls3_comm_passg_py_df, get_cls3_comm_passg_sql_df)


def test_all_carriers_in_natrail(get_cls1_cls3_comm_passg_py_df, get_carrier_df):
    carriers_py_cd = (
        get_cls1_cls3_comm_passg_py_df.groupby(["carrier", "rr_group"])[
            "link_fuel_consmp"
        ]
        .sum()
        .reset_index()
        .dropna(subset=["link_fuel_consmp"])
        .drop(columns=["link_fuel_consmp"])
        .sort_values(["rr_group", "carrier"])
        .reset_index(drop=True)
    )
    get_carrier_df_srt = get_carrier_df.sort_values(
        ["rr_group", "carrier"]
    ).reset_index(drop=True)
    pd.testing.assert_frame_equal(carriers_py_cd, get_carrier_df_srt)


def test_milemx_cls1_grp_cnty_1_oth_carrier_grp_st_1(get_cls1_cls3_comm_passg_py_df):
    is_cls1_milemx_cnty_1 = np.allclose(
        (
            get_cls1_cls3_comm_passg_py_df.loc[lambda df: df.rr_group == "Class I"]
            .groupby(["carrier", "friylab", "stcntyfips"])
            .milemx.sum()
            .values
        ),
        1,
    )
    is_notcls1_milemx_st_1 = np.allclose(
        (
            get_cls1_cls3_comm_passg_py_df.loc[lambda df: df.rr_group != "Class I"]
            .groupby(["carrier", "friylab"])
            .milemx.sum()
            .values
        ),
        1,
    )
    assert is_cls1_milemx_cnty_1 & is_notcls1_milemx_st_1


def test_county_cls1_prop_cnt_tots(
    get_county_cls1_prop_py_cd, get_county_cls1_prop_input
):
    county_cls1_prop_py_cd_prcsd = (
        get_county_cls1_prop_py_cd.groupby("stcntyfips")
        .county_pct.mean()
        .reset_index()
        .rename(columns={"stcntyfips": "FIPS", "county_pct": "CountyPCT"})
        .assign(FIPS=lambda df: df.FIPS.astype("int64"))
    )
    pd.testing.assert_frame_equal(
        get_county_cls1_prop_input, county_cls1_prop_py_cd_prcsd
    )


def test_cls1_fuel_consump_tots_by_cnty_by_st(
    get_county_cls1_prop_py_cd, fueluserail2019_input_df
):
    is_cls1_sum_link_fuel_eq_cnty_fuel = np.allclose(
        get_county_cls1_prop_py_cd.cnty_cls1_fuel_consmp,
        get_county_cls1_prop_py_cd.link_fuel_consmp,
    )

    is_cls1_st_fuel_x_cntypct_eq_cnty_fuel = np.allclose(
        (
            get_county_cls1_prop_py_cd.county_pct
            * get_county_cls1_prop_py_cd.st_fuel_consmp
        ),
        get_county_cls1_prop_py_cd.cnty_cls1_fuel_consmp,
    )

    cls1_st_totals_input_df = pd.merge(
        get_county_cls1_prop_py_cd, fueluserail2019_input_df, on=["carrier", "friylab"]
    )
    is_cls1_input_fuel_eq_st_fuel = np.allclose(
        cls1_st_totals_input_df.st_fuel_consmp_x,
        cls1_st_totals_input_df.st_fuel_consmp_y,
    )

    assert (
        is_cls1_sum_link_fuel_eq_cnty_fuel
        & is_cls1_st_fuel_x_cntypct_eq_cnty_fuel
        & is_cls1_input_fuel_eq_st_fuel
    )


def test_notcls1_fuel_consump_tots_by_by_st(
    get_cls3_comm_passg_py_df, fueluserail2019_input_df
):
    cls3_pass_comut_st_totals = (
        get_cls3_comm_passg_py_df.rename(
            columns={
                "totfuelconsump": "st_fuel_consmp",
                "netfuelconsump": "link_fuel_consmp",
            }
        )
        .assign(
            friylab=lambda df: df.rr_netgrp.map(
                {"Freight": "Fcat", "Industrial": "IYcat", "Yard": "IYcat"}
            ),
        )
        .groupby(["carrier", "friylab"])
        .agg(
            link_fuel_consmp=("link_fuel_consmp", "sum"),
            st_fuel_consmp=("st_fuel_consmp", "mean"),
        )
        .reset_index()
    )

    is_notcls1_sum_link_fuel_eq_st_fuel = np.allclose(
        cls3_pass_comut_st_totals.link_fuel_consmp,
        cls3_pass_comut_st_totals.st_fuel_consmp,
    )

    cls3_pass_comut_st_totals_input_df = pd.merge(
        cls3_pass_comut_st_totals, fueluserail2019_input_df, on=["carrier", "friylab"]
    )
    is_notcls1_input_fuel_eq_st_fuel = np.allclose(
        cls3_pass_comut_st_totals_input_df.st_fuel_consmp_x,
        cls3_pass_comut_st_totals_input_df.st_fuel_consmp_y,
    )

    assert is_notcls1_sum_link_fuel_eq_st_fuel & is_notcls1_input_fuel_eq_st_fuel
