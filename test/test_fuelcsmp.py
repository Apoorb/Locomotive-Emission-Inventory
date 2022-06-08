"""
Tests fuelcsmp module.
"""
import os
import pytest
import inflection
import pandas as pd
import numpy as np
from locoerlt.utilis import PATH_RAW, PATH_INTERIM
from locoerlt.fuelcsmp import get_fuel_consmp_by_cnty_carrier, preprc_fuelusg


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
path_fill_missing_yardnames = os.path.join(
    PATH_INTERIM,
    "gis_debugging",
    "north_america_rail_2021",
    "filled_missing_yards.xlsx",
)
path_natrail2020_csv = os.path.join(PATH_INTERIM, "North_American_Rail_Lines.csv")
path_natrail2020_old = os.path.join(PATH_RAW, "archive", "NatRail_2020.csv")
path_cls1_cntpct = os.path.join(PATH_RAW, "2019CountyPct.csv")
path_fueluserail2019 = os.path.join(PATH_RAW, "RR_2019FuelUsage.csv")
path_rail_carrier_grp = os.path.join(PATH_RAW, "rail_carrier_grp2020.csv")
path_sql_df = os.path.join(PATH_INTERIM, "testing", "fuelCompOutMar5.csv")


@pytest.fixture()
def fueluserail2019_input_df():
    """Get the raw 2019 fuel usage. This is the reference for all tests."""
    return preprc_fuelusg(path_fueluserail2019)


@pytest.fixture()
def get_cls1_cls3_comm_passg_py_df(request):
    """Get the fuel usage by carriers and at link-level."""
    txrail_milemx_cls_1_3_comut_pasng_19 = get_fuel_consmp_by_cnty_carrier(
        path_natrail2020_=request.param,
        path_rail_carrier_grp_=path_rail_carrier_grp,
        path_fueluserail2019_=path_fueluserail2019,
        path_cls1_cntpct_=path_cls1_cntpct,
        path_fill_missing_yardnames_=path_fill_missing_yardnames,
        map_rrgrp_=map_rrgrp,
        filter_st=("TX",),
    )
    return txrail_milemx_cls_1_3_comut_pasng_19


@pytest.fixture()
def get_county_cls1_freight_prop_py_cd(get_cls1_cls3_comm_passg_py_df):
    """Recompute class 1 fuel usage at statelevel by carrier."""
    county_cls1_freight_prop_py_cd = (
        get_cls1_cls3_comm_passg_py_df.loc[
            lambda df: (df.rr_group == "Class I") & (df.friylab == "Fcat")
        ]
        .groupby(["stcntyfips", "carrier", "friylab"])
        .agg(
            totnetmiles=("totnetmiles", "mean"),
            milemx=("milemx", "sum"),
            st_fuel_consmp_by_cls1=("st_fuel_consmp", "mean"),
            st_fuel_consmp_all_cls1=("st_fuel_consmp_all_cls1", "mean"),
            cnty_fuel_consmp_all_cls1=("cnty_cls1_all_fuel_consmp", "mean"),
            cnty_fuel_consmp_by_cls1=("link_fuel_consmp", "sum"),
            county_pct=("county_pct", "mean"),
        )
        .reset_index()
    )
    county_cls1_freight_prop_py_cd["cnty_fuel_consmp_by_cls1_2"] = (
        county_cls1_freight_prop_py_cd.milemx
        * county_cls1_freight_prop_py_cd.cnty_fuel_consmp_all_cls1
    )

    county_cls1_freight_prop_py_cd_1 = county_cls1_freight_prop_py_cd.assign(
        st_fuel_consmp_by_cls1_estimated=lambda df: (
            df.groupby(["carrier", "friylab"])["cnty_fuel_consmp_by_cls1"].transform(
                sum
            )
        ),
        st_fuel_consmp_all_cls1_estimated=lambda df: (
            df.groupby(["friylab"])["cnty_fuel_consmp_by_cls1"].transform(sum)
        ),
    )

    return county_cls1_freight_prop_py_cd_1


@pytest.fixture()
def get_county_cls1_prop_input():
    """Get county percentages of class 1 fuel distribution."""
    cls1_cntpct = pd.read_csv(path_cls1_cntpct)
    return cls1_cntpct


@pytest.fixture()
def get_cls3_comm_passg_py_df(get_cls1_cls3_comm_passg_py_df):
    """Get class 3, commuter, and passenger train fuel consumption."""
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
def get_carrier_df(request):
    """Get the list of carriers in Texas."""
    filter_out_carriers = request.param
    rail_carrier_grp = pd.read_csv(path_rail_carrier_grp, index_col=0)
    rail_carrier_grp_fil = rail_carrier_grp.loc[
        lambda df: ~df.carrier.isin(filter_out_carriers)
    ]
    return rail_carrier_grp_fil


@pytest.mark.parametrize(
    "get_cls1_cls3_comm_passg_py_df, get_carrier_df",
    [(path_natrail2020_old, ("",)), (path_natrail2020_csv, ("TNMR", "WTLC", "TSE"))],
    ids=["2020 NatRail Data", "2021 NatRail Data"],
    indirect=True,
)
def test_all_carriers_in_natrail(get_cls1_cls3_comm_passg_py_df, get_carrier_df):
    """Test that NATRAIL has all carriers identified by TTI."""
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

    missing_carriers = set.difference(
        set(get_carrier_df_srt.carrier), set(carriers_py_cd.carrier)
    )
    print(
        "Following carriers (if any) are missing from the NatRail data: "
        f"{missing_carriers}"
    )
    pd.testing.assert_frame_equal(carriers_py_cd, get_carrier_df_srt)


@pytest.mark.parametrize(
    "get_cls1_cls3_comm_passg_py_df",
    [path_natrail2020_csv],
    ids=["2021 NatRail Data"],
    indirect=True,
)
def test_milemx_cls1_grp_cnty_1_oth_carrier_grp_st_1(get_cls1_cls3_comm_passg_py_df):
    """
    Test that the milemix for line-haul class 1 carriers is 1 at county level.
    Test that the milemix for line-haul non-class 1 carriers is 1 at state level.
    """
    is_cls1_milemx_cnty_1 = np.allclose(
        (
            get_cls1_cls3_comm_passg_py_df.loc[
                lambda df: (df.rr_group == "Class I") & (df.friylab == "Fcat")
            ]
            .groupby(["friylab", "stcntyfips"])
            .milemx.sum()
            .values
        ),
        1,
    )
    is_notcls1freight_milemx_st_1 = np.allclose(
        (
            get_cls1_cls3_comm_passg_py_df.loc[
                lambda df: (df.rr_group != "Class I")
                | ((df.rr_group != "Class I") & (df.friylab != "IYcat"))
            ]
            .groupby(["carrier", "friylab"])
            .milemx.sum()
            .values
        ),
        1,
    )
    assert is_cls1_milemx_cnty_1 & is_notcls1freight_milemx_st_1


@pytest.mark.parametrize(
    "get_cls1_cls3_comm_passg_py_df",
    [path_natrail2020_csv],
    ids=["2021 NatRail Data"],
    indirect=True,
)
def test_county_all_cls1_prop_cnt_tots(
    get_county_cls1_freight_prop_py_cd, get_county_cls1_prop_input
):
    """
    Test class 1 output control totals are same as input control totals.
    """
    county_all_cls1_prop_py_cd_prcsd = (
        get_county_cls1_freight_prop_py_cd.assign(
            county_pct_estimated=lambda df: (
                df.cnty_fuel_consmp_all_cls1 / df.st_fuel_consmp_all_cls1_estimated
            )
        )
        .groupby("stcntyfips")
        .agg(CountyPCT=("county_pct_estimated", "mean"))
        .reset_index()
        .rename(columns={"stcntyfips": "FIPS"})
        .assign(FIPS=lambda df: df.FIPS.astype("int64"))
    )
    pd.testing.assert_frame_equal(
        get_county_cls1_prop_input, county_all_cls1_prop_py_cd_prcsd
    )


@pytest.mark.parametrize(
    "get_cls1_cls3_comm_passg_py_df",
    [path_natrail2020_csv],
    ids=["2021 NatRail Data"],
    indirect=True,
)
def test_county_all_cls1_state_tots(get_county_cls1_freight_prop_py_cd):
    """Test the estimated statewide total for class 1 carriers with the input.
    Total class 1 statewide fuel consumption matches between input and output.
     Fuel consumption by carrier at state level does not match as we did not use
     the fuel information by individual carrier when distributing the fuel cons
     umption to different counties. If we distributed the fuel consumption by
     individual carrier to differnt counties we would have counties with no
     track miles for that carrier, causing issue with statewide total."""
    # TODO: Use TransCAD or some other software to allocate fuel to different
    #  counties and class 1 carriers, such that the recomputed fuel for each
    #  carrier at state level matches the observed data. This test should
    #  fail as of 4/16/2021.
    st_estimated_all = (
        get_county_cls1_freight_prop_py_cd.st_fuel_consmp_all_cls1_estimated
    )
    st_observed_all = get_county_cls1_freight_prop_py_cd.st_fuel_consmp_all_cls1
    st_estimated_by_carrier = (
        get_county_cls1_freight_prop_py_cd.st_fuel_consmp_by_cls1_estimated
    )
    st_observed_data_by_carrier = (
        get_county_cls1_freight_prop_py_cd.st_fuel_consmp_by_cls1
    )
    print(
        "Will fail. Use TransCAD or some other software to allocate fuel to "
        "different counties and class 1 carriers, such that the recomputed "
        "fuel for each carrier at state level matches the observed data. "
    )
    will_pass = np.allclose(st_estimated_all, st_observed_all)
    will_fail = not np.allclose(st_estimated_by_carrier, st_observed_data_by_carrier)
    assert will_pass and will_fail


@pytest.mark.parametrize(
    "get_cls1_cls3_comm_passg_py_df",
    [path_natrail2020_csv],
    ids=["2021 NatRail Data"],
    indirect=True,
)
def test_county_all_cls1_state_tots_using_fuel_data(
    get_county_cls1_freight_prop_py_cd, fueluserail2019_input_df
):
    """Test the estimated statewide total for class 1 carriers with the input.
    Total class 1 statewide fuel consumption matches between input and output.
     Fuel consumption by carrier at state level does not match as we did not use
     the fuel information by individual carrier when distributing the fuel cons
     umption to different counties. If we distributed the fuel consumption by
     individual carrier to differnt counties we would have counties with no
     track miles for that carrier, causing issue with statewide total."""
    # TODO: Use TransCAD or some other software to allocate fuel to different
    #  counties and class 1 carriers, such that the recomputed fuel for each
    #  carrier at state level matches the observed data. This test should
    #  fail as of 4/16/2021.
    cls1_st_totals_input_df = pd.merge(
        get_county_cls1_freight_prop_py_cd,
        fueluserail2019_input_df,
        on=["carrier", "friylab"],
    )
    is_cls1_input_fuel_eq_st_fuel_estimated = np.allclose(
        cls1_st_totals_input_df.st_fuel_consmp_by_cls1_estimated,
        cls1_st_totals_input_df.st_fuel_consmp,
    )
    is_cls1_input_fuel_eq_st_fuel_data = np.allclose(
        cls1_st_totals_input_df.st_fuel_consmp_by_cls1,
        cls1_st_totals_input_df.st_fuel_consmp,
    )
    print(
        "Will fail. Use TransCAD or some other software to allocate fuel to "
        "different counties and class 1 carriers, such that the recomputed "
        "fuel for each carrier at state level matches the observed data. "
    )
    will_fail = (
        not is_cls1_input_fuel_eq_st_fuel_estimated & is_cls1_input_fuel_eq_st_fuel_data
    )
    assert will_fail


@pytest.mark.parametrize(
    "get_cls1_cls3_comm_passg_py_df",
    [path_natrail2020_csv],
    ids=["2021 NatRail Data"],
    indirect=True,
)
def test_notcls1_fuel_consump_tots_by_st(
    get_cls3_comm_passg_py_df, fueluserail2019_input_df
):
    """
    Test non-class 1 statewide fuel consumption matches between input and
    output.
    """
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
            )
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
