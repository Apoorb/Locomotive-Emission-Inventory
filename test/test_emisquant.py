"""
Tests emisquant module.
"""
import os
from io import StringIO
import pytest
import glob
import pandas as pd
import numpy as np
from locoerlt.utilis import (
    PATH_RAW,
    PATH_INTERIM,
    PATH_PROCESSED,
    get_out_file_tsmp,
    xwalk_ssc_desc_4_rr_grp_netgrp,
)
from locoerlt.emisquant import process_proj_fac
from locoerlt.fuelcsmp import preprc_fuelusg, preprc_link
from test.test_emisrt import get_nox_pm10_pm25_voc_epa_em_fac, hap_speciation


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
xwalk_ssc_desc_4_rr_grp_netgrp_df_ = pd.read_csv(
    xwalk_ssc_desc_4_rr_grp_netgrp, sep=","
).assign(scc_description_level_4=lambda df: df.scc_description_level_4.str.strip())
path_fill_missing_yardnames = os.path.join(
    PATH_INTERIM,
    "gis_debugging",
    "north_america_rail_2021",
    "filled_missing_yards.xlsx",
)
path_emisquant = glob.glob(
    os.path.join(PATH_PROCESSED, "emis_quant_loco_[0-9]*-*-*.csv")
)[0]
path_emisquant_agg = glob.glob(
    os.path.join(PATH_PROCESSED, "emis_quant_loco_agg_[0-9]*-*-*.csv")
)[0]
path_proj_fac = os.path.join(PATH_INTERIM, "Projection Factors 04132021.xlsx")
path_cls1_cntpct = os.path.join(PATH_RAW, "2019CountyPct.csv")
path_fuel_consump = glob.glob(
    os.path.join(PATH_INTERIM, f"fuelconsump_2019_tx_*-*-*.csv")
)[0]
path_fueluserail2019 = os.path.join(PATH_RAW, "RR_2019FuelUsage.csv")
path_natrail2020_csv = os.path.join(PATH_INTERIM, "North_American_Rail_Lines.csv")
path_rail_carrier_grp = os.path.join(PATH_RAW, "rail_carrier_grp2020.csv")
# ERTAC Class 1 Rates
path_ertac = os.path.join(PATH_INTERIM, "testing", "2019TTIVs2017ERTAC_QAQC.csv")
path_out_qaqc_ertac = os.path.join(
    PATH_PROCESSED, "tti_vs_ertac_cls1_freight_2017.xlsx"
)


@pytest.fixture()
def get_prc_nat_rail():
    return preprc_link(
        path_natrail2020_=path_natrail2020_csv,
        path_rail_carrier_grp_=path_rail_carrier_grp,
        path_fill_missing_yardnames_=path_fill_missing_yardnames,
        map_rrgrp_=map_rrgrp,
    )


@pytest.fixture()
def get_fuel_consump():
    return pd.read_csv(path_fuel_consump, index_col=0)


@pytest.fixture()
def fueluserail2019_input_df():
    return preprc_fuelusg(path_fueluserail2019)


@pytest.fixture()
def get_emis_quant():
    return pd.read_csv(path_emisquant, index_col=0)


@pytest.fixture()
def get_emis_quant_agg_across_carriers():
    return pd.read_csv(path_emisquant_agg, index_col=0)


@pytest.fixture()
def get_ertac_2017_df():
    return pd.read_csv(path_ertac)


@pytest.fixture()
def get_proj_fac():
    return process_proj_fac(path_proj_fac)


@pytest.fixture()
def get_county_cls1_prop_input():
    cls1_cntpct = pd.read_csv(path_cls1_cntpct)
    return cls1_cntpct


def test_ertac_tx_fips_matches_tti_fips():
    ...
    # TODO: Write Test


def test_fuel_consump_in_emis_quant_vs_input_except_yards(
    get_emis_quant_agg_across_carriers, get_fuel_consump
):
    get_emis_quant_agg_across_carriers_county_scc = (
        get_emis_quant_agg_across_carriers.loc[
            lambda df: (df.year == 2019)
            & (df.pollutant == "CO")
            & (df.scc_description_level_4 != "Yard Locomotives")
        ]
        .groupby(["stcntyfips", "scc_description_level_4"])
        .agg(
            county_scc_fuel_consump=("county_carr_friy_yardnm_fuel_consmp_by_yr", "sum")
        )
        .reset_index()
        .sort_values(["stcntyfips", "scc_description_level_4"])
        .reset_index(drop=True)
    )

    get_fuel_consump_county_scc = (
        get_fuel_consump.merge(
            xwalk_ssc_desc_4_rr_grp_netgrp_df_, on=["rr_group", "rr_netgrp"]
        )
        .loc[lambda df: (df.scc_description_level_4 != "Yard Locomotives")]
        .groupby(["stcntyfips", "scc_description_level_4"])
        .agg(county_scc_fuel_consump=("link_fuel_consmp", "sum"))
        .reset_index()
        .sort_values(["stcntyfips", "scc_description_level_4"])
        .reset_index(drop=True)
    )

    test_data = pd.merge(
        get_emis_quant_agg_across_carriers_county_scc,
        get_fuel_consump_county_scc,
        on=["stcntyfips", "scc_description_level_4"],
        suffixes=["_post", "_pre"],
    )

    assert np.allclose(
        test_data.county_scc_fuel_consump_post, test_data.county_scc_fuel_consump_pre
    )


def test_cls1_2017county_fuel_consmp_not_equal_with_ertac(
    get_emis_quant_agg_across_carriers, get_ertac_2017_df
):
    get_emis_quant_agg_cls1_fri_17 = (
        get_emis_quant_agg_across_carriers.loc[
            lambda df: (
                (
                    df.scc_description_level_4.isin(
                        ["Line Haul Locomotives: Class I Operations"]
                    )
                )
                & (df.year == 2017)
            )
        ]
        .groupby(["county_name", "stcntyfips"])
        .county_carr_friy_yardnm_fuel_consmp_by_yr.min()
        .reset_index()
    )

    get_ertac_2017_df_fil = (
        get_ertac_2017_df.drop(columns="2019 TTI")
        .rename(columns={"2017 ERTAC": "ertac_2017"})
        .assign(
            FIPS=lambda df: df.FIPS.astype(float),
            ertac_2017=lambda df: pd.to_numeric(df.ertac_2017, errors="coerce"),
        )
    )
    get_emis_quant_agg_cls1_fri_17_ertac = (
        get_emis_quant_agg_cls1_fri_17.merge(
            get_ertac_2017_df_fil, left_on="stcntyfips", right_on="FIPS", how="outer"
        )
        .rename(columns={"county_carr_friy_yardnm_fuel_consmp_by_yr": "tti_2017"})
        .assign(
            tti_min_ertac=lambda df: np.round(df.tti_2017 - df.ertac_2017, 2),
            per_dif_tti_ertac=lambda df: np.round(
                100 * df.tti_min_ertac / df.ertac_2017, 2
            ),
        )
    )
    get_emis_quant_agg_cls1_fri_17_ertac.to_excel(path_out_qaqc_ertac)
    assert all(
        (
            get_emis_quant_agg_cls1_fri_17_ertac.loc[
                lambda df: df.per_dif_tti_ertac > 0, "per_dif_tti_ertac"
            ].dropna()
        )
        >= 2.17
    )


def test_milemx_tot(get_emis_quant, get_prc_nat_rail):
    county_carr_miles = (
        get_emis_quant.groupby(
            ["year", "stcntyfips", "carrier", "scc_description_level_4", "pollutant"]
        )
        .agg(
            county_carr_friy_yardnm_miles_by_yr=(
                "county_carr_friy_yardnm_miles_by_yr",
                "sum",
            )
        )
        .reset_index()
    )

    # TREX somehow has yard fuel consumption. For reporting it should likely
    # to freight.
    get_prc_nat_rail.loc[lambda df: df.carrier == "TREX", "friylab"] = "Fcat"
    natrail_county_carr_miles = (
        get_prc_nat_rail.merge(
            xwalk_ssc_desc_4_rr_grp_netgrp_df_, on=["rr_group", "rr_netgrp"]
        )
        .groupby(["stcntyfips", "carrier", "scc_description_level_4"])
        .miles.sum()
        .reset_index()
    )
    county_carr_miles_test = county_carr_miles.merge(
        natrail_county_carr_miles,
        on=["stcntyfips", "carrier", "scc_description_level_4"],
    )
    assert np.allclose(
        county_carr_miles_test.county_carr_friy_yardnm_miles_by_yr,
        county_carr_miles_test.miles,
    )


def test_state_fuel_totals(
    get_emis_quant, fueluserail2019_input_df, remove_carriers=("BNSF", "UP", "KCS")
):
    # TODO: Use TransCAD or some other software to allocate fuel to different
    #  counties and class 1 carriers, such that the recomputed fuel for each
    #  carrier at state level matches the observed data. Current hack in to
    #  let the state totals by carriers not match the observed value.
    st_carr_fuel_consump = (
        get_emis_quant.loc[
            lambda df: (df.year == 2019) & (~df.carrier.isin(remove_carriers))
        ]
        .merge(xwalk_ssc_desc_4_rr_grp_netgrp_df_, on=["rr_group", "rr_netgrp"])
        .groupby(["year", "carrier", "friylab", "pollutant"])
        .agg(st_fuel_by_carr_act=("county_carr_friy_yardnm_fuel_consmp_by_yr", "sum"))
        .reset_index()
    )

    st_scc_fuel_consump_test = st_carr_fuel_consump.merge(
        fueluserail2019_input_df, on=["carrier", "friylab"]
    )

    assert np.allclose(
        st_scc_fuel_consump_test.st_fuel_by_carr_act,
        st_scc_fuel_consump_test.st_fuel_consmp,
    )


def test_county_control_tot_equal_to_freight_cls1(
    get_emis_quant_agg_across_carriers, get_county_cls1_prop_input
):
    get_emis_quant_agg_across_carriers[
        "st_em_quant_by_yr"
    ] = get_emis_quant_agg_across_carriers.groupby(
        ["year", "scc_description_level_4", "pollutant"]
    ).em_quant.transform(
        sum
    )
    get_emis_quant_agg_cls1 = get_emis_quant_agg_across_carriers.loc[
        lambda df: (
            df.scc_description_level_4.isin(
                ["Line Haul Locomotives: Class I Operations"]
            )
        )
    ].assign(countypct=lambda df: df.em_quant / df.st_em_quant_by_yr)
    cnt_pct = get_county_cls1_prop_input.rename(columns={"FIPS": "stcntyfips"})
    get_emis_quant_agg_cls1_test = get_emis_quant_agg_cls1.merge(
        cnt_pct, on="stcntyfips"
    )
    assert np.allclose(
        get_emis_quant_agg_cls1_test.CountyPCT, get_emis_quant_agg_cls1_test.countypct
    )


def test_proj_rt_from_emis(get_emis_quant, get_proj_fac):
    # CO2, NH3, and CO have constant rates, so we can get the projection
    # factors from the emission rates for these pollutants.
    co2_nh3_co = get_emis_quant.loc[
        lambda df: (df.pollutant.isin(["CO2", "NH3", "CO"]))
        & (df.scc_description_level_4 != "Yard Locomotives")
    ].assign(
        carrier=lambda df: df.carrier.fillna(-99),
        rr_group=lambda df: df.rr_group.fillna(-99),
    )
    co2_nh3_co_emis_2019 = (
        co2_nh3_co.loc[lambda df: df.year == 2019]
        .drop(columns="year")
        .rename(columns={"em_quant": "em_quant_2019"})
        .filter(
            items=[
                "stcntyfips",
                "carrier",
                "rr_netgrp",
                "rr_group",
                "scc_description_level_4",
                "yardname_v1",
                "pollutant",
                "em_quant_2019",
            ]
        )
    )

    co2_nh3_co_emis_with_2019 = (
        co2_nh3_co.merge(
            co2_nh3_co_emis_2019,
            on=[
                "stcntyfips",
                "carrier",
                "rr_netgrp",
                "rr_group",
                "scc_description_level_4",
                "yardname_v1",
                "pollutant",
            ],
        )
        .assign(proj_fac_calc=lambda df: df.em_quant / df.em_quant_2019)
        .merge(get_proj_fac, on=["year", "rr_group"], how="left")
        .dropna(subset=["proj_fac_calc"])
    )

    mask = ~np.isclose(
        np.round(co2_nh3_co_emis_with_2019.proj_fac_calc, 5),
        np.round(co2_nh3_co_emis_with_2019.proj_fac, 5),
    )
    test = co2_nh3_co_emis_with_2019[mask]

    assert np.allclose(
        np.round(co2_nh3_co_emis_with_2019.proj_fac_calc, 5),
        np.round(co2_nh3_co_emis_with_2019.proj_fac, 5),
    )


def test_co2_fac(get_emis_quant_agg_across_carriers):
    co2_emis_fac = get_emis_quant_agg_across_carriers.loc[
        lambda df: df.pollutant == "CO2"
    ]
    co2_emis_fac["emis_fac_estim"] = (
        co2_emis_fac.em_quant / co2_emis_fac.county_carr_friy_yardnm_fuel_consmp_by_yr
    )
    test_co2_estim = co2_emis_fac.dropna(subset=["emis_fac_estim"])
    estim_close_to_coded = np.allclose(
        test_co2_estim.emis_fac_estim, test_co2_estim.em_fac
    )
    emis_fac_equal_intended = all(co2_emis_fac.em_fac == 2778 * 0.99 * (44 / 12))
    assert estim_close_to_coded and emis_fac_equal_intended


def test_so2_fac(get_emis_quant_agg_across_carriers):
    so2_emis_fac = get_emis_quant_agg_across_carriers.loc[
        lambda df: df.pollutant == "SO2"
    ]
    so2_emis_fac["emis_fac_estim"] = (
        so2_emis_fac.em_quant / so2_emis_fac.county_carr_friy_yardnm_fuel_consmp_by_yr
    )
    test_so2_estim = so2_emis_fac.dropna(subset=["emis_fac_estim"])
    estim_close_to_coded = np.allclose(
        test_so2_estim.emis_fac_estim, test_so2_estim.em_fac
    )
    so2_emis_fac_2011 = so2_emis_fac[so2_emis_fac.year == 2011]
    so2_emis_fac_2012_50 = so2_emis_fac[so2_emis_fac.year >= 2012]
    so2_2011_500_ppm_s = (0.1346 * 23809.5) * 0.97 * (64 / 32) * 500 * 1e-6
    so2_2012_50_15_ppm_s = (0.1346 * 23809.5) * 0.97 * (64 / 32) * 15 * 1e-6
    emis_fac_equal_intended = all(
        so2_emis_fac_2011.em_fac == so2_2011_500_ppm_s
    ) & np.allclose(so2_emis_fac_2012_50.em_fac, so2_2012_50_15_ppm_s)
    assert estim_close_to_coded and emis_fac_equal_intended


def test_nh3_fac(get_emis_quant_agg_across_carriers):
    nh3_emis_fac = get_emis_quant_agg_across_carriers.loc[
        lambda df: df.pollutant == "NH3"
    ]
    nh3_emis_fac["emis_fac_estim"] = (
        nh3_emis_fac.em_quant / nh3_emis_fac.county_carr_friy_yardnm_fuel_consmp_by_yr
    )
    test_nh3_estim = nh3_emis_fac.dropna(subset=["emis_fac_estim"])
    estim_close_to_coded = np.allclose(
        test_nh3_estim.emis_fac_estim, test_nh3_estim.em_fac
    )
    emis_fac_equal_intended = all(nh3_emis_fac.em_fac == 1.83e-04 * 453.592)
    assert estim_close_to_coded and emis_fac_equal_intended


def test_co_fac(get_emis_quant_agg_across_carriers):
    co_emis_fac = get_emis_quant_agg_across_carriers.loc[
        lambda df: df.pollutant == "CO"
    ]
    co_emis_fac["emis_fac_estim"] = (
        co_emis_fac.em_quant / co_emis_fac.county_carr_friy_yardnm_fuel_consmp_by_yr
    )
    test_co_estim = co_emis_fac.dropna(subset=["emis_fac_estim"])
    estim_close_to_coded = np.allclose(
        test_co_estim.emis_fac_estim, test_co_estim.em_fac
    )
    co_emis_fac_cls1_passng_comm = co_emis_fac.loc[
        lambda df: df.scc_description_level_4.isin(
            [
                "Line Haul Locomotives: Class I Operations",
                "Line Haul Locomotives: Passenger Trains (Amtrak)",
                "Line Haul Locomotives: Commuter Lines",
            ]
        )
    ]
    co_emis_fac_cls3 = co_emis_fac.loc[
        lambda df: df.scc_description_level_4.isin(
            ["Line Haul Locomotives: Class II / III Operations"]
        )
    ]
    co_emis_fac_yard = co_emis_fac.loc[
        lambda df: df.scc_description_level_4.isin(["Yard Locomotives"])
    ]

    emis_fac_equal_intended = (
        np.allclose(co_emis_fac_cls1_passng_comm.em_fac, 1.28 * 20.8)
        & np.allclose(co_emis_fac_cls3.em_fac, 1.28 * 18.2)
        & np.allclose(co_emis_fac_yard.em_fac, 1.83 * 15.2)
    )
    assert emis_fac_equal_intended and estim_close_to_coded


def test_nox_pm10_pm25_voc_epa_em_fac_2011_40(
    get_emis_quant_agg_across_carriers, get_nox_pm10_pm25_voc_epa_em_fac
):
    nox_pm10_pm25_voc_2011_40 = get_emis_quant_agg_across_carriers.loc[
        lambda df: (df.pollutant.isin(["NOX", "PM10-PRI", "PM25-PRI", "VOC"]))
        & (df.year < 2040),
        [
            "year",
            "pollutant",
            "scc_description_level_4",
            "em_fac",
            "em_quant",
            "county_carr_friy_yardnm_fuel_consmp_by_yr",
        ],
    ].reset_index(drop=True)
    nox_pm10_pm25_voc_2011_40["emis_fac_estim"] = (
        nox_pm10_pm25_voc_2011_40.em_quant
        / nox_pm10_pm25_voc_2011_40.county_carr_friy_yardnm_fuel_consmp_by_yr
    )
    test_nox_pm10_pm25_voc_estim = nox_pm10_pm25_voc_2011_40.dropna(
        subset=["emis_fac_estim"]
    )
    estim_close_to_coded = np.allclose(
        test_nox_pm10_pm25_voc_estim.emis_fac_estim, test_nox_pm10_pm25_voc_estim.em_fac
    )
    testdf = pd.merge(
        nox_pm10_pm25_voc_2011_40,
        get_nox_pm10_pm25_voc_epa_em_fac,
        left_on=["year", "pollutant", "scc_description_level_4"],
        right_on=["year", "pollutant", "scc_description_level_4"],
        suffixes=("_epa2009", "_emisfacout"),
    )
    emis_fac_equal_intended = np.allclose(
        testdf.em_fac_epa2009, testdf.em_fac_emisfacout
    )
    assert emis_fac_equal_intended and estim_close_to_coded


def test_nox_pm10_pm25_voc_epa_em_fac_2040_50(
    get_emis_quant_agg_across_carriers, get_nox_pm10_pm25_voc_epa_em_fac
):
    nox_pm10_pm25_voc_2040_50 = get_emis_quant_agg_across_carriers.loc[
        lambda df: (df.pollutant.isin(["NOX", "PM10-PRI", "PM25-PRI", "VOC"]))
        & (df.year >= 2040),
        [
            "year",
            "pollutant",
            "scc_description_level_4",
            "em_fac",
            "em_quant",
            "county_carr_friy_yardnm_fuel_consmp_by_yr",
        ],
    ].reset_index(drop=True)
    nox_pm10_pm25_voc_2040_50["emis_fac_estim"] = (
        nox_pm10_pm25_voc_2040_50.em_quant
        / nox_pm10_pm25_voc_2040_50.county_carr_friy_yardnm_fuel_consmp_by_yr
    )
    test_nox_pm10_pm25_voc_estim = nox_pm10_pm25_voc_2040_50.dropna(
        subset=["emis_fac_estim"]
    )
    estim_close_to_coded = np.allclose(
        test_nox_pm10_pm25_voc_estim.emis_fac_estim, test_nox_pm10_pm25_voc_estim.em_fac
    )
    testdf = pd.merge(
        nox_pm10_pm25_voc_2040_50,
        get_nox_pm10_pm25_voc_epa_em_fac,
        left_on=["year", "pollutant", "scc_description_level_4"],
        right_on=["year", "pollutant", "scc_description_level_4"],
        suffixes=("_epa2009", "_emisfacout"),
        how="left",
    )
    testdf["em_fac_emisfacout"] = testdf.groupby(
        ["pollutant", "scc_description_level_4"]
    ).em_fac_emisfacout.ffill()
    emis_fac_equal_intended = np.allclose(
        testdf.em_fac_epa2009, testdf.em_fac_emisfacout
    )

    assert emis_fac_equal_intended and estim_close_to_coded


def test_lead_speciation(get_emis_quant_agg_across_carriers):
    pm10_fac = (
        get_emis_quant_agg_across_carriers.loc[
            lambda df: (df.pollutant.isin(["PM10-PRI"]))
        ]
        .filter(items=["year", "pollutant", "scc_description_level_4", "em_fac"])
        .reset_index(drop=True)
        .rename(columns={"em_fac": "em_fac_input", "pollutant": "input_pollutant_code"})
        .assign(
            pollutant=str(7439921),
            pollutant_desc="Lead",
            em_fac=lambda df: df.em_fac_input * 8.405e-05,
        )
    )

    test_df = get_emis_quant_agg_across_carriers.merge(
        pm10_fac,
        left_on=["year", "scc_description_level_4", "pollutant"],
        right_on=["year", "scc_description_level_4", "pollutant"],
    )
    test_df["emis_fac_estim"] = (
        test_df.em_quant / test_df.county_carr_friy_yardnm_fuel_consmp_by_yr
    )
    test_lead_estim = test_df.dropna(subset=["emis_fac_estim"])
    estim_close_to_coded = np.allclose(
        test_lead_estim.emis_fac_estim, test_lead_estim.em_fac_x
    )
    emis_fac_equal_intended = np.allclose(test_df.em_fac_y, test_df.em_fac_x)
    assert estim_close_to_coded and emis_fac_equal_intended


def test_all_year_present(get_emis_quant_agg_across_carriers):
    are_there_40_years_in_each_group = all(
        (
            get_emis_quant_agg_across_carriers.groupby(
                ["county_name", "scc", "yardname_v1", "pollutant"]
            ).year.count()
        ).values
        == (2050 - 2011) + 1
    )
    assert are_there_40_years_in_each_group
