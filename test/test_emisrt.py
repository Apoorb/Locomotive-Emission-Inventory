"""
Tests emisrt module.
"""
import os
import pytest
import pandas as pd
import numpy as np
from locoerlt.utilis import PATH_RAW, PATH_INTERIM, get_out_file_tsmp
from locoerlt.emisrt import (
    hap_speciation_mult,
)

st = get_out_file_tsmp()


# Speciation table
path_hap_speciation = os.path.join(
    PATH_INTERIM,
    "epa_speciation_table",
    "power_query",
    "AugmentationProfileAssignmentFactors_Rail_2285002xxx_04072021.xlsx",
)
# Emission factors table.
path_nox_pm10_hc_epa_em_fac = os.path.join(
    PATH_INTERIM, "epa_emission_rates", "epa_2009_emission_rates_nox_pm10_hc.xlsx"
)

# Output emission factors path.
path_emis_rate = os.path.join(PATH_INTERIM, f"emission_factor_{st}.csv")


@pytest.fixture()
def get_out_emis_fac():
    return pd.read_csv(path_emis_rate, index_col=0)


@pytest.fixture()
def get_nox_pm10_pm25_voc_epa_em_fac():
    nox_pm10_hc_df = pd.read_excel(path_nox_pm10_hc_epa_em_fac)
    map_ssc_desc_4 = {
        "large_line_haul": "Line Haul Locomotives: Class I Operations",
        "small_rr": "Line Haul Locomotives: Class II / III Operations",
        "passenger_commuter": [
            "Line Haul Locomotives: Passenger Trains (Amtrak)",
            "Line Haul Locomotives: Commuter Lines",
        ],
        "large_switch": "Yard Locomotives",
    }
    nox_pm10_hc_df_impute = (
        nox_pm10_hc_df.assign(
            scc_description_level_4=lambda df: df.carriers.map(map_ssc_desc_4)
        )
        .explode("scc_description_level_4")
        .dropna(subset=["scc_description_level_4"])
        .filter(items=["scc_description_level_4", "pollutant", "year", "em_fac"])
    )
    voc_df = nox_pm10_hc_df_impute.loc[nox_pm10_hc_df_impute.pollutant == "HC"].assign(
        pollutant="VOC", em_fac=lambda df: df.em_fac * 1.053
    )

    pm25_df = nox_pm10_hc_df_impute.loc[
        nox_pm10_hc_df_impute.pollutant == "PM10-PRI"
    ].assign(pollutant="PM25-PRI", em_fac=lambda df: df.em_fac * 0.97)

    nox_pm10_pm25_voc_df = pd.concat(
        [nox_pm10_hc_df_impute, voc_df, pm25_df]
    ).reset_index(drop=True)
    return nox_pm10_pm25_voc_df


@pytest.fixture()
def hap_speciation():
    return hap_speciation_mult(path_hap_speciation)


def test_co2_fac(get_out_emis_fac):
    co2_emis_fac = get_out_emis_fac.loc[lambda df: df.pollutant == "CO2"]
    assert all(co2_emis_fac.em_fac == 2778 * 0.99 * (44 / 12))


def test_so2_fac(get_out_emis_fac):
    so2_emis_fac = get_out_emis_fac.loc[lambda df: df.pollutant == "SO2"]
    so2_emis_fac_2011 = so2_emis_fac[so2_emis_fac.anals_yr == 2011]
    so2_emis_fac_2012_50 = so2_emis_fac[so2_emis_fac.anals_yr >= 2012]
    so2_2011_500_ppm_s = (0.1346 * 23809.5) * 0.97 * (64 / 32) * 500 * 1e-6
    so2_2012_50_15_ppm_s = (0.1346 * 23809.5) * 0.97 * (64 / 32) * 15 * 1e-6
    assert all(so2_emis_fac_2011.em_fac == so2_2011_500_ppm_s) & np.allclose(
        so2_emis_fac_2012_50.em_fac, so2_2012_50_15_ppm_s
    )


def test_nh3_fac(get_out_emis_fac):
    nh3_emis_fac = get_out_emis_fac.loc[lambda df: df.pollutant == "NH3"]
    assert all(nh3_emis_fac.em_fac == 1.83e-04 * 453.592)


def test_co_fac(get_out_emis_fac):
    co_emis_fac = get_out_emis_fac.loc[lambda df: df.pollutant == "CO"]
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
    assert (
        np.allclose(co_emis_fac_cls1_passng_comm.em_fac, 1.28 * 20.8)
        & np.allclose(co_emis_fac_cls3.em_fac, 1.28 * 18.2)
        & np.allclose(co_emis_fac_yard.em_fac, 1.83 * 15.2)
    )


def test_nox_pm10_pm25_voc_epa_em_fac_2011_40(
    get_out_emis_fac, get_nox_pm10_pm25_voc_epa_em_fac
):
    nox_pm10_pm25_voc_2011_40 = get_out_emis_fac.loc[
        lambda df: (df.pollutant.isin(["NOX", "PM10-PRI", "PM25-PRI", "VOC"]))
        & (df.anals_yr < 2040),
        ["anals_yr", "pollutant", "scc_description_level_4", "em_fac"],
    ].reset_index(drop=True)
    testdf = pd.merge(
        nox_pm10_pm25_voc_2011_40,
        get_nox_pm10_pm25_voc_epa_em_fac,
        left_on=["anals_yr", "pollutant", "scc_description_level_4"],
        right_on=["year", "pollutant", "scc_description_level_4"],
        suffixes=("_epa2009", "_emisfacout"),
    )
    assert np.allclose(testdf.em_fac_epa2009, testdf.em_fac_emisfacout)


def test_nox_pm10_pm25_voc_epa_em_fac_2040_50(
    get_out_emis_fac, get_nox_pm10_pm25_voc_epa_em_fac
):
    nox_pm10_pm25_voc_2040_50 = get_out_emis_fac.loc[
        lambda df: (df.pollutant.isin(["NOX", "PM10-PRI", "PM25-PRI", "VOC"]))
        & (df.anals_yr >= 2040),
        ["anals_yr", "pollutant", "scc_description_level_4", "em_fac"],
    ].reset_index(drop=True)
    testdf = pd.merge(
        nox_pm10_pm25_voc_2040_50,
        get_nox_pm10_pm25_voc_epa_em_fac,
        left_on=["anals_yr", "pollutant", "scc_description_level_4"],
        right_on=["year", "pollutant", "scc_description_level_4"],
        suffixes=("_epa2009", "_emisfacout"),
        how="left",
    )
    testdf["em_fac_emisfacout"] = testdf.groupby(
        ["pollutant", "scc_description_level_4"]
    ).em_fac_emisfacout.ffill()

    assert np.allclose(testdf.em_fac_epa2009, testdf.em_fac_emisfacout)


def test_hap_speciation(get_out_emis_fac, hap_speciation):
    pm25_voc = (
        get_out_emis_fac.loc[lambda df: (df.pollutant.isin(["PM25-PRI", "VOC"]))]
        .filter(items=["anals_yr", "pollutant", "scc_description_level_4", "em_fac"])
        .reset_index(drop=True)
        .rename(columns={"em_fac": "em_fac_input", "pollutant": "input_pollutant_code"})
    )

    hap_em_fac = (
        hap_speciation.merge(
            pm25_voc, on=["input_pollutant_code", "scc_description_level_4"]
        )
        .assign(
            em_fac=lambda df: df.multiplication_factor * df.em_fac_input,
            output_pollutant_code=lambda df: df.output_pollutant_code.astype(str),
        )
        .filter(
            items=[
                "anals_yr",
                "scc_description_level_4",
                "output_pollutant_code",
                "multiplication_factor",
                "em_fac_input",
                "em_fac",
            ]
        )
    )

    test_df = get_out_emis_fac.merge(
        hap_em_fac,
        left_on=["anals_yr", "scc_description_level_4", "pollutant"],
        right_on=["anals_yr", "scc_description_level_4", "output_pollutant_code"],
    )
    assert np.allclose(test_df.em_fac_y, test_df.em_fac_x)


def test_lead_speciation(get_out_emis_fac):
    pm10_fac = (
        get_out_emis_fac.loc[lambda df: (df.pollutant.isin(["PM10-PRI"]))]
        .filter(items=["anals_yr", "pollutant", "scc_description_level_4", "em_fac"])
        .reset_index(drop=True)
        .rename(columns={"em_fac": "em_fac_input", "pollutant": "input_pollutant_code"})
        .assign(
            pollutant=str(7439921),
            pollutant_desc="Lead",
            em_fac=lambda df: df.em_fac_input * 8.405e-05,
        )
    )

    test_df = get_out_emis_fac.merge(
        pm10_fac,
        left_on=["anals_yr", "scc_description_level_4", "pollutant"],
        right_on=["anals_yr", "scc_description_level_4", "pollutant"],
    )
    assert np.allclose(test_df.em_fac_y, test_df.em_fac_x)


def test_all_year_pollutant_scc_present(get_out_emis_fac):
    get_out_emis_fac_1 = get_out_emis_fac.assign(
        scc=lambda df: pd.Categorical(df.scc),
        anals_yr=lambda df: pd.Categorical(df.anals_yr),
        pollutant=lambda df: pd.Categorical(df.pollutant),
    )
    is_1_value_in_all_grp_ = all(
        np.ravel(
            get_out_emis_fac_1.groupby(["scc", "anals_yr", "pollutant"]).count().values
        )
        == 1
    )
    assert is_1_value_in_all_grp_
