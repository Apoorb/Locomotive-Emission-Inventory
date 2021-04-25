"""
Tests uncntr_cntr_emisquant module.
"""
import os
from io import StringIO
import csv
from itertools import chain
import pytest
import glob
import pandas as pd
import numpy as np
from locoerlt.utilis import PATH_RAW, PATH_INTERIM, PATH_PROCESSED


path_emisquant_agg = glob.glob(
    os.path.join(PATH_PROCESSED, "emis_quant_loco_agg_[0-9]*-*-*.csv")
)[0]

path_uncntr_emisquant = glob.glob(
    os.path.join(PATH_PROCESSED, "uncntr_emis_quant_[0-9]*-*-*.csv")
)[0]

path_cntr_emisquant = glob.glob(
    os.path.join(PATH_PROCESSED, "cntr_emis_quant_[0-9]*-*-*.csv")
)[0]

path_txled_counties = os.path.join(PATH_RAW, "txled_counties.csv")


@pytest.fixture()
def get_txled_counties():
    txled_counties_rows = []
    with open(path_txled_counties, newline="") as csvfile:
        txled_counties_rd = csv.reader(csvfile, delimiter=",")
        for row in txled_counties_rd:
            txled_counties_rows.append(row)
    txled_counties = list(chain.from_iterable(txled_counties_rows))
    txled_counties_prc = list(map(lambda item: item.strip().lower(), txled_counties))
    return txled_counties_prc


@pytest.fixture()
def get_emis_quant_agg():
    return pd.read_csv(path_emisquant_agg, index_col=0)


@pytest.fixture()
def get_uncntr_emisquant():
    return pd.read_csv(path_uncntr_emisquant, index_col=0)


@pytest.fixture()
def get_cntr_emisquant():
    return pd.read_csv(path_cntr_emisquant, index_col=0)


def test_controlled_emis_quant(
    get_emis_quant_agg, get_cntr_emisquant, get_txled_counties
):
    us_ton_to_grams = 907185
    test_emisquant = get_emis_quant_agg.merge(
        get_cntr_emisquant,
        on=[
            "year",
            "stcntyfips",
            "scc_description_level_4",
            "yardname_v1",
            "pollutant",
        ],
        how="outer",
        suffixes=["_pre", "_post"],
    )
    txled_counties = get_txled_counties
    test_emisquant_nox_txled = test_emisquant.loc[
        lambda df: (
            (df.pollutant == "NOX")
            & (df.county_name_pre.str.lower().isin(txled_counties))
        )
    ]
    are_all_nox_em_quant_in_txled_counties_adjusted = np.allclose(
        test_emisquant_nox_txled.controlled_em_quant_ton,
        test_emisquant_nox_txled.em_quant_pre * (1 - 6.2 / 100) / us_ton_to_grams,
    )

    test_emisquant_non_txled = test_emisquant.loc[
        lambda df: ~(
            (df.pollutant == "NOX")
            & (df.county_name_pre.str.lower().isin(txled_counties))
        )
    ]
    are_all_non_txled_em_quant_unadjusted = np.allclose(
        test_emisquant_non_txled.controlled_em_quant_ton,
        test_emisquant_non_txled.em_quant_pre / us_ton_to_grams,
    )
    assert (
        are_all_nox_em_quant_in_txled_counties_adjusted
        and are_all_non_txled_em_quant_unadjusted
    )


@pytest.mark.parametrize("analysis_year_deri_benefits", [27206.2667])
def test_uncontrolled_emis_quant(
    get_emis_quant_agg,
    get_uncntr_emisquant,
    get_txled_counties,
    analysis_year_deri_benefits,
):
    us_ton_to_grams = 907185
    test_emisquant = get_emis_quant_agg.merge(
        get_uncntr_emisquant,
        on=[
            "year",
            "stcntyfips",
            "scc_description_level_4",
            "yardname_v1",
            "pollutant",
        ],
        how="outer",
        suffixes=["_pre", "_post"],
    )
    test_emisquant_nox = test_emisquant.loc[lambda df: ((df.pollutant == "NOX"))]

    total_deri_benefits_in_analysis_year = (
        test_emisquant_nox.uncontrolled_em_quant_ton
        - test_emisquant_nox.em_quant_pre / us_ton_to_grams
    ).sum()
    total_deri_benefits_in_analysis_year = np.round(
        total_deri_benefits_in_analysis_year, 4
    )
    assert total_deri_benefits_in_analysis_year == analysis_year_deri_benefits
