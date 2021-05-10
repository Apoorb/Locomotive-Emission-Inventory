import pytest
import os
import numpy as np
import pandas as pd
import xml.etree.ElementTree as ET
import test.test_uncntr_cntr_cersxml as test_uncntr_cntr_cersxml
from locoerlt.utilis import PATH_RAW, PATH_INTERIM, PATH_PROCESSED
from locoerlt.fuelcsmp import preprc_fuelusg
from locoerlt.emisquant import process_proj_fac

path_cntr_xml = os.path.join(PATH_PROCESSED, "cntr_cers_tx.xml")
path_uncntr_xml = os.path.join(PATH_PROCESSED, "uncntr_cers_tx.xml")
path_fueluserail2019 = os.path.join(PATH_RAW, "RR_2019FuelUsage.csv")
path_proj_fac = os.path.join(PATH_INTERIM, "Projection Factors 04132021.xlsx")
cntr_tree = ET.parse(path_cntr_xml)
co2_emis_fac = 2778 * 0.99 * (44 / 12)
us_ton_to_grams=907185


def get_state_fuel_consump_xml():
    cntr_xml_df = test_uncntr_cntr_cersxml.get_annual_o3d_emissions_df_from_xml(
        templ_tree=cntr_tree,
        pol_tot_em_ton_col_nm="controlled_em_quant_ton_str",
        pol_tot_em_daily_ton_col_nm="controlled_em_quant_ton_daily_str",
    )["annual_df"]
    cntr_xml_df_fil = (
        cntr_xml_df
        .loc[lambda df: df.pollutant_str == "CO2"]
        .assign(
            stcntyfips=lambda df: df.stcntyfips_str.astype(int),
            ssc=lambda df: df.ssc_str.astype(dtype=np.int64),
            controlled_em_quant_ton=lambda df:
            df.controlled_em_quant_ton_str.astype(float),
            fuel_consump=lambda df: (df.controlled_em_quant_ton * us_ton_to_grams
                                     / co2_emis_fac),
            year=2020
        )
        .filter(items=["stcntyfips", "ssc", "year", "fuel_consump"])
    )

    tx_cntr_emis = \
        cntr_xml_df_fil.groupby(["ssc"])["fuel_consump"].sum().reset_index()
    return tx_cntr_emis


def get_state_fuel_consump_raw():
    fuel_use_2019 = preprc_fuelusg(path_fueluserail2019).assign(year=2019)
    fuel_use_2019_fil = (
        fuel_use_2019
        .assign(
            rr_group_proj=lambda df: np.select(
                [
                    ((df.carrier.isin(["BNSF", "UP", "KCS"]))
                    & (df.friylab == "Fcat")),
                    df.carrier.isin(["AMTK"]),
                    df.carrier.isin(["TREX", "DART"]),
                    (~ (df.carrier.isin(["BNSF", "UP", "KCS", "AMTK", "TREX", "DART"]))
                     & (df.friylab == "Fcat")),
                    (df.friylab == "IYcat"),
                ],
                [
                    "Class I",
                    "Passenger",
                    "Commuter",
                    "Class III",
                    "Class I"
                ],
                np.nan
            )
        )
    )

    proj_fac_2020 = (
        process_proj_fac(path_proj_fac)
        .loc[lambda df: df.year == 2020]
        .assign(ref_year=2019)
        .rename(columns={"year": "proj_year"})
    )
    fuel_use_2020 = (
        pd.merge(
            fuel_use_2019_fil,
            proj_fac_2020,
            left_on=["rr_group_proj", "year"],
            right_on=["rr_group", "ref_year"]
        )
        .assign(
            st_fuel_consmp_2020=lambda df: df.proj_fac * df.st_fuel_consmp,
            ssc=lambda df: np.select(
                [
                    ((df.carrier.isin(["BNSF", "UP", "KCS"]))
                     & (df.friylab == "Fcat")),
                    ((df.carrier.isin(["AMTK"]))
                     & (df.friylab == "Fcat")),
                    ((df.carrier.isin(["TREX", "DART"]))
                     & (df.friylab == "Fcat")),
                    (~ (df.carrier.isin(["BNSF", "UP", "KCS", "AMTK", "TREX", "DART",
                                         "TNMR", "WTLC", "TSE"]))
                     & (df.friylab == "Fcat")),
                    (~(df.carrier.isin(["AMTK", "TNMR", "WTLC", "TSE"]))
                     & (df.friylab == "IYcat")),
                ],
                [
                    2285002006,
                    2285002008,
                    2285002009,
                    2285002007,
                    2285002010
                ],
                np.nan
            )
        )
        .groupby(["ssc"]).st_fuel_consmp_2020.sum().reset_index()
    )
    return fuel_use_2020


def test_statewide_total_fuel_match_input():
    fuel_use_2020 = get_state_fuel_consump_raw()
    tx_cntr_emis = get_state_fuel_consump_xml()
    test_cntr_in_xml = fuel_use_2020.merge(tx_cntr_emis, on="ssc")
    assert np.allclose(test_cntr_in_xml.st_fuel_consmp_2020,
                test_cntr_in_xml.fuel_consump, 0.1)


def test_txled_red_match_input():
    ...


def test_deri_inc_match_input():
    ...


def test_tti_estimate_match_ertac():
    ...


def test_tti_estimate_match_erg():
    ...
