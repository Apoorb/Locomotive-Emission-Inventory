import pytest
import os
import numpy as np
import pandas as pd
import xml.etree.ElementTree as ET
import test.test_uncntr_cntr_cersxml as test_uncntr_cntr_cersxml
from locoerlt.utilis import PATH_RAW, PATH_INTERIM, PATH_PROCESSED
from locoerlt.fuelcsmp import preprc_fuelusg
from locoerlt.emisquant import process_proj_fac
import test.test_yard_cers_xml_epa as test_yard_cers_xml_epa

path_cntr_xml_txaer = os.path.join(PATH_PROCESSED, "cntr_cers_tx.xml")
path_cntr_xml_epa = os.path.join(
    PATH_PROCESSED, "eis_stagging_tables", "nonpoint_bridgetool_cntr.xml"
)
path_uncntr_xml_txaer = os.path.join(PATH_PROCESSED, "uncntr_cers_tx.xml")
path_uncntr_xml_epa = os.path.join(
    PATH_PROCESSED, "eis_stagging_tables", "nonpoint_bridgetool_uncntr.xml"
)
path_cntr_yard_xml = os.path.join(
    PATH_PROCESSED, "eis_stagging_tables", "yard_bridgetool_cntr.xml"
)

path_uncntr_yard_xml = os.path.join(
    PATH_PROCESSED, "eis_stagging_tables", "yard_bridgetool_cntr.xml"
)

path_fueluserail2019 = os.path.join(PATH_RAW, "RR_2019FuelUsage.csv")
path_proj_fac = os.path.join(PATH_INTERIM, "Projection Factors 04132021.xlsx")
co2_emis_fac = 2778 * 0.99 * (44 / 12)
us_ton_to_grams = 907185
ns1 = {
    "header": "http://www.exchangenetwork.net/schema/header/2",
    "payload": "http://www.exchangenetwork.net/schema/cer/1",
}


def get_state_fuel_consmp_yard_xml(path_xml):
    cntr_tree = ET.parse(path_xml)
    cntr_yard_xml_df = test_yard_cers_xml_epa.get_annual_o3d_emissions_df_from_xml(
        templ_tree=cntr_tree,
        pol_tot_em_ton_col_nm="em_quant_ton",
        pol_tot_em_daily_ton_col_nm="em_quant_ton_daily",
    )["annual_df"]
    cntr_yard_xml_df_fil = (
        cntr_yard_xml_df.loc[lambda df: df.pollutant == "CO2"]
        .assign(
            stcntyfips=lambda df: df.stcntyfips.astype(int),
            em_quant_ton=lambda df: df.em_quant_ton.astype(float),
            fuel_consump=lambda df: (df.em_quant_ton * us_ton_to_grams / co2_emis_fac),
            year=2020,
            ssc=2285002010,
        )
        .filter(items=["stcntyfips", "ssc", "fuel_consump"])
    )

    tx_cntr_emis = (
        cntr_yard_xml_df_fil.groupby(["ssc"])["fuel_consump"].sum().reset_index()
    )
    return tx_cntr_emis


def get_state_fuel_consump_xml(path_xml, ns):
    cntr_tree = ET.parse(path_xml)
    cntr_xml_df = test_uncntr_cntr_cersxml.get_annual_o3d_emissions_df_from_xml(
        templ_tree=cntr_tree,
        pol_tot_em_ton_col_nm="em_quant_ton",
        pol_tot_em_daily_ton_col_nm="em_quant_ton_daily",
        ns=ns,
    )["annual_df"]
    cntr_xml_df_fil = (
        cntr_xml_df.loc[lambda df: df.pollutant_str == "CO2"]
        .assign(
            stcntyfips=lambda df: df.stcntyfips_str.astype(int),
            ssc=lambda df: df.ssc_str.astype(dtype=np.int64),
            em_quant_ton=lambda df: df.em_quant_ton.astype(float),
            fuel_consump=lambda df: (df.em_quant_ton * us_ton_to_grams / co2_emis_fac),
            year=2020,
        )
        .filter(items=["stcntyfips", "ssc", "year", "fuel_consump"])
    )

    tx_cntr_emis = cntr_xml_df_fil.groupby(["ssc"])["fuel_consump"].sum().reset_index()
    return tx_cntr_emis


ns2 = {
    "header": "http://www.exchangenetwork.net/schema/header/2",
    "payload": "http://www.exchangenetwork.net/schema/cer/2",
}


def get_state_fuel_consump_raw():
    fuel_use_2019 = preprc_fuelusg(path_fueluserail2019).assign(year=2019)
    fuel_use_2019_fil = fuel_use_2019.assign(
        rr_group_proj=lambda df: np.select(
            [
                ((df.carrier.isin(["BNSF", "UP", "KCS"])) & (df.friylab == "Fcat")),
                df.carrier.isin(["AMTK"]),
                df.carrier.isin(["TREX", "DART"]),
                (
                    ~(df.carrier.isin(["BNSF", "UP", "KCS", "AMTK", "TREX", "DART"]))
                    & (df.friylab == "Fcat")
                ),
                (df.friylab == "IYcat"),
            ],
            ["Class I", "Passenger", "Commuter", "Class III", "Class I"],
            np.nan,
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
            right_on=["rr_group", "ref_year"],
        )
        .assign(
            st_fuel_consmp_2020=lambda df: df.proj_fac * df.st_fuel_consmp,
            ssc=lambda df: np.select(
                [
                    ((df.carrier.isin(["BNSF", "UP", "KCS"])) & (df.friylab == "Fcat")),
                    ((df.carrier.isin(["AMTK"])) & (df.friylab == "Fcat")),
                    ((df.carrier.isin(["TREX", "DART"])) & (df.friylab == "Fcat")),
                    (
                        ~(
                            df.carrier.isin(
                                [
                                    "BNSF",
                                    "UP",
                                    "KCS",
                                    "AMTK",
                                    "TREX",
                                    "DART",
                                    "TNMR",
                                    "WTLC",
                                    "TSE",
                                ]
                            )
                        )
                        & (df.friylab == "Fcat")
                    ),
                    (
                        ~(df.carrier.isin(["AMTK", "TNMR", "WTLC", "TSE"]))
                        & (df.friylab == "IYcat")
                    ),
                ],
                [2285002006, 2285002008, 2285002009, 2285002007, 2285002010],
                np.nan,
            ),
        )
        .groupby(["ssc"])
        .st_fuel_consmp_2020.sum()
        .reset_index()
    )
    return fuel_use_2020


@pytest.mark.parametrize(
    "path_xml, ns",
    [
        (path_cntr_xml_txaer, ns1),
        (path_cntr_xml_epa, ns2),
        (path_uncntr_xml_txaer, ns1),
        (path_uncntr_xml_epa, ns2),
    ],
)
def test_statewide_total_fuel_match_input(path_xml, ns):
    fuel_use_2020 = get_state_fuel_consump_raw()
    tx_cntr_emis = get_state_fuel_consump_xml(path_xml=path_xml, ns=ns)
    test_cntr_in_xml = fuel_use_2020.merge(tx_cntr_emis, on="ssc")
    assert np.allclose(
        test_cntr_in_xml.st_fuel_consmp_2020, test_cntr_in_xml.fuel_consump, 0.1
    )


@pytest.mark.parametrize("path_xml", [path_uncntr_yard_xml, path_cntr_yard_xml])
def test_statewide_total_yard_fuel_match_input(path_xml):
    fuel_use_2020 = get_state_fuel_consump_raw()
    tx_cntr_yard_emis = get_state_fuel_consmp_yard_xml(path_xml)
    test_cntr_in_xml = fuel_use_2020.merge(tx_cntr_yard_emis, on="ssc")
    assert np.allclose(
        test_cntr_in_xml.st_fuel_consmp_2020, test_cntr_in_xml.fuel_consump, 0.1
    )
#
#
# def test_txled_red_match_input():
#     ...
#
#
# def test_deri_inc_match_input():
#     ...
#
#
# def test_tti_estimate_match_ertac():
#     ...
#
#
# def test_tti_estimate_match_erg():
#     ...
