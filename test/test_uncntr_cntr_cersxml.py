import os
import xml
import xml.etree.ElementTree as ET
import numpy as np
import pytest
from lxml import etree as lxml_etree
import glob
import pandas as pd
from locoerlt.utilis import PATH_RAW, PATH_INTERIM, PATH_PROCESSED, get_snake_case_dict
from locoerlt.uncntr_cntr_cersxml import clean_up_cntr_emisquant, clean_up_uncntr_emisquant

path_out_cntr = os.path.join(PATH_PROCESSED, "cntr_cers_tx.xml")
path_out_uncntr = os.path.join(PATH_PROCESSED, "uncntr_cers_tx.xml")
path_uncntr_emisquant = glob.glob(
    os.path.join(PATH_PROCESSED, "uncntr_emis_quant_[0-9]*-*-*.csv")
)[0]
path_cntr_emisquant = glob.glob(
    os.path.join(PATH_PROCESSED, "cntr_emis_quant_[0-9]*-*-*.csv")
)[0]


path_county = os.path.join(PATH_RAW, "Texas_County_Boundaries.csv")
ns = {
    "header": "http://www.exchangenetwork.net/schema/header/2",
    "payload": "http://www.exchangenetwork.net/schema/cer/1",
}

tx_counties = pd.read_csv(path_county)
tx_counties_list = list(
    tx_counties.rename(columns=get_snake_case_dict(tx_counties.columns))
        .fips_st_cnty_cd.astype(str)
        .str.strip()
)
tx_counties_list.sort()

non_point_scc_list = [
    "2285002006",
    "2285002007",
    "2285002008",
    "2285002009",
    "2285002010",
]
non_point_scc_list.sort()


def get_annual_o3d_emissions_df_from_xml(
    templ_tree: xml.etree.ElementTree.Element,
    pol_tot_em_ton_col_nm: str,
    pol_tot_em_daily_ton_col_nm: str
):
    templ_root = templ_tree.getroot()
    loc_elem_list = templ_root.findall(
        ".//payload:Location", ns)
    annual_dict = {
        "stcntyfips_str": [],
        "ssc_str": [],
        "pollutant_str": [],
        pol_tot_em_ton_col_nm: [],
    }

    o3d_dict = {
        "stcntyfips_str": [],
        "ssc_str": [],
        "pollutant_str": [],
        pol_tot_em_daily_ton_col_nm: [],
    }

    for loc_or_county in loc_elem_list:
        fips_elem = loc_or_county.find("payload:StateAndCountyFIPSCode", ns)
        loc_em_prc_or_scc_elem_list = loc_or_county.findall(
            "payload:LocationEmissionsProcess", ns)
        for loc_em_prc_or_scc in loc_em_prc_or_scc_elem_list:
            scc_elem = loc_em_prc_or_scc.find(
                "payload:SourceClassificationCode", ns)
            annual_pol_code_list = loc_em_prc_or_scc.findall(
                ".//payload:ReportingPeriod"
                "/[payload:ReportingPeriodTypeCode='A']"
                "/*"
                "/payload:PollutantCode",
                ns,
            )
            annual_pol_tot_em_list = loc_em_prc_or_scc.findall(
                ".//payload:ReportingPeriod"
                "/[payload:ReportingPeriodTypeCode='A']"
                "/*"
                "/payload:TotalEmissions",
                ns,
            )
            for annual_pol_code_elem, annual_pol_tot_em_elem \
                    in zip(annual_pol_code_list, annual_pol_tot_em_list):
                annual_dict["stcntyfips_str"].append(fips_elem.text)
                annual_dict["ssc_str"].append(scc_elem.text)
                annual_dict["pollutant_str"].append(annual_pol_code_elem.text)
                annual_dict[pol_tot_em_ton_col_nm].append(
                    annual_pol_tot_em_elem.text)

            o3d_pol_code_list = loc_em_prc_or_scc.findall(
                ".//payload:ReportingPeriod"
                "/[payload:ReportingPeriodTypeCode='O3D']"
                "/*"
                "/payload:PollutantCode",
                ns,
            )
            o3d_pol_tot_em_list = loc_em_prc_or_scc.findall(
                ".//payload:ReportingPeriod"
                "/[payload:ReportingPeriodTypeCode='O3D']"
                "/*"
                "/payload:TotalEmissions",
                ns,
            )

            for o3d_pol_code_elem, o3d_pol_tot_em_elem \
                    in zip(o3d_pol_code_list, o3d_pol_tot_em_list):
                o3d_dict["stcntyfips_str"].append(fips_elem.text)
                o3d_dict["ssc_str"].append(scc_elem.text)
                o3d_dict["pollutant_str"].append(o3d_pol_code_elem.text)
                o3d_dict[pol_tot_em_daily_ton_col_nm].append(
                    o3d_pol_tot_em_elem.text)
    annual_df = pd.DataFrame(annual_dict)
    o3d_df = pd.DataFrame(o3d_dict)
    return {
        "annual_df": annual_df,
        "o3d_df": o3d_df
    }


def test_cntr_input_output_data_equal():
    cntr_tree = ET.parse(path_out_cntr)
    annual_o3d_dict = get_annual_o3d_emissions_df_from_xml(
        templ_tree=cntr_tree,
        pol_tot_em_ton_col_nm="controlled_em_quant_ton_str",
        pol_tot_em_daily_ton_col_nm="controlled_em_quant_ton_daily_str"
    )
    annual_df = annual_o3d_dict["annual_df"]
    o3d_df = annual_o3d_dict["o3d_df"]
    cntr_emisquant_2020_fil_scc = clean_up_cntr_emisquant(
        path_cntr_emisquant_=path_cntr_emisquant
    )["raw_data"]

    test_data_annual = pd.merge(
        cntr_emisquant_2020_fil_scc,
        annual_df,
        on=["stcntyfips_str", "ssc_str", "pollutant_str"],
        how="left",
        suffixes=["_in", "_xml"]
    )
    test_data_o3d = pd.merge(
        cntr_emisquant_2020_fil_scc,
        o3d_df,
        on=["stcntyfips_str", "ssc_str", "pollutant_str"],
        how="left",
        suffixes=["_in", "_xml"]
    ).dropna(subset=["controlled_em_quant_ton_daily_str_xml"])

    assert (
        np.allclose(
            test_data_annual.controlled_em_quant_ton_str_in.astype(float),
            test_data_annual.controlled_em_quant_ton_str_xml.astype(float)
        )
        & np.allclose(
            test_data_o3d.controlled_em_quant_ton_daily_str_in.astype(float),
            test_data_o3d.controlled_em_quant_ton_daily_str_xml.astype(float)
        )
    ), "Input not equal to output. Check the xml creation."


def test_uncntr_input_output_data_equal():
    uncntr_tree = ET.parse(path_out_uncntr)
    annual_o3d_dict = get_annual_o3d_emissions_df_from_xml(
        templ_tree=uncntr_tree,
        pol_tot_em_ton_col_nm="uncontrolled_em_quant_ton_str",
        pol_tot_em_daily_ton_col_nm="uncontrolled_em_quant_ton_daily_str"
    )
    annual_df = annual_o3d_dict["annual_df"]
    o3d_df = annual_o3d_dict["o3d_df"]
    uncntr_emisquant_2020_fil_scc = clean_up_uncntr_emisquant(
        path_uncntr_emisquant_=path_uncntr_emisquant
    )["raw_data"]

    test_data_annual = pd.merge(
        uncntr_emisquant_2020_fil_scc,
        annual_df,
        on=["stcntyfips_str", "ssc_str", "pollutant_str"],
        how="left",
        suffixes=["_in", "_xml"]
    )
    test_data_o3d = pd.merge(
        uncntr_emisquant_2020_fil_scc,
        o3d_df,
        on=["stcntyfips_str", "ssc_str", "pollutant_str"],
        how="left",
        suffixes=["_in", "_xml"]
    ).dropna(subset=["uncontrolled_em_quant_ton_daily_str_xml"])

    assert (
        np.allclose(
            test_data_annual.uncontrolled_em_quant_ton_str_in.astype(float),
            test_data_annual.uncontrolled_em_quant_ton_str_xml.astype(float)
        )
        & np.allclose(
            test_data_o3d.uncontrolled_em_quant_ton_daily_str_in.astype(float),
            test_data_o3d.uncontrolled_em_quant_ton_daily_str_xml.astype(float)
        )
    ), "Input not equal to output. Check the xml creation."