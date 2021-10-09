import pytest
import os
import xml
import xml.etree.ElementTree as ET
import numpy as np
import glob
import pandas as pd
from locoerlt.utilis import PATH_PROCESSED

path_uncntr_emisquant = glob.glob(
    os.path.join(PATH_PROCESSED, "uncntr_emis_quant_[0-9]*-*-*.csv")
)[0]
path_cntr_emisquant = glob.glob(
    os.path.join(PATH_PROCESSED, "cntr_emis_quant_[0-9]*-*-*.csv")
)[0]
path_uncntr_yard_xml = os.path.join(
    PATH_PROCESSED, "eis_stagging_tables", "yard_bridgetool_uncntr.xml"
)
# path_cntr_yard_xml = os.path.join(
#     PATH_PROCESSED, "eis_stagging_tables", "yard_bridgetool_cntr.xml"
# )
path_cntr_yard_xml = (
    r"C:\Users\a-bibeka\Texas A&M Transportation Institute"
    r"\HMP - TCEQ Projects - Documents"
    r"\2020 Texas Statewide Locomotive and Rail Yard EI"
    r"\Tasks\Task8_Reports\Draft_Deliverables_24Sep2021"
    r"\Appendix E 2020_yard_epa_eis.xml"

)

ns = {
    "header": "http://www.exchangenetwork.net/schema/header/2",
    "payload": "http://www.exchangenetwork.net/schema/cer/2",
}


def get_annual_o3d_emissions_df_from_xml(
    templ_tree: xml.etree.ElementTree.Element,
    pol_tot_em_ton_col_nm: str,
    pol_tot_em_daily_ton_col_nm: str,
):
    templ_root = templ_tree.getroot()
    fac_site_elem_list = templ_root.findall(".//payload:FacilitySite", ns)
    annual_dict = {
        "stcntyfips": [],
        "eis_fac_id": [],
        "pollutant": [],
        pol_tot_em_ton_col_nm: [],
    }

    o3d_dict = {
        "stcntyfips": [],
        "eis_fac_id": [],
        "pollutant": [],
        pol_tot_em_daily_ton_col_nm: [],
    }

    for fac_site in fac_site_elem_list:
        eis_fac_id = fac_site.find(".//payload:FacilitySiteIdentifier", ns)
        fips_elem = fac_site.find(".//payload:StateAndCountyFIPSCode", ns)
        unit_em_prc = fac_site.find(".//payload:UnitEmissionsProcess", ns)
        annual_pol_code_list = unit_em_prc.findall(
            ".//payload:ReportingPeriod"
            "/[payload:ReportingPeriodTypeCode='A']"
            "/*"
            "/payload:PollutantCode",
            ns,
        )
        annual_pol_tot_em_list = unit_em_prc.findall(
            ".//payload:ReportingPeriod"
            "/[payload:ReportingPeriodTypeCode='A']"
            "/*"
            "/payload:TotalEmissions",
            ns,
        )
        for annual_pol_code_elem, annual_pol_tot_em_elem in zip(
            annual_pol_code_list, annual_pol_tot_em_list
        ):
            annual_dict["stcntyfips"].append(int(fips_elem.text))
            annual_dict["eis_fac_id"].append(int(eis_fac_id.text))
            annual_dict["pollutant"].append(annual_pol_code_elem.text)
            annual_dict[pol_tot_em_ton_col_nm].append(
                float(annual_pol_tot_em_elem.text)
            )

        o3d_pol_code_list = unit_em_prc.findall(
            ".//payload:ReportingPeriod"
            "/[payload:ReportingPeriodTypeCode='O3D']"
            "/*"
            "/payload:PollutantCode",
            ns,
        )
        o3d_pol_tot_em_list = unit_em_prc.findall(
            ".//payload:ReportingPeriod"
            "/[payload:ReportingPeriodTypeCode='O3D']"
            "/*"
            "/payload:TotalEmissions",
            ns,
        )

        for o3d_pol_code_elem, o3d_pol_tot_em_elem in zip(
            o3d_pol_code_list, o3d_pol_tot_em_list
        ):
            o3d_dict["stcntyfips"].append(int(fips_elem.text))
            o3d_dict["eis_fac_id"].append(int(eis_fac_id.text))
            o3d_dict["pollutant"].append(o3d_pol_code_elem.text)
            o3d_dict[pol_tot_em_daily_ton_col_nm].append(
                float(o3d_pol_tot_em_elem.text)
            )
    annual_df = pd.DataFrame(annual_dict)
    o3d_df = pd.DataFrame(o3d_dict)
    return {"annual_df": annual_df, "o3d_df": o3d_df}


def test_uncntr_input_output_data_equal():
    uncntr_tree = ET.parse(path_uncntr_yard_xml)
    annual_o3d_dict = get_annual_o3d_emissions_df_from_xml(
        templ_tree=uncntr_tree,
        pol_tot_em_ton_col_nm="uncontrolled_em_quant_ton",
        pol_tot_em_daily_ton_col_nm="uncontrolled_em_quant_ton_daily",
    )
    annual_df = annual_o3d_dict["annual_df"]
    o3d_df = annual_o3d_dict["o3d_df"]
    uncntr_emisquant = pd.read_csv(path_uncntr_emisquant)
    uncntr_emisquant_yard_2020 = (
        uncntr_emisquant.loc[
            lambda df: (df.scc_description_level_4 == "Yard Locomotives")
            & (df.year == 2020)
        ]
        .rename(columns={"eis_facility_id": "eis_fac_id"})
        .assign(
            uncontrolled_em_quant_ton_daily=lambda df: df.uncontrolled_em_quant_ton
            / 365
        )
        .filter(
            items=[
                "stcntyfips",
                "eis_fac_id",
                "pollutant",
                "uncontrolled_em_quant_ton",
                "uncontrolled_em_quant_ton_daily",
            ]
        )
    )

    test_data_annual = pd.merge(
        uncntr_emisquant_yard_2020,
        annual_df,
        on=["eis_fac_id", "pollutant"],
        suffixes=["_in", "_xml"],
    )

    test_data_o3d = pd.merge(
        uncntr_emisquant_yard_2020,
        o3d_df,
        on=["eis_fac_id", "pollutant"],
        suffixes=["_in", "_xml"],
    )

    assert np.allclose(
        test_data_annual.uncontrolled_em_quant_ton_in,
        test_data_annual.uncontrolled_em_quant_ton_xml,
    ) & np.allclose(
        test_data_o3d.uncontrolled_em_quant_ton_daily_in,
        test_data_o3d.uncontrolled_em_quant_ton_daily_xml,
    ), "Input not equal to output. Check the xml creation."


def test_cntr_input_output_data_equal():
    cntr_tree = ET.parse(path_cntr_yard_xml)
    annual_o3d_dict = get_annual_o3d_emissions_df_from_xml(
        templ_tree=cntr_tree,
        pol_tot_em_ton_col_nm="controlled_em_quant_ton",
        pol_tot_em_daily_ton_col_nm="controlled_em_quant_ton_daily",
    )
    annual_df = annual_o3d_dict["annual_df"]
    o3d_df = annual_o3d_dict["o3d_df"]
    cntr_emisquant = pd.read_csv(path_cntr_emisquant)
    cntr_emisquant_yard_2020 = (
        cntr_emisquant.loc[
            lambda df: (df.scc_description_level_4 == "Yard Locomotives")
            & (df.year == 2020)
        ]
        .rename(columns={"eis_facility_id": "eis_fac_id"})
        .assign(
            controlled_em_quant_ton_daily=lambda df: df.controlled_em_quant_ton / 365
        )
        .filter(
            items=[
                "stcntyfips",
                "eis_fac_id",
                "pollutant",
                "controlled_em_quant_ton",
                "controlled_em_quant_ton_daily",
            ]
        )
    )

    test_data_annual = pd.merge(
        cntr_emisquant_yard_2020,
        annual_df,
        on=["eis_fac_id", "pollutant"],
        suffixes=["_in", "_xml"],
    )

    test_data_o3d = pd.merge(
        cntr_emisquant_yard_2020,
        o3d_df,
        on=["eis_fac_id", "pollutant"],
        suffixes=["_in", "_xml"],
    )

    assert np.allclose(
        test_data_annual.controlled_em_quant_ton_in,
        test_data_annual.controlled_em_quant_ton_xml,
    ) & np.allclose(
        test_data_o3d.controlled_em_quant_ton_daily_in,
        test_data_o3d.controlled_em_quant_ton_daily_xml,
    ), "Input not equal to output. Check the xml creation."
