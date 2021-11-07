import xml.etree.ElementTree as ET
from lxml import etree as lxml_etree
import glob
import copy
import pandas as pd
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname("__file__"), "..")))
from locoerlt.utilis import PATH_RAW, PATH_INTERIM, PATH_PROCESSED, get_snake_case_dict
from locoerlt.cersxml_templ import (
    set_creation_datetime,
    set_document_id,
    register_all_namespaces,
)


def qc_clean_up_uncntr_emisquant(uncntr_emisquant, uncntr_emisquant_no_yardnm):
    uncntr_emisquant_no_yardnm_qc = pd.merge(
        uncntr_emisquant.loc[
            lambda df: ~(df.scc_description_level_4 == "Yard " "Locomotives")
        ],
        uncntr_emisquant_no_yardnm,
        on=[
            "year",
            "stcntyfips",
            "county_name",
            "dat_cat_code",
            "sector_description",
            "scc_description_level_1",
            "scc_description_level_2",
            "scc_description_level_3",
            "scc",
            "scc_description_level_4",
            "pol_type",
            "pollutant",
            "pol_desc",
        ],
        suffixes=["_raw", "_grp"],
        how="inner",
    )
    assert all(
        (
            uncntr_emisquant_no_yardnm_qc.em_fac_raw
            == uncntr_emisquant_no_yardnm_qc.em_fac_grp
        )
        & (
            uncntr_emisquant_no_yardnm_qc.uncontrolled_em_quant_ton_raw
            == uncntr_emisquant_no_yardnm_qc.uncontrolled_em_quant_ton_grp
        )
    ), "Re-check groupby on the data. Aggregation is not correct."


def clean_up_uncntr_emisquant(path_uncntr_emisquant_, year_):
    uncntr_emisquant = pd.read_csv(path_uncntr_emisquant_, index_col=0)
    uncntr_emisquant_no_yardnm = (
        uncntr_emisquant.groupby(
            [
                "year",
                "stcntyfips",
                "county_name",
                "dat_cat_code",
                "sector_description",
                "scc_description_level_1",
                "scc_description_level_2",
                "scc_description_level_3",
                "scc",
                "scc_description_level_4",
                "pol_type",
                "pollutant",
                "pol_desc",
            ]
        )
        .agg(
            em_fac=("em_fac", "mean"),
            uncontrolled_em_quant_ton=("uncontrolled_em_quant_ton", "sum"),
        )
        .reset_index()
    )
    qc_clean_up_uncntr_emisquant(uncntr_emisquant, uncntr_emisquant_no_yardnm)
    uncntr_emisquant_yr_fil_scc = (
        uncntr_emisquant_no_yardnm.loc[lambda df: df.year == year_]
        .assign(
            stcntyfips_str=lambda df: df.stcntyfips.astype(int).astype(str),
            ssc_str=lambda df: df.scc.astype(str).str.split(".", expand=True)[0],
            pollutant_str=lambda df: df.pollutant.astype(str),
            uncontrolled_em_quant_ton_str=lambda df: df.uncontrolled_em_quant_ton.astype(
                str
            ),
            uncontrolled_em_quant_ton_daily_str=lambda df: (
                df.uncontrolled_em_quant_ton / 365
            ).astype(str),
        )
        .filter(
            items=[
                "stcntyfips_str",
                "ssc_str",
                "pollutant_str",
                "uncontrolled_em_quant_ton_str",
                "uncontrolled_em_quant_ton_daily_str",
            ]
        )
        .reset_index(drop=True)
    )
    uncntr_emisquant_yr_fil_scc_grp = uncntr_emisquant_yr_fil_scc.groupby(
        ["stcntyfips_str", "ssc_str"]
    )
    return {
        "raw_data": uncntr_emisquant_yr_fil_scc,
        "grps": uncntr_emisquant_yr_fil_scc_grp,
    }


def qc_clean_uncntr_emisquant(cntr_emisquant, cntr_emisquant_no_yardnm):
    cntr_emisquant_no_yardnm_qc = pd.merge(
        cntr_emisquant.loc[
            lambda df: ~(df.scc_description_level_4 == "Yard " "Locomotives")
        ],
        cntr_emisquant_no_yardnm,
        on=[
            "year",
            "stcntyfips",
            "county_name",
            "dat_cat_code",
            "sector_description",
            "scc_description_level_1",
            "scc_description_level_2",
            "scc_description_level_3",
            "scc",
            "scc_description_level_4",
            "pol_type",
            "pollutant",
            "pol_desc",
        ],
        suffixes=["_raw", "_grp"],
        how="inner",
    )
    assert all(
        (
            cntr_emisquant_no_yardnm_qc.em_fac_raw
            == cntr_emisquant_no_yardnm_qc.em_fac_grp
        )
        & (
            cntr_emisquant_no_yardnm_qc.controlled_em_quant_ton_raw
            == cntr_emisquant_no_yardnm_qc.controlled_em_quant_ton_grp
        )
    ), "Re-check groupby on the data. Aggregation is not correct."


def clean_up_cntr_emisquant(path_cntr_emisquant_, year_):
    cntr_emisquant = pd.read_csv(path_cntr_emisquant_, index_col=0)
    cntr_emisquant_no_yardnm = (
        cntr_emisquant.groupby(
            [
                "year",
                "stcntyfips",
                "county_name",
                "dat_cat_code",
                "sector_description",
                "scc_description_level_1",
                "scc_description_level_2",
                "scc_description_level_3",
                "scc",
                "scc_description_level_4",
                "pol_type",
                "pollutant",
                "pol_desc",
            ]
        )
        .agg(
            em_fac=("em_fac", "mean"),
            controlled_em_quant_ton=("controlled_em_quant_ton", "sum"),
        )
        .reset_index()
    )
    qc_clean_uncntr_emisquant(cntr_emisquant, cntr_emisquant_no_yardnm)
    cntr_emisquant_yr_fil_scc = (
        cntr_emisquant_no_yardnm.loc[lambda df: df.year == year_]
        .assign(
            stcntyfips_str=lambda df: df.stcntyfips.astype(int).astype(str),
            ssc_str=lambda df: df.scc.astype(str).str.split(".", expand=True)[0],
            pollutant_str=lambda df: df.pollutant.astype(str),
            controlled_em_quant_ton_str=lambda df: df.controlled_em_quant_ton.astype(
                str
            ),
            controlled_em_quant_ton_daily_str=lambda df: (
                df.controlled_em_quant_ton / 365
            ).astype(str),
        )
        .filter(
            items=[
                "stcntyfips_str",
                "ssc_str",
                "pollutant_str",
                "controlled_em_quant_ton_str",
                "controlled_em_quant_ton_daily_str",
            ]
        )
    )
    cntr_emisquant_yr_fil_scc_grp = cntr_emisquant_yr_fil_scc.groupby(
        ["stcntyfips_str", "ssc_str"]
    )
    return {
        "raw_data": cntr_emisquant_yr_fil_scc,
        "grps": cntr_emisquant_yr_fil_scc_grp,
    }


def get_uncntr_cntr_xml(
    path_xml_templ,
    grp_uncntr_cntr,
    pol_ton_col: str,
    pol_ton_daily_col: str,
    tx_counties_list,
    non_point_scc_list,
    year_,
    doc_id,
):
    templ_tree = ET.parse(path_xml_templ)
    ns = {
        "header": "http://www.exchangenetwork.net/schema/header/2",
        "payload": "http://www.exchangenetwork.net/schema/cer/1",
    }
    templ_root = templ_tree.getroot()
    templ_root_header = templ_root.find("header:Header", ns)
    set_document_id(templ_root, doc_id=doc_id)
    set_creation_datetime(templ_root_header, ns=ns)
    cers_template = templ_tree.find(".//payload:Location/...", ns)
    location_template = templ_tree.find(".//payload:Location", ns)
    location_template_cpy = copy.deepcopy(location_template)
    cers_template.remove(location_template)
    year_elem = cers_template.find("payload:EmissionsYear", ns)
    year_elem.text = f"{year_}"
    county = tx_counties_list[0]
    scc = non_point_scc_list[0]
    for county in tx_counties_list:
        location_template_cpy_cpy = copy.deepcopy(location_template_cpy)
        fips_elem = location_template_cpy_cpy.find("payload:StateAndCountyFIPSCode", ns)
        fips_elem.text = f"{county}"
        locationemissionsprocess_template = location_template_cpy_cpy.find(
            "payload:LocationEmissionsProcess", ns
        )
        locationemissionsprocess_template_cpy = copy.deepcopy(
            locationemissionsprocess_template
        )
        location_template_cpy_cpy.remove(locationemissionsprocess_template)
        for scc in non_point_scc_list:
            locationemissionsprocess_template_cpy_cpy = copy.deepcopy(
                locationemissionsprocess_template_cpy
            )
            scc_elem = locationemissionsprocess_template_cpy_cpy.find(
                "payload:SourceClassificationCode", ns
            )
            scc_elem.text = scc
            try:
                relevant_data = grp_uncntr_cntr.get_group((county, scc))
                reportingperiod_annual_reportingperiodemissions = (
                    locationemissionsprocess_template_cpy_cpy.findall(
                        "*/[payload:ReportingPeriodTypeCode='A']"
                        "/payload:ReportingPeriodEmissions",
                        ns,
                    )
                )
                for (
                    reportingperiodemission
                ) in reportingperiod_annual_reportingperiodemissions:
                    cur_pollutant = reportingperiodemission.find(
                        "payload:PollutantCode", ns
                    ).text
                    cur_pollutant_emission = reportingperiodemission.find(
                        "payload:TotalEmissions", ns
                    )
                    replace_value = relevant_data.loc[
                        lambda df: df.pollutant_str == cur_pollutant,
                        pol_ton_col,
                    ].values[0]
                    cur_pollutant_emission.text = replace_value
                reportingperiod_daily_reportingperiodemissions = (
                    locationemissionsprocess_template_cpy_cpy.findall(
                        "*/[payload:ReportingPeriodTypeCode='O3D']"
                        "/payload:ReportingPeriodEmissions",
                        ns,
                    )
                )
                for (
                    reportingperiodemission
                ) in reportingperiod_daily_reportingperiodemissions:
                    cur_pollutant = reportingperiodemission.find(
                        "payload:PollutantCode", ns
                    ).text
                    cur_pollutant_emission = reportingperiodemission.find(
                        "payload:TotalEmissions", ns
                    )
                    replace_value = relevant_data.loc[
                        lambda df: df.pollutant_str == cur_pollutant,
                        pol_ton_daily_col,
                    ].values[0]
                    cur_pollutant_emission.text = replace_value
            except KeyError:
                pass
            location_template_cpy_cpy.append(locationemissionsprocess_template_cpy_cpy)
        cers_template.append(location_template_cpy_cpy)
    return templ_tree


def write_xml(xml_tree, path_out_xml):
    path_out_dirty_xml = path_out_xml.replace(".xml", "_unformatted.xml")
    xml_tree.write(path_out_dirty_xml, encoding="utf-8", xml_declaration=True)
    # https://stackoverflow.com/questions/5086922/python-pretty-xml-printer-with-lxml
    # https://stackoverflow.com/questions/48788915/how-to-avoid-incorrect-indentation-in-generated-xml-file-when-inserting-elementt
    lxml_etree_root = lxml_etree.parse(
        path_out_dirty_xml,
        parser=lxml_etree.XMLParser(remove_blank_text=True, remove_comments=True),
    )
    lxml_etree_root.write(
        path_out_xml, pretty_print=True, encoding="utf-8", xml_declaration=True
    )
    os.remove(path_out_dirty_xml)


if __name__ == "__main__":
    path_uncntr_emisquant = glob.glob(
        os.path.join(PATH_PROCESSED, "uncntr_emis_quant_[0-9]*-*-*.csv")
    )[0]
    path_cntr_emisquant = glob.glob(
        os.path.join(PATH_PROCESSED, "cntr_emis_quant_[0-9]*-*-*.csv")
    )[0]
    path_xml_templ = os.path.join(PATH_INTERIM, "xml_rail_templ_tti.xml")
    path_county = os.path.join(PATH_RAW, "Texas_County_Boundaries.csv")
    tx_counties = pd.read_csv(path_county)
    tx_counties_list = list(
        tx_counties.rename(columns=get_snake_case_dict(tx_counties.columns))
        .fips_st_cnty_cd.astype(str)
        .str.strip()
    )
    tx_counties_list.sort()
    assert len(tx_counties_list) == 254, (
        "There are 254 counties in Texas. "
        "Check why you are getting more or "
        "less than 254."
    )
    non_point_scc_list = [
        "2285002006",
        "2285002007",
        "2285002008",
        "2285002009",
        "2285002010",
    ]
    non_point_scc_list.sort()

    year_list = list(range(2011, 2051))  # add the list of years
    for year in year_list:

        path_out_cntr = os.path.join(PATH_PROCESSED, "TexAER_XMLs", f"cntr_{year}_TexAER.xml")
        path_out_uncntr = os.path.join(PATH_PROCESSED, "TexAER_XMLs", f"uncntr_{year}_TexAER.xml")
        uncntr_emisquant_yr_fil_scc_dict = clean_up_uncntr_emisquant(
            path_uncntr_emisquant_=path_uncntr_emisquant, year_=year
        )
        cntr_emisquant_yr_fil_scc_dict = clean_up_cntr_emisquant(
            path_cntr_emisquant_=path_cntr_emisquant, year_=year
        )
        uncntr_emisquant_yr_fil_scc = uncntr_emisquant_yr_fil_scc_dict["raw_data"]
        cntr_emisquant_yr_fil_scc = cntr_emisquant_yr_fil_scc_dict["raw_data"]
        uncntr_emisquant_yr_fil_scc_grp = uncntr_emisquant_yr_fil_scc_dict["grps"]
        cntr_emisquant_yr_fil_scc_grp = cntr_emisquant_yr_fil_scc_dict["grps"]
        assert (
            set(uncntr_emisquant_yr_fil_scc.reset_index().stcntyfips_str)
            - set(tx_counties_list)
        ) == set(), (
            "uncntr_emisquant_yr_fil_scc counties should be a subset of all "
            "Texas counties"
        )
        assert (
                       set(cntr_emisquant_yr_fil_scc.stcntyfips_str) - set(tx_counties_list)
        ) == set(), (
            "cntr_emisquant_yr_fil_scc counties should be a subset of all "
            "Texas counties"
        )

        register_all_namespaces(path_xml_templ)
        uncntr_xml_tree = get_uncntr_cntr_xml(
            path_xml_templ=path_xml_templ,
            grp_uncntr_cntr=uncntr_emisquant_yr_fil_scc_grp,
            pol_ton_col="uncontrolled_em_quant_ton_str",
            pol_ton_daily_col="uncontrolled_em_quant_ton_daily_str",
            tx_counties_list=tx_counties_list,
            non_point_scc_list=non_point_scc_list,
            year_=year,
            doc_id=f"locomotives_uncntr_TexAER_{year}_xml",
        )
        write_xml(xml_tree=uncntr_xml_tree, path_out_xml=path_out_uncntr)

        cntr_xml_tree = get_uncntr_cntr_xml(
            path_xml_templ=path_xml_templ,
            grp_uncntr_cntr=cntr_emisquant_yr_fil_scc_grp,
            pol_ton_col="controlled_em_quant_ton_str",
            pol_ton_daily_col="controlled_em_quant_ton_daily_str",
            tx_counties_list=tx_counties_list,
            non_point_scc_list=non_point_scc_list,
            year_=year,
            doc_id=f"locomotives_cntr_TexAER_{year}_xml",
        )
        write_xml(xml_tree=cntr_xml_tree, path_out_xml=path_out_cntr)
