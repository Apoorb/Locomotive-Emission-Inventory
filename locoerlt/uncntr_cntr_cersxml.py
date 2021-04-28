import os
import xml
import xml.etree.ElementTree as ET
from lxml import etree as lxml_etree
import time
import datetime
import glob
import copy
import pandas as pd
from locoerlt.utilis import PATH_RAW, PATH_INTERIM, PATH_PROCESSED, get_snake_case_dict


if __name__ == "__main__":
    path_uncntr_emisquant = glob.glob(
        os.path.join(PATH_PROCESSED, "uncntr_emis_quant_[0-9]*-*-*.csv")
    )[0]
    path_cntr_emisquant = glob.glob(
        os.path.join(PATH_PROCESSED, "cntr_emis_quant_[0-9]*-*-*.csv")
    )[0]
    path_xml_templ = os.path.join(PATH_INTERIM, "xml_rail_templ_tti.xml")
    path_county = os.path.join(PATH_RAW, "Texas_County_Boundaries.csv")
    path_out_debug = os.path.join(PATH_INTERIM, "debug", "debug_xml.xml")
    templ_tree = ET.parse(path_xml_templ)
    uncntr_emisquant = pd.read_csv(path_uncntr_emisquant, index_col=0)
    cntr_emisquant = pd.read_csv(path_cntr_emisquant, index_col=0)

    uncntr_emisquant_no_yardnm = (
        uncntr_emisquant
        .groupby(['year', 'stcntyfips', 'county_name', 'dat_cat_code',
                  'sector_description', 'scc_description_level_1',
                  'scc_description_level_2', 'scc_description_level_3', 'scc',
                  'scc_description_level_4', 'pol_type', 'pollutant', 'pol_desc'
                  ])
        .agg(
            em_fac=("em_fac", "mean"),
            uncontrolled_em_quant_ton=("uncontrolled_em_quant_ton", "sum")
        )
        .reset_index()
    )

    uncntr_emisquant_no_yardnm_qc = (
        pd.merge(
            uncntr_emisquant.loc[
                lambda df: ~ (df.scc_description_level_4 == 'Yard '
                                                            'Locomotives')],
            uncntr_emisquant_no_yardnm,
            on=['year', 'stcntyfips', 'county_name', 'dat_cat_code',
                  'sector_description', 'scc_description_level_1',
                  'scc_description_level_2', 'scc_description_level_3', 'scc',
                  'scc_description_level_4', 'pol_type', 'pollutant', 'pol_desc'],
            suffixes=["_raw", "_grp"],
            how="inner",
        )
    )

    assert all(
        (uncntr_emisquant_no_yardnm_qc.em_fac_raw
            == uncntr_emisquant_no_yardnm_qc.em_fac_grp)
        & (uncntr_emisquant_no_yardnm_qc.uncontrolled_em_quant_ton_raw
            == uncntr_emisquant_no_yardnm_qc.uncontrolled_em_quant_ton_grp)
    ), "Re-check groupby on the data. Aggregation is not correct."

    cntr_emisquant_no_yardnm = (
        cntr_emisquant
        .groupby(['year', 'stcntyfips', 'county_name', 'dat_cat_code',
                  'sector_description', 'scc_description_level_1',
                  'scc_description_level_2', 'scc_description_level_3', 'scc',
                  'scc_description_level_4', 'pol_type', 'pollutant', 'pol_desc'
                  ])
        .agg(
            em_fac=("em_fac", "mean"),
            controlled_em_quant_ton=("controlled_em_quant_ton", "sum")
        )
        .reset_index()
    )

    cntr_emisquant_no_yardnm_qc = (
        pd.merge(
            cntr_emisquant.loc[
                lambda df: ~ (df.scc_description_level_4 == 'Yard '
                                                            'Locomotives')],
            cntr_emisquant_no_yardnm,
            on=['year', 'stcntyfips', 'county_name', 'dat_cat_code',
                  'sector_description', 'scc_description_level_1',
                  'scc_description_level_2', 'scc_description_level_3', 'scc',
                  'scc_description_level_4', 'pol_type', 'pollutant', 'pol_desc'],
            suffixes=["_raw", "_grp"],
            how="inner",
        )
    )

    assert all(
        (cntr_emisquant_no_yardnm_qc.em_fac_raw
            == cntr_emisquant_no_yardnm_qc.em_fac_grp)
        & (cntr_emisquant_no_yardnm_qc.controlled_em_quant_ton_raw
            == cntr_emisquant_no_yardnm_qc.controlled_em_quant_ton_grp)
    ), "Re-check groupby on the data. Aggregation is not correct."

    ns = {
        "header": "http://www.exchangenetwork.net/schema/header/2",
        "payload": "http://www.exchangenetwork.net/schema/cer/1",
    }
    tx_counties = pd.read_csv(path_county)
    tx_counties_list = list(
        tx_counties
        .rename(columns=get_snake_case_dict(tx_counties.columns))
        .fips_st_cnty_cd
        .astype(str)
        .str.strip()
    )
    assert len(tx_counties_list) == 254, "There are 254 counties in Texas. " \
                                        "Check why you are getting more or " \
                                        "less than 254."
    non_point_scc_list = ["2285002006", "2285002007", "2285002008",
                          "2285002009", "2285002010"]

    uncntr_emisquant_2020_fil_scc = (
        uncntr_emisquant_no_yardnm
        .loc[lambda df: df.year == 2020]
        .assign(
            stcntyfips_str=lambda df: df.stcntyfips.astype(int).astype(str),
            ssc_str=lambda df: df.scc.astype(str).str.split(".", expand=True)[
                0],
            pollutant_str=lambda df: df.pollutant.astype(str),
            uncontrolled_em_quant_ton_str=lambda
                df: df.uncontrolled_em_quant_ton.astype(str),
            uncontrolled_em_quant_ton_daily_str=lambda
                df: (df.uncontrolled_em_quant_ton / 365).astype(str),
        )
        .loc[lambda df: df.ssc_str.isin(non_point_scc_list)]
        .filter(items=["stcntyfips_str", "ssc_str", "pollutant_str",
                       "uncontrolled_em_quant_ton_str",
                       "uncontrolled_em_quant_ton_daily_str"])
    )
    uncntr_emisquant_2020_fil_scc_grp = (
        uncntr_emisquant_2020_fil_scc.groupby(
            ["stcntyfips_str", "ssc_str"]
        )
    )
    uncntr_emisquant_2020_fil_scc_grp.groups.keys()

    assert ((set(uncntr_emisquant_2020_fil_scc.reset_index().stcntyfips_str)
             - set(tx_counties_list)) == set()), \
        ("uncntr_emisquant_2020_fil_scc counties should be a subset of all "
         "Texas counties")

    cntr_emisquant_2020_fil_scc = (
        cntr_emisquant_no_yardnm
        .loc[lambda df: df.year == 2020]
        .assign(
            stcntyfips_str=lambda df: df.stcntyfips.astype(int).astype(str),
            ssc_str=lambda df: df.scc.astype(str).str.split(".", expand=True)[
                0],
            controlled_em_quant_ton_str=lambda
                df: df.controlled_em_quant_ton.astype(str),
            controlled_em_quant_ton_daily_str=lambda
                df: (df.controlled_em_quant_ton / 365).astype(str),
        )
        .loc[lambda df: df.ssc_str.isin(non_point_scc_list)]
        .filter(items=["stcntyfips_str", "ssc_str",
                       "controlled_em_quant_ton_str",
                       "controlled_em_quant_ton_daily_str"])
    )
    assert ((set(cntr_emisquant_2020_fil_scc.stcntyfips_str)
             - set(tx_counties_list)) == set()), \
        ("cntr_emisquant_2020_fil_scc counties should be a subset of all "
         "Texas counties")

    cers_template = templ_tree.find(".//payload:Location/...", ns)
    location_template = templ_tree.find(".//payload:Location", ns)
    location_template_cpy = copy.deepcopy(location_template)
    cers_template.remove(location_template)
    county = tx_counties_list[0]
    scc = non_point_scc_list[0]
    for county in tx_counties_list:
        location_template_cpy_cpy = copy.deepcopy(location_template_cpy)
        fips_elem = (
            location_template_cpy_cpy.find("payload:StateAndCountyFIPSCode", ns)
        )
        fips_elem.text = f"{county}"
        locationemissionsprocess_template = location_template_cpy_cpy.find(
            "payload:LocationEmissionsProcess", ns)
        locationemissionsprocess_template_cpy = copy.deepcopy(locationemissionsprocess_template)
        location_template_cpy_cpy.remove(locationemissionsprocess_template)
        for scc in non_point_scc_list:
            locationemissionsprocess_template_cpy_cpy = copy.deepcopy(locationemissionsprocess_template_cpy)
            scc_elem = locationemissionsprocess_template_cpy_cpy.find(
                "payload:SourceClassificationCode", ns)
            scc_elem.text = scc
            try:
                relevant_data = uncntr_emisquant_2020_fil_scc_grp.get_group(
                    (county, scc))
                reportingperiod_annual_reportingperiodemissions = (
                    locationemissionsprocess_template_cpy_cpy\
                    .findall(
                        "*/[payload:ReportingPeriodTypeCode='A']"
                        "/payload:ReportingPeriodEmissions", ns
                ))
                for reportingperiodemission in reportingperiod_annual_reportingperiodemissions:
                    cur_pollutant = (reportingperiodemission
                        .find("payload:PollutantCode", ns).text)
                    cur_pollutant_emission = (reportingperiodemission
                        .find("payload:TotalEmissions", ns))
                    replace_value = (
                        relevant_data.loc[lambda df: df.pollutant_str ==cur_pollutant,
                    "uncontrolled_em_quant_ton_str"].values[0]
                    )
                    cur_pollutant_emission.text = replace_value
                reportingperiod_daily_reportingperiodemissions = (
                    locationemissionsprocess_template_cpy_cpy \
                        .findall(
                        "*/[payload:ReportingPeriodTypeCode='O3D']"
                        "/payload:ReportingPeriodEmissions", ns
                    ))
                for reportingperiodemission in reportingperiod_daily_reportingperiodemissions:
                    cur_pollutant = (reportingperiodemission
                        .find("payload:PollutantCode", ns).text)
                    cur_pollutant_emission = (reportingperiodemission
                        .find("payload:TotalEmissions", ns))
                    replace_value = (
                        relevant_data.loc[lambda df: df.pollutant_str ==cur_pollutant,
                    "uncontrolled_em_quant_ton_daily_str"].values[0]
                    )
                    cur_pollutant_emission.text = replace_value
            except KeyError as keyerr:
                pass
            location_template_cpy_cpy.append(locationemissionsprocess_template_cpy_cpy)
        cers_template.append(location_template_cpy_cpy)
    templ_tree.write(path_out_debug)

    path_out_debug_2 = path_out_debug.replace("debug_xml",
                                              "xml_lxml_debug_xml")
    #https://stackoverflow.com/questions/5086922/python-pretty-xml-printer-with-lxml
    #https://stackoverflow.com/questions/48788915/how-to-avoid-incorrect-indentation-in-generated-xml-file-when-inserting-elementt
    lxml_etree_root = lxml_etree.parse(
        path_out_debug,
        parser=lxml_etree.XMLParser(remove_blank_text=True,
                                    remove_comments=True))
    lxml_etree_root.write(path_out_debug_2, pretty_print=True)


