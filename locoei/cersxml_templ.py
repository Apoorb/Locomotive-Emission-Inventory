import xml
import xml.etree.ElementTree as ET
from lxml import etree as lxml_etree
import time
import datetime
import glob
import copy
import pandas as pd
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname("__file__"), "..")))
from locoerlt.utilis import PATH_RAW, PATH_INTERIM


def set_document_id(
    templ_root_: xml.etree.ElementTree.Element, doc_id="locomotives_cers_aerr_2020_xml"
) -> None:
    templ_root_.set("id", doc_id)


def set_creation_datetime(
    templ_root_header_: xml.etree.ElementTree.Element, ns
) -> None:
    creation_datetime = templ_root_header_.find("header:CreationDateTime", ns)
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%dT%H:%M:%S")
    creation_datetime.text = st


def set_data_cat_prop(
    templ_root_header_: xml.etree.ElementTree.Element, ns, data_cat_value_text="Nonroad"
) -> None:
    data_cat_prop = templ_root_header_.find(
        "./header:Property/[header:PropertyName='DataCategory']", ns
    )
    data_cat_value = data_cat_prop.find("header:PropertyValue", ns)
    data_cat_value.text = data_cat_value_text


def modify_template_header(
    templ_root_: xml.etree.ElementTree.Element, ns
) -> xml.etree.ElementTree.Element:
    templ_root_header_ = templ_root_.find("header:Header", ns)
    set_creation_datetime(templ_root_header_=templ_root_header_, ns=ns)
    set_data_cat_prop(templ_root_header_=templ_root_header_, ns=ns)
    return templ_root_header_


def delele_253_county_loc(templ_root_payload_cers_: xml.etree.ElementTree.Element,):
    counter_ = 1
    for child in templ_root_payload_cers_.findall("payload:Location", ns):
        if counter_ > 1:
            templ_root_payload_cers_.remove(child)
        counter_ += 1


def set_generic_stateandcountyfipscode(
    templ_root_payload_cers_loc_: xml.etree.ElementTree.Element,
):
    templ_root_payload_cers_loc_.find(
        "payload:StateAndCountyFIPSCode", ns
    ).text = "five_digit_tx_county_fips"


def delete_2_extra_scc_loc_em_prc(
    templ_root_payload_cers_loc_: xml.etree.ElementTree.Element,
):
    counter_ = 1
    for child in templ_root_payload_cers_loc_.findall(
        "payload:LocationEmissionsProcess", ns
    ):
        if counter_ > 1:
            templ_root_payload_cers_loc_.remove(child)
        counter_ += 1


def set_generic_sourceclassificationcode_emtype(
    templ_root_payload_cers_loc_locemprc_: xml.etree.ElementTree.Element,
):
    templ_root_payload_cers_loc_locemprc_scc = templ_root_payload_cers_loc_locemprc_.find(
        "payload:SourceClassificationCode", ns
    )
    templ_root_payload_cers_loc_locemprc_scc.text = "ten_digit_scc"
    templ_root_payload_cers_loc_locemprc_emtype = templ_root_payload_cers_loc_locemprc_.find(
        "payload:EmissionsTypeCode", ns
    )
    templ_root_payload_cers_loc_locemprc_emtype.text = "X"


def get_annual_o3d_pollutants_erg(
    templ_root_payload_cers_loc_locemprc_report_period_A_: xml.etree.ElementTree.Element,
    templ_root_payload_cers_loc_locemprc_report_period_O3D_: xml.etree.ElementTree.Element,
    ns_,
) -> dict:
    annual_pollutant_elements = templ_root_payload_cers_loc_locemprc_report_period_A_.findall(
        "payload:ReportingPeriodEmissions" "/payload:PollutantCode", ns_
    )
    o3d_pollutant_elements = templ_root_payload_cers_loc_locemprc_report_period_O3D_.findall(
        "payload:ReportingPeriodEmissions" "/payload:PollutantCode", ns_
    )
    return {
        "annual_pollutant_elements": annual_pollutant_elements,
        "o3d_pollutant_elements": o3d_pollutant_elements,
    }


def set_all_emissions_to_zero(
    templ_root_payload_cers_loc_locemprc_: xml.etree.ElementTree.Element, ns_: dict
):
    for totemis in templ_root_payload_cers_loc_locemprc_.findall(
        ".//payload:TotalEmissions", ns
    ):
        totemis.text = "0"


def delete_pollutant_complexes_not_in_tti_output(
    missing_tti_pollutants: list,
    templ_root_payload_cers_: xml.etree.ElementTree.Element,
    reporting_period: str,
):
    for pol in list(missing_tti_pollutants):
        delete_report_period_em_tags_list = templ_root_payload_cers_.findall(
            "payload:Location"
            "/payload:LocationEmissionsProcess"
            "/payload:ReportingPeriod"
            f"/[payload:ReportingPeriodTypeCode='{reporting_period}']"
            "/payload:ReportingPeriodEmissions"
            f"/[payload:PollutantCode='{pol}']",
            ns,
        )
        delete_report_period_em_tag_parent_list = templ_root_payload_cers_.findall(
            "payload:Location"
            "/payload:LocationEmissionsProcess"
            "/payload:ReportingPeriod"
            f"/[payload:ReportingPeriodTypeCode='{reporting_period}']"
            "/payload:ReportingPeriodEmissions"
            f"/[payload:PollutantCode='{pol}']...",
            ns,
        )
        for bad_child, parent in zip(
            delete_report_period_em_tags_list, delete_report_period_em_tag_parent_list
        ):
            parent.remove(bad_child)


def deepcopy_reportingperiodemissions_complex_for_template(
    templ_root_payload_cers_loc_locemprc_report_period_A_: xml.etree.ElementTree.Element,
) -> xml.etree.ElementTree.Element:
    templ_pol_rep_per_emis = templ_root_payload_cers_loc_locemprc_report_period_A_.find(
        "payload:ReportingPeriodEmissions" "/[payload:PollutantCode='100414']", ns
    )
    templ_pol_rep_per_cpy = copy.deepcopy(templ_pol_rep_per_emis)
    return templ_pol_rep_per_cpy


def create_pollutant_complexes_in_tti_output_not_in_erg(
    extra_tti_pollutants: list,
    templ_root_payload_cers_loc_locemprc_report_period_A_: xml.etree.ElementTree.Element,
    templ_pol_rep_per_cpy_: xml.etree.ElementTree.Element,
):
    for pol_elem in extra_tti_pollutants:
        templ_pol_rep_per_cpy_.find("payload:PollutantCode", ns).text = f"{pol_elem}"
        templ_pol_rep_per_cpy_.find("payload:TotalEmissions", ns).text = "0"
        templ_pol_rep_per_cpy_.find(
            "payload:EmissionsUnitofMeasureCode", ns
        ).text = "TON"
        templ_pol_rep_per_cpy_cpy = copy.deepcopy(templ_pol_rep_per_cpy_)
        templ_root_payload_cers_loc_locemprc_report_period_A_.append(
            templ_pol_rep_per_cpy_cpy
        )


def modify_payload(
    templ_root_: xml.etree.ElementTree.Element,
    tti_pol_list_a_: list,
    tti_pol_list_o3d_: list,
    ns: dict,
) -> xml.etree.ElementTree.Element:

    templ_root_.find(
        ".//header:Property/[header:PropertyValue='Nonroad']/header:PropertyValue", ns
    ).text = "Nonpoint"
    templ_root_payload = templ_root_.find("header:Payload", ns)
    templ_root_payload_cers = templ_root_payload.find("payload:CERS", ns)
    delele_253_county_loc(templ_root_payload_cers_=templ_root_payload_cers)
    assert len(templ_root_payload_cers.findall("payload:Location", ns)) == 1, (
        "Above operation didn't delete all " "counties."
    )
    templ_root_.find(".//payload:UserIdentifier", ns).text = "XCMCLAIN"

    templ_root_payload_cers_loc = templ_root_payload_cers.find("payload:Location", ns)
    set_generic_stateandcountyfipscode(
        templ_root_payload_cers_loc_=templ_root_payload_cers_loc
    )
    delete_2_extra_scc_loc_em_prc(
        templ_root_payload_cers_loc_=templ_root_payload_cers_loc
    )
    assert (
        len(templ_root_payload_cers_loc.findall("payload:LocationEmissionsProcess", ns))
        == 1
    ), "Above operation didn't delete all counties."
    templ_root_payload_cers_loc_locemprc = templ_root_payload_cers_loc.find(
        "payload:LocationEmissionsProcess", ns
    )
    set_generic_sourceclassificationcode_emtype(
        templ_root_payload_cers_loc_locemprc_=templ_root_payload_cers_loc_locemprc
    )
    templ_root_payload_cers_loc_locemprc_report_period_A = templ_root_payload_cers_loc_locemprc.find(
        "payload:ReportingPeriod/[payload:ReportingPeriodTypeCode='A']", ns
    )
    templ_root_payload_cers_loc_locemprc_report_period_O3D = templ_root_payload_cers_loc_locemprc.find(
        "payload:ReportingPeriod/[payload:ReportingPeriodTypeCode='O3D']", ns
    )
    annual_o3d_pol_dict = get_annual_o3d_pollutants_erg(
        templ_root_payload_cers_loc_locemprc_report_period_A_=templ_root_payload_cers_loc_locemprc_report_period_A,
        templ_root_payload_cers_loc_locemprc_report_period_O3D_=templ_root_payload_cers_loc_locemprc_report_period_O3D,
        ns_=ns,
    )
    annual_pollutant_elements = annual_o3d_pol_dict["annual_pollutant_elements"]
    txerr_pols_a = set(map(lambda elem: elem.text, annual_pollutant_elements))
    missing_tti_pollutants_a = txerr_pols_a - tti_pol_list_a_
    extra_tti_pollutants_a = tti_pol_list_a_ - txerr_pols_a

    o3d_pollutant_elements = annual_o3d_pol_dict["o3d_pollutant_elements"]
    txerr_pols_o3d = set(map(lambda elem: elem.text, o3d_pollutant_elements))
    missing_tti_pollutants_o3d = txerr_pols_o3d - tti_pol_list_o3d_

    set_all_emissions_to_zero(
        templ_root_payload_cers_loc_locemprc_=templ_root_payload_cers_loc_locemprc,
        ns_=ns,
    )
    delete_pollutant_complexes_not_in_tti_output(
        missing_tti_pollutants=missing_tti_pollutants_a,
        templ_root_payload_cers_=templ_root_payload_cers,
        reporting_period="A",
    )
    delete_pollutant_complexes_not_in_tti_output(
        missing_tti_pollutants=missing_tti_pollutants_o3d,
        templ_root_payload_cers_=templ_root_payload_cers,
        reporting_period="O3D",
    )

    templ_pol_rep_per_cpy = deepcopy_reportingperiodemissions_complex_for_template(
        templ_root_payload_cers_loc_locemprc_report_period_A_=templ_root_payload_cers_loc_locemprc_report_period_A
    )
    create_pollutant_complexes_in_tti_output_not_in_erg(
        extra_tti_pollutants=extra_tti_pollutants_a,
        templ_pol_rep_per_cpy_=templ_pol_rep_per_cpy,
        templ_root_payload_cers_loc_locemprc_report_period_A_=templ_root_payload_cers_loc_locemprc_report_period_A,
    )

    all_reporting_periods = templ_root_payload_cers_loc_locemprc.findall(
        "payload:ReportingPeriod/payload:ReportingPeriodEmissions", ns
    )
    spec_pol = [
        "100414",
        "106990",
        "107028",
        "108883",
        "110543",
        "120127",
        "123386",
        "129000",
        "1330207",
        "1746016",
        "18540299",
        "191242",
        "193395",
        "19408743",
        "205992",
        "206440",
        "207089",
        "208968",
        "218019",
        "3268879",
        "35822469",
        "39001020",
        "50000",
        "50328",
        "51207319",
        "53703",
        "540841",
        "56553",
        "57117314",
        "57117416",
        "57117449",
        "57653857",
        "67562394",
        "70648269",
        "71432",
        "72918219",
        "7439965",
        "7439976",
        "7440020",
        "7440382",
        "75070",
        "83329",
        "85018",
        "86737",
        "91203",
    ]
    for rep_per in all_reporting_periods:
        emis_calc_mthd_cd = ET.Element("cer:EmissionCalculationMethodCode")
        emis_calc_comment = ET.Element("cer:EmissionsComment")
        pol = rep_per.find("payload:PollutantCode", ns).text
        if pol in ["7439921", "NH3"]:
            emis_calc_mthd_cd.text = "13"
            if pol == "7439921":
                emis_calc_comment.text = "Based on https://www.tceq.texas.gov/assets/public/implementation/air/am/contracts/reports/ei/582155153802FY15-20150826-erg-locomotive_2014aerr_inventory_trends_2008to2040.pdf"
            elif pol == "NH3":
                emis_calc_comment.text = "Based on Table III-6, 2nd row in https://19january2017snapshot.epa.gov/sites/production/files/2015-08/documents/eiip_areasourcesnh3.pdf"
        elif pol in ["CO", "CO2", "NOX", "PM10-PRI", "PM25-PRI", "SO2", "VOC"]:
            emis_calc_mthd_cd.text = "8"
            if pol == "NOX":
                emis_calc_comment.text = "Based on https://nepis.epa.gov/Exe/ZyPURL.cgi?Dockey=P100500B.txt. Also considers TxLED factor."
            elif pol == "CO2":
                emis_calc_comment.text = (
                    "Based on https://nepis.epa.gov/Exe/ZyPURL.cgi?Dockey=P100500B.txt"
                )
            elif pol == "SO2":
                emis_calc_comment.text = (
                    "Based on https://nepis.epa.gov/Exe/ZyPURL.cgi?Dockey=P100500B.txt"
                )
            else:
                emis_calc_comment.text = (
                    "Based on https://nepis.epa.gov/Exe/ZyPURL.cgi?Dockey=P100500B.txt"
                )
        elif pol in spec_pol:
            emis_calc_mthd_cd.text = "5"
            emis_calc_comment.text = "Most recent EPA speciation table"
        else:
            raise ValueError("pollutant not handled in if-elif.")
        rep_per.append(emis_calc_mthd_cd)
        rep_per.append(emis_calc_comment)


def print_xml_lines(tree_elem: xml.etree.ElementTree.Element, max_lines=10) -> None:
    line_no = 1
    for line in (
        ET.tostring(tree_elem, encoding="utf8").decode("utf8").split("\n")[0:max_lines]
    ):
        print(f"Line number {line_no}. Element: ", line)
        line_no += 1


def print_elem(parent_elem: xml.etree.ElementTree.Element, num_sub_elem=20):
    counter_ = 1
    for elem in parent_elem.iter():
        print(elem)
        if counter_ == num_sub_elem:
            break
        counter_ += 1


def print_child(parent_elem: xml.etree.ElementTree.Element, num_children=20):
    counter_ = 1
    for elem in parent_elem:
        print(elem)
        if counter_ == num_children:
            break
        counter_ += 1


def register_all_namespaces(filename):
    namespaces = dict([node for _, node in ET.iterparse(filename, events=["start-ns"])])
    for ns in namespaces:
        ET.register_namespace(ns, namespaces[ns])


if __name__ == "__main__":
    path_emis_rate = glob.glob(
        os.path.join(PATH_INTERIM, f"emission_factor_[" f"0-9]*-*-*.csv")
    )[0]
    tti_pol_list_a = set(pd.read_csv(path_emis_rate).pollutant.unique())
    tti_pol_list_o3d = set(["NOX", "VOC", "CO"])
    path_dir_templ = os.path.join(PATH_RAW, "ERG")
    path_templ = os.path.join(path_dir_templ, "rail2020-Uncontrolled.xml")
    path_out_templ = os.path.join(PATH_INTERIM, "xml_rail_templ_tti.xml")
    register_all_namespaces(path_templ)
    templ_tree = ET.parse(path_templ)
    templ_root = templ_tree.getroot()
    print_xml_lines(tree_elem=templ_root, max_lines=40)
    ns = {
        "header": "http://www.exchangenetwork.net/schema/header/2",
        "payload": "http://www.exchangenetwork.net/schema/cer/1",
    }

    set_document_id(templ_root_=templ_root)
    print(templ_root.attrib)
    templ_root_header = modify_template_header(templ_root_=templ_root, ns=ns)
    print_xml_lines(tree_elem=templ_root, max_lines=40)
    modify_payload(
        templ_root_=templ_root,
        tti_pol_list_a_=tti_pol_list_a,
        tti_pol_list_o3d_=tti_pol_list_o3d,
        ns=ns,
    )
    templ_tree.write(path_out_templ, encoding="utf-8", xml_declaration=True)
    lxml_etree_root = lxml_etree.parse(
        str(path_out_templ),
        parser=lxml_etree.XMLParser(remove_blank_text=True, remove_comments=True),
    )
    lxml_etree_root.write(
        str(path_out_templ), pretty_print=True, encoding="utf-8", xml_declaration=True
    )
