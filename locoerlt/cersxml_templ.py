import os
import xml
import xml.etree.ElementTree as ET
import time
import datetime
import glob
import copy
import pandas as pd
from locoerlt.utilis import PATH_RAW, PATH_INTERIM, PATH_PROCESSED


def set_document_id(templ_root_: xml.etree.ElementTree.Element,
                    doc_id="locomotives_cers_aerr_2020_xml") -> None:
    templ_root_.set("id", doc_id)



def set_creation_datetime(templ_root_header_: xml.etree.ElementTree.Element) -> None:
    creation_datetime = templ_root_header_.find("header:CreationDateTime", ns)
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%dT%H:%M:%S")
    creation_datetime.text = st


def set_data_cat_prop(
    templ_root_header_: xml.etree.ElementTree.Element, data_cat_value_text="Nonroad"
) -> None:
    data_cat_prop = templ_root_header_.find(
        "./header:Property/[header:PropertyName='DataCategory']", ns
    )
    data_cat_value = data_cat_prop.find("header:PropertyValue", ns)
    data_cat_value.text = data_cat_value_text


def modify_template_header(
    templ_root_: xml.etree.ElementTree.Element,
) -> xml.etree.ElementTree.Element:
    templ_root_header_ = templ_root_.find("header:Header", ns)
    set_creation_datetime(templ_root_header_=templ_root_header_)
    set_data_cat_prop(templ_root_header_=templ_root_header_)
    return templ_root_header_


def delele_253_county_loc(
    templ_root_payload_cers_: xml.etree.ElementTree.Element,
):
    counter_ = 1
    for child in templ_root_payload_cers_.findall("payload:Location", ns):
        if counter_ > 1:
            templ_root_payload_cers_.remove(child)
        counter_ += 1


def set_generic_stateandcountyfipscode(
        templ_root_payload_cers_loc_: xml.etree.ElementTree.Element,
):
    templ_root_payload_cers_loc_.find("payload:StateAndCountyFIPSCode",
                                      ns).text = "five_digit_tx_county_fips"


def delete_2_extra_scc_loc_em_prc(
        templ_root_payload_cers_loc_: xml.etree.ElementTree.Element,
):
    counter_ = 1
    for child in templ_root_payload_cers_loc_.findall(
            "payload:LocationEmissionsProcess", ns):
        if counter_ > 1:
            templ_root_payload_cers_loc_.remove(child)
        counter_ += 1


def set_generic_sourceclassificationcode_emtype(
    templ_root_payload_cers_loc_locemprc_: xml.etree.ElementTree.Element,
):
    templ_root_payload_cers_loc_locemprc_scc = (
        templ_root_payload_cers_loc_locemprc_.find(
            "payload:SourceClassificationCode", ns)
    )
    templ_root_payload_cers_loc_locemprc_scc.text = "ten_digit_scc"
    templ_root_payload_cers_loc_locemprc_emtype = (
        templ_root_payload_cers_loc_locemprc_.find(
            "payload:EmissionsTypeCode", ns)
    )
    templ_root_payload_cers_loc_locemprc_emtype.text = "X"


def get_annual_o3d_pollutants_erg(
    templ_root_payload_cers_loc_locemprc_report_period_A_: xml.etree.ElementTree.Element,
    templ_root_payload_cers_loc_locemprc_report_period_O3D_: xml.etree.ElementTree.Element,
    ns_: dict[str, str]
) -> dict[xml.etree.ElementTree.Element, xml.etree.ElementTree.Element]:
    annual_pollutant_elements = (
        templ_root_payload_cers_loc_locemprc_report_period_A_
        .findall(
            "payload:ReportingPeriodEmissions"
            "/payload:PollutantCode"
            ,
            ns_
        )
    )
    o3d_pollutant_elements = (
        templ_root_payload_cers_loc_locemprc_report_period_O3D_
        .findall(
            "payload:ReportingPeriodEmissions"
            "/payload:PollutantCode"
            ,
            ns_
        )
    )
    return {
        "annual_pollutant_elements": annual_pollutant_elements,
        "o3d_pollutant_elements": o3d_pollutant_elements
    }


def delete_pollutant_complexes_not_in_tti_output(
    missing_tti_pollutants: list,
    templ_root_payload_cers_: xml.etree.ElementTree.Element,

):
    for pol in list(missing_tti_pollutants):
        delete_report_period_em_tags_list = templ_root_payload_cers_.findall(
            "payload:Location"
            "/payload:LocationEmissionsProcess"
            "/payload:ReportingPeriod"
            "/[payload:ReportingPeriodTypeCode='A']"
            "/payload:ReportingPeriodEmissions"
            f"/[payload:PollutantCode='{pol}']"
            ,
            ns
        )
        delete_report_period_em_tag_parent_list = \
            templ_root_payload_cers_.findall(
                "payload:Location"
                "/payload:LocationEmissionsProcess"
                "/payload:ReportingPeriod"
                "/[payload:ReportingPeriodTypeCode='A']"
                "/payload:ReportingPeriodEmissions"
                f"/[payload:PollutantCode='{pol}']..."
                ,
                ns
        )
        for bad_child, parent in zip(delete_report_period_em_tags_list,
                                     delete_report_period_em_tag_parent_list):
            parent.remove(bad_child)


def deepcopy_reportingperiodemissions_complex_for_template(
        templ_root_payload_cers_loc_locemprc_report_period_A_:
        xml.etree.ElementTree.Element,
) -> xml.etree.ElementTree.Element:
    templ_pol_rep_per_emis = \
        templ_root_payload_cers_loc_locemprc_report_period_A_.find(
                "payload:ReportingPeriodEmissions"
                "/[payload:PollutantCode='100414']"
                ,
                ns
        )
    templ_pol_rep_per_cpy = copy.deepcopy(templ_pol_rep_per_emis)
    return templ_pol_rep_per_cpy


def create_pollutant_complexes_in_tti_output_not_in_erg(
    extra_tti_pollutants: list,
    templ_root_payload_cers_loc_locemprc_report_period_A_:
    xml.etree.ElementTree.Element,
    templ_pol_rep_per_cpy_: xml.etree.ElementTree.Element,
):
    for pol_elem in extra_tti_pollutants:
        templ_pol_rep_per_cpy_.find("payload:PollutantCode", ns).text = f"{pol_elem}"
        templ_pol_rep_per_cpy_.find("payload:TotalEmissions", ns).text = "0"
        templ_pol_rep_per_cpy_.find("payload:EmissionsUnitofMeasureCode",
                                    ns).text = "TON"
        templ_pol_rep_per_cpy_cpy = copy.deepcopy(templ_pol_rep_per_cpy_)
        templ_root_payload_cers_loc_locemprc_report_period_A_.append(
            templ_pol_rep_per_cpy_cpy
        )

def modify_payload(
    templ_root_: xml.etree.ElementTree.Element,
) -> xml.etree.ElementTree.Element:

    templ_root_payload = templ_root_.find("header:Payload", ns)
    templ_root_payload_cers = templ_root_payload.find("payload:CERS", ns)
    delele_253_county_loc(
        templ_root_payload_cers_=templ_root_payload_cers
    )
    assert len(templ_root_payload_cers.findall(
        "payload:Location", ns)) == 1, "Above operation didn't delete all " \
                                       "counties."
    templ_root_payload_cers_loc = templ_root_payload_cers.find(
        "payload:Location", ns)
    set_generic_stateandcountyfipscode(
        templ_root_payload_cers_loc_=templ_root_payload_cers_loc
    )
    delete_2_extra_scc_loc_em_prc(
        templ_root_payload_cers_loc_=templ_root_payload_cers_loc
    )
    assert len(templ_root_payload_cers_loc.findall(
        "payload:LocationEmissionsProcess", ns)) == 1, (
        "Above operation didn't delete all counties.")
    templ_root_payload_cers_loc_locemprc = templ_root_payload_cers_loc.find(
        "payload:LocationEmissionsProcess", ns
    )
    set_generic_sourceclassificationcode_emtype(
        templ_root_payload_cers_loc_locemprc_
        =templ_root_payload_cers_loc_locemprc,
    )
    templ_root_payload_cers_loc_locemprc_report_period_A = (
        templ_root_payload_cers_loc_locemprc.find(
            "payload:ReportingPeriod/[payload:ReportingPeriodTypeCode='A']", ns)
    )
    templ_root_payload_cers_loc_locemprc_report_period_O3D = (
        templ_root_payload_cers_loc_locemprc.find(
            "payload:ReportingPeriod/[payload:ReportingPeriodTypeCode='O3D']", ns)
    )
    annual_o3d_pol_dict = get_annual_o3d_pollutants_erg(
        templ_root_payload_cers_loc_locemprc_report_period_A_
        =templ_root_payload_cers_loc_locemprc_report_period_A,
        templ_root_payload_cers_loc_locemprc_report_period_O3D_
        =templ_root_payload_cers_loc_locemprc_report_period_O3D,
        ns_=ns
    )
    annual_pollutant_elements = annual_o3d_pol_dict["annual_pollutant_elements"]
    txerr_pols = set(map(lambda elem: elem.text, annual_pollutant_elements))
    missing_tti_pollutants = txerr_pols - tti_pol_list
    extra_tti_pollutants = tti_pol_list - txerr_pols
    delete_pollutant_complexes_not_in_tti_output(
        missing_tti_pollutants=missing_tti_pollutants,
        templ_root_payload_cers_=templ_root_payload_cers
    )
    templ_pol_rep_per_cpy = (
        deepcopy_reportingperiodemissions_complex_for_template(
            templ_root_payload_cers_loc_locemprc_report_period_A_
            =templ_root_payload_cers_loc_locemprc_report_period_A,
        )
    )
    create_pollutant_complexes_in_tti_output_not_in_erg(
        extra_tti_pollutants=extra_tti_pollutants,
        templ_pol_rep_per_cpy_=templ_pol_rep_per_cpy,
        templ_root_payload_cers_loc_locemprc_report_period_A_=templ_root_payload_cers_loc_locemprc_report_period_A
    )


def print_xml_lines(tree_elem: xml.etree.ElementTree.Element, max_lines=10) -> None:
    line_no = 1
    for line in (
        ET.tostring(tree_elem, encoding="utf8").decode("utf8").split("\n")[0:max_lines]
    ):
        print(f"Line number {line_no}. Element: ", line)
        line_no += 1


def print_elem(
        parent_elem: xml.etree.ElementTree.Element,
        num_sub_elem=20,
):
    counter_ = 1
    for elem in parent_elem.iter():
        print(elem)
        if counter_ == num_sub_elem:
            break
        counter_ += 1


def print_child(
        parent_elem: xml.etree.ElementTree.Element,
        num_children=20,
):
    counter_ = 1
    for elem in parent_elem:
        print(elem)
        if counter_ == num_children:
            break
        counter_ += 1


if __name__ == "__main__":
    path_uncntr_emisquant = glob.glob(
        os.path.join(PATH_PROCESSED, "uncntr_emis_quant_[0-9]*-*-*.csv")
    )[0]

    path_cntr_emisquant = glob.glob(
        os.path.join(PATH_PROCESSED, "cntr_emis_quant_[0-9]*-*-*.csv")
    )[0]
    uncntr_emisquant = pd.read_csv(path_uncntr_emisquant, index_col=0)
    cntr_emisquant = pd.read_csv(path_cntr_emisquant, index_col=0)
    tti_pol_list = set(uncntr_emisquant.pollutant.unique())
    path_dir_templ = os.path.join(PATH_RAW, "ERG")
    path_templ = os.path.join(path_dir_templ, "rail2020-Uncontrolled.xml")
    path_out_templ = os.path.join(PATH_INTERIM, "xml_rail_templ_tti.xml")
    templ_tree = ET.parse(path_templ)
    templ_root = templ_tree.getroot()
    print_xml_lines(tree_elem=templ_root, max_lines=20)
    ns = {
        "header": "http://www.exchangenetwork.net/schema/header/2",
        "payload": "http://www.exchangenetwork.net/schema/cer/1",
    }

    set_document_id(templ_root_=templ_root)
    print(templ_root.attrib)
    templ_root_header = modify_template_header(templ_root_=templ_root)
    print_xml_lines(tree_elem=templ_root, max_lines=20)
    templ_tree.write(path_out_templ)
