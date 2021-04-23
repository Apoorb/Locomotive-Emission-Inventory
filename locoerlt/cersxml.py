import os
import xml
import xml.etree.ElementTree as ET
import time
import datetime
import copy

path_dir_templ = (
    r"C:\Users\a-bibeka\Texas A&M Transportation Institute"
    r"\HMP - TCEQ Projects - Documents"
    r"\2020 Texas Statewide Locomotive and Rail Yard EI\Tasks"
    r"\Task5_ Statewide_2020_AERR_EI\Ref\MOVESsccXMLformat\Output"
)
path_templ = os.path.join(path_dir_templ, "MOVESsccXMLformat_Test_xml.xml")
templ_tree = ET.parse(path_templ)
templ_root = templ_tree.getroot()
ns = {
    "header": "http://www.exchangenetwork.net/schema/header/2",
    "payload": "http://www.exchangenetwork.net/schema/cer/1",
}


def print_xml_lines(tree_elem: xml.etree.ElementTree.Element, max_lines=10) -> None:
    line_no = 1
    for line in (
        ET.tostring(tree_elem, encoding="utf8").decode("utf8").split("\n")[0:max_lines]
    ):
        print(f"Line number {line_no}. Element: ", line)
        line_no += 1


def set_document_id(templ_root_: xml.etree.ElementTree.Element) -> None:
    templ_root_.set("id", "locomotives_cers_aerr_2020_xml")


def set_creation_datetime(templ_root_header_: xml.etree.ElementTree.Element) -> None:
    creation_datetime = templ_root_header_.find("header:CreationDateTime", ns)
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%dT%H:%M:%S")
    creation_datetime.text = st


def set_data_cat_prop(
    templ_root_header_: xml.etree.ElementTree.Element, doc_title=""
) -> None:
    data_cat_prop = templ_root_header_.find(
        "./header:Property/[header:PropertyName='DataCategory']", ns
    )
    data_cat_value = data_cat_prop.find("header:PropertyValue", ns)
    data_cat_value.text = "Nonpoint"


def modify_template_header(
    templ_root_: xml.etree.ElementTree.Element,
) -> xml.etree.ElementTree.Element:
    templ_root_header_ = templ_root_.find("header:Header", ns)
    set_creation_datetime(templ_root_header_=templ_root_header_)
    set_data_cat_prop(templ_root_header_=templ_root_header_)
    return templ_root_header_


set_document_id(templ_root_=templ_root)
print(templ_root.attrib)
templ_root_header = modify_template_header(templ_root_=templ_root)
print_xml_lines(tree_elem=templ_root, max_lines=18)


templ_root_payload = templ_root.find("header:Payload", ns)
templ_root_payload_cers = templ_root_payload.find("payload:CERS", ns)
templ_root_county_48001 = templ_root_payload_cers.find(
    "payload:Location/[payload:StateAndCountyFIPSCode='48001']", ns
)
templ_root_county_48141 = templ_root_payload_cers.find(
    "payload:Location/[payload:StateAndCountyFIPSCode='48141']", ns
)
templ_root_payload_cers.remove(templ_root_county_48001)
for child in templ_root_payload_cers:
    if child != templ_root_county_48141:
        print(child.tag)

print_xml_lines(tree_elem=templ_root_payload_cers)
for line in (
    ET.tostring(templ_root_payload_cers, encoding="utf8")
    .decode("utf8")[0:1000]
    .split("\n")
):
    print(line)
