import pytest
import os
import glob
import pandas as pd
import xml
import xml.etree.ElementTree as ET
from locoerlt.utilis import PATH_RAW, PATH_INTERIM

# Output emission factors path.
path_emis_rate = glob.glob(os.path.join(PATH_INTERIM, f"emission_factor_["
                                               f"0-9]*-*-*.csv"))[0]
ns = {
    "header": "http://www.exchangenetwork.net/schema/header/2",
    "payload": "http://www.exchangenetwork.net/schema/cer/1",
}
path_xml_templ = os.path.join(PATH_INTERIM, "xml_rail_templ_tti.xml")


@pytest.fixture()
def get_uniq_pol_list():
    return set(pd.read_csv(path_emis_rate).pollutant.unique())


@pytest.fixture()
def get_annual_pol_list_xml():
    templ_tree = ET.parse(path_xml_templ)
    templ_root = templ_tree.getroot()
    templ_root_pol_code_elem = templ_root.findall(
        ".//payload:ReportingPeriod"
        "/[payload:ReportingPeriodTypeCode='A']"
        "/*"
        "/payload:PollutantCode", ns
    )
    return set([elem.text for elem in templ_root_pol_code_elem])


@pytest.fixture()
def get_o3d_pol_list_xml():
    templ_tree = ET.parse(path_xml_templ)
    templ_root = templ_tree.getroot()
    templ_root_pol_code_elem = templ_root.findall(
        ".//payload:ReportingPeriod"
        "/[payload:ReportingPeriodTypeCode='O3D']"
        "/*"
        "/payload:PollutantCode", ns
    )
    return set([elem.text for elem in templ_root_pol_code_elem])


def test_get_annual_pol_list_xml_equal_input_list(
    get_annual_pol_list_xml,
    get_uniq_pol_list
):
    assert get_annual_pol_list_xml == get_uniq_pol_list


def test_get_o3d_pol_list_xml_equal_input_list(
    get_o3d_pol_list_xml
):
    assert get_o3d_pol_list_xml == set(["CO", "NH3", "NOX", "PM10-PRI",
                                       "PM25-PRI", "SO2", "VOC"])




