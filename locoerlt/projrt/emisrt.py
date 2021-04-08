"""
Develop emission rate table for expected list of pollutants.
"""
import os
import numpy as np
import pandas as pd
from locoerlt.utilis import PATH_INTERIM, PATH_PROCESSED


def expected_pol_list() -> pd.DataFrame:
    """"""
    ...


def get_fleet_mix() -> pd.DataFrame():
    """"""
    ...


def epa_tech_report_rates() -> pd.DataFrame:
    """"""
    ...


def mass_balance_rates() -> pd.DataFrame:
    """"""
    ...


def hap_speciation_rates() -> pd.DataFrame:
    """"""
    ...


def cap_hap_combined_rates() -> pd.DataFrame:
    """"""
    ...


if __name__ == "__main__":

    # Expected Pollutant List: NEI 2017-->Nonpoint-->Expected Pollutant List
    # for Nonpoint SCCs
    # https://www.epa.gov/air-emissions-inventories/2017-national-emissions-inventory-nei-data
    # https://www.epa.gov/sites/production/files/2018-07/np_expected_poll_list_complete_v1.xlsx
    path_exp_pol_list = os.path.join(
        PATH_INTERIM, "epa_pol_list", "np_expected_poll_list_complete_v1.xlsx"
    )
    # Hazardous air pollutants speciation table from EPA NEI 2017 supporting
    # docs: NEI 2017 --> Supporting Data and Summaries --> nonpoint/
    # --> 2017Rail_HAP_AugmentationProfileAssignmentFactors_20200128.xlsx
    # https://www.epa.gov/air-emissions-inventories/2017-national-emissions-inventory-nei-data
    # https://gaftp.epa.gov/air/nei/2017/doc/supporting_data/nonpoint/2017Rail_HAP_AugmentationProfileAssignmentFactors_20200128.xlsx
    path_hap_speciation = os.path.join(
        PATH_INTERIM,
        "epa_speciation_table",
        "2017Rail_HAP_AugmentationProfileAssignmentFactors_20200128.xlsx",
    )
