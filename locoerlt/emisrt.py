"""
Develop emission rate table for expected list of pollutants.
"""
import time
import datetime
import os
import numpy as np
import inflection
import pandas as pd
from locoerlt.utilis import (
    PATH_RAW,
    PATH_INTERIM,
    PATH_PROCESSED,
    get_out_file_tsmp,
    cleanup_prev_output,
)


def expected_pol_list(path_exp_pol_list_: str) -> pd.DataFrame:
    """
    Get the expected list of pollutants from the 2017 expected list of
    pollutants file:
    https://www.epa.gov/sites/production/files/2018-07/np_expected_poll_list_complete_v1.xlsx

    Change "Xylenes" to 1330207 to match the nomenclature in speciation table.

    Parameters
    ----------
    path_exp_pol_list_
        Path to the expected list of pollutants xlsx.

    Returns
    -------
    pd.DataFrame
        Dataframe of expected list of pollutants.
    """
    x_pol = pd.ExcelFile(path_exp_pol_list_)
    pol_df = x_pol.parse("NP Expected Pollutants List")
    pol_lab = x_pol.parse("Xwalk_pollutant_descriptions")
    pol_cols = [
        col
        for col in pol_df.columns
        if col not in ("EPA Tool?", "SCC", "SCC Description", "Sector")
    ]

    pol_lab_fil = pol_lab.filter(
        items=["pollutant", "poltype", "poldesc", "polcat", "group"]
    ).rename(
        columns={
            "group": "pol_grp",
            "polcat": "pol_cat",
            "poltype": "pol_type",
            "poldesc": "pol_desc",
        }
    )
    pol_df_fil_ = (
        pol_df.loc[lambda df: df.Sector == "Mobile - Locomotives"]
        .drop(columns=["EPA Tool?"])
        .melt(
            id_vars=["SCC", "SCC Description", "Sector"],
            value_vars=pol_cols,
            var_name="pollutant",
            value_name="is_needed",
        )
        .dropna(subset=["is_needed"])
        .merge(pol_lab_fil, on="pollutant", how="left")
        .rename(
            columns={"SCC": "scc", "SCC Description": "scc_desc", "Sector": "sector"}
        )
        .assign(pollutant=lambda df: (df.pollutant.replace({"Xylenes": 1330207})))
    )
    return pol_df_fil_


def hap_speciation_mult(path_hap_speciation_: str) -> pd.DataFrame:
    """
    Use the most recent EPA speciation table. This speciation table would
    likely be used for 2020 NEI.

    Parameters
    ----------
    path_hap_speciation_:
        Path to the speciation data.

    Returns
    -------
    pd.DataFrame
        Cleaned speciation table.
    """
    x_hap = pd.ExcelFile(path_hap_speciation_)
    speciation_2020 = x_hap.parse("Non Point 2020 Speciation Table")

    hap_rename_map = {
        col: inflection.underscore(col).replace(" ", "_")
        for col in speciation_2020.columns
    }
    speciation_2020_fil_ = (
        speciation_2020.rename(columns=hap_rename_map)
        .rename(columns={"scc_assignment": "scc", "data_category_code": "dat_cat_code"})
        .sort_values(by=["input_pollutant_code", "output_pollutant_description", "scc"])
        .reset_index(drop=True)
        .filter(
            items=[
                "dat_cat_code",
                "scc",
                "scc_description_level_1",
                "scc_description_level_2",
                "scc_description_level_3",
                "scc_description_level_4",
                "sector_description",
                "output_pollutant_code",
                "output_pollutant_description",
                "input_pollutant_code",
                "input_pollutant_description",
                "multiplication_factor",
            ]
        )
    )
    return speciation_2020_fil_


def pb_speciation_builder(speciation_2020_fil_: pd.DataFrame) -> pd.DataFrame:
    """
    Create speciation table for lead based on the ERG Table 5-6 Hazardous Air
    Pollutant Speciation Profile for Locomotove Activities. ERG used 2011
    NEI speciation table.
    """
    pb_template_df = speciation_2020_fil_.drop_duplicates(
        [
            "dat_cat_code",
            "scc",
            "scc_description_level_1",
            "scc_description_level_2",
            "scc_description_level_3",
            "scc_description_level_4",
            "sector_description",
        ]
    )
    pb_speciation_df = pb_template_df.assign(
        input_pollutant_code="PM10-PRI",
        input_pollutant_description="PM10 Primary (Filt + Cond)",
        output_pollutant_code=7439921,
        output_pollutant_description="Lead",
        multiplication_factor=8.405e-05,  # ERG report
    )
    return pb_speciation_df


def epa_tech_report_fac(path_nox_pm10_hc_epa_em_fac_: str) -> pd.DataFrame:
    """
    Use the 2009 (most recent) EPA technical highlights emission factors for
    locomotives for NOx, PM10, and HC:
    https://nepis.epa.gov/Exe/ZyPURL.cgi?Dockey=P100500B.txt

    EPA provides emission rates till 2040. 2040 rates are extended till 2050 to
    get a conservative estimate of emission rates post 2040.

    Parameters
    ----------
    path_nox_pm10_hc_epa_em_fac_:
        Excel file created from power BI. Contains NOx, PM10, and HC rates from
        2006 to 2040 in long format.

    Returns
    -------
    pd.DataFrame
        NOx, PM10, and HC rates from 2006 to 2050.

    """
    map_ssc_desc_4 = {
        "large_line_haul": "Line Haul Locomotives: Class I Operations",
        "small_rr": "Line Haul Locomotives: Class II / III Operations",
        "passenger_commuter": [
            "Line Haul Locomotives: Passenger Trains (Amtrak)",
            "Line Haul Locomotives: Commuter Lines",
        ],
        "large_switch": "Yard Locomotives",
    }
    nox_pm10_hc_epa_em_fac = pd.read_excel(path_nox_pm10_hc_epa_em_fac_)
    nox_pm10_hc_epa_em_fac_impute_ = (
        nox_pm10_hc_epa_em_fac.assign(
            year=lambda df: np.select(
                [df.year == 2040, df.year != 2040],
                [set(np.arange(2040, 2051)), df.year],
                # Fill 2040 values to years 2040 to 2050
                np.nan,
            ),
            scc_description_level_4=lambda df: df.carriers.map(map_ssc_desc_4),
        )
        .explode("year")  # Fill 2040 values to years 2040 to 2050
        .explode("scc_description_level_4")
        .dropna(subset=["scc_description_level_4"])
        .filter(items=["scc_description_level_4", "pollutant", "year", "em_fac"])
    )
    return nox_pm10_hc_epa_em_fac_impute_


def em_fac_template(
    all_pol_df: pd.DataFrame, speciation_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Create a template that will be used to prepare emission rates for
    different pollutants.

    Parameters
    ----------
    all_pol_df:
        Get "scc", "pollutant", "pol_type", "pol_desc" columns from this
        dataset.
    speciation_df:
        Get "dat_cat_code", "scc", "scc_description_level_1",
        "scc_description_level_2", "scc_description_level_3",
        "scc_description_level_4", "sector_description" from this dataset.

    Returns
    -------
    pd.DataFrame
        Template that will be used to prepare emission rates for different
        pollutants.
    """
    cap_df = all_pol_df.query("pol_type=='CAP'").filter(
        items=["scc", "pollutant", "pol_type", "pol_desc"]
    )
    em_fac_df_template_ = (
        speciation_df[
            [
                "dat_cat_code",
                "scc",
                "scc_description_level_1",
                "scc_description_level_2",
                "scc_description_level_3",
                "scc_description_level_4",
                "sector_description",
            ]
        ]
        .drop_duplicates()
        .merge(cap_df, on=["scc"])
        .assign(anals_yr=np.nan, em_fac=np.nan, em_units="grams/gallon")
    )
    return em_fac_df_template_


def co2_fac(em_fac_df_template_: pd.DataFrame) -> pd.DataFrame:
    """
    Get CO2 emission rates.
    10,084 grams/gallon = 2778 * 0.99 * (44 / 12)

    Diesel carbon content per gallon of diesel * oxidation factor * molecular wt
    of CO2 by C: https://nepis.epa.gov/Exe/ZyPURL.cgi?Dockey=P1001YTF.txt
    """
    co2_em_fac_df = em_fac_df_template_[lambda df: df.pollutant == "CO"].assign(
        pollutant="CO2",
        pol_type="GHG",
        pol_desc="Carbon Dioxide",
    )
    co2_em_fac_df_1 = co2_em_fac_df.assign(
        em_fac=2778 * 0.99 * (44 / 12),
        anals_yr=[list(np.arange(2011, 2051, 1))] * len(co2_em_fac_df),
    ).explode(column="anals_yr")
    return co2_em_fac_df_1


def co_fac(em_fac_df_template_: pd.DataFrame) -> pd.DataFrame:
    """
    Use table 1, 2, and 3 from the 2009 (most recent) EPA technical
    highlights emission factors for locomotives:
    https://nepis.epa.gov/Exe/ZyPURL.cgi?Dockey=P100500B.txt
    """
    co_em_fac_df = em_fac_df_template_[lambda df: df.pollutant == "CO"]

    co_em_fac_df_1 = co_em_fac_df.assign(
        em_fac=lambda df: np.select(
            [
                df.scc_description_level_4.isin(
                    [
                        "Line Haul Locomotives: Class I Operations",
                        "Line Haul Locomotives: Passenger Trains (Amtrak)",
                        "Line Haul Locomotives: Commuter Lines",
                    ]
                ),
                df.scc_description_level_4.isin(
                    ["Line Haul Locomotives: Class II / III Operations"]
                ),
                df.scc_description_level_4.isin(["Yard Locomotives"]),
            ],
            [
                1.28 * 20.8,
                1.28 * 18.2,
                1.83 * 15.2,
            ],
            np.nan,
        ),
        anals_yr=[list(np.arange(2011, 2051, 1))] * len(co_em_fac_df),
    ).explode(column="anals_yr")
    return co_em_fac_df_1


def nh3_fac(em_fac_df_template_: pd.DataFrame) -> pd.DataFrame:
    """
    0.083 grams/gallon = 1.83e-04 * 453.592
    Based on Table III-6, 2nd row in
    https://19january2017snapshot.epa.gov/sites/production/files/2015-08/documents/eiip_areasourcesnh3.pdf

    """
    nh3_em_fac_df = em_fac_df_template_[lambda df: df.pollutant == "NH3"]
    nh3_em_fac_df_1 = nh3_em_fac_df.assign(
        em_fac=1.83e-04 * 453.592,
        anals_yr=[list(np.arange(2011, 2051, 1))] * len(nh3_em_fac_df),
    ).explode(column="anals_yr")
    return nh3_em_fac_df_1


def so2_fac(
    em_fac_df_template_: pd.DataFrame, pre_2011_sulfur_ppm=500, post_2011_sulfur_ppm=15
) -> pd.DataFrame:
    """
    **3.10861594** grams/gallon for 2011 **0.09325847817** grams/gallon for
    2012 and
    beyond.

    1. Diesel density in metric tons/ bbl is 0.1346.
    Metric ton/ bbl to grams/gallon conversion is 23,809.5. Source: Table  MM--1
    in https://www.govinfo.gov/content/pkg/FR-2009-10-30/pdf/E9-23315.pdf

    2. 97%: % of sulphur coverting to SO2. Source:
    https://nepis.epa.gov/Exe/ZyPDF.cgi?Dockey=P10001SB.pdf

    3. Molecular wt of SO2 by S: 64/32

    4. Sulfur content:  https://clean-diesel.org/nonroad.html and
    EPA420-F-04-032: https://nepis.epa.gov/Exe/ZyPURL.cgi?Dockey=P10001RN.txt

    4.1 15 ppm 2012 and after

    4.2 500 ppm 2011 and before # ERG uses 300 ppm

    Parameters
    ----------
    em_fac_df_template_:
        Emission factor template.
    pre_2011_sulfur_ppm:
        2011 and pre-2011 sulfur content. I (Apoorb) am using 500 ppm, which, is
        the regulatory limit.
    post_2011_sulfur_ppm:
            2012 and post-2012 sulfur content. I (Apoorb) am using 15 ppm,
            which, is the regulatory limit (ULSD).
    Returns
    -------
    pd.DataFrame
        SO2 emission rate.
    """
    so2_em_fac_df = em_fac_df_template_[lambda df: df.pollutant == "SO2"]
    so2_em_fac_df_1 = (
        so2_em_fac_df.assign(
            anals_yr=[list(np.arange(2011, 2051, 1))] * len(so2_em_fac_df),
        )
        .explode(column="anals_yr")
        .assign(
            em_fac=lambda df: np.select(
                [
                    df.anals_yr <= 2011,
                    df.anals_yr >= 2012,
                ],
                [
                    (0.1346 * 23809.5) * 0.97 * (64 / 32) * pre_2011_sulfur_ppm * 1e-6,
                    (0.1346 * 23809.5) * 0.97 * (64 / 32) * post_2011_sulfur_ppm * 1e-6,
                ],
                np.nan,
            ),
        )
    )
    return so2_em_fac_df_1


def epa_2009_proj_table_fac(
    em_fac_df_template_: pd.DataFrame,
    pollutant: str,
    epa_2009_rts: pd.DataFrame,
) -> pd.DataFrame:
    """
    Provides emission rates based on the data from epa_tech_report_fac()
    function.

    Parameters
    ----------
    em_fac_df_template_
    pollutant:
        Pollutant for which rates are needed. Should be one of the following:
        [NOX, PM10, PM25, VOC]
    epa_2009_rts

    Returns
    -------
    pd.DataFrame
        Emission factors for 2011 to 2050 for the "pollutant" provided by the user.
    """
    if pollutant not in ["NOX", "PM10-PRI", "PM25-PRI", "VOC"]:
        raise ValueError(
            f"Cannot handle pollutant {pollutant} "
            f"Function only handles the following pollutants: "
            f"NOX, PM10-PRI, PM25-PRI, VOC"
        )
    pol_em_fac_df = em_fac_df_template_[lambda df: df.pollutant == pollutant]
    pol_em_fac_df_1 = (
        pol_em_fac_df.drop(columns="em_fac")
        .assign(
            anals_yr=[list(np.arange(2011, 2051, 1))] * len(pol_em_fac_df),
        )
        .explode(column="anals_yr")
        .merge(
            epa_2009_rts,
            left_on=["scc_description_level_4", "pollutant", "anals_yr"],
            right_on=["scc_description_level_4", "pollutant", "year"],
            how="left",
        )
        .drop(columns="year")
    )
    return pol_em_fac_df_1


def create_pm25_fac(epa_2009_rts: pd.DataFrame) -> pd.DataFrame:
    """
    Provides PM 25 emission rates based on the data from epa_tech_report_fac()
    function. The output of this function will go into epa_2009_proj_table_fac()
    to give the final emission rates for PM25.
    PM 2.5  = 97% of PM 10; based on:
    https://nepis.epa.gov/Exe/ZyPURL.cgi?Dockey=P1001YTF.txt
    """
    pm25_epa_em_fac_impute_ = epa_2009_rts[
        lambda df: df.pollutant == "PM10-PRI"
    ].assign(
        pollutant="PM25-PRI",
        em_fac=lambda df: df.em_fac * 0.97,  # Page 4 of P100500B.pdf
    )
    return pm25_epa_em_fac_impute_


def create_voc_fac(epa_2009_rts: pd.DataFrame) -> pd.DataFrame:
    """
    Provides VOC emission rates based on the data from epa_tech_report_fac()
    function. The output of this function will go into epa_2009_proj_table_fac()
    to give the final emission rates for PM25.
    VOC = 1.053 * HC; based on:
    https://nepis.epa.gov/Exe/ZyPURL.cgi?Dockey=P1001YTF.txt
    """
    voc_epa_em_fac_impute = epa_2009_rts[lambda df: df.pollutant == "HC"].assign(
        pollutant="VOC",
        em_fac=lambda df: df.em_fac * 1.053
        # Page 4 of P100500B.pdf
    )
    return voc_epa_em_fac_impute


def explode_speciation(speciation_2020_fil_: pd.DataFrame) -> pd.DataFrame:
    """
    Create extra rows in the speciation data for different years.
    """
    speciation_2020_fil_1 = speciation_2020_fil_.assign(
        anals_yr=[list(np.arange(2011, 2051, 1))] * len(speciation_2020_fil_)
    ).explode("anals_yr")
    return speciation_2020_fil_1


def hap_fac(
    voc_pm25_em_fac_list: list[pd.DataFrame],
    speciation_2020_fil_expd_: pd.DataFrame,
    pol_type="HAP",
) -> pd.DataFrame:
    """
    Get the emission rate for HAPs by using the PM 2.5 and VOC emission rates
    and multiplication factor from speciation table to convert them to
    different HAP pollutant emission rate.
    """
    voc_pm25_fac_df_1 = pd.concat(voc_pm25_em_fac_list)

    hap_em_fac_df_1 = (
        speciation_2020_fil_expd_.merge(
            (
                voc_pm25_fac_df_1.rename(columns={"em_fac": "em_fac_input_pol"}).drop(
                    columns=["pol_type"]
                )
            ),
            left_on=[
                "dat_cat_code",
                "scc",
                "scc_description_level_1",
                "scc_description_level_2",
                "scc_description_level_3",
                "scc_description_level_4",
                "sector_description",
                "input_pollutant_code",
                "input_pollutant_description",
                "anals_yr",
            ],
            right_on=[
                "dat_cat_code",
                "scc",
                "scc_description_level_1",
                "scc_description_level_2",
                "scc_description_level_3",
                "scc_description_level_4",
                "sector_description",
                "pollutant",
                "pol_desc",
                "anals_yr",
            ],
            how="left",
        )
        .drop(columns=["pollutant", "pol_desc"])
        .assign(em_fac=lambda df: df.em_fac_input_pol * df.multiplication_factor)
        .drop(columns=["em_fac_input_pol"])
    )
    rename_hap_map = {
        "dat_cat_code",
        "scc",
        "scc_description_level_1",
        "scc_description_level_2",
        "scc_description_level_3",
        "scc_description_level_4",
        "sector_description",
        "output_pollutant_code",
        "output_pollutant_description",
        "input_pollutant_code",
        "input_pollutant_description",
        "multiplication_factor",
        "anals_yr",
        "em_units",
        "em_fac",
    }
    hap_em_fac_df_2 = (
        hap_em_fac_df_1.assign(pol_type=pol_type)
        .rename(
            columns={
                "output_pollutant_code": "pollutant",
                "output_pollutant_description": "pol_desc",
            }
        )
        .filter(
            items=[
                "dat_cat_code",
                "scc",
                "scc_description_level_1",
                "scc_description_level_2",
                "scc_description_level_3",
                "scc_description_level_4",
                "sector_description",
                "pollutant",
                "pol_type",
                "pol_desc",
                "anals_yr",
                "em_units",
                "em_fac",
            ]
        )
    )
    return hap_em_fac_df_2


if __name__ == "__main__":
    st = get_out_file_tsmp()
    # Expected Pollutant List: NEI 2017-->Nonpoint-->Expected Pollutant List
    # for Nonpoint SCCs
    # https://www.epa.gov/air-emissions-inventories/2017-national-emissions
    # -inventory-nei-data
    # https://www.epa.gov/sites/production/files/2018-07
    # /np_expected_poll_list_complete_v1.xlsx
    path_exp_pol_list = os.path.join(
        PATH_INTERIM, "epa_pol_list", "np_expected_poll_list_complete_v1.xlsx"
    )
    # Hazardous air pollutants speciation table from EPA NEI 2017 supporting
    # docs: NEI 2017 --> Supporting Data and Summaries --> nonpoint/
    # --> 2017Rail_HAP_AugmentationProfileAssignmentFactors_20200128.xlsx
    # https://www.epa.gov/air-emissions-inventories/2017-national-emissions
    # -inventory-nei-data
    # https://gaftp.epa.gov/air/nei/2017/doc/supporting_data/nonpoint
    # /2017Rail_HAP_AugmentationProfileAssignmentFactors_20200128.xlsx
    path_hap_speciation = os.path.join(
        PATH_INTERIM,
        "epa_speciation_table",
        "power_query",
        "AugmentationProfileAssignmentFactors_Rail_2285002xxx_04072021.xlsx",
    )

    # Emission factors table.
    path_nox_pm10_hc_epa_em_fac = os.path.join(
        PATH_INTERIM, "epa_emission_rates", "epa_2009_emission_rates_nox_pm10_hc.xlsx"
    )

    # Final Output
    path_emission_fac_out = os.path.join(PATH_INTERIM, f"emission_factor_{st}.csv")
    path_emission_fac_out_pat = os.path.join(PATH_INTERIM, r"emission_factor_*-*-*.csv")
    cleanup_prev_output(path_emission_fac_out_pat)

    pol_df_fil = expected_pol_list(path_exp_pol_list)
    speciation_2020_fil = hap_speciation_mult(path_hap_speciation)
    pb_speciation_2011 = pb_speciation_builder(speciation_2020_fil)
    nox_pm10_hc_epa_em_fac_impute = epa_tech_report_fac(path_nox_pm10_hc_epa_em_fac)
    em_fac_df_template = em_fac_template(
        all_pol_df=pol_df_fil, speciation_df=speciation_2020_fil
    )

    em_fac_res_dict = {}
    em_fac_res_dict["co2"] = co2_fac(em_fac_df_template_=em_fac_df_template)
    em_fac_res_dict["co"] = co_fac(em_fac_df_template_=em_fac_df_template)
    em_fac_res_dict["nh3"] = nh3_fac(em_fac_df_template_=em_fac_df_template)
    em_fac_res_dict["so2"] = so2_fac(em_fac_df_template_=em_fac_df_template)
    em_fac_res_dict["nox"] = epa_2009_proj_table_fac(
        em_fac_df_template_=em_fac_df_template,
        pollutant="NOX",
        epa_2009_rts=nox_pm10_hc_epa_em_fac_impute,
    )
    em_fac_res_dict["pm10"] = epa_2009_proj_table_fac(
        em_fac_df_template_=em_fac_df_template,
        pollutant="PM10-PRI",
        epa_2009_rts=nox_pm10_hc_epa_em_fac_impute,
    )
    pm25_epa_em_fac_impute = create_pm25_fac(epa_2009_rts=nox_pm10_hc_epa_em_fac_impute)
    em_fac_res_dict["pm25"] = epa_2009_proj_table_fac(
        em_fac_df_template_=em_fac_df_template,
        pollutant="PM25-PRI",
        epa_2009_rts=pm25_epa_em_fac_impute,
    )
    voc_epa_em_fac_impute = create_voc_fac(epa_2009_rts=nox_pm10_hc_epa_em_fac_impute)
    em_fac_res_dict["voc"] = epa_2009_proj_table_fac(
        em_fac_df_template_=em_fac_df_template,
        pollutant="VOC",
        epa_2009_rts=voc_epa_em_fac_impute,
    )
    speciation_2020_fil_expd = explode_speciation(
        speciation_2020_fil_=speciation_2020_fil
    )

    pb_speciation_2011_expd = explode_speciation(
        speciation_2020_fil_=pb_speciation_2011
    )
    em_fac_res_dict["hap"] = hap_fac(
        voc_pm25_em_fac_list=[em_fac_res_dict["pm25"], em_fac_res_dict["voc"]],
        speciation_2020_fil_expd_=speciation_2020_fil_expd,
    )

    em_fac_res_dict["pb"] = hap_fac(
        voc_pm25_em_fac_list=[em_fac_res_dict["pm10"]],
        speciation_2020_fil_expd_=pb_speciation_2011_expd,
        pol_type="CAP",
    )
    ghg_cap_hap_em_fac = pd.concat(em_fac_res_dict.values())
    ghg_cap_hap_em_fac.to_csv(path_emission_fac_out)
