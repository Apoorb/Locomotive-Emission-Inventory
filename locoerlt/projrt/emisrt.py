"""
Develop emission rate table for expected list of pollutants.
"""
import os
import numpy as np
import inflection
import pandas as pd
from locoerlt.utilis import PATH_RAW, PATH_INTERIM, PATH_PROCESSED


def expected_pol_list(path_exp_pol_list_: str) -> pd.DataFrame:
    """"""
    x_pol = pd.ExcelFile(path_exp_pol_list_)
    x_pol.sheet_names
    pol_df = x_pol.parse('NP Expected Pollutants List')
    pol_lab = x_pol.parse('Xwalk_pollutant_descriptions')
    pol_cols = [col for col in pol_df.columns
                if col not in ('EPA Tool?', 'SCC', 'SCC Description', 'Sector')]

    pol_lab_fil = (
        pol_lab
            .filter(
            items=["pollutant", "poltype", "poldesc", "polcat", "group"])
            .rename(columns={
            "group": "pol_grp", "polcat": "pol_cat", "poltype": "pol_type",
            "poldesc": "pol_desc"})
    )
    pol_df_fil = (
        pol_df
            .loc[lambda df: df.Sector == "Mobile - Locomotives"]
            .drop(columns=["EPA Tool?"])
            .melt(
            id_vars=['SCC', 'SCC Description', 'Sector'],
            value_vars=pol_cols,
            var_name="pollutant",
            value_name="is_needed")
            .dropna(subset=["is_needed"])
            .merge(pol_lab_fil, on="pollutant", how="left")
            .rename(columns={
            "SCC": "scc", "SCC Description": "scc_desc", "Sector": "sector"})
            .assign(pollutant=lambda df: (
            df.pollutant.replace({"Xylenes": 1330207}))
                    )
    )
    return pol_df_fil

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


def hap_speciation_rates(path_hap_speciation_: str) -> pd.DataFrame:
    """"""
    x_hap = pd.ExcelFile(path_hap_speciation_)
    speciation_2020 = x_hap.parse('Non Point 2020 Speciation Table')

    hap_rename_map = {col: inflection.underscore(col).replace(" ", "_")
                      for col in speciation_2020.columns}

    speciation_2020_fil = (
        speciation_2020
            .rename(columns=hap_rename_map).rename(columns={
            "scc_assignment": "scc", "data_category_code": 'dat_cat_code'})
            .sort_values(by=["input_pollutant_code",
                             "output_pollutant_description", "scc"])
            .reset_index(drop=True)
            .filter(items=[
            "dat_cat_code",
            "scc",
            'scc_description_level_1',
            'scc_description_level_2',
            'scc_description_level_3',
            'scc_description_level_4',
            'sector_description',
            "output_pollutant_code",
            "output_pollutant_description",
            "input_pollutant_code",
            "input_pollutant_description",
            "multiplication_factor"])
    )
    return speciation_2020_fil


def cap_hap_combined_rates() -> pd.DataFrame:
    """"""
    ...


if __name__ == "__main__":
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
        PATH_INTERIM,
        "epa_emission_rates",
        "epa_2009_emission_rates_nox_pm10_hc.xlsx"
    )

    map_ssc_desc_4 = {
        "large_line_haul": 'Line Haul Locomotives: Class I Operations',
        'small_rr': 'Line Haul Locomotives: Class II / III Operations',
        'passenger_commuter': [
            'Line Haul Locomotives: Passenger Trains (Amtrak)',
            'Line Haul Locomotives: Commuter Lines',
        ],
        'large_switch': 'Yard Locomotives'
    }
    nox_pm10_hc_epa_em_fac = pd.read_excel(path_nox_pm10_hc_epa_em_fac)
    nox_pm10_hc_epa_em_fac_impute = (
        nox_pm10_hc_epa_em_fac
        .assign(
            year=lambda df: np.select(
                [
                    df.year == 2040,
                    df.year != 2040],
                [
                    set(np.arange(2040, 2051)),
                    df.year
                ],
                np.nan
            ),
            scc_description_level_4=lambda df: df.carriers.map(map_ssc_desc_4)
        )
        .explode("year") # Fill 2040 values to years 2040 to 2050
        .explode("scc_description_level_4")
        .dropna(subset=["scc_description_level_4"])
        .filter(items=["scc_description_level_4", 'pollutant', 'year', 'em_fac'])
    )

    pol_df_fil = expected_pol_list(path_exp_pol_list)
    speciation_2020_fil = hap_speciation_rates(path_hap_speciation)

    cap_df=(
        pol_df_fil.query("pol_type=='CAP'")
            .filter(items=["scc", "pollutant", "pol_type", "pol_desc"])
    )
    em_fac_df_template = (
        speciation_2020_fil[
            ['dat_cat_code', 'scc', 'scc_description_level_1',
             'scc_description_level_2', 'scc_description_level_3',
             'scc_description_level_4', 'sector_description']]
        .drop_duplicates()
        .merge(cap_df, on=["scc"])
        .assign(
            anals_yr=np.nan,
            em_fac=np.nan,
            em_units="grams/gallon"
        )
    )

    # Lead ?

    co_em_fac_df = em_fac_df_template[lambda df: df.pollutant == "CO"]

    co_em_fac_df_1 = (
        co_em_fac_df
        .assign(
            em_fac=lambda df: np.select(
                [
                    df.scc_description_level_4.isin(
                        ['Line Haul Locomotives: Class I Operations',
                         'Line Haul Locomotives: Passenger Trains (Amtrak)',
                         'Line Haul Locomotives: Commuter Lines'
                         ]),
                    df.scc_description_level_4.isin(
                        ['Line Haul Locomotives: Class II / III Operations']),
                    df.scc_description_level_4.isin(
                        ['Yard Locomotives']),
                ],
                [
                    1.28 * 20.8,  # P100500B.pdf Table 1 and 3
                    1.28 * 18.2,  # P100500B.pdf Table 1 and 3
                    1.83 * 15.2  # P100500B.pdf Table 2 and 3
                ],
                np.nan
            ),
            anals_yr=[list(np.arange(2011, 2051, 1))] * len(co_em_fac_df)
        )
        .explode(column="anals_yr")
    )

    co2_em_fac_df = (
        em_fac_df_template[lambda df: df.pollutant == "CO"]
        .assign(
            pollutant="CO2",
            pol_type="GHG",
            pol_desc="Carbon Dioxide",
        )
    )

    co2_em_fac_df_1 = (
        co2_em_fac_df
        .assign(
            em_fac=2778 * 0.99 * (44/12),
            # Diesel carbon content per gallon
            # of diesel * oxidation factor * molecular wt of CO2 by C
            # https://nepis.epa.gov/Exe/ZyPURL.cgi?Dockey=P1001YTF.txt
            anals_yr=[list(np.arange(2011, 2051, 1))] * len(co2_em_fac_df)
        )
        .explode(column="anals_yr")
    )

    nh3_em_fac_df = em_fac_df_template[lambda df: df.pollutant == "NH3"]
    nh3_em_fac_df_1 = (
        nh3_em_fac_df
        .assign(
            em_fac=1.83e-04 * 453.592,
            # Table III-6, 2nd row
            # https://19january2017snapshot.epa.gov/sites/production/files/2015-08/documents/eiip_areasourcesnh3.pdf
            anals_yr=[list(np.arange(2011, 2051, 1))] * len(nh3_em_fac_df)
        )
        .explode(column="anals_yr")
    )

    so2_em_fac_df = em_fac_df_template[lambda df: df.pollutant == "SO2"]

    so2_em_fac_df_1 = (
        so2_em_fac_df
        .assign(
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
                    (0.1346 * 23809.5) * 0.97 * (64/32) * 500 * 1e-6,
                    (0.1346 * 23809.5) * 0.97 * (64 / 32) * 15 * 1e-6
                    # Diesel density in metric tons/ bbl is 0.1346.
                    # metric ton/ bbl to grams/gallon conversion is
                    # 23,809.5. Source: TAble MM--1 in
                    # https://www.govinfo.gov/content/pkg/FR-2009-10-30/pdf/E9-23315.pdf
                    # 97%: % of sulphur coverting to SO2.
                    # https://nepis.epa.gov/Exe/ZyPDF.cgi?Dockey=P10001SB.pdf
                    # molecular wt of SO2 by S: 64/32
                    # 15 ppm 2012 and after
                    # 500 ppm 2011 and before # ERG uses 300 ppm
                    # https://clean-diesel.org/nonroad.html
                    # EPA420-F-04-032: https://nepis.epa.gov/Exe/ZyPURL.cgi
                    # ?Dockey=P10001RN.txt
                ],
                np.nan
            ),
        )
    )

    nox_em_fac_df = em_fac_df_template[lambda df: df.pollutant == "NOX"]
    nox_em_fac_df_1 = (
        nox_em_fac_df.drop(columns="em_fac")
        .assign(
            anals_yr=[list(np.arange(2011, 2051, 1))] * len(so2_em_fac_df),
        )
        .explode(column="anals_yr")
        .merge(nox_pm10_hc_epa_em_fac_impute,
               left_on=["scc_description_level_4", 'pollutant', 'anals_yr'],
               right_on=["scc_description_level_4", 'pollutant', 'year'],
               how="left")
        .drop(columns='year')
    )

    pm10_em_fac_df = em_fac_df_template[lambda df: df.pollutant == "PM10-PRI"]
    pm10_em_fac_df_1 = (
        pm10_em_fac_df.drop(columns="em_fac")
        .assign(
            anals_yr=[list(np.arange(2011, 2051, 1))] * len(so2_em_fac_df),
        )
        .explode(column="anals_yr")
        .merge(nox_pm10_hc_epa_em_fac_impute,
               left_on=["scc_description_level_4", 'pollutant', 'anals_yr'],
               right_on=["scc_description_level_4", 'pollutant', 'year'],
               how="left")
        .drop(columns='year')
    )


    pm25_epa_em_fac_impute = (
        nox_pm10_hc_epa_em_fac_impute[lambda df: df.pollutant == "PM10-PRI"]
        .assign(
            pollutant="PM25-PRI",
            em_fac=lambda df: df.em_fac * 0.97 # Page 4 of P100500B.pdf
        )
    )
    pm25_em_fac_df = em_fac_df_template[lambda df: df.pollutant == "PM25-PRI"]
    pm25_em_fac_df_1 = (
        pm25_em_fac_df.drop(columns="em_fac")
        .assign(
            anals_yr=[list(np.arange(2011, 2051, 1))] * len(so2_em_fac_df),
        )
        .explode(column="anals_yr")
        .merge(pm25_epa_em_fac_impute,
               left_on=["scc_description_level_4", 'pollutant', 'anals_yr'],
               right_on=["scc_description_level_4", 'pollutant', 'year'],
               how="left")
        .drop(columns='year')
    )

    voc_epa_em_fac_impute = (
        nox_pm10_hc_epa_em_fac_impute[lambda df: df.pollutant == "HC"]
        .assign(
            pollutant="VOC",
            em_fac=lambda df: df.em_fac * 1.053 # Page 4 of P100500B.pdf
        )
    )

    voc_em_fac_df = em_fac_df_template[lambda df: df.pollutant == "VOC"]
    voc_em_fac_df_1 = (
        voc_em_fac_df.drop(columns="em_fac")
        .assign(
            anals_yr=[list(np.arange(2011, 2051, 1))] * len(so2_em_fac_df),
        )
        .explode(column="anals_yr")
        .merge(voc_epa_em_fac_impute,
               left_on=["scc_description_level_4", 'pollutant', 'anals_yr'],
               right_on=["scc_description_level_4", 'pollutant', 'year'],
               how="left")
        .drop(columns='year')
    )

    speciation_2020_fil_1 = (
        speciation_2020_fil
        .assign(
            anals_yr=[list(np.arange(2011, 2051, 1))] * len(speciation_2020_fil)
        )
        .explode("anals_yr")
    )
    voc_pm25_fac_df_1 = pd.concat([pm25_em_fac_df_1, voc_em_fac_df_1])

    hap_em_fac_df_1 = (
        speciation_2020_fil_1
        .merge(
            (
                voc_pm25_fac_df_1
                .rename(columns={"em_fac": "em_fac_input_pol"})
                .drop(columns=["pol_type"])
            ),
            left_on=['dat_cat_code', 'scc', 'scc_description_level_1',
                      'scc_description_level_2', 'scc_description_level_3',
                      'scc_description_level_4', 'sector_description',
                      'input_pollutant_code', 'input_pollutant_description',
                      'anals_yr'],
            right_on =['dat_cat_code', 'scc', 'scc_description_level_1',
                      'scc_description_level_2', 'scc_description_level_3',
                      'scc_description_level_4', 'sector_description',
                      'pollutant', 'pol_desc',
                      'anals_yr'],

            how="left"
        )
        .drop(columns=["pollutant", "pol_desc"])
        .assign(
            em_fac=lambda df: df.em_fac_input_pol * df.multiplication_factor
        )
        .drop(columns=["em_fac_input_pol"])
        .filter()
    )

    rename_hap_map = {
        'dat_cat_code', 'scc', 'scc_description_level_1',
        'scc_description_level_2', 'scc_description_level_3',
        'scc_description_level_4', 'sector_description',
        'output_pollutant_code', 'output_pollutant_description',
        'input_pollutant_code', 'input_pollutant_description',
        'multiplication_factor', 'anals_yr', 'em_units', 'em_fac'
    }
    hap_em_fac_df_2 = (
        hap_em_fac_df_1
        .assign(pol_type="HAP")
        .rename(columns={
            'output_pollutant_code': 'pollutant',
            'output_pollutant_description': 'pol_desc'})
        .filter(items=[
            'dat_cat_code', 'scc', 'scc_description_level_1',
            'scc_description_level_2', 'scc_description_level_3',
            'scc_description_level_4', 'sector_description', 'pollutant',
            'pol_type', 'pol_desc', 'anals_yr', 'em_units', 'em_fac'
        ])
    )

    ghg_cap_hap_em_fac = pd.concat(
        [
            co2_em_fac_df_1,
            co_em_fac_df_1,
            nox_em_fac_df_1,
            voc_em_fac_df_1,
            nh3_em_fac_df_1,
            so2_em_fac_df_1,
            pm10_em_fac_df_1,
            pm25_em_fac_df_1,
            hap_em_fac_df_2
        ]
    )
    # Testing
    ###########################################################################
    # Pollutants needed based on expected pollutant list and not in
    # speciation table and vice-versa.
    pol_df_fil_speciation = (
        pol_df_fil
            .merge(
            speciation_2020_fil.loc[lambda df: df.multiplication_factor != 0],
                left_on=["pollutant", "scc"],
                right_on=["output_pollutant_code", "scc"],
                how="outer"
        )
            .loc[lambda df: ~ df.pollutant.isin(["CO", "NH3", "NOX", "PM10-PRI",
                                                 "PM25-PRI", "SO2", "VOC"])]
    )
