import glob
import pandas as pd
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname("__file__"), "..")))
from locoerlt.utilis import PATH_PROCESSED


if __name__ == "__main__":
    path_uncntr_emisquant = glob.glob(
        os.path.join(PATH_PROCESSED, "uncntr_emis_quant_[0-9]*-*-*.csv")
    )[0]
    path_cntr_emisquant = glob.glob(
        os.path.join(PATH_PROCESSED, "cntr_emis_quant_[0-9]*-*-*.csv")
    )[0]
    uncntr_emisquant = pd.read_csv(path_uncntr_emisquant, index_col=0)
    path_out_dir = os.path.join(PATH_PROCESSED, "report_summaries")
    path_statewide_19_sum = os.path.join(path_out_dir, "statewide_19_sum.xlsx")
    path_statewide_20_cap_ghg_cntr = os.path.join(
        path_out_dir, "statewide_20_cap_ghg_cntr.xlsx"
    )
    statewide_fuel_usage_19 = (
        uncntr_emisquant.loc[
            lambda df: df.year.isin([2019]) & (df.pollutant.isin(["CO"]))
        ]
        .groupby(
            [
                "year",
                "scc_description_level_4",
            ]
        )
        .agg(
            statewide_fuel_by_scc=("county_carr_friy_yardnm_fuel_consmp_by_yr", "sum"),
        )
    )
    statewide_fuel_usage_19.to_excel(path_statewide_19_sum)

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
    uncntr_emisquant_19_20_cap_ghg = (
        uncntr_emisquant_no_yardnm.loc[
            lambda df: df.year.isin([2019, 2020]) & (df.pol_type.isin(["CAP", "GHG"]))
        ]
        .assign(pollutant=lambda df: df.pollutant.replace("7439921", "Lead"))
        .filter(
            [
                "year",
                "county_name",
                "scc",
                "scc_description_level_4",
                "pollutant",
                "uncontrolled_em_quant_ton",
            ]
        )
        .set_index(
            ["year", "county_name", "scc", "scc_description_level_4", "pollutant"]
        )
        .unstack()
    )

    uncntr_emisquant_20_cap_ghg_statewide = (
        uncntr_emisquant_no_yardnm.loc[
            lambda df: df.year.isin([2020]) & (df.pol_type.isin(["CAP", "GHG"]))
        ]
        .assign(pollutant=lambda df: df.pollutant.replace("7439921", "Lead"))
        .groupby(["year", "scc_description_level_4", "pollutant"])
        .uncontrolled_em_quant_ton.sum()
        .reset_index()
        .set_index(["year", "scc_description_level_4", "pollutant"])
        .unstack()
    )

    cntr_emisquant = pd.read_csv(path_cntr_emisquant, index_col=0)
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

    cntr_emisquant_19_20_cap_ghg = (
        cntr_emisquant_no_yardnm.loc[
            lambda df: df.year.isin([2019, 2020]) & (df.pol_type.isin(["CAP", "GHG"]))
        ]
        .assign(pollutant=lambda df: df.pollutant.replace("7439921", "Lead"))
        .filter(
            [
                "year",
                "county_name",
                "scc",
                "scc_description_level_4",
                "pollutant",
                "controlled_em_quant_ton",
            ]
        )
        .set_index(
            ["year", "county_name", "scc", "scc_description_level_4", "pollutant"]
        )
        .unstack()
    )

    cntr_emisquant_20_cap_ghg_statewide = (
        cntr_emisquant_no_yardnm.loc[
            lambda df: df.year.isin([2020]) & (df.pol_type.isin(["CAP", "GHG"]))
        ]
        .assign(pollutant=lambda df: df.pollutant.replace("7439921", "Lead"))
        .groupby(["year", "scc_description_level_4", "pollutant"])
        .controlled_em_quant_ton.sum()
        .reset_index()
            .assign(
            scc_description_level_4=lambda df: pd.Categorical(
                df.scc_description_level_4, [
                    "Line Haul Locomotives: Class I Operations",
                    "Line Haul Locomotives: Class II / III Operations",
                    "Line Haul Locomotives: Passenger Trains (Amtrak)",
                    "Line Haul Locomotives: Commuter Lines",
                    "Yard Locomotives",
                ]
            )
        )
        .set_index(["year", "scc_description_level_4", "pollutant"])
        .unstack()
        .sort_index()
        .droplevel(0, axis=1)
        .filter(items=["VOC", "CO",	"NOX", "CO2", "SO2", "NH3",	"PM10-PRI",
                       "PM25-PRI"
                       ])
    )

    cntr_emisquant_20_cap_ghg_statewide.to_excel(path_statewide_20_cap_ghg_cntr)

    # uncntr_cntr_emisquant_19_20_cap_ghg_statewide = pd.merge(
    #     uncntr_emisquant_20_cap_ghg_statewide,
    #     cntr_emisquant_20_cap_ghg_statewide,
    #     left_index=True,
    #     right_index=True
    # )

    # TODO: List Yard Locations
