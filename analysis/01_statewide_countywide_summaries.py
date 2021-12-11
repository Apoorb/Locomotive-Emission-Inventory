"""
Get Statewide summaries of CAP and CAP precursors.
Created by: Apoorb
"""
import glob
import pandas as pd
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname("__file__"), "..")))
from locoerlt.utilis import PATH_PROCESSED


if __name__ == "__main__":
    # Read controlled and uncontrolled emissions data.
    path_uncntr_emisquant = glob.glob(
        os.path.join(PATH_PROCESSED, "uncntr_emis_quant_[0-9]*-*-*.csv")
    )[0]
    path_cntr_emisquant = glob.glob(
        os.path.join(PATH_PROCESSED, "cntr_emis_quant_[0-9]*-*-*.csv")
    )[0]
    # Output locations.
    uncntr_emisquant = pd.read_csv(path_uncntr_emisquant, index_col=0)
    path_out_dir = os.path.join(PATH_PROCESSED, "report_summaries")
    path_statewide_19_sum = os.path.join(path_out_dir, "statewide_19_sum.xlsx")
    path_statewide_20_cap_ghg_cntr = os.path.join(
        path_out_dir, "statewide_20_cap_ghg_cntr.xlsx"
    )
    path_county_level_cap_ghg_cntr = os.path.join(
        path_out_dir, "countylevel_20_cap_ghg_cntr.xlsx"
    )
    path_county_level_cap_ghg_uncntr = os.path.join(
        path_out_dir, "countylevel_20_cap_ghg_uncntr.xlsx"
    )
    # Filter to 2019 and CO and get fuel usage.
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

    # Uncontrolled. Remove yardnames from the aggregation. Get emissions at
    # year, SCC, and county-level.
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

    # Uncontrolled. Filter to 2019 and 2020 emissions. Keep GHG, CAP and CAP
    # precursors. Aggregate at county level.
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

    uncntr_emisquant_20_cap_ghg_cntylev_out = (
        uncntr_emisquant_19_20_cap_ghg
        .droplevel(0, axis=1)
        .filter(items=["VOC", "CO", "NOX", "SO2", "NH3", "PM10-PRI",
                           "PM25-PRI", "Lead"
                           ])
        .loc[lambda df: ~ df[["VOC", "CO", "NOX", "SO2", "NH3", "PM10-PRI",
                           "PM25-PRI", "Lead"]].eq(0).all(axis=1)]
        .reset_index()
        .loc[lambda df: df.year == 2020]
        .drop("scc_description_level_4", axis=1)
    )
    uncntr_emisquant_20_cap_ghg_cntylev_out_swkd = (
        uncntr_emisquant_20_cap_ghg_cntylev_out.copy())
    pol_cols = ['VOC', 'CO', 'NOX', 'SO2', 'NH3', 'PM10-PRI', 'PM25-PRI',
                'Lead']
    uncntr_emisquant_20_cap_ghg_cntylev_out_swkd[pol_cols] = (
        uncntr_emisquant_20_cap_ghg_cntylev_out_swkd[pol_cols])/ 365
    with pd.ExcelWriter(path_county_level_cap_ghg_uncntr) as f:
        uncntr_emisquant_20_cap_ghg_cntylev_out_swkd.to_excel(f, "uncnty_20_cntr_emis_swkd", index=False)
        uncntr_emisquant_20_cap_ghg_cntylev_out.to_excel(f, "uncnty_20_cntr_emis_ann", index=False)


    # Uncontrolled. Filter to 2019 and 2020 emissions. Keep GHG, CAP and CAP
    # precursors. Aggregate at statewide level.
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
    # Controlled. Remove yardnames from the aggregation. Get emissions at
    # year, SCC, and county-level.
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
    # Controlled. Filter to 2019 and 2020 emissions. Keep GHG, CAP and CAP
    # precursors. Aggregate at county level.
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
    # Controlled. Filter to 2019 and 2020 emissions. Keep GHG, CAP and CAP
    # precursors. Aggregate at statewide level.
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
        .filter(items=["VOC", "CO",	"NOX", "SO2", "NH3",	"PM10-PRI",
                       "PM25-PRI", "Lead"
                       ])
    )

    cntr_emisquant_20_cap_ghg_statewide.to_excel(path_statewide_20_cap_ghg_cntr)

    cntr_emisquant_20_cap_ghg_cntylev_out = (
        cntr_emisquant_19_20_cap_ghg
        .droplevel(0, axis=1)
        .filter(items=["VOC", "CO", "NOX", "SO2", "NH3", "PM10-PRI",
                           "PM25-PRI", "Lead"
                           ])
        .loc[lambda df: ~ df[["VOC", "CO", "NOX", "SO2", "NH3", "PM10-PRI",
                           "PM25-PRI", "Lead"]].eq(0).all(axis=1)]
        .reset_index()
        .loc[lambda df: df.year == 2020]
        .drop("scc_description_level_4", axis=1)
    )
    cntr_emisquant_20_cap_ghg_cntylev_out_swkd = (
        cntr_emisquant_20_cap_ghg_cntylev_out.copy())
    pol_cols = ['VOC', 'CO', 'NOX', 'SO2', 'NH3', 'PM10-PRI', 'PM25-PRI',
                'Lead']
    cntr_emisquant_20_cap_ghg_cntylev_out_swkd[pol_cols] = (
        cntr_emisquant_20_cap_ghg_cntylev_out_swkd[pol_cols])/ 365
    with pd.ExcelWriter(path_county_level_cap_ghg_cntr) as f:
        cntr_emisquant_20_cap_ghg_cntylev_out_swkd.to_excel(f, "cnty_20_cntr_emis_swkd", index=False)
        cntr_emisquant_20_cap_ghg_cntylev_out.to_excel(f, "cnty_20_cntr_emis_ann", index=False)

    # uncntr_cntr_emisquant_19_20_cap_ghg_statewide = pd.merge(
    #     uncntr_emisquant_20_cap_ghg_statewide,
    #     cntr_emisquant_20_cap_ghg_statewide,
    #     left_index=True,
    #     right_index=True
    # )

    # TODO: List Yard Locations


