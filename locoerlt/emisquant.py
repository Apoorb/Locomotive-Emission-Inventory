import pandas as pd
import numpy as np
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname("__file__"), '..')))
from locoerlt.utilis import (
    PATH_RAW,
    PATH_INTERIM,
    PATH_PROCESSED,
    get_out_file_tsmp,
    cleanup_prev_output,
    xwalk_ssc_desc_4_rr_grp_netgrp,
)


def process_proj_fac(
    path_proj_fac_: str,
    freight_rr_group=("Class I", "Class III"),
    pass_commut_rr_group=("Passenger", "Commuter"),
) -> pd.DataFrame:
    """
    Use R-1 projection factors (actual data from 2011 to 2020. Use AEO 2021
    travel projection factors from 2021 onwards.

    Return projection factors by railroad groups.
    """
    x1 = pd.ExcelFile(path_proj_fac_)
    freight_proj_fac = x1.parse(
        "Recommended_Proj", skiprows=1, usecols=["Year", "Freight"]
    ).rename(columns={"Year": "year", "Freight": "proj_fac"})
    pass_commute_proj_fac = x1.parse(
        "Recommended_Proj", skiprows=1, usecols=["Year", "Passenger"]
    ).rename(columns={"Year": "year", "Passenger": "proj_fac"})

    freight_proj_fac_1 = freight_proj_fac.assign(
        rr_group=[freight_rr_group] * len(freight_proj_fac)
    ).explode("rr_group")

    pass_commute_proj_fac_1 = pass_commute_proj_fac.assign(
        rr_group=[pass_commut_rr_group] * len(pass_commute_proj_fac)
    ).explode("rr_group")
    proj_fac_ = pd.concat([freight_proj_fac_1, pass_commute_proj_fac_1])
    return proj_fac_


def process_county(county_df_: pd.DataFrame) -> pd.DataFrame:
    """
    Use  "Texas County Boundary" dataset to get county five digit FIPS and
    county name map. Source:
    https://gis-txdot.opendata.arcgis.com/datasets/texas-county-boundaries
    /data?geometry=-133.426%2C24.483%2C-66.673%2C37.611
    """
    county_df_fil_ = county_df_.filter(items=["CNTY_NM", "FIPS_ST_CNTY_CD"]).rename(
        columns={"CNTY_NM": "county_name", "FIPS_ST_CNTY_CD": "stcntyfips"}
    )
    return county_df_fil_


def project_filt_fuel_consump(
    fuel_consump_: pd.DataFrame, proj_fac_: pd.DataFrame
) -> pd.DataFrame:
    """
    Filter fuel consumption dataset and merge the projection factors to it.
    """
    fuel_consump_prj_ = (
        fuel_consump_.filter(
            items=[
                "fraarcid",
                "net",
                "miles",
                "stcntyfips",
                "carrier",
                "friylab",
                "rr_netgrp",
                "rr_group",
                "link_fuel_consmp",
                "yardname",
                "start_lat",
                "start_long",
            ]
        )
        .rename(columns={"link_fuel_consmp": "link_fuel_consmp_2019"})
        .assign(
            year=[list(np.arange(2011, 2051))] * len(fuel_consump_),
        )
        .explode("year")
        .merge(proj_fac_, on=["rr_group", "year"], how="outer")
        .assign(
            link_fuel_consmp_by_yr=lambda df: (df.proj_fac * df.link_fuel_consmp_2019),
            year=lambda df: df.year.astype(int),
        )
    )
    return fuel_consump_prj_


def merge_cnty_nm_to_fuel_proj(
    fuel_consump_prj_: pd.DataFrame, county_df_fil_: pd.DataFrame
) -> pd.DataFrame:
    """
    Add county names to the fuel consumption dataset.
    """
    fuel_consump_prj_by_cnty_ = (
        fuel_consump_prj_.assign(
            yardname_v1=lambda df: np.select(
                [
                    df.rr_netgrp == "Yard",
                    df.rr_netgrp == "Freight",
                    df.rr_netgrp == "Industrial",
                ],
                [df.yardname, -99, -99],
                -9999,
            ),
            start_lat=lambda df: np.select(
                [
                    df.rr_netgrp == "Yard",
                    df.rr_netgrp == "Freight",
                    df.rr_netgrp == "Industrial",
                ],
                [df.start_lat, -99, -99],
                -9999,
            ),
            start_long=lambda df: np.select(
                [
                    df.rr_netgrp == "Yard",
                    df.rr_netgrp == "Freight",
                    df.rr_netgrp == "Industrial",
                ],
                [df.start_long, -99, -99],
                -9999,
            ),
        )
        .groupby(
            [
                "year",
                "stcntyfips",
                "carrier",
                "friylab",
                "yardname_v1",
                "rr_netgrp",
                "rr_group",
            ]
        )
        .agg(
            county_carr_friy_yardnm_fuel_consmp_by_yr=("link_fuel_consmp_by_yr", "sum"),
            county_carr_friy_yardnm_miles_by_yr=("miles", "sum"),
            start_lat=("start_lat", "first"),
            start_long=("start_long", "first"),
        )
        .reset_index()
        .merge(county_df_fil_, on="stcntyfips", how="outer")
    )
    return fuel_consump_prj_by_cnty_


def add_scc_desc_to_fuel_proj_cnty(
    fuel_consump_prj_by_cnty_: pd.DataFrame,
    xwalk_ssc_desc_4_rr_grp_netgrp=xwalk_ssc_desc_4_rr_grp_netgrp,
) -> pd.DataFrame:
    """
    Add EPA SCC description to fuel consumption + county name + Projection data.
    """
    xwalk_ssc_desc_4_rr_grp_netgrp_df_ = pd.read_csv(
        xwalk_ssc_desc_4_rr_grp_netgrp, sep=","
    ).assign(
        scc_description_level_4=lambda df: (df.scc_description_level_4.str.strip())
    )

    assert set(
        fuel_consump_prj_by_cnty_.loc[
            lambda df: (df.rr_group == "Passenger"), "rr_netgrp"
        ].unique()
    ) == {"Freight"}, (
        "Above mapping does not consider Amtrak on industrial leads and "
        "yards. This is inline with how fuel consumption is coded for Amtrak."
    )

    assert set(
        fuel_consump_prj_by_cnty_.loc[
            lambda df: ((df.rr_group == "Commuter") & (df.carrier == "DART")),
            "rr_netgrp",
        ].unique()
    ) == {"Freight"}, (
        "Above mapping does not consider DART on industrial leads and "
        "yards. This is inline with how fuel consumption is coded for DART."
    )

    assert set(
        fuel_consump_prj_by_cnty_.loc[
            lambda df: ((df.rr_group == "Commuter") & (df.carrier == "TREX")),
            "rr_netgrp",
        ].unique()
    ) == {"Freight", "Industrial", "Yard"}, (
        "Above mapping considers TREX on Freight, industrial leads, and "
        "yards. This is inline with how fuel consumption is coded for TREX."
    )

    fuel_consump_prj_by_cnty_scc_ = fuel_consump_prj_by_cnty_.merge(
        xwalk_ssc_desc_4_rr_grp_netgrp_df_, on=["rr_group", "rr_netgrp"], how="outer"
    )
    return fuel_consump_prj_by_cnty_scc_


def get_emis_quant(
    path_fuel_consump_: str,
    path_emis_rt_: str,
    path_proj_fac_: str,
    path_county_: str,
) -> dict:
    """
    Get the emission quantity using the fuel consumption, emission rates,
    projection factors, and county name datasets.
    """
    fuel_consump_ = pd.read_csv(path_fuel_consump_, index_col=0)
    emis_rt_ = pd.read_csv(path_emis_rt_, index_col=0)
    proj_fac_ = process_proj_fac(path_proj_fac_)
    county_df_ = pd.read_csv(path_county_)
    county_df_fil_ = process_county(county_df_)
    fuel_consump_prj_ = project_filt_fuel_consump(fuel_consump_, proj_fac_)
    fuel_consump_prj_by_cnty_ = merge_cnty_nm_to_fuel_proj(
        fuel_consump_prj_, county_df_fil_
    )
    fuel_consump_prj_by_cnty_scc_ = add_scc_desc_to_fuel_proj_cnty(
        fuel_consump_prj_by_cnty_
    )
    emis_quant_ = (
        fuel_consump_prj_by_cnty_scc_.merge(
            emis_rt_,
            left_on=["scc_description_level_4", "year"],
            right_on=["scc_description_level_4", "anals_yr"],
            how="outer",
        )
        .assign(
            em_quant=lambda df: df.em_fac
            * df.county_carr_friy_yardnm_fuel_consmp_by_yr,
            year=lambda df: df.year.astype("Int32"),
        )
        .drop(columns=["anals_yr", "em_units"])
    )

    emis_quant_agg = (
        emis_quant_.groupby(
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
                "yardname_v1",
                "pol_type",
                "pollutant",
                "pol_desc",
            ]
        )
        .agg(
            em_fac=("em_fac", "mean"),
            em_quant=("em_quant", "sum"),
            county_carr_friy_yardnm_fuel_consmp_by_yr=(
                "county_carr_friy_yardnm_fuel_consmp_by_yr",
                "sum",
            ),
            county_carr_friy_yardnm_miles_by_yr=(
                "county_carr_friy_yardnm_miles_by_yr",
                "sum",
            ),
            start_lat=("start_lat", "first"),
            start_long=("start_long", "first"),
        )
        .reset_index()
    )

    return {"emis_quant": emis_quant_, "emis_quant_agg": emis_quant_agg}


if __name__ == "__main__":
    st = get_out_file_tsmp()
    path_fuel_consump = os.path.join(PATH_INTERIM, f"fuelconsump_2019_tx_{st}.csv")
    path_emis_rt = os.path.join(PATH_INTERIM, f"emission_factor_{st}.csv")
    path_proj_fac = os.path.join(PATH_INTERIM, "Projection Factors 04132021.xlsx")
    path_county = os.path.join(PATH_RAW, "Texas_County_Boundaries.csv")
    path_out_emisquant = os.path.join(PATH_PROCESSED, f"emis_quant_loco_{st}.csv")
    path_out_emisquant_agg = os.path.join(
        PATH_PROCESSED, f"emis_quant_loco_agg" f"_{st}.csv"
    )

    path_out_emisquant_pat = os.path.join(PATH_PROCESSED, f"emis_quant_loco_*-*-*.csv")
    cleanup_prev_output(path_out_emisquant_pat)

    fuel_consump = pd.read_csv(path_fuel_consump, index_col=0)

    emis_quant_res = get_emis_quant(
        path_fuel_consump_=path_fuel_consump,
        path_emis_rt_=path_emis_rt,
        path_proj_fac_=path_proj_fac,
        path_county_=path_county,
    )
    emis_quant_res["emis_quant"].to_csv(path_out_emisquant)
    emis_quant_res["emis_quant_agg"].to_csv(path_out_emisquant_agg)
