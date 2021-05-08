import pandas as pd
import numpy as np
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname("__file__"), "..")))
from locoerlt.utilis import (
    PATH_RAW,
    PATH_INTERIM,
    PATH_PROCESSED,
    get_snake_case_dict,
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
        fuel_consump_prj_.groupby(
            [
                "year",
                "stcntyfips",
                "carrier",
                "friylab",
                "rr_netgrp",
                "rr_group",
            ]
        )
        .agg(
            county_carr_friy_yardnm_fuel_consmp_by_yr=("link_fuel_consmp_by_yr", "sum"),
            county_carr_friy_yardnm_miles_by_yr=("miles", "sum"),
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

    assert (
        set(
            fuel_consump_prj_by_cnty_.loc[
                lambda df: ((df.rr_group == "Commuter") & (df.carrier == "DART")),
                "rr_netgrp",
            ].unique()
        )
        == {"Freight"}
    ), (
        "Above mapping does not consider DART on industrial leads and "
        "yards. This is inline with how fuel consumption is coded for DART."
    )

    assert (
        set(
            fuel_consump_prj_by_cnty_.loc[
                lambda df: ((df.rr_group == "Commuter") & (df.carrier == "TREX")),
                "rr_netgrp",
            ].unique()
        )
        == {"Freight", "Industrial", "Yard"}
    ), (
        "Above mapping considers TREX on Freight, industrial leads, and "
        "yards. This is inline with how fuel consumption is coded for TREX."
    )

    fuel_consump_prj_by_cnty_scc_ = fuel_consump_prj_by_cnty_.merge(
        xwalk_ssc_desc_4_rr_grp_netgrp_df_, on=["rr_group", "rr_netgrp"], how="outer"
    )
    return fuel_consump_prj_by_cnty_scc_


def prc_ertac_2017_yard_vals(path_ertac_2017_: str, fuel_consump_: pd.DataFrame):
    """
    Clean fuel consumption by yard from ERTAC 2017 data.
    """
    x1 = pd.ExcelFile(path_ertac_2017_)
    ertac_2017_yard = x1.parse("2017 Emissions", usecols=range(0, 39))
    ertac_2017_yard_tx = (
        ertac_2017_yard.rename(
            columns=get_snake_case_dict(columns=ertac_2017_yard.columns)
        )
        .loc[lambda df: (df.state_id == 48) & (df.final_2016_fuel_use != 0)]
        .rename(columns={"yard_name": "yardname_v1", "fips": "stcntyfips"})
        .filter(
            items=[
                "eis_facility_id",
                "stcntyfips",
                "yardname_v1",
                "site_latitude",
                "site_longitude",
                "final_2016_fuel_use",
            ]
        )
    )
    return ertac_2017_yard_tx


def distr_yard_fuel_usage_by_ertac_2017_yard_vals(
    fuel_consump_prj_by_cnty_scc_: pd.DataFrame,
    ertac_2017_yard_vals_: pd.DataFrame,
):
    """Distribute Yard fuel usage at county level to different yards using
    the ertac data. This function is a later addition, that's why it's not
    well integrated with the logic. We are not incorporating ERTAC yards
    where NARL do data."""
    fuel_consump_prj_by_cnty_scc_not_yards = (
        fuel_consump_prj_by_cnty_scc_.loc[
            lambda df: df.scc_description_level_4 != "Yard Locomotives"
        ]
        .assign(
            eis_facility_id=-99,
            yardname_v1=-99,
            site_latitude=-99,
            site_longitude=-99,
        )
        .filter(
            items=[
                "year",
                "stcntyfips",
                "carrier",
                "friylab",
                "rr_netgrp",
                "rr_group",
                "county_carr_friy_yardnm_fuel_consmp_by_yr",
                "county_carr_friy_yardnm_miles_by_yr",
                "county_name",
                "scc_description_level_4",
                "eis_facility_id",
                "yardname_v1",
                "site_latitude",
                "site_longitude",
            ]
        )
    )

    fuel_consump_prj_by_cnty_scc_yards = (
        fuel_consump_prj_by_cnty_scc_.loc[
            lambda df: df.scc_description_level_4 == "Yard Locomotives"
        ]
        .groupby(["year", "stcntyfips", "county_name", "scc_description_level_4"])
        .county_carr_friy_yardnm_fuel_consmp_by_yr.sum()
        .reset_index()
        .assign(
            st_yard_industrial_fuel_consmp_by_yr=lambda df: (
                df.groupby("year").county_carr_friy_yardnm_fuel_consmp_by_yr.transform(
                    sum
                )
            ),
            county_carr_friy_yardnm_fuel_consmp_by_yr=np.nan,
        )
        .merge(ertac_2017_yard_vals_, on=["stcntyfips"])
        .assign(
            tot_st_yard_fuel_usage=lambda df: (
                df.groupby(["year"]).final_2016_fuel_use.transform(sum)
            ),
            state_to_yard_mix=lambda df: (
                df.final_2016_fuel_use / df.tot_st_yard_fuel_usage
            ),
            county_carr_friy_yardnm_fuel_consmp_by_yr=lambda df: (
                df.st_yard_industrial_fuel_consmp_by_yr * df.state_to_yard_mix
            ),
            carrier=np.nan,
            friylab="IYcat",
            rr_group=np.nan,
            rr_netgrp="Yard",
        )
        .filter(
            items=[
                "year",
                "stcntyfips",
                "carrier",
                "friylab",
                "rr_netgrp",
                "rr_group",
                "county_carr_friy_yardnm_fuel_consmp_by_yr",
                "county_carr_friy_yardnm_miles_by_yr",
                "county_name",
                "scc_description_level_4",
                "eis_facility_id",
                "yardname_v1",
                "site_latitude",
                "site_longitude",
            ]
        )
    )

    test_fuel_df = pd.merge(
        (
            fuel_consump_prj_by_cnty_scc_.loc[
                lambda df: df.scc_description_level_4 == "Yard Locomotives"
            ]
            .groupby("year")
            .county_carr_friy_yardnm_fuel_consmp_by_yr.sum()
        ),
        (
            fuel_consump_prj_by_cnty_scc_yards.groupby(
                "year"
            ).county_carr_friy_yardnm_fuel_consmp_by_yr.sum()
        ),
        left_index=True,
        right_index=True,
    )
    assert np.allclose(
        test_fuel_df.county_carr_friy_yardnm_fuel_consmp_by_yr_x,
        test_fuel_df.county_carr_friy_yardnm_fuel_consmp_by_yr_y,
    )

    fuel_consump_prj_by_cnty_scc_prc = pd.concat(
        [fuel_consump_prj_by_cnty_scc_not_yards, fuel_consump_prj_by_cnty_scc_yards]
    )
    return fuel_consump_prj_by_cnty_scc_prc


def get_emis_quant(
    path_fuel_consump_: str,
    path_emis_rt_: str,
    path_proj_fac_: str,
    path_county_: str,
    path_ertac_2017_: str,
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
    ertac_2017_yard_vals = prc_ertac_2017_yard_vals(
        path_ertac_2017_=path_ertac_2017_, fuel_consump_=fuel_consump_
    )

    fuel_consump_prj_by_cnty_scc_prc = distr_yard_fuel_usage_by_ertac_2017_yard_vals(
        fuel_consump_prj_by_cnty_scc_=fuel_consump_prj_by_cnty_scc_,
        ertac_2017_yard_vals_=ertac_2017_yard_vals,
    )

    emis_quant_ = (
        fuel_consump_prj_by_cnty_scc_prc.merge(
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
                "eis_facility_id",
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
            site_latitude=("site_latitude", "first"),
            site_longitude=("site_longitude", "first"),
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

    path_ertac_2017 = os.path.join(PATH_RAW, "ERTAC_RAIL_YARDS_2017__V.107_7.3.19.xlsx")

    prc_ertac_2017_yard_vals(
        path_ertac_2017_=path_ertac_2017, fuel_consump_=fuel_consump
    )

    emis_quant_res = get_emis_quant(
        path_fuel_consump_=path_fuel_consump,
        path_emis_rt_=path_emis_rt,
        path_proj_fac_=path_proj_fac,
        path_county_=path_county,
        path_ertac_2017_=path_ertac_2017,
    )
    emis_quant_res["emis_quant"].to_csv(path_out_emisquant)
    emis_quant_res["emis_quant_agg"].to_csv(path_out_emisquant_agg)
