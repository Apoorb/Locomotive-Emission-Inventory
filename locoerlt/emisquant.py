import os
import pandas as pd
import numpy as np
from io import StringIO
from locoerlt.utilis import PATH_RAW, PATH_INTERIM, PATH_PROCESSED


def process_county(county_df_: pd.DataFrame) -> pd.DataFrame:
    """
    Use  "Texas County Boundary" dataset to get county five digit FIPS and
    county name map. Source:
    https://gis-txdot.opendata.arcgis.com/datasets/texas-county-boundaries/data?geometry=-133.426%2C24.483%2C-66.673%2C37.611
    """
    county_df_fil_ = (
        county_df_
        .filter(
            items=["CNTY_NM", "FIPS_ST_CNTY_CD"])
        .rename(
            columns={"CNTY_NM": "county_name", "FIPS_ST_CNTY_CD": "stcntyfips"}
        )
    )
    return county_df_fil_


def project_filt_fuel_consump(
        fuel_consump_: pd.DataFrame, proj_fac_: pd.DataFrame) -> pd.DataFrame:
    """
    Filter fuel consumption dataset and merge the projection factors to it.
    """
    # FixME: Projection Factors for Class I, III, and Yard would be different
    #  from passenger and commuter. Fix this.
    fuel_consump_prj_ = (
        fuel_consump_.filter(
            items=[
                "stcntyfips",
                "carrier",
                "friylab",
                "rr_netgrp",
                "rr_group",
                "netfuelconsump",
            ]
        )
        .rename(columns={"netfuelconsump": "netfuelconsump_2019"})
        .assign(
            year=[list(np.arange(2011, 2051))] * len(fuel_consump_),
        )
        .explode("year")
        .merge(proj_fac_, on="year", how="outer")
        .assign(
            fuelconsump=lambda df: df.aeo_with_covid_travel_freight_proj_fac
            * df.netfuelconsump_2019,
            year=lambda df: df.year.astype(int),
        )
    )
    assert_proj_fuel_merge(fuel_consump_prj_)
    return fuel_consump_prj_


def assert_proj_fuel_merge(fuel_consump_prj_):
    """
    Test if the fuel consumption and project factor merge was successful.
    """
    assert ~fuel_consump_prj_.isna().any().any(), "Check dataframe for nan"
    assert (
        fuel_consump_prj_.loc[lambda df: df.year == 2019]
            .eval("netfuelconsump_2019 == fuelconsump")
            .all()
    ), (
        "netfuelconsump_2019 should be equal to fuelconsump as we are using "
        "2019 fuel data. Check the projection factors and make sure they are "
        "normalized to 2019 value."
    )


def merge_cnty_nm_to_fuel_proj(fuel_consump_prj_: pd.DataFrame,
                               county_df_fil_: pd.DataFrame) -> pd.DataFrame:
    """
    Add county names to the fuel consumption dataset.
    """
    fuel_consump_prj_by_cnty_ = (
        fuel_consump_prj_.groupby(
            ["year", "stcntyfips", "carrier", "friylab", "rr_netgrp", "rr_group"]
        )
        .agg(fuelconsump=("fuelconsump", "sum"))
        .reset_index()
        .merge(county_df_fil_, on="stcntyfips", how="outer")
    )
    return fuel_consump_prj_by_cnty_


def add_scc_desc_to_fuel_proj_cnty(
        fuel_consump_prj_by_cnty_: pd.DataFrame) -> pd.DataFrame:
    """
    Add EPA SCC description to fuel consumption + county name + Projection data.
    """
    xwalk_ssc_desc_4_rr_grp_friy = StringIO(
        """scc_description_level_4,rr_group,friylab
        Line Haul Locomotives: Class I Operations,Class I,Fcat
        Line Haul Locomotives: Class II / III Operations,Class III,Fcat
        Line Haul Locomotives: Passenger Trains (Amtrak),Passenger,Fcat
        Line Haul Locomotives: Commuter Lines,Commuter,Fcat
        Line Haul Locomotives: Commuter Lines,Commuter,IYcat
        Yard Locomotives,Class I,IYcat
        Yard Locomotives,Class III,IYcat
    """
    )
    xwalk_ssc_desc_4_rr_grp_friy_df_ = pd.read_csv(
        xwalk_ssc_desc_4_rr_grp_friy, sep=","
    ).assign(scc_description_level_4=lambda df: df.scc_description_level_4.str.strip())

    fuel_consump_prj_by_cnty_scc_ = fuel_consump_prj_by_cnty_.merge(
        xwalk_ssc_desc_4_rr_grp_friy_df_, on=["rr_group", "friylab"], how="outer"
    )
    return fuel_consump_prj_by_cnty_scc_


def get_emis_quant(
    path_fuel_consump_: str,
    path_emis_rt_: str,
    path_proj_fac_: str,
    path_county_: str,
) -> pd.DataFrame:
    """
    Get the emission quantity using the fuel consumption, emission rates,
    projection factors, and county name datasets.
    """
    fuel_consump_ = pd.read_csv(path_fuel_consump_, index_col=0)
    emis_rt_ = pd.read_csv(path_emis_rt_, index_col=0)
    proj_fac_ = pd.read_excel(path_proj_fac_)
    county_df_ = pd.read_csv(path_county_)
    county_df_fil_ = process_county(county_df_)
    fuel_consump_prj_ = project_filt_fuel_consump(fuel_consump_, proj_fac_)
    fuel_consump_prj_by_cnty_ = merge_cnty_nm_to_fuel_proj(
        fuel_consump_prj_, county_df_fil_)
    fuel_consump_prj_by_cnty_scc_ = add_scc_desc_to_fuel_proj_cnty(
        fuel_consump_prj_by_cnty_)
    emis_quant_ = (
        fuel_consump_prj_by_cnty_scc_.merge(
            emis_rt_,
            left_on=["scc_description_level_4", "year"],
            right_on=["scc_description_level_4", "anals_yr"],
            how="outer",
        )
        .assign(
            em_quant=lambda df: df.em_fac * df.fuelconsump,
            year=lambda df: df.year.astype("Int32"),
        )
        .drop(columns=["anals_yr", "em_units", "friylab"])
    )
    return emis_quant_


if __name__ == "__main__":

    path_fuel_consump = os.path.join(PATH_INTERIM, "fuelconsump_2019_tx_2021-04-13.csv")
    path_emis_rt = os.path.join(PATH_INTERIM, "emission_factor_2021-04-13.csv")
    path_proj_fac = os.path.join(PATH_INTERIM, "aeo_2021_travel_freight_proj_fac.xlsx")
    path_county = os.path.join(PATH_RAW, "Texas_County_Boundaries.csv")
    path_out_emisquant = os.path.join(PATH_PROCESSED, "emis_quant_loco.csv")

    emis_quant = get_emis_quant(
        path_fuel_consump_=path_fuel_consump,
        path_emis_rt_=path_emis_rt,
        path_proj_fac_=path_proj_fac,
        path_county_=path_county,
    )
    emis_quant.to_csv(path_out_emisquant)