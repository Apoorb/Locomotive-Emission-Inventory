import os
import pandas as pd
import numpy as np
from io import StringIO
from locoerlt.utilis import PATH_RAW, PATH_INTERIM, PATH_PROCESSED


if __name__ == "__main__":

    path_fuel_consump = os.path.join(PATH_INTERIM, "fuelconsump_2019_tx_2021-04-13.csv")
    path_emis_rt = os.path.join(PATH_INTERIM, "emission_factor_2021-04-13.csv")
    path_proj_fac = os.path.join(PATH_INTERIM, "aeo_2021_travel_freight_proj_fac.xlsx")
    path_county = os.path.join(PATH_RAW, "Texas_County_Boundaries.csv")
    fuel_consump = pd.read_csv(path_fuel_consump, index_col=0)
    emis_rt = pd.read_csv(path_emis_rt, index_col=0)
    proj_fac = pd.read_excel(path_proj_fac)
    county_df = pd.read_csv(path_county)

    county_df_fil = county_df.filter(items=["CNTY_NM", "FIPS_ST_CNTY_CD"]).rename(
        columns={"CNTY_NM": "county_name", "FIPS_ST_CNTY_CD": "stcntyfips"}
    )

    fuel_consump_prj = (
        fuel_consump.filter(
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
            year=[list(np.arange(2011, 2051))] * len(fuel_consump),
        )
        .explode("year")
        .merge(proj_fac, on="year", how="outer")
        .assign(
            fuelconsump=lambda df: df.aeo_with_covid_travel_freight_proj_fac
            * df.netfuelconsump_2019,
            year=lambda df: df.year.astype(int),
        )
    )

    assert ~fuel_consump_prj.isna().any().any(), "Check dataframe for nan"
    assert (
        fuel_consump_prj.loc[lambda df: df.year == 2019]
        .eval("netfuelconsump_2019 == fuelconsump")
        .all()
    ), (
        "netfuelconsump_2019 should be equal to fuelconsump as we are using "
        "2019 fuel data. Check the projection factors and make sure they are "
        "normalized to 2019 value."
    )

    fuel_consump_prj_by_cnty = (
        fuel_consump_prj.groupby(
            ["year", "stcntyfips", "carrier", "friylab", "rr_netgrp", "rr_group"]
        )
        .agg(fuelconsump=("fuelconsump", "sum"))
        .reset_index()
        .merge(county_df_fil, on="stcntyfips", how="outer")
    )

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
    xwalk_ssc_desc_4_rr_grp_friy_df = pd.read_csv(
        xwalk_ssc_desc_4_rr_grp_friy, sep=","
    ).assign(scc_description_level_4=lambda df: df.scc_description_level_4.str.strip())

    fuel_consump_prj_by_cnty_scc = fuel_consump_prj_by_cnty.merge(
        xwalk_ssc_desc_4_rr_grp_friy_df, on=["rr_group", "friylab"], how="outer"
    )

    emis_quant = (
        fuel_consump_prj_by_cnty_scc.merge(
            emis_rt,
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
