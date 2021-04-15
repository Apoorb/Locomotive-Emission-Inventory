"""
Tests emisquant module.
"""
import os
import pytest
import inflection
import pandas as pd
import numpy as np
from locoerlt.utilis import (PATH_RAW, PATH_INTERIM, PATH_PROCESSED,
                             get_out_file_tsmp)
from locoerlt.emisquant import process_proj_fac

st = get_out_file_tsmp()
path_out_emisquant = os.path.join(PATH_PROCESSED,
                                  f"emis_quant_loco_{st}.csv")
path_proj_fac = os.path.join(PATH_INTERIM, "Projection Factors 04132021.xlsx")
path_cls1_cntpct = os.path.join(PATH_RAW, "2019CountyPct.csv")
path_fuel_consump = os.path.join(PATH_INTERIM,
                                 f"fuelconsump_2019_tx_{st}.csv")


@pytest.fixture()
def get_fuel_consump():
    return pd.read_csv(path_fuel_consump, index_col=0)


@pytest.fixture()
def get_emis_quant():
    return pd.read_csv(path_out_emisquant, index_col=0)


@pytest.fixture()
def get_proj_fac():
    return process_proj_fac(path_proj_fac)


@pytest.fixture()
def get_county_cls1_prop_input():
    cls1_cntpct = pd.read_csv(path_cls1_cntpct)
    return cls1_cntpct


def test_county_control_tot_cls1(get_emis_quant, get_fuel_consump,
                                 get_county_cls1_prop_input):
    fuel_df_cls1 = get_fuel_consump.loc[
        lambda df: (df.carrier.isin(["BNSF", "UP", "KCS"])) & (df.friylab ==
                                                               "Fcat")]

    fuel_df_cls1_agg = fuel_df_cls1.groupby(
        ['carrier', 'friylab', "stcntyfips"]).agg(
        st_fuel_consmp=("st_fuel_consmp", "mean"),
        cnty_cls1_fuel_consmp_1=("cnty_cls1_fuel_consmp", "mean"),
        cnty_cls1_fuel_consmp_2=("link_fuel_consmp", "sum"),
    ).reset_index()
    fuel_df_cls1_agg["st_fuel_consmp_computed"] = (
        fuel_df_cls1_agg
        .groupby(['carrier', 'friylab'])
        .cnty_cls1_fuel_consmp_1.transform(sum))


    # CO2, NH3, and CO have constant rates, so we can get the projection
    # factors from the emission rates for these pollutants.
    co2_nh3_co = get_emis_quant.loc[
       lambda df: (df.pollutant.isin(["CO2"])) & (df.year == 2019)]
    co2_nh3_co["st_fuel_consmp_by_yr"] = co2_nh3_co.groupby(
        ['year', 'carrier', 'friylab', 'pollutant']
    ).county_fuel_consmp_by_yr.transform(sum)

    co2_nh3_co_fil = co2_nh3_co.loc[
        lambda df: (df.carrier.isin(["BNSF", "UP", "KCS"])) & (df.friylab ==
                                                               "Fcat")]

    test = co2_nh3_co_fil.merge(fuel_df_cls1_agg, on=['carrier', 'friylab',
                                           "stcntyfips"],how="right")
    test.groupby('carrier').sum()
    test_1 = test.loc[
        ~ np.isclose(test.cnty_cls1_fuel_consmp_1,
                     test.county_fuel_consmp_by_yr)]



    co2_nh3_co["county_pct"] = co2_nh3_co["county_fuel_consmp_by_yr"] / co2_nh3_co["st_fuel_consmp_by_yr"]
    cnt_pct = get_county_cls1_prop_input.rename(columns={"FIPS": "stcntyfips"})
    co2_nh3_co_test = (
        co2_nh3_co
        .merge(cnt_pct, on="stcntyfips")
    )



def test_proj_rt_from_emis(get_emis_quant, get_proj_fac):
    # CO2, NH3, and CO have constant rates, so we can get the projection
    # factors from the emission rates for these pollutants.
    co2_nh3_co = get_emis_quant.loc[
       lambda df: (df.pollutant.isin(["CO2", "NH3", "CO"]))]
    co2_nh3_co_emis_2019 = (
       co2_nh3_co.loc[lambda df: df.year == 2019]
       .drop(columns="year")
       .rename(columns = {"em_quant": "em_quant_2019"})
       .filter(items=[
           'stcntyfips', 'carrier', 'yardname_axb', 'rr_netgrp', 'rr_group',
           'scc_description_level_4', "pollutant", 'em_quant_2019'
       ])
    )

    co2_nh3_co_emis_with_2019 = (
        co2_nh3_co
        .merge(
            co2_nh3_co_emis_2019,
            on=['stcntyfips', 'carrier', 'yardname_axb', 'rr_netgrp', 'rr_group',
                'scc_description_level_4', 'pollutant']
        )
        .assign(proj_fac_calc=lambda df: df.em_quant/ df.em_quant_2019)
        .merge(get_proj_fac, on=["year", "rr_group"], how="left")
        .dropna(subset=["proj_fac_calc"])
    )
    assert np.allclose(np.round(co2_nh3_co_emis_with_2019.proj_fac_calc, 5),
                       np.round(co2_nh3_co_emis_with_2019.proj_fac, 5),
                )