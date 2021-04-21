import os
import csv
from itertools import chain
import numpy as np
import pandas as pd
import glob
from locoerlt.utilis import (PATH_RAW, PATH_INTERIM, PATH_PROCESSED,
                             get_snake_case_dict)


def get_txled_factors(
    path_txled_counties_: str,
    path_texas_counties_: str
) -> pd.DataFrame:
    """ Get a dataframe of txled factors by counties where txled program is
    active."""
    tx_counties = pd.read_csv(path_texas_counties_)
    txled_counties_rows = []
    with open(path_txled_counties_, newline='') as csvfile:
        txled_counties_rd = csv.reader(csvfile, delimiter=',')
        for row in txled_counties_rd:
            txled_counties_rows.append(row)

    txled_counties = list(chain.from_iterable(txled_counties_rows))
    txled_counties_prc = list(map(lambda item: item.strip().lower(),
                                  txled_counties))

    assert len(txled_counties_prc) == 110, (
        "There should be 110 counties based on TCEQ website. Check why there "
        "are less or more counties.")

    txled_counties_prc_df = (
        tx_counties
        .assign(
            pollutant="NOX",
            txled_fac=1)
    )

    txled_counties_prc_df.loc[
        lambda df: df['CNTY_NM'].str.lower().isin(txled_counties_prc),
        "txled_fac"
    ] = (1 - 6.2 / 100)

    assert len(txled_counties_prc_df.loc[lambda df: df.txled_fac != 1]) == \
           110, (
        "There should be 110 counties based on TCEQ website. Check why there "
        "are less or more counties.")
    return txled_counties_prc_df


def get_deri_quantity_red(
    path_deri_loco_regions_: str,
    path_deri_loco_nox_red_yr_: str,
    controlled_emis_quant_txled_: pd.DataFrame,
    map_deri_region_madhu_areas={
        "Austin": "Austin",
        "Beaumont/Port Arthur": "Beaumont",
        "Dallas/Fort Worth": "Dallas/Fort Worth",
        "Houston/Galveston/Brazoria": "Houston",
        "San Antonio": "San Antonio",
        "Tyler/Longview": "Tyler"
    },
    us_ton_to_grams=907185
) -> dict:
    """

    Parameters
    ----------
    path_deri_loco_regions_
    path_deri_loco_nox_red_yr_
    controlled_emis_quant_txled_
    map_deri_region_madhu_areas
    us_ton_to_grams

    Returns
    -------

    """
    deri_regions = (
        pd.read_json(path_deri_loco_regions_, orient="index")
        .assign(
            no_counties_reg=lambda df: df.counties.apply(lambda x: len(x)),
    )
        .explode("counties")
        .assign(
            tp_county_nm=lambda df: df.counties.str.lower().str.strip()
        )
    )

    emis_quant_region_deri = (
        controlled_emis_quant_txled_
        .loc[lambda df: (
            df.pollutant == "NOX"
            ) &
            (df.scc_description_level_4.isin(
            ['Yard Locomotives', 'Line Haul Locomotives: Class I Operations',
             'Line Haul Locomotives: Class II / III Operations'],
        ))]

        .groupby(["tp_county_nm", "year"])
        .agg(
            em_quant_yard_county=("em_quant", "sum"),
        )
        .reset_index()
        .merge(deri_regions, on=["tp_county_nm"])
        .groupby(["region", "year"])
        .agg(
            em_quant_yard_region=("em_quant_yard_county", "sum"),
        )
        .reset_index()
        .assign(
            em_quant_yard_region_tons=lambda df: (
                    df.em_quant_yard_region / us_ton_to_grams)
        )
    )

    deri_loco_nox_red_yr = pd.read_excel(path_deri_loco_nox_red_yr_,
                                         "Locomotive")

    deri_loco_nox_red_yr_prcd = (
        deri_loco_nox_red_yr
        .rename(columns=get_snake_case_dict(deri_loco_nox_red_yr))
        .assign(region=lambda df: df.area.map(map_deri_region_madhu_areas),
                nox_red_tons_per_yr=lambda df: (
                    df.total_nox_reduction_tons_/df.activity_life),
                year=lambda df: (
                    df[["year", "bnefits_expiry_year"]]
                    .apply(lambda st_end: range(st_end[0], st_end[1]), axis=1))
                )
        .explode("year")
        .filter(items=[
            "region",
            "year",
            "nox_red_tons_per_yr",
        ])
        .groupby([ "region", "year"])
        .agg(nox_red_tons_per_yr=("nox_red_tons_per_yr", "sum"))
        .reset_index()
    )

    assert (np.round(deri_loco_nox_red_yr_prcd.nox_red_tons_per_yr.sum(),4)
            == 51202.7567), ("Sum of NOx reduction benefits not matching the "
                             "input data. Check calculations.")

    deri_loco_nox_red_yr_prcd_emis_quant_region = (
        deri_loco_nox_red_yr_prcd
        .merge(
            emis_quant_region_deri,
            on=["region","year"],
            how="inner"
        )
        .assign(
            em_quant_yard_region_tons_cntr=lambda df: (
                df.em_quant_yard_region_tons - df.nox_red_tons_per_yr
            ),
            deri_regional_em_reduc_fac=lambda df: (
                df.em_quant_yard_region_tons_cntr / df.em_quant_yard_region_tons
            ),
            pollutant="NOX"
        )
        .filter(items=[
            'region', 'year', 'nox_red_tons_per_yr', "pollutant",
            'em_quant_yard_region_tons', 'em_quant_yard_region_tons_cntr',
            'deri_regional_em_reduc_fac'])
    )

    deri_loco_nox_red_yr_prcd_emis_quant_county = (
        deri_loco_nox_red_yr_prcd_emis_quant_region
        .merge(deri_regions, on=["region"])
        .filter(items=[
            'counties', 'year', "pollutant", 'deri_regional_em_reduc_fac'])
        .assign(
            tp_county_nm=lambda df: df.counties.str.lower().str.strip()
        )
    )
    return {
        "deri_loco_nox_red_yr_prcd_emis_quant_region":
            deri_loco_nox_red_yr_prcd_emis_quant_region,
        "deri_loco_nox_red_yr_prcd_emis_quant_county":
            deri_loco_nox_red_yr_prcd_emis_quant_county
    }


def get_controlled_txled(
    emis_quant_agg_: pd.DataFrame,
    txled_fac_: pd.DataFrame
) -> pd.DataFrame:
    """

    Parameters
    ----------
    emis_quant_agg_
    txled_fac_

    Returns
    -------

    """
    controlled_emis_quant_txled = (
        emis_quant_agg_.assign(
            tp_county_nm=lambda df: df.county_name.str.lower().str.strip())
            .merge(
            (txled_fac_
             .assign(
                tp_county_nm=lambda df: df.CNTY_NM.str.lower().str.strip())
             .filter(items=["tp_county_nm", "pollutant", "txled_fac"])
             ),
            on=["tp_county_nm", "pollutant"],
            how="left"
        )
            .assign(
            txled_fac=lambda df: df.txled_fac.fillna(1),
            controlled_em_quant_txled=lambda df: df.txled_fac * df.em_quant
        )
    )
    return controlled_emis_quant_txled


def get_deri_controlled_fac(
    controlled_emis_quant_txled_: pd.DataFrame,
    deri_county_fac_: pd.DataFrame
) -> pd.DataFrame:
    """

    Parameters
    ----------
    controlled_emis_quant_txled_
    deri_county_fac_

    Returns
    -------

    """
    controlled_emis_quant_txled_deri = (
        controlled_emis_quant_txled_
        .merge(
            (deri_county_fac
            .filter(items=["tp_county_nm", "year", "pollutant",
                           "deri_regional_em_reduc_fac"])
            ),
            on=["tp_county_nm", "year", "pollutant"],
            how="left"
        ))

    controlled_emis_quant_txled_deri["deri_fac"] = (
        controlled_emis_quant_txled_deri.deri_regional_em_reduc_fac.fillna(1))

    controlled_emis_quant_txled_deri["controlled_em_quant_txled_deri"] = (
        controlled_emis_quant_txled_deri.controlled_em_quant_txled
        * controlled_emis_quant_txled_deri.deri_fac
    )
    return controlled_emis_quant_txled_deri


if __name__ == "__main__":
    path_txled_counties = os.path.join(PATH_RAW, "txled_counties.csv")
    path_texas_counties = os.path.join(PATH_RAW, "Texas_County_Boundaries.csv")
    path_deri_loco_regions = os.path.join(PATH_RAW,
                                          "deri_loco_regions.json")
    path_deri_loco_nox_red_yr = os.path.join(
        PATH_RAW, "DERI_List_20190831_Loco_Area_Summary.xlsx")
    path_emisquant_agg = glob.glob(os.path.join(
        PATH_PROCESSED, "emis_quant_loco_agg_[0-9]*-*-*.csv"))[0]
    path_out_uncntr_cntr = os.path.join(PATH_PROCESSED,
                                        "uncntr_cntr_emis_quant.csv")
    emis_quant_agg = pd.read_csv(path_emisquant_agg, index_col=0)

    txled_fac = get_txled_factors(
        path_txled_counties_=path_txled_counties,
        path_texas_counties_=path_texas_counties
    )

    controlled_emis_quant_txled = get_controlled_txled(
        emis_quant_agg_=emis_quant_agg,
        txled_fac_=txled_fac
    )

    deri_red_fac_dict = deri_nox_red_quants = get_deri_quantity_red(
        path_deri_loco_regions_=path_deri_loco_regions,
        path_deri_loco_nox_red_yr_=path_deri_loco_nox_red_yr,
        controlled_emis_quant_txled_=controlled_emis_quant_txled
    )

    deri_county_fac = deri_red_fac_dict[
        "deri_loco_nox_red_yr_prcd_emis_quant_county"]

    controlled_emis_quant_txled_deri = get_deri_controlled_fac(
        controlled_emis_quant_txled_=controlled_emis_quant_txled,
        deri_county_fac_=deri_county_fac
    )

    controlled_emis_quant_txled_deri_out = (
        controlled_emis_quant_txled_deri
        .rename(
            columns={"em_quant": "uncontrolled_em_quant"}
        )
        .filter(
            items=[
                'year', 'stcntyfips', 'county_name', 'dat_cat_code',
                'sector_description', 'scc_description_level_1',
                'scc_description_level_2', 'scc_description_level_3', 'scc',
                'scc_description_level_4', 'yardname_axb', 'pol_type',
                'pollutant',
                'pol_desc', 'em_fac', 'uncontrolled_em_quant',
                'county_carr_friy_yardnm_fuel_consmp_by_yr',
                'county_carr_friy_yardnm_miles_by_yr',
                'txled_fac', 'controlled_em_quant_txled',
                'deri_regional_em_reduc_fac',
                'deri_fac',
                'controlled_em_quant_txled_deri'
            ]
        )
    )

    controlled_emis_quant_txled_deri_out.to_csv(path_out_uncntr_cntr)

