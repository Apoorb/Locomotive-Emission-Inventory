import os
import csv
from itertools import chain
import numpy as np
import pandas as pd
from locoerlt.utilis import PATH_RAW, PATH_INTERIM, get_snake_case_dict


if __name__ == "__main__":
    path_txled_counties = os.path.join(PATH_RAW, "txled_counties.csv")
    path_texas_counties = os.path.join(PATH_RAW, "Texas_County_Boundaries.csv")
    path_deri_loco_regions = os.path.join(PATH_RAW,
                                          "deri_loco_regions.json")
    path_deri_loco_nox_red_yr = os.path.join(
        PATH_RAW, "DERI_List_20190831_Loco_Area_Summary.xlsx")
    tx_counties = pd.read_csv(path_texas_counties)
    txled_counties_rows = []
    with open(path_txled_counties, newline='') as csvfile:
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
        .assign(txled_fac=1)
    )

    txled_counties_prc_df.loc[
        lambda df: df['CNTY_NM'].str.lower().isin(txled_counties_prc),
        "txled_fac"
    ] = (1 - 6.2 / 100)

    assert len(txled_counties_prc_df.loc[lambda df: df.txled_fac != 1]) == \
           110, (
        "There should be 110 counties based on TCEQ website. Check why there "
        "are less or more counties.")

    deri_regions = (
        pd.read_json(path_deri_loco_regions, orient="index")
        .assign(
            no_counties_reg=lambda df: df.counties.apply(lambda x: len(x))
        )
        .explode("counties")
    )

    deri_loco_nox_red_yr = pd.read_excel(path_deri_loco_nox_red_yr,
                                         "Locomotive")

    map_deri_region_madhu_areas = {
        "Austin": "Austin",
        "Beaumont/Port Arthur": "Beaumont",
        "Dallas/Fort Worth": "Dallas/Fort Worth",
        "Houston/Galveston/Brazoria": "Houston",
        "San Antonio": "San Antonio",
        "Tyler/Longview": "Tyler"
    }
    deri_loco_nox_red_yr_prcd = (
        deri_loco_nox_red_yr
        .rename(columns=get_snake_case_dict(deri_loco_nox_red_yr))
        .assign(region=lambda df: df.area.map(map_deri_region_madhu_areas),
                scc_description_level_4="Yard Locomotives",
                nox_red_tons_per_yr=lambda df: (
                    df.total_nox_reduction_tons_/df.activity_life),
                benefit_years=lambda df: (
                    df[["year", "bnefits_expiry_year"]]
                    .apply(lambda st_end: range(st_end[0], st_end[1]), axis=1))
                )
        .explode("benefit_years")
        .filter(items=[
            "scc_description_level_4",
            "region",
            "benefit_years",
            "nox_red_tons_per_yr",
        ])
        .groupby([ "scc_description_level_4", "region","benefit_years"])
        .agg(nox_red_tons_per_yr=("nox_red_tons_per_yr", "sum"))
        .reset_index()
    )

    assert (np.round(deri_loco_nox_red_yr_prcd.nox_red_tons_per_yr.sum(),4)
            == 51202.7567), ("Sum of NOx reduction benefits not matching the "
                             "input data. Check calculations.")
