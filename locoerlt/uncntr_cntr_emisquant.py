import csv
from itertools import chain
import numpy as np
import pandas as pd
import glob
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
)


def get_txled_factors(
    path_txled_counties_: str, path_texas_counties_: str
) -> pd.DataFrame:
    """Get a dataframe of txled factors by counties where txled program is
    active."""
    tx_counties = pd.read_csv(path_texas_counties_)
    txled_counties_rows = []
    with open(path_txled_counties_, newline="") as csvfile:
        txled_counties_rd = csv.reader(csvfile, delimiter=",")
        for row in txled_counties_rd:
            txled_counties_rows.append(row)
    txled_counties = list(chain.from_iterable(txled_counties_rows))
    txled_counties_prc = list(map(lambda item: item.strip().lower(), txled_counties))

    assert len(txled_counties_prc) == 110, (
        "There should be 110 counties based on TCEQ website. Check why there "
        "are less or more counties."
    )
    txled_counties_prc_df = tx_counties.assign(pollutant="NOX", txled_fac=1)
    txled_counties_prc_df.loc[
        lambda df: df["CNTY_NM"].str.lower().isin(txled_counties_prc), "txled_fac"
    ] = (1 - 6.2 / 100)
    assert len(txled_counties_prc_df.loc[lambda df: df.txled_fac != 1]) == 110, (
        "There should be 110 counties based on TCEQ website. Check why there "
        "are less or more counties."
    )
    return txled_counties_prc_df


def get_controlled_txled(
    emis_quant_agg_: pd.DataFrame, txled_fac_: pd.DataFrame, us_ton_to_grams=907185
) -> pd.DataFrame:
    """
    We are considering that the emis_quant_agg_ dataframe already accounts
    for the fuel reduction due to DERI. To get the final controlled emission
    we just factor in the TxLED NOx emission reduction.
    """
    controlled_emis_quant = (
        emis_quant_agg_.assign(
            tp_county_nm=lambda df: df.county_name.str.lower().str.strip()
        )
        .merge(
            (
                txled_fac_.assign(
                    tp_county_nm=lambda df: df.CNTY_NM.str.lower().str.strip()
                ).filter(items=["tp_county_nm", "pollutant", "txled_fac"])
            ),
            on=["tp_county_nm", "pollutant"],
            how="left",
        )
        .assign(
            txled_fac=lambda df: df.txled_fac.fillna(1),
            controlled_em_quant=lambda df: df.txled_fac * df.em_quant,
            controlled_em_quant_ton=lambda df: (
                df.controlled_em_quant / us_ton_to_grams
            ),
        )
    )
    return controlled_emis_quant


def assert_deri_tot(deri_df, column=""):
    assert np.round(deri_df[column].sum(), 4) == 27206.2667, (
        "Sum of NOx reduction benefits not matching the "
        "input data. Check calculations."
    )


def get_deri_quantity_red(
    path_deri_loco_regions_: str,
    path_deri_loco_nox_red_yr_: str,
    map_deri_region_madhu_areas={
        "Austin": "Austin",
        "Beaumont/Port Arthur": "Beaumont",
        "Dallas/Fort Worth": "Dallas/Fort Worth",
        "Houston/Galveston/Brazoria": "Houston",
        "San Antonio": "San Antonio",
        "Tyler/Longview": "Tyler",
    },
) -> dict:
    """
    Get the average extra emission per county per year if DERI was not
    implemented.
    """
    deri_regions = (
        pd.read_json(path_deri_loco_regions_, orient="index")
        .assign(
            no_counties_reg=lambda df: df.counties.apply(lambda x: len(x)),
        )
        .explode("counties")
        .assign(tp_county_nm=lambda df: df.counties.str.lower().str.strip())
    )
    deri_loco_nox_red_yr = pd.read_excel(path_deri_loco_nox_red_yr_, "Locomotive")
    deri_loco_nox_red_yr_prcd = (
        deri_loco_nox_red_yr.rename(columns=get_snake_case_dict(deri_loco_nox_red_yr))
        .assign(
            region=lambda df: df.area.map(map_deri_region_madhu_areas),
            nox_red_tons_per_yr=lambda df: (
                df.total_nox_reduction_tons_ / df.activity_life
            ),
            year=lambda df: (
                df[["year", "bnefits_expiry_year"]].apply(
                    lambda st_end: range(st_end[0], st_end[1]), axis=1
                )
            ),
        )
        .explode("year")
        .filter(
            items=[
                "region",
                "year",
                "nox_red_tons_per_yr",
            ]
        )
        .groupby(["region", "year"])
        .agg(nox_red_tons_per_yr=("nox_red_tons_per_yr", "sum"))
        .reset_index()
    )

    deri_loco_nox_red_yr_prcd_analysis_yr = deri_loco_nox_red_yr_prcd.loc[
        lambda df: (2011 <= df.year) & (df.year <= 2050)
    ]
    assert_deri_tot(deri_loco_nox_red_yr_prcd_analysis_yr, "nox_red_tons_per_yr")

    deri_loco_nox_red_yr_prcd_emis_quant_region = (
        deri_loco_nox_red_yr_prcd_analysis_yr.merge(
            deri_regions, on=["region"], how="inner"
        )
        .assign(
            nox_red_tons_per_yr_per_region=lambda df: df.nox_red_tons_per_yr,
            pollutant="NOX",
            tp_county_nm=lambda df: df.counties.str.lower().str.strip(),
            scc_description_level_4="Yard Locomotives",
        )
        .filter(
            items=[
                "region",
                "tp_county_nm",
                "scc_description_level_4",
                "year",
                "nox_red_tons_per_yr_per_region",
                "pollutant",
            ]
        )
    )
    return deri_loco_nox_red_yr_prcd_emis_quant_region


def get_deri_uncontrolled_quant(
    emis_quant_agg_: pd.DataFrame,
    deri_loco_nox_red_yr_prcd_emis_quant_region_: pd.DataFrame,
    us_ton_to_grams=907185,
) -> dict:
    """"""
    uncontrolled_emis_quant_deri = emis_quant_agg_.assign(
        tp_county_nm=lambda df: df.county_name.str.lower().str.strip(),
    ).merge(
        (
            deri_loco_nox_red_yr_prcd_emis_quant_region_.filter(
                items=[
                    "region",
                    "tp_county_nm",
                    "year",
                    "scc_description_level_4",
                    "pollutant",
                    "nox_red_tons_per_yr_per_region",
                ]
            )
        ),
        on=["tp_county_nm", "year", "scc_description_level_4", "pollutant"],
        how="left",
    )
    region_county_yard_count = (
        uncontrolled_emis_quant_deri.dropna(subset=["em_quant"])
        .filter(items=["region", "tp_county_nm", "yardname_v1"])
        .drop_duplicates()
        .dropna(subset=["region"])
        .groupby("region")
        .agg(no_counties_yards=("yardname_v1", "count"))
        .reset_index()
    )

    uncontrolled_emis_quant_deri_1 = (
        uncontrolled_emis_quant_deri.merge(
            region_county_yard_count, on=["region"], how="left"
        )
        .assign(
            nox_red_tons_per_yr_per_county_per_yard=lambda df: df.nox_red_tons_per_yr_per_region
            / df.no_counties_yards
        )
        .assign(
            nox_red_tons_per_yr_per_county_per_yard=lambda df: df.nox_red_tons_per_yr_per_county_per_yard.fillna(
                0
            ),
            em_quant_ton=lambda df: df.em_quant / us_ton_to_grams,
            uncontrolled_em_quant_ton=lambda df: (
                df.em_quant_ton + df.nox_red_tons_per_yr_per_county_per_yard
            ),
        )
    )
    assert_deri_tot(
        uncontrolled_emis_quant_deri_1, "nox_red_tons_per_yr_per_county_per_yard"
    )

    deri_emis_red_by_yard_summary = uncontrolled_emis_quant_deri_1.loc[
        lambda df: (df.nox_red_tons_per_yr_per_county_per_yard != 0),
        [
            "year",
            "region",
            "no_counties_yards",
            "stcntyfips",
            "county_name",
            "sector_description",
            "scc_description_level_1",
            "scc_description_level_2",
            "scc_description_level_3",
            "scc",
            "scc_description_level_4",
            "yardname_v1",
            "eis_facility_id",
            "pol_type",
            "pollutant",
            "pol_desc",
            "site_latitude",
            "site_longitude",
            "nox_red_tons_per_yr_per_region",
            "nox_red_tons_per_yr_per_county_per_yard",
        ],
    ].sort_values(by=["year", "region", "stcntyfips", "yardname_v1"])
    return {
        "uncontrolled_emis_quant_deri_1": uncontrolled_emis_quant_deri_1,
        "deri_emis_red_by_yard_summary": deri_emis_red_by_yard_summary,
    }


if __name__ == "__main__":
    st = get_out_file_tsmp()
    path_txled_counties = os.path.join(PATH_RAW, "txled_counties.csv")
    path_texas_counties = os.path.join(PATH_RAW, "Texas_County_Boundaries.csv")
    path_deri_loco_regions = os.path.join(PATH_RAW, "deri_loco_regions.json")
    path_deri_loco_nox_red_yr = os.path.join(
        PATH_RAW, "DERI_List_20190831_Loco_Area_Summary.xlsx"
    )
    path_emisquant_agg = glob.glob(
        os.path.join(PATH_PROCESSED, "emis_quant_loco_agg_[0-9]*-*-*.csv")
    )[0]
    path_out_uncntr_pat = os.path.join(
        PATH_PROCESSED, f"uncntr_emis_quant_[0-9]*-*-*.csv"
    )
    path_txled_prc_out = os.path.join(PATH_PROCESSED, "txled_factors_by_county_prc.csv")
    path_deri_prc_out = os.path.join(PATH_PROCESSED, "deri_factors_by_county_prc.csv")
    cleanup_prev_output(path_out_uncntr_pat)
    path_out_uncntr = os.path.join(PATH_PROCESSED, f"uncntr_emis_quant_{st}.csv")
    path_out_cntr_pat = os.path.join(PATH_PROCESSED, f"cntr_emis_quant_[0-9]*-*-*.csv")
    cleanup_prev_output(path_out_cntr_pat)
    path_out_cntr = os.path.join(PATH_PROCESSED, f"cntr_emis_quant_{st}.csv")
    emis_quant_agg = pd.read_csv(path_emisquant_agg, index_col=0)

    txled_fac = get_txled_factors(
        path_txled_counties_=path_txled_counties,
        path_texas_counties_=path_texas_counties,
    )
    txled_fac.to_csv(path_txled_prc_out)
    controlled_emis_quant = get_controlled_txled(
        emis_quant_agg_=emis_quant_agg, txled_fac_=txled_fac
    )

    deri_loco_nox_red_yr_prcd_emis_quant_region = get_deri_quantity_red(
        path_deri_loco_regions_=path_deri_loco_regions,
        path_deri_loco_nox_red_yr_=path_deri_loco_nox_red_yr,
    )
    uncontrolled_emis_quant_deri_dict = get_deri_uncontrolled_quant(
        emis_quant_agg_=emis_quant_agg,
        deri_loco_nox_red_yr_prcd_emis_quant_region_=deri_loco_nox_red_yr_prcd_emis_quant_region,
    )
    uncontrolled_emis_quant_deri = uncontrolled_emis_quant_deri_dict[
        "uncontrolled_emis_quant_deri_1"
    ]

    deri_emis_red_by_yard_summary = uncontrolled_emis_quant_deri_dict[
        "deri_emis_red_by_yard_summary"
    ]

    deri_rename_cols = {
        "year": "Year",
        "region": "Region",
        "stcntyfips": "FIPS",
        "county_name": "County",
        "sector_description": "Sector Description",
        "scc_description_level_1": "SCC Description Level 1",
        "scc_description_level_2": "SCC Description Level 2",
        "scc_description_level_3": "SCC Description Level 3",
        "scc": "SCC",
        "scc_description_level_4": "SCC Description Level 4",
        "yardname_v1": "Yard Name",
        "pol_type": "Pollutant Type",
        "pollutant": "Pollutant",
        "pol_desc": "Pollutant Description",
        "site_latitude": "Latitude",
        "site_longitude": "Longitude",
        "nox_red_tons_per_yr_per_region": "NOx Reduction per Year per Region",
        "no_counties_yards": "No of Counties and Yards in Region",
        "nox_red_tons_per_yr_per_county_per_yard": "NOx Reduction per Year per County and Yard",
    }
    deri_emis_red_by_yard_summary_1 = deri_emis_red_by_yard_summary.rename(
        columns=deri_rename_cols
    ).filter(items=deri_rename_cols.values())
    deri_emis_red_by_yard_summary_1.to_csv(path_deri_prc_out)

    uncontrolled_emis_quant_deri.to_csv(path_out_uncntr)
    controlled_emis_quant.to_csv(path_out_cntr)
