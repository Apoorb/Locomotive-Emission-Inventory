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


if __name__ == "__main__":
    st = get_out_file_tsmp()
    path_txled_counties = os.path.join(PATH_RAW, "txled_counties.csv")
    path_texas_counties = os.path.join(PATH_RAW, "Texas_County_Boundaries.csv")
    path_emisquant = glob.glob(
        os.path.join(PATH_PROCESSED, "emis_quant_loco_[0-9]*-*-*.csv")
    )[0]
    path_out_cntr_pat = os.path.join(
        PATH_PROCESSED, f"harris_cntr_emis_quant_[0-9]*-*-*.csv"
    )
    cleanup_prev_output(path_out_cntr_pat)
    path_out_cntr = os.path.join(PATH_PROCESSED, f"harris_cntr_emis_quant_{st}.xlsx")
    txled_fac = get_txled_factors(
        path_txled_counties_=path_txled_counties,
        path_texas_counties_=path_texas_counties,
    )
    txled_fac_harris = txled_fac.loc[txled_fac.CNTY_NM.str.lower() == "harris"]
    txled_fac_harris_val = txled_fac_harris.txled_fac.values[0]

    emis_quant = pd.read_csv(path_emisquant, index_col=0)
    filter_pol = ["CO2", "CO", "NH3", "SO2", "NOX", "PM10-PRI", "PM25-PRI", "VOC"]
    emis_quant_harris = emis_quant.loc[
        (emis_quant.county_name.str.lower() == "harris")
        & (emis_quant.pollutant.isin(filter_pol))
    ]
    us_ton_to_grams = 907185
    emis_quant_harris["cntr_em_quant"] = emis_quant_harris["em_quant"]
    emis_quant_harris.loc[lambda df: df.pollutant == "NOX", "cntr_em_quant"] = (
        emis_quant_harris.loc[lambda df: df.pollutant == "NOX", "em_quant"]
        * txled_fac_harris_val
    )
    emis_quant_harris["cntr_em_quant_tons"] = (
        emis_quant_harris["cntr_em_quant"] / us_ton_to_grams
    )
    emis_quant_harris["TxLED Factor"] = 1
    emis_quant_harris.loc[
        lambda df: df.pollutant == "NOX", "TxLED Factor"
    ] = txled_fac_harris_val

    emis_quant_harris.columns
    rename_map = {
        "year": "Year",
        "county_name": "County",
        "carrier": "Carrier",
        "county_carr_friy_yardnm_fuel_consmp_by_yr": "Fuel Consumption (Gallon)",
        "county_carr_friy_yardnm_miles_by_yr": "Track Miles",
        "scc_description_level_4": "SCC Desc 4",
        "yardname_v1": "Yard Name",
        "site_latitude": "Yard Lat",
        "site_longitude": "Yard Long",
        "scc": "SCC",
        "pollutant": "Pollutant",
        "em_fac": "Emission Factor (Grams/Gallon)",
        "TxLED Factor": "TxLED Factor",
        "cntr_em_quant": "Controlled Emissions (Grams)",
        "cntr_em_quant_tons": "Controlled Emissions (Tons)",
    }
    emis_quant_harris = emis_quant_harris.filter(items=rename_map.keys()).rename(
        columns=rename_map
    )
    emis_quant_harris.to_excel(path_out_cntr, index=False)
