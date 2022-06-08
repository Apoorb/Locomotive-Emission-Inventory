import os
import glob
import pandas as pd
import pyodbc
from locoerlt.utilis import PATH_PROCESSED
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname("__file__"), "..")))


def set_yard_uncntr_cntr_xml(
    path_nonpoint_brgtool_,
    path_uncntr_cntr_emisquant_,
    emis_quant_ton_col="uncontrolled_em_quant_ton",
):
    conn = pyodbc.connect(
        r"""Driver={0};DBQ={1}""".format(
            "{Microsoft Access Driver (*.mdb, *.accdb)}", path_nonpoint_brgtool_
        )
    )
    cursor = conn.cursor()
    uncntr_or_cntr_emisquant = pd.read_csv(path_uncntr_cntr_emisquant_)
    uncntr_or_cntr_emisquant_nonpoint_2020 = (
        uncntr_or_cntr_emisquant.loc[
            lambda df: (df.scc_description_level_4 != "Yard Locomotives")
            & (df.year == 2020)
        ]
        .filter(
            items=["stcntyfips", "county_name", "scc", "pollutant", emis_quant_ton_col]
        )
        .rename(
            columns={
                "stcntyfips": "StateAndCountyFIPSCode",
                "scc": "SourceClassificationCode",
                emis_quant_ton_col: "TotalEmissions",
            }
        )
    )
    set_emissiosn_yard_xml(
        uncntr_or_cntr_emisquant_nonpoint_2020_=uncntr_or_cntr_emisquant_nonpoint_2020,
        conn_=conn,
        cursor_=cursor,
    )
    conn.close()


def set_emissiosn_yard_xml(uncntr_or_cntr_emisquant_nonpoint_2020_, conn_, cursor_):
    emissiosns_cols = [
        "StateAndCountyFIPSCode",
        "SourceClassificationCode",
        "ReportingPeriodTypeCode",
        "PollutantCode",
        "TotalEmissions",
        "EmissionsUnitofMeasureCode",
        "EmissionCalculationMethodCode",
    ]
    uncntr_or_cntr_emisquant_nonpoint_emisssions_a = uncntr_or_cntr_emisquant_nonpoint_2020_.assign(
        ReportingPeriodTypeCode="A",
        PollutantCode=lambda df: df.pollutant,
        TotalEmissions=lambda df: df.TotalEmissions,
        EmissionsUnitofMeasureCode="TON",
        EmissionCalculationMethodCode="8",
    ).filter(
        items=emissiosns_cols
    )
    uncntr_or_cntr_emisquant_nonpoint_emisssions_o3d = (
        uncntr_or_cntr_emisquant_nonpoint_2020_.loc[
            lambda df: df.pollutant.isin(
                ["CO", "NH3", "NOX", "PM10-PRI", "PM25-PRI", "SO2", "VOC"]
            )
        ]
        .assign(
            ReportingPeriodTypeCode="O3D",
            PollutantCode=lambda df: df.pollutant,
            TotalEmissions=lambda df: df.TotalEmissions / 365,
            EmissionsUnitofMeasureCode="TON",
            EmissionCalculationMethodCode="8",
        )
        .filter(items=emissiosns_cols)
    )

    uncntr_or_cntr_emisquant_nonpoint_emisssions = pd.concat(
        [
            uncntr_or_cntr_emisquant_nonpoint_emisssions_a,
            uncntr_or_cntr_emisquant_nonpoint_emisssions_o3d,
        ]
    ).reset_index(drop=True)
    cursor_.execute("DELETE * FROM Emissions")
    conn_.commit()
    sql = """ INSERT INTO Emissions ({0}) 
              VALUES (?,?,?,?,?,?,?) """.format(
        ",".join(emissiosns_cols)
    )
    cursor_.executemany(
        sql, uncntr_or_cntr_emisquant_nonpoint_emisssions.itertuples(index=False)
    )
    conn_.commit()


if __name__ == "__main__":
    path_uncntr_emisquant = glob.glob(
        os.path.join(PATH_PROCESSED, "uncntr_emis_quant_[0-9]*-*-*.csv")
    )[0]
    path_cntr_emisquant = glob.glob(
        os.path.join(PATH_PROCESSED, "cntr_emis_quant_[0-9]*-*-*.csv")
    )[0]
    path_uncntr_nonpoint_brgtool = os.path.join(
        PATH_PROCESSED, "eis_stagging_tables", "nonpoint_bridgetool_uncntr.accdb"
    )
    path_cntr_nonpoint_brgtool = os.path.join(
        PATH_PROCESSED, "eis_stagging_tables", "nonpoint_bridgetool_cntr.accdb"
    )

    set_yard_uncntr_cntr_xml(
        path_nonpoint_brgtool_=path_uncntr_nonpoint_brgtool,
        path_uncntr_cntr_emisquant_=path_uncntr_emisquant,
        emis_quant_ton_col="uncontrolled_em_quant_ton",
    )
    set_yard_uncntr_cntr_xml(
        path_nonpoint_brgtool_=path_cntr_nonpoint_brgtool,
        path_uncntr_cntr_emisquant_=path_cntr_emisquant,
        emis_quant_ton_col="controlled_em_quant_ton",
    )
