import os
import glob
import pandas as pd
import pyodbc
from locoerlt.utilis import PATH_RAW, PATH_PROCESSED, get_snake_case_dict


def get_epa_eis_facility_unit_prc_identifiers(
    path_epa_eis_info_: str, path_cntr_emisquant_: str
):
    """
    Use the data provided by EPA's Janice to find the EIS unit and process
    identifiers.
    """
    epa_yard_info = pd.read_csv(path_epa_eis_info_)
    cntr_emisquant = pd.read_csv(path_cntr_emisquant_)
    unique_yards_ertac_2017 = (
        cntr_emisquant.loc[
            lambda df: (df.scc_description_level_4 == "Yard Locomotives")
            & (df.year == 2020)
            & (df.pollutant == "CO")
        ]
        .drop_duplicates(["stcntyfips", "eis_facility_id", "yardname_v1"])
        .filter(items=["stcntyfips", "eis_facility_id", "yardname_v1"])
        .assign(
            ertac_yards=1,
        )
        .reset_index(drop=True)
    )

    epa_yard_info_fil = (
        epa_yard_info.loc[lambda df: df.state == "TX"]
        .rename(columns=get_snake_case_dict(epa_yard_info.columns))
        .assign(epa_yards=1)
    )

    assert (
        epa_yard_info_fil.groupby("eis_facility_id")["county"].count().max() == 1
    ), "eis_facility_id is not unique."

    unique_yards_ertac_2017_epa_unit_prc_identifiers = unique_yards_ertac_2017.merge(
        epa_yard_info_fil,
        left_on="eis_facility_id",
        right_on="eis_facility_id",
        how="left",
    ).filter(
        items=[
            "eis_facility_id",
            "eis_unit_id",
            "eis_process_id",
        ]
    )
    return unique_yards_ertac_2017_epa_unit_prc_identifiers


def set_yard_uncntr_cntr_xml(
    path_yard_brgtool_,
    path_uncntr_cntr_emisquant_,
    path_yard_prcs_,
    emis_quant_ton_col="uncontrolled_em_quant_ton",
):
    conn = pyodbc.connect(
        r"""Driver={0};DBQ={1}""".format(
            "{Microsoft Access Driver (*.mdb, *.accdb)}", path_yard_brgtool_
        )
    )
    cursor = conn.cursor()
    facilitysite = pd.read_sql("""SELECT * FROM FacilitySite""", conn)
    emissiosns = pd.read_sql("""SELECT *  FROM Emissions""", conn)
    uncntr_or_cntr_emisquant = pd.read_csv(path_uncntr_cntr_emisquant_)
    yard_prcs = pd.read_excel(path_yard_prcs_)
    uncntr_cntr_emisquant_yards_2020 = (
        uncntr_or_cntr_emisquant.loc[
            lambda df: (df.scc_description_level_4 == "Yard Locomotives")
            & (df.year == 2020)
        ]
        .merge(yard_prcs, on=["eis_facility_id"], how="left")
        .filter(
            items=[
                "stcntyfips",
                "county_name",
                "scc_description_level_4",
                "eis_facility_id",
                "eis_unit_id",
                "eis_process_id",
                "yardname_v1",
                "pollutant",
                "site_latitude",
                "site_longitude",
                emis_quant_ton_col,
            ]
        )
        .rename(
            columns={
                "stcntyfips": "StateAndCountyFIPSCode",
                "eis_facility_id": "EISFacilitySiteIdentifier",
                "eis_unit_id": "EISEmissionsUnitIdentifier",
                "eis_process_id": "EISEmissionsProcessIdentifier",
                "site_latitude": "LatitudeMeasure",
                "site_longitude": "LongitudeMeasure",
                "yardname_v1": "FacilitySiteName",
                emis_quant_ton_col: "TotalEmissions",
            }
        )
    )
    set_facilitysite_yard_xml(
        uncntr_cntr_emisquant_yards_2020_=uncntr_cntr_emisquant_yards_2020,
        conn_=conn,
        cursor_=cursor,
    )
    set_emissiosn_yard_xml(
        uncntr_cntr_emisquant_yards_2020_=uncntr_cntr_emisquant_yards_2020,
        conn_=conn,
        cursor_=cursor,
    )
    conn.close()


def set_facilitysite_yard_xml(uncntr_cntr_emisquant_yards_2020_, conn_, cursor_):
    """Set the values in the FacilitySite table."""
    facilitysite_fil_cols = [
        "StateAndCountyFIPSCode",
        "EISFacilitySiteIdentifier",
        "FacilitySiteName",
        "LatitudeMeasure",
        "LongitudeMeasure",
    ]
    uncntr_cntr_emisquant_yards_fac_info = (
        uncntr_cntr_emisquant_yards_2020_.filter(items=facilitysite_fil_cols)
        .drop_duplicates()
        .reset_index(drop=True)
    )
    cursor_.execute("DELETE * FROM FacilitySite")
    conn_.commit()
    sql = """ INSERT INTO FacilitySite ({0}) 
              VALUES (?,?,?,?,?) """.format(
        ",".join(facilitysite_fil_cols)
    )
    cursor_.executemany(
        sql, uncntr_cntr_emisquant_yards_fac_info.itertuples(index=False)
    )
    conn_.commit()

    cursor_.execute("SELECT * FROM FacilitySite")
    cursor_.fetchall()


def set_emissiosn_yard_xml(uncntr_cntr_emisquant_yards_2020_, conn_, cursor_):
    emissiosns_cols = [
        "StateAndCountyFIPSCode",
        "EISFacilitySiteIdentifier",
        "EISEmissionsUnitIdentifier",
        "EISEmissionsProcessIdentifier",
        "ReportingPeriodTypeCode",
        "EmissionOperatingTypeCode",
        "PollutantCode",
        "TotalEmissions",
        "EmissionsUnitofMeasureCode",
        "EmissionCalculationMethodCode",
    ]

    uncntr_cntr_emisquant_yards_2020_emisssions_a = (
        uncntr_cntr_emisquant_yards_2020_.assign(
            ReportingPeriodTypeCode="A",
            EmissionOperatingTypeCode="R",
            PollutantCode=lambda df: df.pollutant,
            EmissionsUnitofMeasureCode="TON",
            EmissionCalculationMethodCode="8",
        ).filter(items=emissiosns_cols)
    )

    uncntr_cntr_emisquant_yards_2020_emisssions_o3d = (
        uncntr_cntr_emisquant_yards_2020_.loc[
            lambda df: df.pollutant.isin(
                ["CO", "NH3", "NOX", "PM10-PRI", "PM25-PRI", "SO2", "VOC"]
            )
        ]
        .assign(
            ReportingPeriodTypeCode="O3D",
            EmissionOperatingTypeCode="R",
            PollutantCode=lambda df: df.pollutant,
            TotalEmissions=lambda df: df.TotalEmissions / 365,
            EmissionsUnitofMeasureCode="TON",
            EmissionCalculationMethodCode="8",
        )
        .filter(items=emissiosns_cols)
    )

    uncntr_cntr_emisquant_yards_emisssions = pd.concat(
        [
            uncntr_cntr_emisquant_yards_2020_emisssions_a,
            uncntr_cntr_emisquant_yards_2020_emisssions_o3d,
        ]
    ).reset_index(drop=True)

    cursor_.execute("DELETE * FROM Emissions")
    conn_.commit()
    sql = """ INSERT INTO Emissions ({0}) 
              VALUES (?,?,?,?,?,?,?,?,?,?) """.format(
        ",".join(emissiosns_cols)
    )
    cursor_.executemany(
        sql, uncntr_cntr_emisquant_yards_emisssions.itertuples(index=False)
    )
    conn_.commit()


if __name__ == "__main__":
    path_epa_eis_info = os.path.join(
        PATH_RAW, "eis_report_16580", "fac_conf_proc_unit_16580.csv"
    )
    path_uncntr_emisquant = glob.glob(
        os.path.join(PATH_PROCESSED, "uncntr_emis_quant_[0-9]*-*-*.csv")
    )[0]
    path_cntr_emisquant = glob.glob(
        os.path.join(PATH_PROCESSED, "cntr_emis_quant_[0-9]*-*-*.csv")
    )[0]
    path_yard_prcs = os.path.join(
        PATH_PROCESSED, "imputed_ertac_yard_2017_with_eis_unit_prc.xlsx"
    )
    path_uncntr_yard_brgtool = os.path.join(
        PATH_PROCESSED, "eis_stagging_tables", "yard_bridgetool_uncntr.accdb"
    )
    path_cntr_yard_brgtool = os.path.join(
        PATH_PROCESSED, "eis_stagging_tables", "yard_bridgetool_cntr.accdb"
    )
    epa_eis_facility_unit_prc_identifiers = get_epa_eis_facility_unit_prc_identifiers(
        path_epa_eis_info_=path_epa_eis_info, path_cntr_emisquant_=path_cntr_emisquant
    )
    epa_eis_facility_unit_prc_identifiers.to_excel(path_yard_prcs, index=False)
    set_yard_uncntr_cntr_xml(
        path_yard_brgtool_=path_uncntr_yard_brgtool,
        path_uncntr_cntr_emisquant_=path_uncntr_emisquant,
        path_yard_prcs_=path_yard_prcs,
        emis_quant_ton_col="uncontrolled_em_quant_ton",
    )
    set_yard_uncntr_cntr_xml(
        path_yard_brgtool_=path_cntr_yard_brgtool,
        path_uncntr_cntr_emisquant_=path_cntr_emisquant,
        path_yard_prcs_=path_yard_prcs,
        emis_quant_ton_col="controlled_em_quant_ton",
    )
