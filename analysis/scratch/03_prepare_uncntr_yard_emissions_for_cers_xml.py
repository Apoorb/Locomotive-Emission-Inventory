import os
import glob
import pandas as pd
import pyodbc
from locoerlt.utilis import (
    PATH_PROCESSED,
)

path_uncntr_emisquant = glob.glob(
    os.path.join(PATH_PROCESSED, "uncntr_emis_quant_[0-9]*-*-*.csv")
)[0]
path_yard_brgtool = os.path.join(
    PATH_PROCESSED, "eis_stagging_tables", "yard_bridgetool_uncntr.accdb")
path_yard_prcs = os.path.join(
    PATH_PROCESSED,
    "imputed_ertac_yard_2017_with_eis_unit_prc.xlsx"
)
conn = pyodbc.connect(r'''Driver={0};DBQ={1}'''.format(
    "{Microsoft Access Driver (*.mdb, *.accdb)}", path_yard_brgtool))
cursor = conn.cursor()
for row in cursor.tables():
    print(row.table_name)
facilitysite = pd.read_sql("""SELECT * FROM FacilitySite""", conn)
emissiosns = pd.read_sql("""SELECT *  FROM Emissions""", conn)
uncntr_emisquant = pd.read_csv(path_uncntr_emisquant)
yard_prcs = pd.read_excel(path_yard_prcs)

uncntr_emisquant_yards_2020 = (
    uncntr_emisquant
    .loc[lambda df: (df.scc_description_level_4 == "Yard Locomotives")
        & (df.year == 2020)
    ]
    .merge(
        yard_prcs,
        on=["eis_facility_id"],
        how="left"
    )
    .filter(items=["stcntyfips", "county_name", "scc_description_level_4",
                   "eis_facility_id", "eis_unit_id", "eis_process_id",
                   "yardname_v1", "pollutant", "site_latitude",
                   "site_longitude", "uncontrolled_em_quant_ton"])
    .rename(columns={
        "stcntyfips": "StateAndCountyFIPSCode",
        "eis_facility_id": "EISFacilitySiteIdentifier",
        "eis_unit_id": "EISEmissionsUnitIdentifier",
        "eis_process_id": "EISEmissionsProcessIdentifier",
        "site_latitude": "LatitudeMeasure",
        "site_longitude": "LongitudeMeasure",
        "yardname_v1": "FacilitySiteName"
    })
)

facilitysite_fil_cols = [
    "StateAndCountyFIPSCode",
    "EISFacilitySiteIdentifier",
    "FacilitySiteName",
    "LatitudeMeasure",
    "LongitudeMeasure"
]
uncntr_emisquant_yards_fac_info = (
    uncntr_emisquant_yards_2020
    .filter(items=facilitysite_fil_cols)
    .drop_duplicates()
    .reset_index(drop=True)
)

cursor.execute("DELETE * FROM FacilitySite")
conn.commit()
sql = ''' INSERT INTO FacilitySite ({0}) 
          VALUES (?,?,?,?,?) '''.format(",".join(facilitysite_fil_cols))
cursor.executemany(sql, uncntr_emisquant_yards_fac_info.itertuples(index=False))
conn.commit()

cursor.execute("SELECT * FROM FacilitySite")
cursor.fetchall()

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

uncntr_emisquant_yards_emisssions_a = (
    uncntr_emisquant_yards_2020
    .assign(
        ReportingPeriodTypeCode="A",
        EmissionOperatingTypeCode="R",
        PollutantCode=lambda df: df.pollutant,
        TotalEmissions=lambda df: df.uncontrolled_em_quant_ton,
        EmissionsUnitofMeasureCode="TON",
        EmissionCalculationMethodCode="8"
    )
    .filter(items=emissiosns_cols)
)


uncntr_emisquant_yards_emisssions_o3d = (
    uncntr_emisquant_yards_2020
    .loc[lambda df: df.pollutant.isin(["CO", "NH3", "NOX", "PM10-PRI",
                                       "PM25-PRI", "SO2", "VOC"])]
    .assign(
        ReportingPeriodTypeCode="O3D",
        EmissionOperatingTypeCode="R",
        PollutantCode=lambda df: df.pollutant,
        TotalEmissions=lambda df: df.uncontrolled_em_quant_ton / 365,
        EmissionsUnitofMeasureCode="TON",
        EmissionCalculationMethodCode="8"
    )
    .filter(items=emissiosns_cols)
)

uncntr_emisquant_yards_emisssions = (
    pd.concat(
        [uncntr_emisquant_yards_emisssions_a,
         uncntr_emisquant_yards_emisssions_o3d
     ]
    )
    .reset_index(drop=True)
)


cursor.execute("DELETE * FROM Emissions")
conn.commit()
sql = ''' INSERT INTO Emissions ({0}) 
          VALUES (?,?,?,?,?,?,?,?,?,?) '''.format(
    ",".join(emissiosns_cols))
cursor.executemany(sql, uncntr_emisquant_yards_emisssions.itertuples(index=False))
conn.commit()

cursor.execute("SELECT * FROM FacilitySite")
cursor.fetchall()

conn.close()