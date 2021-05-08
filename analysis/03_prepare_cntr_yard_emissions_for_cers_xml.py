import os
import glob
import pandas as pd
import pyodbc
from locoerlt.utilis import (
    PATH_PROCESSED,
)

path_cntr_emisquant = glob.glob(
    os.path.join(PATH_PROCESSED, "cntr_emis_quant_[0-9]*-*-*.csv")
)[0]
path_yard_brgtool = os.path.join(
    PATH_PROCESSED, "eis_stagging_tables", "yard_bridgetool_cntr.accdb")
conn = pyodbc.connect(r'''Driver={0};DBQ={1}'''.format(
    "{Microsoft Access Driver (*.mdb, *.accdb)}", path_yard_brgtool))
cursor = conn.cursor()
for row in cursor.tables():
    print(row.table_name)
facilitysite = pd.read_sql("""SELECT * FROM FacilitySite""", conn)
emissiosns = pd.read_sql("""SELECT *  FROM Emissions""", conn)

cntr_emisquant = pd.read_csv(path_cntr_emisquant)
cntr_emisquant_yards_2020 = (
    cntr_emisquant
    .loc[lambda df: (df.scc_description_level_4 == "Yard Locomotives")
        & (df.year == 2020)
    ]
    .filter(items=["stcntyfips", "county_name", "scc_description_level_4",
                   "eis_facility_id", "yardname_v1", "pollutant",
                   "site_latitude", "site_longitude",
                   "uncontrolled_em_quant_ton"])
    .rename(columns={
        "stcntyfips": "StateAndCountyFIPSCode",
        "eis_facility_id": "EISFacilitySiteIdentifier",
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
cntr_emisquant_yards_fac_info = (
    cntr_emisquant_yards_2020
    .filter(items=facilitysite_fil_cols)
    .drop_duplicates()
    .reset_index(drop=True)
)

cursor.execute("DELETE * FROM FacilitySite")
conn.commit()
sql = ''' INSERT INTO FacilitySite ({0}) 
          VALUES (?,?,?,?,?) '''.format(",".join(facilitysite_fil_cols))
cursor.executemany(sql, cntr_emisquant_yards_fac_info.itertuples(index=False))
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

cntr_emisquant_yards_emisssions_a = (
    cntr_emisquant_yards_2020
    .assign(
        EISEmissionsUnitIdentifier="code_not_found_on_epa",
        EISEmissionsProcessIdentifier="code_not_found_on_epa",
        ReportingPeriodTypeCode="A",
        EmissionOperatingTypeCode="code_not_found_on_epa",
        PollutantCode=lambda df: df.pollutant,
        TotalEmissions=lambda df: df.uncontrolled_em_quant_ton,
        EmissionsUnitofMeasureCode="TON",
        EmissionCalculationMethodCode="8"
    )
    .filter(items=emissiosns_cols)
)


cntr_emisquant_yards_emisssions_o3d = (
    cntr_emisquant_yards_2020
    .loc[lambda df: df.pollutant.isin(["CO", "NH3", "NOX", "PM10-PRI",
                                       "PM25-PRI", "SO2", "VOC"])]
    .assign(
        EISEmissionsUnitIdentifier="code_not_found_on_epa",
        EISEmissionsProcessIdentifier="code_not_found_on_epa",
        ReportingPeriodTypeCode="O3D",
        EmissionOperatingTypeCode="code_not_found_on_epa",
        PollutantCode=lambda df: df.pollutant,
        TotalEmissions=lambda df: df.uncontrolled_em_quant_ton / 365,
        EmissionsUnitofMeasureCode="TON",
        EmissionCalculationMethodCode="8"
    )
    .filter(items=emissiosns_cols)
)

cntr_emisquant_yards_emisssions = (
    pd.concat(
        [cntr_emisquant_yards_emisssions_a,
         cntr_emisquant_yards_emisssions_o3d
     ]
    )
    .reset_index(drop=True)
)


cursor.execute("DELETE * FROM Emissions")
conn.commit()
sql = ''' INSERT INTO Emissions ({0}) 
          VALUES (?,?,?,?,?,?,?,?,?,?) '''.format(
    ",".join(emissiosns_cols))
cursor.executemany(sql, cntr_emisquant_yards_emisssions.itertuples(index=False))
conn.commit()

cursor.execute("SELECT * FROM FacilitySite")
cursor.fetchall()


conn.close()