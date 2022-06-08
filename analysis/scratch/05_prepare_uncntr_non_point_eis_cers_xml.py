import os
import glob
import pandas as pd
import pyodbc
from locoerlt.utilis import PATH_PROCESSED

path_uncntr_emisquant = glob.glob(
    os.path.join(PATH_PROCESSED, "uncntr_emis_quant_[0-9]*-*-*.csv")
)[0]
path_nonpoint_brgtool = os.path.join(
    PATH_PROCESSED, "eis_stagging_tables", "loco_nonpoint_uncntr.accdb"
)
conn = pyodbc.connect(
    r"""Driver={0};DBQ={1}""".format(
        "{Microsoft Access Driver (*.mdb, *.accdb)}", path_nonpoint_brgtool
    )
)
cursor = conn.cursor()
for row in cursor.tables():
    print(row.table_name)
emissiosns = pd.read_sql("""SELECT *  FROM Emissions""", conn)

uncntr_emisquant = pd.read_csv(path_uncntr_emisquant)
uncntr_emisquant_nonpoint_2020 = (
    uncntr_emisquant.loc[
        lambda df: (df.scc_description_level_4 != "Yard Locomotives")
        & (df.year == 2020)
    ]
    .filter(
        items=[
            "stcntyfips",
            "county_name",
            "scc",
            "pollutant",
            "uncontrolled_em_quant_ton",
        ]
    )
    .rename(
        columns={
            "stcntyfips": "StateAndCountyFIPSCode",
            "scc": "SourceClassificationCode",
        }
    )
)


emissiosns_cols = [
    "StateAndCountyFIPSCode",
    "SourceClassificationCode",
    "ReportingPeriodTypeCode",
    "PollutantCode",
    "TotalEmissions",
    "EmissionsUnitofMeasureCode",
    "EmissionCalculationMethodCode",
]

uncntr_emisquant_nonpoint_emisssions_a = uncntr_emisquant_nonpoint_2020.assign(
    ReportingPeriodTypeCode="A",
    PollutantCode=lambda df: df.pollutant,
    TotalEmissions=lambda df: df.uncontrolled_em_quant_ton,
    EmissionsUnitofMeasureCode="TON",
    EmissionCalculationMethodCode="8",
).filter(items=emissiosns_cols)

uncntr_emisquant_nonpoint_emisssions_o3d = (
    uncntr_emisquant_nonpoint_2020.loc[
        lambda df: df.pollutant.isin(
            ["CO", "NH3", "NOX", "PM10-PRI", "PM25-PRI", "SO2", "VOC"]
        )
    ]
    .assign(
        ReportingPeriodTypeCode="O3D",
        PollutantCode=lambda df: df.pollutant,
        TotalEmissions=lambda df: df.uncontrolled_em_quant_ton / 365,
        EmissionsUnitofMeasureCode="TON",
        EmissionCalculationMethodCode="8",
    )
    .filter(items=emissiosns_cols)
)

uncntr_emisquant_nonpoint_emisssions = pd.concat(
    [uncntr_emisquant_nonpoint_emisssions_a, uncntr_emisquant_nonpoint_emisssions_o3d]
).reset_index(drop=True)


cursor.execute("DELETE * FROM Emissions")
conn.commit()
sql = """ INSERT INTO Emissions ({0}) 
          VALUES (?,?,?,?,?,?,?) """.format(
    ",".join(emissiosns_cols)
)
cursor.executemany(sql, uncntr_emisquant_nonpoint_emisssions.itertuples(index=False))
conn.commit()

cursor.execute("SELECT * FROM Emissions")
cursor.fetchall()


# conn.close()
