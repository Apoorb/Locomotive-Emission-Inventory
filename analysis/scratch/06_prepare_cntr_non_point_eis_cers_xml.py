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
path_txled = os.path.join(
    PATH_PROCESSED, "txled_factors_by_county_prc.csv"
)
path_nonpoint_brgtool = os.path.join(
    PATH_PROCESSED, "eis_stagging_tables", "loco_nonpoint_cntr.accdb")
conn = pyodbc.connect(r'''Driver={0};DBQ={1}'''.format(
    "{Microsoft Access Driver (*.mdb, *.accdb)}", path_nonpoint_brgtool))
cursor = conn.cursor()
for row in cursor.tables():
    print(row.table_name)
emissiosns = pd.read_sql("""SELECT *  FROM Emissions""", conn)
cntr_measure = pd.read_sql("""SELECT *  FROM ControlMeasure""", conn)
cntr_pol = pd.read_sql("""SELECT *  FROM ControlPollutant""", conn)

cntr_emisquant = pd.read_csv(path_cntr_emisquant)
cntr_emisquant_nonpoint_2020 = (
    cntr_emisquant
    .loc[lambda df: (df.scc_description_level_4 != "Yard Locomotives")
        & (df.year == 2020)
    ]
    .filter(items=["stcntyfips", "county_name", "scc",
                   "pollutant", "controlled_em_quant_ton"])
    .rename(columns={
        "stcntyfips": "StateAndCountyFIPSCode",
        "scc": "SourceClassificationCode"
    })
)


cntr_measure_cols = [
    "StateAndCountyFIPSCode",
    "SourceClassificationCode",
    "ControlMeasureCode",
]

cntr_pol_cols = [
    "StateAndCountyFIPSCode",
    "SourceClassificationCode",
    "PollutantCode",
    "PercentControlMeasureReductionEfficiency"
]

temp_fips_scc = cntr_emisquant_nonpoint_2020[
    ["StateAndCountyFIPSCode", "SourceClassificationCode"]].drop_duplicates()
txled_df = pd.read_csv(path_txled)
txled_df_fil = (
    txled_df
    .loc[lambda df: df.txled_fac != 1]
    .assign(
        ControlMeasureCode=30,
        PercentControlMeasureReductionEfficiency = lambda df: (
                1 - df.txled_fac) * 100
    )
    .rename(columns={"FIPS_ST_CNTY_CD": "StateAndCountyFIPSCode",
                     "pollutant": "PollutantCode"})
    .filter(items=["StateAndCountyFIPSCode", "PollutantCode",
                   "ControlMeasureCode",
                   "PercentControlMeasureReductionEfficiency"])
    .merge(temp_fips_scc, on="StateAndCountyFIPSCode")
)

txled_df_fil_cntr_measure = (
    txled_df_fil
    .filter(items=cntr_measure_cols)
)

txled_df_fil_cntr_pol = (
    txled_df_fil
    .filter(items=cntr_pol_cols)
)


cursor.execute("DELETE * FROM ControlMeasure")
conn.commit()
cursor.execute("DELETE * FROM ControlPollutant")
conn.commit()

sql = ''' INSERT INTO ControlMeasure ({0}) 
          VALUES (?,?,?) '''.format(
    ",".join(cntr_measure_cols))
cursor.executemany(sql, txled_df_fil_cntr_measure.itertuples(index=False))
conn.commit()

sql = ''' INSERT INTO ControlPollutant ({0}) 
          VALUES (?,?,?,?) '''.format(
    ",".join(cntr_pol_cols))
cursor.executemany(sql, txled_df_fil_cntr_pol.itertuples(index=False))
conn.commit()


emissiosns_cols = [
    "StateAndCountyFIPSCode",
    "SourceClassificationCode",
    "ReportingPeriodTypeCode",
    "PollutantCode",
    "TotalEmissions",
    "EmissionsUnitofMeasureCode",
    "EmissionCalculationMethodCode",
]

cntr_emisquant_nonpoint_emisssions_a = (
    cntr_emisquant_nonpoint_2020
    .assign(
        ReportingPeriodTypeCode="A",
        PollutantCode=lambda df: df.pollutant,
        TotalEmissions=lambda df: df.controlled_em_quant_ton,
        EmissionsUnitofMeasureCode="TON",
        EmissionCalculationMethodCode="8"
    )
    .filter(items=emissiosns_cols)
)

cntr_emisquant_nonpoint_emisssions_o3d = (
    cntr_emisquant_nonpoint_2020
    .loc[lambda df: df.pollutant.isin(["CO", "NH3", "NOX", "PM10-PRI",
                                       "PM25-PRI", "SO2", "VOC"])]
    .assign(
        ReportingPeriodTypeCode="O3D",
        PollutantCode=lambda df: df.pollutant,
        TotalEmissions=lambda df: df.controlled_em_quant_ton / 365,
        EmissionsUnitofMeasureCode="TON",
        EmissionCalculationMethodCode="8"
    )
    .filter(items=emissiosns_cols)
)

uncntr_emisquant_nonpoint_emisssions = (
    pd.concat(
        [cntr_emisquant_nonpoint_emisssions_a,
         cntr_emisquant_nonpoint_emisssions_o3d
         ]
    )
    .reset_index(drop=True)
)


cursor.execute("DELETE * FROM Emissions")
conn.commit()
sql = ''' INSERT INTO Emissions ({0}) 
          VALUES (?,?,?,?,?,?,?) '''.format(
    ",".join(emissiosns_cols))
cursor.executemany(sql, uncntr_emisquant_nonpoint_emisssions.itertuples(index=False))
conn.commit()

cursor.execute("SELECT * FROM Emissions")
cursor.fetchall()


# conn.close()