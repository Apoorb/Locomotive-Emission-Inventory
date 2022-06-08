"""
Output Line-Haul Emission Distribution
Created by: Apoorb
Created on: 06/03/2022
"""
from pathlib import Path
import pandas as pd
import geopandas as gpd
import numpy as np
import psycopg2
from sqlalchemy import create_engine

from locoei.utilis import PATH_RAW, PATH_INTERIM, PATH_PROCESSED


path_fuel_consump = Path.joinpath(
    Path(PATH_INTERIM), r"fuelconsump_2019_tx_2021-10-09.csv"
)
path_cntr_emis = Path.joinpath(Path(PATH_PROCESSED), r"cntr_emis_quant_2021-12-20.csv")
path_natrail2020 = Path.joinpath(
    Path(PATH_RAW), "North_American_Rail_Lines", "North_American_Rail_Lines.shp"
)

# Read NARL, fuel consumption and controlled emissions data.
########################################################################################
fuel_cnsp = pd.read_csv(path_fuel_consump)
cntr_emis = pd.read_csv(path_cntr_emis, index_col=0)
cntr_emis_19_20 = cntr_emis.loc[cntr_emis.year.isin([2019, 2020])]
narl20_gpd = gpd.read_file(path_natrail2020)

# Class I emission by line.
########################################################################################
# Fcat filters to line-haul
fuel_cnsp_cls1 = fuel_cnsp.loc[
    lambda df: (df.rr_group == "Class I") & (df.friylab == "Fcat")
].filter(items=["stcntyfips", "fraarcid", "milemx"])
# Within county milemx distributes the fuel/ emissions to different line segments.
assert np.allclose(fuel_cnsp_cls1.groupby(["stcntyfips"]).milemx.sum(), 1)
cntr_emis_19_20_linehaul_cls1 = cntr_emis_19_20.loc[
    lambda df: df.scc_description_level_4 == "Line Haul Locomotives: Class I Operations"
].filter(
    items=[
        "year",
        "stcntyfips",
        "county_name",
        "scc",
        "scc_description_level_4",
        "pol_type",
        "pollutant",
        "pol_desc",
        "em_fac",
        "controlled_em_quant_ton",
    ]
)
# Explode cntr_emis_19_20_linehaul_cls1 data by county into data by lines.
cntr_cls1_lh = fuel_cnsp_cls1.merge(cntr_emis_19_20_linehaul_cls1, on="stcntyfips")
cntr_cls1_lh["emis_linehaul"] = (
    cntr_cls1_lh.controlled_em_quant_ton * cntr_cls1_lh.milemx
)
# Merge the above emissions data by line with NARL shapefile.
narl20_cls1_gpd = narl20_gpd.filter(
    items=[
        "FRAARCID",
        "FRFRANODE",
        "TOFRANODE",
        "STFIPS",
        "CNTYFIPS",
        "STCNTYFIPS",
        "STATEAB",
        "COUNTRY",
        "FRADISTRCT",
        "RROWNER1",
        "RROWNER2",
        "RROWNER3",
        "TRKRGHTS1",
        "TRKRGHTS2",
        "TRKRGHTS3",
        "TRKRGHTS4",
        "TRKRGHTS5",
        "TRKRGHTS6",
        "TRKRGHTS7",
        "TRKRGHTS8",
        "TRKRGHTS9",
        "SUBDIV",
        "YARDNAME",
        "PASSNGR",
        "STRACNET",
        "TRACKS",
        "CARDDIRECT",
        "NET",
        "MILES",
        "KM",
        "TIMEZONE",
        "IM_RT_TYPE",
        "DBLSTK",
        "geometry",
    ]
).loc[lambda df: df.FRAARCID.isin(cntr_cls1_lh.fraarcid)]

assert (
    set(cntr_cls1_lh.fraarcid).symmetric_difference(set(narl20_cls1_gpd.FRAARCID))
    == set()
)
assert len(cntr_cls1_lh.fraarcid.unique()) == len(narl20_cls1_gpd)
narl20_cls1_gpd = narl20_cls1_gpd.merge(
    cntr_cls1_lh, left_on=["FRAARCID"], right_on=["fraarcid"]
)


# Class III emission by line.
########################################################################################
# Emission data is by class 3 total while mile mix (statewide) is by carrier. Distribute
# class III emissions by carrier and then use mile mix to distribute emissions over the
# lines.
fuel_cnsp_cls3_p_c = fuel_cnsp.loc[
    lambda df: (df.rr_group.isin(["Class III", "Passenger", "Commuter"]))
    & (df.friylab == "Fcat")
].filter(items=["rr_group", "carrier", "fraarcid", "milemx", "link_fuel_consmp"])
# Mile Mix is at statewide level by carrier:
assert np.allclose(fuel_cnsp_cls3_p_c.groupby(["rr_group", "carrier"]).milemx.sum(), 1)
# Get the proportion of fuel across different carriers. Will distribute statewide
# class III emissions based on this.
carrier_prop = (
    fuel_cnsp_cls3_p_c.groupby(["rr_group", "carrier"])
    .agg(carrier_st_fuel_consmp=("link_fuel_consmp", "sum"))
    .reset_index()
)
carrier_prop["scc_st_fuel_consmp"] = carrier_prop.groupby(
    "rr_group"
).carrier_st_fuel_consmp.transform(sum)
carrier_prop["carrier_dist"] = (
    carrier_prop.carrier_st_fuel_consmp / carrier_prop.scc_st_fuel_consmp
)
carrier_prop["scc_description_level_4"] = carrier_prop.rr_group.map(
    {
        "Class III": "Line Haul Locomotives: Class II / III Operations",
        "Passenger": "Line Haul Locomotives: Passenger Trains (Amtrak)",
        "Commuter": "Line Haul Locomotives: Commuter Lines",
    }
)
carrier_prop = carrier_prop[["scc_description_level_4", "carrier", "carrier_dist"]]
# Get statewide emissions:
cntr_emis_19_20_linehaul_cls3_p_c = (
    cntr_emis_19_20.loc[
        lambda df: df.scc_description_level_4.isin(
            [
                "Line Haul Locomotives: Class II / III Operations",
                "Line Haul Locomotives: Passenger Trains (Amtrak)",
                "Line Haul Locomotives: Commuter Lines",
            ]
        )
    ]
    .groupby(
        ["year", "scc", "scc_description_level_4", "pol_type", "pollutant", "pol_desc"]
    )
    .controlled_em_quant_ton.sum()
    .reset_index()
)
# Get statewide emissions by carrier:
cntr_emis_19_20_linehaul_cls3_p_c_ = cntr_emis_19_20_linehaul_cls3_p_c.merge(
    carrier_prop, on="scc_description_level_4"
)
cntr_emis_19_20_linehaul_cls3_p_c_["emis_ton_by_carrier"] = (
    cntr_emis_19_20_linehaul_cls3_p_c_.controlled_em_quant_ton
    * cntr_emis_19_20_linehaul_cls3_p_c_.carrier_dist
)
cntr_emis_19_20_linehaul_cls3_p_c_ = cntr_emis_19_20_linehaul_cls3_p_c_.drop(
    columns="controlled_em_quant_ton"
)
cntr_cls3_pc_c_lh = cntr_emis_19_20_linehaul_cls3_p_c_.merge(
    fuel_cnsp_cls3_p_c, on=["carrier"]
)
cntr_cls3_pc_c_lh["emis_linehaul"] = (
    cntr_cls3_pc_c_lh.emis_ton_by_carrier * cntr_cls3_pc_c_lh.milemx
)
cntr_cls3_pc_c_lh.columns
cntr_cls3_pc_c_lh_ = (
    cntr_cls3_pc_c_lh.groupby(
        [
            "year",
            "rr_group",
            "fraarcid",
            "scc",
            "scc_description_level_4",
            "pol_type",
            "pollutant",
            "pol_desc",
        ]
    )
    .emis_linehaul.sum()
    .reset_index()
)


narl20_cls3_p_c_gpd = narl20_gpd.filter(
    items=[
        "FRAARCID",
        "FRFRANODE",
        "TOFRANODE",
        "STFIPS",
        "CNTYFIPS",
        "STCNTYFIPS",
        "STATEAB",
        "COUNTRY",
        "FRADISTRCT",
        "RROWNER1",
        "RROWNER2",
        "RROWNER3",
        "TRKRGHTS1",
        "TRKRGHTS2",
        "TRKRGHTS3",
        "TRKRGHTS4",
        "TRKRGHTS5",
        "TRKRGHTS6",
        "TRKRGHTS7",
        "TRKRGHTS8",
        "TRKRGHTS9",
        "SUBDIV",
        "YARDNAME",
        "PASSNGR",
        "STRACNET",
        "TRACKS",
        "CARDDIRECT",
        "NET",
        "MILES",
        "KM",
        "TIMEZONE",
        "IM_RT_TYPE",
        "DBLSTK",
        "geometry",
    ]
).loc[lambda df: df.FRAARCID.isin(cntr_cls3_pc_c_lh_.fraarcid)]

assert (
    set(cntr_cls3_pc_c_lh.fraarcid).symmetric_difference(
        set(narl20_cls3_p_c_gpd.FRAARCID)
    )
    == set()
)
assert len(cntr_cls3_pc_c_lh.fraarcid.unique()) == len(narl20_cls3_p_c_gpd)

narl20_cls3_p_c_gpd = narl20_cls3_p_c_gpd.merge(
    cntr_cls3_pc_c_lh_, left_on=["FRAARCID"], right_on=["fraarcid"]
)

set(narl20_cls1_gpd.columns).symmetric_difference(set(narl20_cls3_p_c_gpd.columns))

narl20_cls1_3_p_c_gpd = pd.concat([narl20_cls1_gpd, narl20_cls3_p_c_gpd])
narl20_cls1_3_p_c_gpd_v1 = (
    narl20_cls1_3_p_c_gpd.filter(
        items=[
            "FRAARCID",
            "FRFRANODE",
            "TOFRANODE",
            "STFIPS",
            "CNTYFIPS",
            "STCNTYFIPS",
            "STATEAB",
            "COUNTRY",
            "FRADISTRCT",
            "RROWNER1",
            "RROWNER2",
            "RROWNER3",
            "TRKRGHTS1",
            "TRKRGHTS2",
            "TRKRGHTS3",
            "TRKRGHTS4",
            "TRKRGHTS5",
            "TRKRGHTS6",
            "TRKRGHTS7",
            "TRKRGHTS8",
            "TRKRGHTS9",
            "SUBDIV",
            "PASSNGR",
            "STRACNET",
            "TRACKS",
            "CARDDIRECT",
            "NET",
            "MILES",
            "KM",
            "TIMEZONE",
            "IM_RT_TYPE",
            "DBLSTK",
            "milemx",
            "year",
            "county_name",
            "scc",
            "scc_description_level_4",
            "pol_type",
            "pollutant",
            "pol_desc",
            "em_fac",
            "emis_linehaul",
            "geometry",
        ]
    )
    .rename(
        columns={
            "FRAARCID": "FRA Arc ID",
            "FRFRANODE": "From FRA Node",
            "TOFRANODE": "To FRA Node",
            "STFIPS": "State FIPS",
            "CNTYFIPS": "County FIPS",
            "STCNTYFIPS": "State-County FIPS",
            "STATEAB": "State Abb",
            "COUNTRY": "Country",
            "FRADISTRCT": "FRA District",
            "RROWNER1": "Railroad Owner 1",
            "RROWNER2": "Railroad Owner 2",
            "RROWNER3": "Railroad Owner 3",
            "TRKRGHTS1": "Trackage Right 1",
            "TRKRGHTS2": "Trackage Right 2",
            "TRKRGHTS3": "Trackage Right 3",
            "TRKRGHTS4": "Trackage Right 4",
            "TRKRGHTS5": "Trackage Right 5",
            "TRKRGHTS6": "Trackage Right 6",
            "TRKRGHTS7": "Trackage Right 7",
            "TRKRGHTS8": "Trackage Right 8",
            "TRKRGHTS9": "Trackage Right 9",
            "SUBDIV": "Subdivision",
            "PASSNGR": "Passenger Service Type",
            "STRACNET": "STRACNET 2018",
            "TRACKS": "Number Main Line Tracks",
            "CARDDIRECT": "Cardinal Dir",
            "NET": "Net",
            "MILES": "Miles",
            "KM": "Km",
            "TIMEZONE": "Time Zone",
            "IM_RT_TYPE": "Intermodal Route",
            "year": "Year",
            "county_name": "County",
            "scc": "SCC",
            "scc_description_level_4": "SCC Description Level 4",
            "pol_type": "Pol Type",
            "pollutant": "Pollutant",
            "pol_desc": "Pol Desc",
            "em_fac": "Emission Factor (grams/gallon)",
            "emis_linehaul": "Emission (U.S. Tons)",
            "geometry": "geometry",
        }
    )
    .loc[
        lambda df: df.Pollutant.isin(
            ["7439921", "CO", "NH3", "NOX", "PM10-PRI", "PM25-PRI", "SO2", "VOC", "CO2"]
        )
    ]
)
narl20_cls1_3_p_c_gpd_v1["State-County FIPS"] = narl20_cls1_3_p_c_gpd_v1[
    "State-County FIPS"
].astype(int)
narl20_cls1_3_p_c_gpd_v1["Emission (U.S. Tons/Mile)"] = (
    narl20_cls1_3_p_c_gpd_v1["Emission (U.S. Tons)"] / narl20_cls1_3_p_c_gpd_v1["Miles"]
)


test = (
    narl20_cls1_3_p_c_gpd_v1.groupby(["SCC Description Level 4", "Pollutant", "Year"])[
        "Emission (U.S. Tons)"
    ]
    .sum()
    .reset_index()
)

conn = psycopg2.connect(f"dbname=postgres user=postgres " f"password=civil123")
conn.set_session(autocommit=True)
cur = conn.cursor()
cur.execute("DROP DATABASE locoei_lh_emis;")
cur.execute("COMMIT")
cur.execute("CREATE DATABASE locoei_lh_emis;")
conn.close()

conn = psycopg2.connect(f"dbname=locoei_lh_emis user=postgres " f"password=civil123")
conn.set_session(autocommit=True)
cur = conn.cursor()
cur.execute("CREATE EXTENSION postgis;")
conn.close()


engine = create_engine("postgresql://postgres:civil123@localhost:5432/locoei_lh_emis")
narl20_cls1_3_p_c_gpd_v1.to_postgis("cls1_cls3_p_c_line_emis", con=engine)
#######################################################################################

#
# co2_emis_fac_grams_per_gal = 10084.14
# us_ton_to_grams=907185
# co2_emis_fac_us_tons_per_gal = co2_emis_fac_grams_per_gal / us_ton_to_grams
# fuel_cnsp_back = narl20_cls1_3_p_c_gpd_v1.loc[
#     lambda df: df.Pollutant == "CO2",
#     ['FRA Arc ID', "Year", "SCC", 'Emission (Tons/Mile)']]
# fuel_cnsp_back["Fuel Consumption (Gallon/Mile)"] = fuel_cnsp_back[
# 'Emission (U.S. Ton/Mile)'] / co2_emis_fac_us_tons_per_gal

