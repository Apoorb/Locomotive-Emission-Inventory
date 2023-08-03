

import geopandas as gpd
from pathlib import Path
import pandas as pd
import numpy as np

p_sam_shp = Path(
    r"E:\OneDrive - Texas A&M Transportation Institute\Documents - HMP - TCEQ Projects"
    r"\FY2022_Locomotive_EI_Improvements\Resources\TxDOT\SAM_Rail\RailNetwork_SAM.shp")
p_sam_flow = Path.joinpath(p_sam_shp.parent, "Freight_LinkFlows.csv")
sam_shp = gpd.read_file(p_sam_shp)
sam_flow = pd.read_csv(p_sam_flow)

sam_shp.columns
sam_flow.columns

assert set(sam_shp.ID.unique()) - set(sam_flow.ID1.unique()) == set(), (
    "All SAM shp ids in the flow file.")

sam_shp_flow = sam_shp.merge(sam_flow, left_on="ID", right_on="ID1")