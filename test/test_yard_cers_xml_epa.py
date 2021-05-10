import os
import xml
import xml.etree.ElementTree as ET
import numpy as np
import glob
import pandas as pd
from locoerlt.utilis import PATH_RAW, PATH_INTERIM, PATH_PROCESSED, get_snake_case_dict
from locoerlt.uncntr_cntr_cersxml import (
    clean_up_cntr_emisquant,
    clean_up_uncntr_emisquant,
)

path_uncntr_emisquant = glob.glob(
    os.path.join(PATH_PROCESSED, "uncntr_emis_quant_[0-9]*-*-*.csv")
)[0]
path_cntr_emisquant = glob.glob(
    os.path.join(PATH_PROCESSED, "cntr_emis_quant_[0-9]*-*-*.csv")
)[0]
path_
