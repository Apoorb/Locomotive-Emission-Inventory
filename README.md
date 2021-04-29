Loco-ERLT
==============================

Develop the locomotive ERLTs for line-haul, yards, industrial sites for Texas

Project Organization
------------

.
├── LICENSE
├── README.md <- The top-level README for developers using this project.
├── analysis
│   └── scratch <- Test issues with analysis.
├── data
│   ├── interim <- Intermediate data that has been transformed.
│   ├── processed <- The final datasets
│   └── raw <- The original, immutable data dump. 
├── docs
│   ├── Makefile
│   ├── commands.rst
│   ├── conf.py
│   ├── getting-started.rst
│   ├── index.rst
│   └── make.bat
├── locoerlt
│   ├── __init__.py
│   ├── cersxml_templ.py <- Get the xml template based on Cody's ERG template.
│   ├── emisquant.py <- 03. Get emission quantities.
│   ├── emisrt.py <- 02. Get emission rates.
│   ├── fuelcsmp.py <- 01. Process fuel data.
│   ├── uncntr_cntr_cersxml.py <- Get the uncontrolled and controlled xmls.
│   ├── uncntr_cntr_emisquant.py <- 04. Get uncontrolled and controlled emissions.
│   └── utilis.py <- Commonly used functions.
├── notebooks
├── references
│   ├── CERS_DET_v2.0.xlsx
│   ├── CERS_FCD_v2 0.doc
│   ├── CERS_Schema_v2.0
│   ├── CERS_Schema_v2.0.zip
│   ├── Rail_Query_mar_4_axb.sql
│   ├── TxLed Rule 114_318 Texas Administrative Code.pdf
│   ├── TxLed Rule 114_319 Texas Administrative Code.pdf
│   ├── ULSD_rule_2004_0511_USEPA.pdf
│   ├── deri_benefit_distribution.pptx
│   ├── eis_training.pdf
│   ├── emission_rates
│   ├── emissioninventorysystem_point inventory.pdf
│   ├── ertac_emission_inventory.pdf
│   ├── explaination_fuel_consumption_distribution.pptx
│   ├── standardize-code-tables-appendix-a.xlsx
│   ├── xml_data_eis.pdf
│   └── yard_ertac_tti_comparision_v1.jpg
├── reports
│   └── figures
├── requirements.txt
├── run_scripts_in_order.cmd
├── setup.py
├── test
│   ├── __init__.py
│   ├── __pycache__
│   ├── test_cersxml_templ.py
│   ├── test_emisquant.py
│   ├── test_emisrt.py
│   ├── test_environment.py
│   ├── test_fuelcsmp.py
│   ├── test_uncntr_cntr.py
│   └── test_uncntr_cntr_cersxml.py
├── tox.ini
├── tree.md
├── tree.txt
└── venv

23 directories, 46 files

--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
