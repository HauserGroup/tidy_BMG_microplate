# tidy_BMG_microplate
Convert XLSX files exported from BMG MARS Data Analysis Software to a tidy CSV file. Currently only tested for LUMIstar Omega reader but might work for PHERAstar, NOVOstar or OMNIstar as well. 

**Requires Python 3.9+**

## Instructions
Install with 

```
python3 -m venv .venv
source .venv/bin/acitvate

pip install .
``` 
and then run 
```
tidy_microplate example/bmg_lumistar_omega.xlsx
```
