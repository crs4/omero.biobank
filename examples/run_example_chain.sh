#!/bin/bash

python create_snp_tables.py
python load_markers.py
python load_genotypes.py
python basic_computations.py
