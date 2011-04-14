"""
Import of PlateWells
,,,,,,,,,,,,,,,,,,,,


Will read in a csv file with the following columns::

  plate_label plate_barcode row column dnalabel dnabarcode volume
  p01         2390920       10  2      d01      3289892389 0.1

Default plate dimensions are provided with a flag

  > import -v PlateWell -i file.csv --plate-shape=30x32

Each import should have flags with the device description....
"""
