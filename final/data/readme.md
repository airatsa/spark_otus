# Occupancy Detection Data Set

Abstract: Experimental data used for binary classification (room occupancy) from Temperature,Humidity,Light and CO2. Ground-truth occupancy was obtained from time stamped pictures that were taken every minute.

https://archive.ics.uci.edu/ml/datasets/Occupancy+Detection+

## Data Set Information

Three data sets are submitted, for training and testing. Ground-truth occupancy was obtained from time stamped pictures that were taken every minute.
For the journal publication, the processing R scripts can be found in:

## Attribute Information

- date time year-month-day hour:minute:second
- Temperature, in Celsius
- Relative Humidity, %
- Light, in Lux
- CO2, in ppm
- Humidity Ratio, Derived quantity from temperature and relative humidity, in kgwater-vapor/kg-air
- Occupancy, 0 or 1, 0 for not occupied, 1 for occupied status

## Files
 - *.txt: original files
 - *.csv: converted to CSV counterparts without index column
 - dataset.csv: all three files concatenated
