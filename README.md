# Dependencies
Pymongo, pandas and openpyxl libraries need to be installed.
# About params.json file  and config directory
Database and quota related adjustments can be made here.

If you want to add a new parameter, it should be added to the envparams under the config folder in accordance with the template in the envparams.

The file with .numbers extension in the repo was added as a step before converting the data table to excel since I use a macOS operating system.

# About db directory
Regarding the database, if it is desired to add a new collection or database, or to change the default names, the relevant changes in the mongo.py file under the db folder should be made according to the template.

When a new collection is added, appropriate changes to the template must be made in the params.json and envparams file under config.

# Run process
The relevant operations will be performed by running the case.py file directly.

It will read the data from case.xlsx and write it to the countries collection under the DB named as 'case' in local Mongo, after performing the related calculations, results will save them in the 'results' collection under the same DB.