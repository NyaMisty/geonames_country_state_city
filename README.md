# Country-State-City based on GeoNames

This repo builds a country-state-city database offers the following feature based on GeoNames:

- All cities, administrative areas, localities with more than 2000 population included
- Comprehensive alias for city & states
- Compatibility with dr5hn/countries-states-cities-database
- Convert WikiDataID to GeoNames
- Precompute GeoNameID mapping from CSV files

## Why this repo
GeoNames offers much more comprehensive data comparing with dr5hn/countries-states-cities-database (CSC database):
- More Names: alias names for various language and common names
- Better Name Matching: names got processed to get better matching rate:
    - ASCII-ize
    - Remove '-Shi' '-Chome' etc for Japan & Chinese names
    - Remove white spaces
- More Metadata: population, time zone, elevation, and more
- Accuracy: GeoName is widely used and can give precise information for each GeoNameID, while CSC database's often contains wrong or duplicate wikiDataID
- Compatibility: All location in CSC dataset is included (except some corner cases), by matching WikiDataID and name specified in CSC database.

## Usage

### Generate the database
(NOTE: You can directly use the precompiled database from GitHub Release if you don't want to generate it)

1. Prepare data:
    - Put dr5hn/countries-states-cities-database's cities.txt into `source/csc/cities.txt`
    - Put GeoName's allCountries dump into `source/geonames/allCountries.txt`
2. Run the generation:
    ```
        python3 src/main.py gen
    ```
3. Data goes to `output` folder

### Run the match for CSV files

Just run:
```
python3 src\main.py convert --input-csv [Input file] --output-csv [Output file] --country-col [ ISO3166-1 Country Code Column Name ] --state-col [ State Name Column Name ] --city-col [ City Name Column Name ]
```
For example, if one wants to convert CSC dataset:
```
python3 src\main.py convert --input-csv source_data\csc\cities.txt --output-csv source_data\csc\cities_converted.csv --country-col country_code --state-col state_name --city-col name
```

### Use the database in program

There's 4 tables, `states` `cities` `state_names` `city_names`
Recommended Workflow:
1. Match the state: Query `state_names` table with country code and state name, get the state geonameid
2. Match the city (1): Query `city_names` table with state geonameid and city name, get the city geonameid
3. Match the city (2): If last step failed, query `city_names` table with country code and city name instead, get the city geonameid
4. Use `states` / `cities` table to get information for corresponding GeoNamesID
