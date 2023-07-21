# ETL Interview Project #

### General ###
This is a configuration driven ETL project written in Python, PySpark and YAML.

### Requirements ###
- The transformations should be configurable with an external DSL (like a configuration
file)
- The functionality should be implemented as a library, without (significant) external
dependencies
- Invalid rows should be collected, with errors describing why they are invalid (logging
them is fine for now)
- The data tables can have a very large number of rows

### Out of scope ###
- Other file formats than CSV
- No need to build a CSV parser unless you really want to - feel free to use a pre-built
CSV parser for the basic splitting and escaping

### Deliverables ###
- Running code and test suite provided through online code repo or in a tar-ball
- Instructions on how to build and run the code with example data
- Short architectural overview and technology choices made
- (Basic) documentation, unless it’s completely self-documenting (to a fellow software
developer)
- List of assumptions or simplifications made
- List of the next steps you would want to do if this were a real project

## Build/Setup ##
There is a requirements.txt file that can be used for installing the packages used (pyspark, yaml and sys). 
I have utilized the following versions but should be compatible with many others:
- python 3.11.4
- pyspark 3.4.1
- pyyaml 6.0

Once setup, you can invoke the ETL process by running:
- spark-submit ./main.py > ../../log/orders-$(date +%Y-%m-%d-%H-%M-%S).log 2>&1

## Architecture ##
### 🗀 /data ###
- This is where the source data lives which would not be the obvious place for it in a production system
### 🗀 /etl ###
- ETL configuration within "definition.yaml" file
- Main Python script (main.py) for the specific data source
### 🗀 /lib ###
- Python class and functions
### 🗀 /log ###
- Location for logs

## Documentation ##
- You are here!
- Function docstrings and code comments

## Assumptions, Simplifications & Next Steps ##
- Code was kept specific to this use case and would need to be further modified and considered for a full platform
- More thought out approach to handle more cases
- More integration work for data sources, data stores, etc
- Functions and functionality had some shortcuts based on time and requirements, in short, they need more love
- Would revisit the testing portion with probably a more robust solution like Great Expectations
- Explore other opportunities with the configuration: automated documentation, DDL generation, etc
- Cleanup and tweak the code (better error handling, efficiency improvements, standards, etc)