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
There is a requirements.txt file that can be used for installing the packages used (pyspark & pyyaml). 
This has been run locally and from gitpod+gitlab, both with success.
I have utilized the following versions but should be compatible with many others:
- python 3.11.4
- pyspark 3.4.1
- pyyaml 6.0

Once setup, you can invoke the ETL process by running the following from /etl/orders/:
- spark-submit ./main.py > ../../log/orders-$(date +%Y-%m-%d-%H-%M-%S).log 2>&1

## Architecture ##
Python and Spark were chosen for their proficiency with data processing and efficiency with large amounts of data. 
I decided on YAML as it is easier for human readability compared to other configuration formats. 
I also decided to utilize SparkSQL as SQL is a powerful and well known language by all data professionals.

### 🗀 /data ###
- This is where the source data lives which would not be the obvious place for it in a production system
### 🗀 /etl/{datasource} ###
- This is where the code lives for each unique ETL process. Each new process would have its own directory, definition file and main.py
- ETL configuration within "definition.yaml" file
- Main Python script (main.py) for the specific data source, calling the main class in /lib
### 🗀 /lib ###
- Python class and functions for configuration parsing, spark data processing and data quality testing
### 🗀 /log ###
- Location for logs. An example log file is within the repo. For a production system, this would most likely not live here or would have a .gitignore to keep them out of VC

## Documentation ##
- You are here!
- Function docstrings and code comments

## Assumptions, Simplifications & Next Steps ##
- Code was kept specific to this use case and would need to be further modified and considered for a full platform
- More thought out approach to handle more cases
- More integration work for data sources, data stores, etc
- Functions and functionality had some shortcuts based on time and requirements. In short, they need more love
- Would revisit the testing portion with probably a more robust solution like Great Expectations
- Explore other opportunities with the configuration: automated documentation, DDL generation, etc
- Cleanup and tweak the code (better error handling, efficiency improvements, unit tests, more classes, standards, etc)