import sys

sys.path.append(sys.path[0] + "/../..")
from lib.sparky import Sparky


def main():
    sparky = Sparky()

    # Parse and return schema for Spark
    source_schema = sparky.create_source_schema('definition.yaml')
    # Read in csv file to Spark schema
    source_df = sparky.read_csv("../../data/orders.csv", source_schema)

    # Run data quality checks on data remove invalid data
    valid_df, invalid_df = sparky.check_data_quality(source_df, 'definition.yaml')

    # Create query to populate target DataFrame
    target_query = sparky.create_target_query('definition.yaml')
    # Run query to populate target DataFrame
    target_df = sparky.run_query(target_query, valid_df)

    # Show correctly processed data in DataFrame that is ready to be used
    print("Target DataFrame:")
    target_df.show()
    # Show invalid data in DataFrame
    print("Invalid DataFrame:")
    invalid_df.show()

    # Now do something with the DataFrames


if __name__ == '__main__':
    main()
