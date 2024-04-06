CREATE_SCHEMA = """
        CREATE DATABASE %s
    """

LOAD_CSV_QUERY = """
    LOAD DATA LOCAL INFILE %s
    INTO TABLE 
        %s
    FIELDS TERMINATED BY ','  -- Adjust based on your file format
    LINES TERMINATED BY '\n'  -- Adjust line terminator if needed
    IGNORE 1 LINES;
"""

