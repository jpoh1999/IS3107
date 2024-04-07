#TODO : find a better approach to store the cast instead of text
CREATE_CASTS_TABLE_SQL = """
    CREATE TABLE movies_casts (
        title VARCHAR(255) PRIMARY KEY,
        director VARCHAR(255),
        cast TEXT, 
        country VARCHAR(255),
        date_added DATE,
        release_year INT
    )
    """

CREATE_RATINGS_TABLE_SQL = """
    CREATE TABLE movies_rating (
        title VARCHAR(255) PRIMARY KEY,
        show_rating FLOAT,
        total_votes INT,
        release_date DATE,
        adult BOOLEAN,
        description TEXT,
        popularity FLOAT,
        genres VARCHAR(255),
        languages TEXT,
        keywords TEXT
    );
    """

CREATE_FINANCE_TABLE_SQL = """
    CREATE TABLE movies_finance (
        title VARCHAR(255) PRIMARY KEY,  -- Ensure titles are unique
        revenue DECIMAL(39, 2),  -- Adjust precision and scale as needed
        budget DECIMAL(39, 2),   -- Adjust precision and scale as needed
        production_companies TEXT,
        production_countries TEXT
    );
    """

INSERT_CASTS_SQL = """
    INSERT INTO movies_cast (
        title, director, cast, country, date_added, release_year
    ) VALUES (
        %s, %s, %s, %s, %s, %s
    );
    """
INSERT_RATINGS_SQL = """
    INSERT INTO movies_rating (
        original_title, vote_average, vote_count, release_date, adult, overview, popularity, genres, spoken_languages, keywords
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    );
    """
INSERT_FINANCE_SQL = """
    INSERT INTO movies_finance (
        original_title, revenue, budget, production_companies, production_countries
    ) VALUES (
        %s, %s, %s, %s, %s
    );
"""

CREATE_QUERIES = [CREATE_CASTS_TABLE_SQL,CREATE_FINANCE_TABLE_SQL,CREATE_RATINGS_TABLE_SQL]
DROP_QUERIES = [
    "DROP TABLE IF EXISTS movies_casts;",
    "DROP TABLE IF EXISTS movies_rating;",
    "DROP TABLE IF EXISTS movies_finance;"
]