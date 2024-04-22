#TODO : find a better approach to store the cast instead of text
CREATE_CASTS_TABLE_SQL = """
    CREATE TABLE movies_casts (
        title VARCHAR(255) PRIMARY KEY,
        type VARCHAR(255),
        director VARCHAR(255),
        cast TEXT, 
        country VARCHAR(255),
        date_added DATE,
        release_year INT,
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

CREATE_MOVIES_DB_SQL = """
    CREATE TABLE dashboard.movies AS
    (
    SELECT 
        datawarehouse.movies_casts.*,
        datawarehouse.movies_rating.show_rating,
        datawarehouse.movies_rating.release_date,
        datawarehouse.movies_rating.genres,
        datawarehouse.movies_rating.languages,
        datawarehouse.movies_rating.keywords,
        datawarehouse.movies_finance.revenue,
        datawarehouse.movies_finance.budget,
        datawarehouse.movies_finance.production_companies,
        datawarehouse.movies_finance.production_countries,
        (datawarehouse.movies_finance.revenue - datawarehouse.movies_finance.budget) AS profit,
        (((datawarehouse.movies_finance.revenue - datawarehouse.movies_finance.budget) / datawarehouse.movies_finance.budget) * 100) AS roi
    FROM
        datawarehouse.movies_casts
    LEFT JOIN 
        datawarehouse.movies_rating
    ON
        datawarehouse.movies_casts.title = datawarehouse.movies_rating.title
    LEFT JOIN 
        datawarehouse.movies_finance
    ON
        datawarehouse.movies_casts.title = datawarehouse.movies_finance.title
    WHERE
        datawarehouse.movies_finance.revenue > 0
    AND
        datawarehouse.movies_finance.budget > 0
    AND 
        datawarehouse.movies_rating.release_date > '1800-01-01'
    );
    """

CREATE_GENRES_DB_SQL = """
    CREATE TABLE dashboard.genres AS 
    WITH RECURSIVE cte AS (
        SELECT
            title,
            release_date,
            SUBSTRING_INDEX(genres, ',', 1) AS DataItem,
            SUBSTRING(genres, CHAR_LENGTH(SUBSTRING_INDEX(genres, ',', 1)) + 2) AS remaining_genre
        FROM
            datawarehouse.movies_rating
        WHERE
            genres > ''
        AND
            release_date > '1800-01-01'
        
        UNION ALL
        
        SELECT
            title,
            release_date,
            SUBSTRING_INDEX(remaining_genre, ',', 1) AS DataItem,
            SUBSTRING(remaining_genre, CHAR_LENGTH(SUBSTRING_INDEX(remaining_genre, ',', 1)) + 2) AS remaining_genre
        FROM
            cte
        WHERE
            remaining_genre > ''
        AND
            release_date > '1800-01-01'
    )
    SELECT
        title,
        YEAR(release_date) AS release_year,
        DataItem AS genre
    FROM
        cte
    ORDER BY
        title;
"""

CREATE_ACTORS_DB_SQL = """
    CREATE TABLE dashboard.actors AS
    WITH RECURSIVE cte AS (
        SELECT
            title,
            release_year,
            SUBSTRING_INDEX(cast, ',', 1) AS DataItem,
            SUBSTRING(cast, CHAR_LENGTH(SUBSTRING_INDEX(cast, ',', 1)) + 2) AS remaining_cast
        FROM
            datawarehouse.movies_casts
        WHERE
            cast > ''
        UNION ALL
        
        SELECT
            title,
            release_year,
            SUBSTRING_INDEX(remaining_cast, ',', 1) AS DataItem,
            SUBSTRING(remaining_cast, CHAR_LENGTH(SUBSTRING_INDEX(remaining_cast, ',', 1)) + 2) AS remaining_cast
        FROM
            cte
        WHERE
            remaining_cast > ''
    )
    SELECT
        title,
        release_year,
        DataItem
    FROM
        cte
    ORDER BY
        title;

"""

''' 
Data needed for ML: 
1. title -
2. show_rating -
3. total_votes -
4. popularity -
5. revenue -
6. budget -
7. profit -
8. ROI -
9. release_year (scaled) - 
10. director -
11. cast -
12. production_companies
13. production_countries
14. description -
15. genres -
16. keywords -
'''
CREATE_MOVIES_ML_SQL = """
    CREATE TABLE machinelearning.movies AS 
    (
    SELECT
        datawarehouse.movies_casts.director,
        datawarehouse.movies_casts.title,
        datawarehouse.movies_casts.cast,
        datawarehouse.movies_casts.release_year,
        datawarehouse.movies_rating.keywords,
        datawarehouse.movies_rating.show_rating,
        datawarehouse.movies_rating.total_votes,
        datawarehouse.movies_rating.popularity,
        datawarehouse.movies_rating.genres,
        datawarehouse.movies_rating.description,
        datawarehouse.movies_finance.production_companies,
        datawarehouse.movies_finance.production_countries,
        datawarehouse.movies_finance.revenue,
        datawarehouse.movies_finance.budget,
        (datawarehouse.movies_finance.revenue - datawarehouse.movies_finance.budget) AS profit,
        (((datawarehouse.movies_finance.revenue - datawarehouse.movies_finance.budget) / datawarehouse.movies_finance.budget) * 100) AS roi
    FROM
        datawarehouse.movies_casts
    LEFT JOIN 
        datawarehouse.movies_rating
    ON
        datawarehouse.movies_casts.title = datawarehouse.movies_rating.title
    LEFT JOIN 
        datawarehouse.movies_finance
    ON 
        datawarehouse.movies_casts.title = datawarehouse.movies_finance.title
    WHERE
        datawarehouse.movies_finance.revenue > 0
    AND
        datawarehouse.movies_finance.budget > 0
    AND 
        datawarehouse.movies_rating.release_date > '1800-01-01'
    );

    """
TEST_SQL = """
    SELECT *
    FROM
        movies_casts
    LIMIT 10;
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

CREATE_QUERIES_DW = [CREATE_CASTS_TABLE_SQL,CREATE_FINANCE_TABLE_SQL,CREATE_RATINGS_TABLE_SQL]
DROP_QUERIES_DW = [
    "DROP TABLE IF EXISTS movies_casts;",
    "DROP TABLE IF EXISTS movies_rating;",
    "DROP TABLE IF EXISTS movies_finance;"
]

CREATE_QUERIES_DB = [CREATE_MOVIES_DB_SQL, CREATE_GENRES_DB_SQL, CREATE_ACTORS_DB_SQL]
DROP_QUERIES_DB = [
    "DROP TABLE IF EXISTS movies;",
    "DROP TABLE IF EXISTS genres;",
    "DROP TABLE IF EXISTS actors;"
]

CREATE_QUERIES_ML = [CREATE_MOVIES_ML_SQL]
DROP_QUERIES_ML = [
    "DROP TABLE IF EXISTS movies;",
]



