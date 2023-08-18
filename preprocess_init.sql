-- create full data

CREATE TABLE FULL_DATA (
    lan VARCHAR(50),
    book_id INT,
    ip VARCHAR(50),
    clicktime DATETIME,
    comefrom VARCHAR(255),
    lat FLOAT,
    lon FLOAT,
    city VARCHAR(100),
    country VARCHAR(100),
    continent VARCHAR(100),
    topic VARCHAR(255),
    media_type VARCHAR(255),
    bibliography_language VARCHAR(255),
    bfulltext TINYINT,
    visit_counts INT,
    visit_span FLOAT,
    last_visit TINYINT
);

-- disable safe mode
SHOW VARIABLES LIKE 'sql_safe_updates';
SET sql_safe_updates = 0;

-- proceed with the DELETE statement (aka col names)
DELETE FROM FULL_DATA WHERE book_id = 0;


CREATE TABLE GEO_PATTERN (
	lon FLOAT, 
    lat FLOAT, 
    lan TINYTEXT, 
    continent TINYTEXT, 
    country TINYTEXT, 
    city TINYTEXT, 
    total_user int,
    total_click int, 
    total_book int
);




-- create weekly data
CREATE TABLE WEEKLY_TREND (
	week_id TINYTEXT,
    continent TINYTEXT,
    country TINYTEXT,
    region_weekly_visits INT,
    unique_ips_weekly INT,
    language_weekly_visits TINYTEXT,
    weekly_most_common_points TEXT,
    top_10_books_weekly TEXT,
    visit_type1_counts INT,
    visit_type2_counts INT,
    visit_type3_counts INT,
    visit_type4_counts INT
);




delete from WEEKLY_TREND where week_id = 'week';
