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


