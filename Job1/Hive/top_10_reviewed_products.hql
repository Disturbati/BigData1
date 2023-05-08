CREATE TABLE IF NOT EXISTS reviews (
    id int,
    productId string,
    userId string,
    profileName string,
    helpfulnessNumerator string,
    helpfulnessDenominator string,
    score int,
    time string,
    summary string,
    text string
    ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' tblproperties ("skip.header.line.count"="1");

LOAD DATA INPATH "/Users/davidegattini/SourceTreeProj/BigData1/dataset/${hiveconf:regexDB}.csv" OVERWRITE INTO TABLE reviews;
-- LOAD DATA LOCAL INPATH "/user/${hiveconf:username}/input/${hiveconf:regexDB}" OVERWRITE INTO TABLE reviews;

CREATE TABLE sorted_reviews_for_products AS(
    SELECT * FROM (
        SELECT count(*) as n_review, year(time) as reviews_year, productId, text
        FROM reviews
        GROUP BY year(time), productId
    )
    ORDER BY n_review DESC
    GROUP BY reviews_year
    LIMIT 10
)

CREATE TABLE final_table AS (
    (SELECT count(*) as n_word, n_review, reviews_year, productId, word
    FROM
        (SELECT (split(text, ' ')) as word, n_review, reviews_year, productId
        FROM sorted_reviews_for_products
        WHERE length(word) > 3
        )
    ORDER BY n_word DESC
    GROUP BY word, productId
    LIMIT 5
    )
) 
