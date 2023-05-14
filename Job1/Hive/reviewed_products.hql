CREATE TABLE IF NOT EXISTS reviews (
    id int,
    productId string,
    userId string,
    profileName string,
    helpfulnessNumerator string,
    helpfulnessDenominator string,
    score int,
    time bigint,
    summary string,
    text string
    ) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
tblproperties ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH "/Users/davidemolitierno/Repositories/BigData1/dataset/${hiveconf:regexDB}.csv" OVERWRITE INTO TABLE reviews;
--LOAD DATA INPATH "/user/${hiveconf:username}/input/${hiveconf:regexDB}.csv" OVERWRITE INTO TABLE reviews;

CREATE TABLE top_counted_reviews AS (
    SELECT cr.n_review, cr.reviews_year, cr.productId
    FROM (
        SELECT count(*) as n_review, year(from_unixtime(time)) as reviews_year, productId,
            row_number() over (
                partition by year(from_unixtime(time)) order by count(*) desc
                ) as row_num
        FROM reviews
        GROUP BY year(from_unixtime(time)), productId
    ) as cr
    WHERE cr.row_num <= 10
);

CREATE TABLE top_reviews_for_year_with_words AS (
    SELECT productID, reviews_year, word, word_count
    FROM (
        SELECT productID, word, count(*) as word_count, year(from_unixtime(time)) as reviews_year,
            row_number() OVER (PARTITION BY productID, year(from_unixtime(time)) ORDER BY count(*) DESC) as rank
        FROM reviews
        LATERAL VIEW explode(split(text, ' ')) wordTable AS word
        GROUP BY productID, year(from_unixtime(time)), word
    ) words
    WHERE rank <= 5
);

CREATE TABLE reviewed_products AS (
   SELECT *
   FROM top_reviews_for_year_with_words JOIN top_counted_reviews
   WHERE top_reviews_for_year_with_words.productId = top_counted_reviews.productId AND top_reviews_for_year_with_words.reviews_year = top_counted_reviews.reviews_year
);