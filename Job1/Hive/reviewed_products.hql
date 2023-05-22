CREATE TABLE IF NOT EXISTS reviews (
    id int,
    productId string,
    userId string,
    profileName string,
    helpfulnessNumerator string,
    helpfulnessDenominator string,
    score int,
    timeunix bigint,
    summary string,
    text string
    ) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES('separatorChar'=',', 'quoteChar'='\"') 
-- LOCATION '/user/${hiveconf:username}/input/${hiveconf:regexDB}'
tblproperties('skip.header.line.count'='1');

-- LOAD DATA LOCAL INPATH "/Users/davidegattini/SourceTreeProj/BigData1/dataset/${hiveconf:regexDB}.csv" OVERWRITE INTO TABLE reviews;
-- LOAD DATA INPATH "hdfs:/user/${hiveconf:username}/input/${hiveconf:regexDB}" OVERWRITE INTO TABLE reviews;
-- Per S3
-- LOAD DATA INPATH "hdfs:/${hiveconf:ip}.ec2.internal:8020/input/${hiveconf:dbName}" OVERWRITE INTO TABLE reviews;
LOAD DATA INPATH "/input/${hiveconf:dbName}" OVERWRITE INTO TABLE reviews;

CREATE TABLE top_counted_reviews AS (
    SELECT cr.n_review, cr.reviews_year, cr.productId
    FROM (
        SELECT count(*) as n_review, year(from_unixtime(CAST(timeunix AS int))) as reviews_year, productId,
            row_number() OVER (
                PARTITION BY year(from_unixtime(CAST(timeunix AS int))) ORDER BY count(*) DESC
                ) as row_num
        FROM reviews
        GROUP BY year(from_unixtime(CAST(timeunix AS int))), productId
    ) as cr
    WHERE cr.row_num <= 10
);

CREATE TABLE top_reviews_for_year_with_words AS (
    SELECT productID, reviews_year, word, word_count
    FROM (
        SELECT productID, word, count(*) as word_count, year(from_unixtime(CAST(timeunix AS int))) as reviews_year,
            row_number() OVER (
                PARTITION BY productID, year(from_unixtime(CAST(timeunix AS int))) ORDER BY count(*) DESC
                ) as row_num
        FROM (
            SELECT productID, LOWER(word) as word, timeunix
            FROM reviews
            LATERAL VIEW explode(split(text, ' ')) wordTable AS word
            WHERE length(word) > 3
        ) lower_words
        GROUP BY productID, year(from_unixtime(CAST(timeunix AS int))), word
    ) words
    WHERE row_num <= 5
);

CREATE TABLE reviewed_products AS (
   SELECT top_reviews_for_year_with_words.reviews_year, top_reviews_for_year_with_words.productID, top_reviews_for_year_with_words.word, top_reviews_for_year_with_words.word_count, top_counted_reviews.n_review
   FROM top_reviews_for_year_with_words JOIN top_counted_reviews
   WHERE top_reviews_for_year_with_words.productId = top_counted_reviews.productId AND top_reviews_for_year_with_words.reviews_year = top_counted_reviews.reviews_year
);

-- INSERT OVERWRITE DIRECTORY '/user/${hiveconf:username}/output/${hiveconf:regexDB}' SELECT * FROM reviewed_products;
-- Per S3
-- TODO: da chiedere all'utenten il parametro di output nello script
-- INSERT OVERWRITE DIRECTORY '${hiveconf:output}' SELECT * FROM reviewed_products;