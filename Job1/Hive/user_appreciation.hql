CREATE TABLE IF NOT EXISTS reviews (
    id int,
    productId string,
    userId string,
    helpfulnessNumerator int,
    helpfulnessDenominator int,
    score int,
    time string,
    summary string,
    text string
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ",";

LOAD DATA LOCAL INPATH "/Users/davidegattini/SourceTreeProj/BigData1/dataset/Reviews.csv" OVERWRITE INTO TABLE reviews;

CREATE TABLE user_reviews_avarage_utility AS
    SELECT userId, AVG(review_utility) as avg_reviews_utility
    FROM
        SELECT userId, (helpfulnessNumerator/helpfulnessDenominator) as review_utility
        FROM reviews
        WHERE helpfulnessNumerator >= 0 and helpfulnessDenominator > 0 and helpfulnessNumerator <= helpfulnessDenominator
    GROUP BY userId
    SORT BY avg_reviews_utility DESC;

SELECT * 
FROM user_reviews_avarage_utility
LIMIT 10;