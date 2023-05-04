CREATE TABLE IF NOT EXISTS reviews (
    id int,
    productId string,
    userId string,
    profileName string,
    helpfulnessNumerator int,
    helpfulnessDenominator int,
    score int,
    time string,
    summary string,
    text string
) ROW FORMAT DELIMITED FIELDS TERMINATED BY "," tblproperties("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH "/Users/davidegattini/SourceTreeProj/BigData1/dataset/Reviews.csv" OVERWRITE INTO TABLE reviews;

CREATE TABLE user_reviews_avarage_utility AS
    SELECT userId, avg(1.0*(helpfulnessNumerator/helpfulnessDenominator)) as avg_reviews_utility
    FROM reviews
    WHERE helpfulnessNumerator >= 0 and helpfulnessDenominator > 0 and helpfulnessNumerator <= helpfulnessDenominator
    GROUP BY userId
    SORT BY avg_reviews_utility DESC;