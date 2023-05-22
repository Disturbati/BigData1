-- DA RIVEDERE PERCHE NELLA FASE FINALE DA OUTOFMEMORYexception

CREATE EXTERNAL TABLE reviews (
    id int,
    productId string,
    userId string,
    profileName string,
    helpfulnessNumerator int,
    helpfulnessDenominator int,
    score int,
    timeunix string,
    summary string,
    text string
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES('separatorChar'=',', 'quoteChar'='\"') 
    -- LOCATION '/user/${hiveconf:username}/input/${hiveconf:regexDB}'
    -- per S3 (bisogna creare una cartella cui nome è dbName, in cui dentro ho il dbName.csv)
    LOCATION '/input/${hiveconf:dbName}'
    tblproperties('skip.header.line.count'='1');

CREATE TABLE reviews_with_high_score AS (
    SELECT userId, productId, score FROM reviews WHERE score >= 4
);

CREATE TABLE joined_reviews AS (
    SELECT l.userId as userId1, r.userId as userId2, l.productId as productId
    FROM reviews_with_high_score l JOIN reviews_with_high_score r
    ON l.productId = r.productId
    WHERE l.userId < r.userId
);

CREATE TABLE couples AS (
    SELECT * 
    FROM(
        SELECT userId1, userId2, collect_set(productId) as productsAffinity
        FROM joined_reviews
        GROUP BY userId1, userId2
    ) t
    WHERE size(productsAffinity) > 2
);

CREATE TABLE users_exploded AS (
    SELECT userId, product1, product2, product3
    FROM (
        SELECT c.userId1, c.userId2, p1.product as product1, p2.product as product2, p3.product as product3
        FROM couples c
        LATERAL VIEW explode(c.productsAffinity) p1 AS product
        LATERAL VIEW explode(c.productsAffinity) p2 AS product
        LATERAL VIEW explode(c.productsAffinity) p3 AS product
        WHERE p1.product < p2.product AND p2.product < p3.product
    ) subquery
    LATERAL VIEW explode(array(subquery.userId1, subquery.userId2)) u AS userId
);

CREATE TABLE affinityGroups AS (
    SELECT collect_set(userId) as usersAffinity, product1, product2, product3
    FROM users_exploded
    GROUP BY product1, product2, product3
);
