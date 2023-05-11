INSERT OVERWRITE DIRECTORY "/user/${hiveconf:username}/output/${hiveconf:regexDB}" SELECT * FROM user_reviews_avarage_utility;

SELECT * FROM user_reviews_avarage_utility LIMIT 10;