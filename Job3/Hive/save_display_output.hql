INSERT OVERWRITE DIRECTORY "/user/${hiveconf:username}/output/${hiveconf:regexDB}" SELECT * FROM affinityGroups;

SELECT * FROM affinityGroups LIMIT 10;