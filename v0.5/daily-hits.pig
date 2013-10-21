/*
    Count of daily hits from Apache combined log

    Usage: pig -f ./daily-hits.pig -p INPUT=/path/to/logs -p OUTPUT=/path/to/output

    Used with Pig v0.5.0

    License: CC0 1.0 [ http://creativecommons.org/publicdomain/zero/1.0/ ]
*/

REGISTER /usr/lib/pig/contrib/piggybank/java/piggybank.jar;
DEFINE CombinedLogLoader org.apache.pig.piggybank.storage.apachelog.CombinedLogLoader();
DEFINE DayExtractor org.apache.pig.piggybank.evaluation.util.apachelogparser.DateExtractor('yyyy-MM-dd');

logs = LOAD '$INPUT' USING CombinedLogLoader
    AS (remote_host:chararray, hyphen:chararray, user:chararray,
        datetime:chararray, method:chararray, path:chararray,
        protocol:chararray, status_code:chararray, response_size:int,
        referer:chararray, user_agent:chararray);

grouped_by_day = GROUP logs BY DayExtractor(datetime) AS day;

count_by_day = FOREACH grouped_by_day GENERATE
    group AS day, COUNT(logs) AS requests;
    
STORE count_by_day INTO '$OUTPUT';
