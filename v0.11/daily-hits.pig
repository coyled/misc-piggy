/*
    Count of daily hits from Apache combined log

    Usage: pig -f ./daily-hits.pig -p INPUT=/path/to/logs -p OUTPUT=/path/to/output

    Used with Pig v0.11.1 (HDP 1.3)

    License: CC0 1.0 [ http://creativecommons.org/publicdomain/zero/1.0/ ]
*/

REGISTER piggybank.jar;
DEFINE CombinedLogLoader org.apache.pig.piggybank.storage.apachelog.CombinedLogLoader();
DEFINE DayExtractor org.apache.pig.piggybank.evaluation.util.apachelogparser.DateExtractor('yyyy-MM-dd');

logs = LOAD '$INPUT' USING CombinedLogLoader
    AS (remote_host:chararray, hyphen:chararray, user:chararray,
        dt:chararray, method:chararray, path:chararray,
        protocol:chararray, status_code:chararray, response_size:int,
        referer:chararray, user_agent:chararray);
        
days_only = FOREACH logs GENERATE DayExtractor(dt) AS day;

grouped_by_day = GROUP days_only BY day;

count_by_day = FOREACH grouped_by_day GENERATE
    group AS day, COUNT(days_only) AS requests;

STORE count_by_day INTO '$OUTPUT';
