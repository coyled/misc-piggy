/*
    Report top 50 URI paths for the given status code from Apache
    combined logs

    Usage: pig -f ./top-paths-by-status.pig -p STATUS=404 -p INPUT=/path/to/logs \
               -p OUTPUT=/path/to/output

    Used with Pig v0.11.1 (HDP 1.3)

    License: CC0 1.0 [ http://creativecommons.org/publicdomain/zero/1.0/ ]
*/

/* UNTESTED */

/* REGISTER /usr/lib/pig/contrib/piggybank/java/piggybank.jar; */
REGISTER piggybank.jar;
DEFINE CombinedLogLoader org.apache.pig.piggybank.storage.apachelog.CombinedLogLoader();

%declare INPUT '/user/dave/davecoyle.com_clf-sample.log'
%declare OUTPUT '/user/dave/pigout/test1'
%declare STATUS 404

logs = LOAD '$INPUT' USING CombinedLogLoader
    AS (remote_host:chararray, hyphen:chararray, user:chararray,
        datetime:chararray, method:chararray, path:chararray,
        protocol:chararray, status_code:chararray, response_size:int,
        referer:chararray, user_agent:chararray);

logs_filtered = FILTER logs BY status_code == '$STATUS';

grouped_by_path = GROUP logs_filtered BY path;

count_by_path = FOREACH grouped_by_path GENERATE
    group AS path, COUNT(logs_filtered) AS requests;

top_50 = LIMIT (ORDER count_by_path BY requests DESC) 50;

STORE top_50 INTO '$OUTPUT';
