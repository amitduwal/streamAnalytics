CREATE OR REPLACE STREAM "SEIZURE_DETECTION_STREAM" 
(
    "patientId"          varchar(64),
    "name"               varchar(64),
    "age"                integer,
    "heartRate"          integer,
    "respiratoryRate"    integer,
    "oxygenSaturation"   integer,
    "seizureDetected"    boolean,
    "seizureDuration"    integer,
    "seizureSeverity"    varchar(32),
    "currentLocation"    varchar(256),
    "hospitalId"         varchar(32),
    "eventTimestamp"     varchar(32)
);

CREATE OR REPLACE PUMP "SEIZURE_PUMP" AS INSERT INTO "SEIZURE_DETECTION_STREAM"
SELECT STREAM 
    "patientId", 
    "name", 
    "age", 
    "heartRate", 
    "respiratoryRate", 
    "oxygenSaturation", 
    "seizureDetected", 
    "seizureDuration", 
    "seizureSeverity", 
    "currentLocation", 
    "hospitalId", 
    "eventTimestamp"
FROM "SOURCE_SQL_STREAM_001"
WHERE "seizureDetected" = TRUE;
