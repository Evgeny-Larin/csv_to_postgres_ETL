INSERT INTO {{ params.logs_schema }}.{{ params.logs_table }} (execution_datetime, event_datetime, event_name, event_status)
VALUES ('{{ ts }}', '{{ params.event_datetime }}', '{{ params.event_name }}', '{{ params.event_status }}')