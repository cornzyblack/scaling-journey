-- When was a particular form created
WITH first_query AS (
select form_id, event_happened_at
from event_forms
where event_type='created'),

-- When was a particular form published
second_query AS
(SELECT *
from event_forms
WHERE event_type='published'),

-- How many edit events happened, for a particular form, between form creation and form publish
third_query AS
(SELECT COUNT(*)
from event_forms
WHERE event_type='edited'
-- AND form_id=<put-form-id here>
)
,

-- How many fields does a form currently have
fourth_query_a AS
(SELECT payload::json->>'fields' AS fields
from event_forms
WHERE event_type='edited'),

foruth_query AS (select JSON_ARRAY_LENGTH(fields::json) AS no_fields
from
fourth_query_a),

-- How many forms currently use language!='en'
fifth_query_a AS (
SELECT payload::json->>'settings' AS settings
FROM
event_forms),

fifth_query AS (
SELECT COUNT(*)
FROM
fifth_query_a
WHERE settings::json->>'language'!='en')

SELECT *
FROM
fifth_query
