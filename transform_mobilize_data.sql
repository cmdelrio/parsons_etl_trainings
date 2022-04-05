-- This script creates a table where each row is a unique
-- Mobilize user that needs to be synced to Action Network
-- It creates this table from the participations table by
-- using the DISTINCT SQL function

CREATE TABLE mobilize_schema.mobilize_users_to_sync AS (

SELECT DISTINCT
    user_id as mobilizeid
  , given_name 
  , family_name 
  , email_address
  , phone_number
  , postal_code
FROM mobilize_schema.mobilize_participations as mob
-- Joining the log table lets us know which records have been synced
-- and which records still need to be synced
LEFT JOIN cormac_scratch.mobilize_to_actionnetwork_log as log 
  on log.mobilizeid = mob.user_id
WHERE log.synced is null

);
