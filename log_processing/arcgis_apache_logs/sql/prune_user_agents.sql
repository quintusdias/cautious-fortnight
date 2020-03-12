-- Get the most recent activity on all user agents. 
with ua_age as (
    select id, max(date) as max_date
    from user_agent_logs
    group by id
)

-- Delete any user agents with no recent activity.
--
-- The foreign key constraint will cascade-delete rows
-- in user_agent_logs.
delete from user_agent_lut
where id in (
    select distinct id
    from ua_age
    where max_date < current_date - interval '14 days'
);
