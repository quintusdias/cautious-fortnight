-- Get the most recent activity on all referers. 
with ip_age as (
    select id, max(date) as max_date
    from ip_address_logs
    group by id
)

-- Delete any IP addresses with no recent activity.
--
-- The foreign key constraint will cascade-delete rows
-- in ip_address_logs.
delete from ip_address_lut
where id in (
    select distinct id
    from ip_age
    where max_date < current_date - interval '14 days'
);
