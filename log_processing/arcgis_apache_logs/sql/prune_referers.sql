-- Get the most recent activity on all referers. 
with ref_age as (
    select id, max(date) as max_date
    from referer_logs
    group by id
)

-- Delete any referers with no recent activity.
--
-- The foreign key constraint will cascade-delete rows
-- in referer_logs.
delete from referer_lut
where id in (
    select distinct id
    from ref_age
    where max_date < current_date - interval '14 days'
);
