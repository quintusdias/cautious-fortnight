-- This saves one full table scan.
with maxdate_cte as (
    select max(date) as maxdate
    from service_logs
),

-- Aggregate the service hits by 1-day intervals
day_cte as (
    select
	id as service_id,
	extract(epoch from date) / 86400 as daynum,
	hits,
        (wms_mapdraws + export_mapdraws) as mapdraws,
        nbytes,
        errors
    FROM service_logs
    where date >= (select maxdate from maxdate_cte) - interval '14 days'
), 
-- normalize the day number into the range [0, 1]
normalize_day_cte as (
    select
        service_id,
        ceil(daynum - (select max(daynum) from day_cte)) + 1 as daynum,
        hits,
	mapdraws,
	nbytes,
	errors
    from day_cte
),
-- aggregate (sum) over the days, services
ag_day_cte as (
    select
        service_id,
        daynum,
        sum(hits) as hits,
        sum(mapdraws) as mapdraws,
        sum(nbytes) as nbytes,
        sum(errors) as errors
    from normalize_day_cte
    group by daynum, service_id
    order by daynum desc, hits desc
),
-- Get the difference between this day and last day
lag_day_cte as (
    select
        service_id,
        daynum,
	hits,
	lead(hits, 1) over (
	    partition by service_id
	    order by daynum desc
	) as prev,
        mapdraws,
        nbytes,
        errors
    from ag_day_cte
    order by daynum desc, hits desc
),
-- formulate the percent change
day_pct_change_cte as (
    select
        service_id,
        daynum,
	hits,
        -- percentage of all hits attributable to a particular service
        hits / sum(hits) over () * 100 as hits_pct,
	-- hits percent change from yesterday
	(hits - prev) / prev * 100 as day_pct_delta,
        mapdraws,
        -- percentage of hits that are mapdraws
        mapdraws / hits * 100 as mapdraw_pct,
	-- convert bytes to GBytes
        nbytes / 1024 ^ 3 as gbytes,
        -- percentage of throughput attributable to a particular service
        nbytes / sum(nbytes) over () * 100 as gbytes_pct,
        errors,
	-- percentage of all hits attributable to errors for a particular service
	errors / sum(hits) over () * 100 as error_pct_all_hits,
	-- percentage of all errors attributable to a particular service
	errors / sum(errors) over () * 100 as error_pct_all_errors
    from lag_day_cte
    where daynum = 1
    order by daynum desc, hits desc
),


-- Aggregate the service hits by 7-day intervals
week_cte as (
    select
	id as service_id,
	extract(epoch from date) / 86400 / 7 as week,
	hits as hits
    FROM service_logs
    where date >= (select maxdate from maxdate_cte) - interval '14 days'
), 
-- normalize the week number into the range [0, 1]
normalize_weeks_cte as (
    select
        service_id,
        ceil(week - (select max(week) from week_cte)) + 1 as week,
        hits
    from week_cte
),
-- aggregate (sum) over the weeks, services
weeks_ag_cte as (
    select
        service_id,
        week,
        sum(hits) as hits
    from normalize_weeks_cte
    group by week, service_id
    order by week desc, hits desc
),
-- Get the difference between this week and last week
week_lag_cte as (
    select
        service_id,
        week,
	hits,
	lead(hits, 1) over (
	    partition by service_id
	    order by week desc
	) as prev
    from weeks_ag_cte
    order by week desc, hits desc
),
-- formulate the percent change
week_pct_change_cte as (
    select
        service_id,
        week,
	hits,
	(hits - prev) / prev * 100 as week_pct_delta
    from week_lag_cte
    where week = 1
    order by week desc, hits desc
)

-- Combine the daily and weekly summaries.  Tidy up the display.
select
    -- rank() does not work with the HTML styling because
    -- the ranks are not necessarily distinct (case when number
    -- of hits is low).  row_number() suits our purposes though.
    row_number() over (
	order by day_pct_change_cte.hits desc
    ) as rank,
    folder_lut.folder,
    service_lut.service,
    service_lut.service_type,
    day_pct_change_cte.hits,
    round(day_pct_change_cte.hits_pct, 1) as "hits %",
    round(day_pct_change_cte.day_pct_delta, 1) as day_pct_delta,
    round(week_pct_change_cte.week_pct_delta, 1) as week_pct_delta,
    round(day_pct_change_cte.mapdraw_pct, 1) as "mapdraw %",
    round(gbytes, 1) as gbytes,
    round(gbytes_pct, 1) as "gbytes %",
    errors,
    round(error_pct_all_hits, 1) as "errors: % of all hits",
    round(error_pct_all_errors, 1) as "errors: % of all errors"
from week_pct_change_cte
     inner join service_lut on week_pct_change_cte.service_id = service_lut.id
     inner join folder_lut on service_lut.folder_id = folder_lut.id
     inner join day_pct_change_cte on service_lut.id = day_pct_change_cte.service_id
where service_lut.active
order by hits desc;
