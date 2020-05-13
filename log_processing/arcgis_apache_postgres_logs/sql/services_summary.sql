-- Part I:  Get yesterday's summary.
with yesterday_cte as (
    select
    	id as service_id,
    	sum(hits) as hits,
        sum(wms_mapdraws + export_mapdraws) as mapdraws,
        sum(nbytes) as nbytes,
        sum(errors) as errors
    FROM service_logs
    where date::date = (select current_date - interval '1 days')::date
    GROUP BY service_id
),

-- Part II:  Get the percentage change of the past day from
-- the day prior to that.
--
day2_cte as (
    select
	id as service_id,
	floor(current_date::date - date::date - 1) as days_ago,
	hits
    FROM service_logs
    where date::date >= (select current_date - interval '2 days')
),

 
-- aggregate (sum) over the days, services
day_ag_cte as (
    select
        service_id,
        days_ago,
        sum(hits) as hits
    from day2_cte
    group by days_ago, service_id
),

-- Get the difference between this week and last week
day_lag_cte as (
    select
        service_id,
        days_ago,
	hits,
	lag(hits, 1) over (
	    partition by service_id
	    order by days_ago desc
	) as prev
    from day_ag_cte
),

-- -- formulate the percent change, restrict results relative to past seven days.
day_percentage_change as (
    select
        service_id,
        (hits - prev) / prev * 100 as pct_delta
    from day_lag_cte
    where days_ago = 0
),

-- Part III:  Get the percentage change of the past 7 days from
-- the 7 days prior to that.
--
-- Get everything over the last two weeks.
weeks2_cte as (
    select
	id as service_id,
	floor((current_date::date - date::date - 1) / 7) as weeks_ago,
	hits
    FROM service_logs
    where date::date >= (select current_date - interval '14 days')
),

 
-- aggregate (sum) over the weeks, services
week_ag_cte as (
    select
        service_id,
        weeks_ago,
        sum(hits) as hits
    from weeks2_cte
    group by weeks_ago, service_id
),

-- Get the difference between this week and last week
week_lag_cte as (
    select
        service_id,
        weeks_ago,
	hits,
	lag(hits, 1) over (
	    partition by service_id
	    order by weeks_ago desc
	) as prev
    from week_ag_cte
),

-- -- formulate the percent change, restrict results relative to past seven days.
week_percentage_change as (
    select
        service_id,
        (hits - prev) / prev * 100 as pct_delta
    from week_lag_cte
    where weeks_ago = 0
),

-- Part IV:  Get the percentage change of the last 30 days from the
-- 30 days before that.
--
-- Get everything over the last two months.
months2_cte as (
    select
	id as service_id,
	floor((current_date::date - date::date - 1) / 30) as months_ago,
	hits
    FROM service_logs
    where date::date >= (select current_date - interval '60 days')
),

 
-- aggregate (sum) over the months, services
month_ag_cte as (
    select
        service_id,
        months_ago,
        sum(hits) as hits
    from months2_cte
    group by months_ago, service_id
),

-- Get the difference between this month and last month
month_lag_cte as (
    select
        service_id,
        months_ago,
	hits,
	lag(hits, 1) over (
	    partition by service_id
	    order by months_ago desc
	) as prev
    from month_ag_cte
),

-- -- formulate the percent change, restrict results relative to past seven days.
monthly_percentage_change as (
    select
        service_id,
        (hits - prev) / prev * 100 as pct_delta
    from month_lag_cte
    where months_ago = 0
)

select
    -- rank() does not work with the HTML styling because
    -- the ranks are not necessarily distinct (case when number
    -- of hits is low).  row_number() suits our purposes though.
    row_number() over (order by y.hits desc) as rank,
    flu.folder,
    slu.service,
    y.hits,
    y.mapdraws / y.hits * 100 as mapdraw_pct,
    y.nbytes / 1024 / 1024 / 1024 as gbytes,
    y.errors,
    round(d.pct_delta::numeric, 1) as day_pct_delta,
    round(w.pct_delta::numeric, 1) as week_pct_delta,
    round(m.pct_delta::numeric, 1) as month_pct_delta
from yesterday_cte y inner join day_percentage_change d     using(service_id)
                     inner join week_percentage_change w    using(service_id)
                     inner join monthly_percentage_change m using(service_id)
		     inner join service_lut slu             on slu.id = m.service_id
		     inner join folder_lut flu              on flu.id = slu.folder_id
order by y.hits desc
;
