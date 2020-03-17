-- Summarize the total hits and bytes by IP address for yesterday.
with today_cte as (
    SELECT
        SUM(logs.hits) as hits,
        SUM(logs.nbytes) as nbytes,
        lut.ip_address
    FROM ip_address_logs logs INNER JOIN ip_address_lut lut ON logs.id = lut.id
    where logs.date::date = '{yesterday}'
    GROUP BY lut.ip_address
)
-- Get the top 5 IPs by bytes
(
    select ip_address from today_cte
    order by hits desc
    limit 5
)
UNION
(
    select ip_address from today_cte
    order by nbytes desc
    limit 5
);
