SELECT
    logs.date,
    f.folder,
    SUM(logs.hits) - SUM(logs.errors) as hits,
    s_lut.service,
    st_lut.name as service_type
FROM service_logs logs
    INNER JOIN service_lut s_lut ON logs.id = s_lut.id
    INNER JOIN folder_lut f on f.id = s_lut.folder_id
    INNER JOIN service_type_lut st_lut on s_lut.service_type_id = st_lut.id
where logs.date > current_date - interval '2 weeks'
GROUP BY logs.date, f.folder, s_lut.service, st_lut.name
ORDER BY logs.date
