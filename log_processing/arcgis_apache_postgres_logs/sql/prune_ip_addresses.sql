delete from ip_address_lut where id in (
      select logs.id
      from ip_address_logs logs
      group by id
      having max(date) < current_date - interval '14 days'
);
