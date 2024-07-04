-- TODO: this is just for testing, need to think about how to deal with partitions
CREATE TABLE data_y2000_to_y2050 PARTITION OF public.data
  FOR VALUES FROM ('2000-01-01 00:00:00+00') TO ('2050-01-01 00:00:00+00');
