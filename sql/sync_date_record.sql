create table sync_date_record 
( 
  id serial, 
  name character varying(32), 
  date character varying(64), 
  day character varying(32), 
  success integer DEFAULT 0, 
  file_count integer DEFAULT 0,
  constraint pk_sync_date_record_id primary key( id) 
);
INSERT INTO public.sync_date_record(name, date, day, success, file_count) VALUES ('capture_img', '2018-01-01 00:00:01', '2018-01-01', 1, 1);
INSERT INTO public.sync_date_record(name, date, day, success, file_count) VALUES ('capture_face', '2018-01-01 00:00:01', '2018-01-01', 1, 1);
INSERT INTO public.sync_date_record(name, date, day, success, file_count) VALUES ('capture_body', '2018-01-01 00:00:01', '2018-01-01', 1, 1);