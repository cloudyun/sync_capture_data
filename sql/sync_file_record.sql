create table sync_file_record 
( 
  id serial, 
  name character varying(32), 
  day character varying(32), 
  file character varying(256), 
  constraint pk_sync_file_record_id primary key( id) 
);