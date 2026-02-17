CREATE TABLE public.rul_file_type (
    file_type_id bigint DEFAULT nextval('rul_file_type_file_type_id_seq'::regclass) NOT NULL,
    file_type_name character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT pk_file_type PRIMARY KEY (file_type_id)
);
