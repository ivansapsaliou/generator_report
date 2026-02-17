CREATE TABLE public.rul_report_entity (
    report_entity_id bigint DEFAULT nextval('rul_report_entity_entity_id_seq'::regclass) NOT NULL,
    report_entity_name character varying(255) NOT NULL,
    system_name character varying(255) NOT NULL,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint
    ,
    CONSTRAINT rul_report_entity_pkey PRIMARY KEY (report_entity_id)
);
