CREATE TABLE public.rul_node_type (
    node_type_id bigint DEFAULT nextval('rul_node_type_node_type_id_seq'::regclass) NOT NULL,
    node_type_name character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_node_type_pkey PRIMARY KEY (node_type_id)
);
