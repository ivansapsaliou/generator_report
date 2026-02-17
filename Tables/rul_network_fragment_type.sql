CREATE TABLE public.rul_network_fragment_type (
    network_fragment_type_id bigint DEFAULT nextval('rul_network_fragment_type_network_fragment_type_id_seq'::regclass) NOT NULL,
    network_fragment_type_name character varying(128),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_network_fragment_type_pkey PRIMARY KEY (network_fragment_type_id)
);

COMMENT ON COLUMN public.rul_network_fragment_type.network_fragment_type_name IS 'Тип фрагмента';
