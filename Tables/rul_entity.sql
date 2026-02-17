CREATE TABLE public.rul_entity (
    entity_id bigint DEFAULT nextval('rul_entity_entity_id_seq'::regclass) NOT NULL,
    entity_name character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_entity_pkey PRIMARY KEY (entity_id)
);

COMMENT ON COLUMN public.rul_entity.entity_name IS 'Вид сущности к которой привязан файл';
