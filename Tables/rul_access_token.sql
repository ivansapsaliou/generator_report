CREATE TABLE public.rul_access_token (
    id bigint DEFAULT nextval('rul_access_token_id_seq'::regclass) NOT NULL,
    token character varying(64) NOT NULL,
    date_set timestamp(6) without time zone NOT NULL,
    description character varying(256)
    ,
    CONSTRAINT pk_access_token_id PRIMARY KEY (id)
);
