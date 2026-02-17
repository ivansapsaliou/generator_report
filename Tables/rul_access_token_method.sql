CREATE TABLE public.rul_access_token_method (
    id bigint DEFAULT nextval('rul_access_token_method_id_seq'::regclass) NOT NULL,
    access_token_id bigint,
    mask character varying(256)
    ,
    CONSTRAINT pk_access_token_method PRIMARY KEY (id)
);
