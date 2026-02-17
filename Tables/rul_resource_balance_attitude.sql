CREATE TABLE public.rul_resource_balance_attitude (
    resource_balance_attitude_id bigint DEFAULT nextval('rul_resource_balance_attitude_resource_balance_attitude_id_seq'::regclass) NOT NULL,
    resource_balance_attitude_name character varying(256)
    ,
    CONSTRAINT rul_resource_balance_attitude_pkey PRIMARY KEY (resource_balance_attitude_id)
);
