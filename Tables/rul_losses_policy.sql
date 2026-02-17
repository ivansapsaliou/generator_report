CREATE TABLE public.rul_losses_policy (
    losses_policy_id bigint DEFAULT nextval('rul_losses_policy_losses_policy_id_seq'::regclass) NOT NULL,
    losses_policy_name character varying(256),
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_losses_policy_pkey PRIMARY KEY (losses_policy_id)
);

COMMENT ON COLUMN public.rul_losses_policy.losses_policy_name IS 'Политика потерь';
