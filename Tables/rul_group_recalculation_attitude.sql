CREATE TABLE public.rul_group_recalculation_attitude (
    group_recalculation_attitude_id bigint DEFAULT nextval('rul_group_recalculation_attit_group_recalculation_attitude__seq'::regclass) NOT NULL,
    group_recalculation_attitude_name character varying(256)
    ,
    CONSTRAINT rul_group_recalculation_attitude_pkey PRIMARY KEY (group_recalculation_attitude_id)
);
