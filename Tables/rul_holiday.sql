CREATE TABLE public.rul_holiday (
    holiday_id bigint DEFAULT nextval('rul_holiday_holiday_id_seq'::regclass) NOT NULL,
    holiday_name character varying(256),
    holiday_date timestamp without time zone NOT NULL,
    op_date timestamp without time zone DEFAULT now() NOT NULL,
    op_user_id bigint NOT NULL,
    deleted smallint DEFAULT 0 NOT NULL
    ,
    CONSTRAINT rul_holiday_pkey PRIMARY KEY (holiday_id)
);

COMMENT ON COLUMN public.rul_holiday.holiday_name IS 'Название праздника';
COMMENT ON COLUMN public.rul_holiday.holiday_date IS 'Дата праздника';
