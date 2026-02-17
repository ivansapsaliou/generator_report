CREATE TABLE public.rul_losses_params (
    losses_params_id bigint DEFAULT nextval('rul_losses_params_losses_params_id_seq'::regclass) NOT NULL,
    accounting_type_node_id bigint,
    mon_work_hours numeric,
    tue_work_hours numeric,
    wed_work_hours numeric,
    thu_work_hours numeric,
    fri_work_hours numeric,
    sat_work_hours numeric,
    sun_work_hours numeric,
    supply_temperature numeric,
    return_temperature numeric,
    recharge_temperature numeric,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    generated numeric
    ,
    CONSTRAINT rul_losses_params_pkey PRIMARY KEY (losses_params_id),
    CONSTRAINT fk_accounting_type_node_id FOREIGN KEY (accounting_type_node_id) REFERENCES rul_accounting_type_node(accounting_type_node_id)
);

COMMENT ON COLUMN public.rul_losses_params.accounting_type_node_id IS 'Ссылка на способ учета';
COMMENT ON COLUMN public.rul_losses_params.mon_work_hours IS 'Кол-во рабочих часов в понедельник';
COMMENT ON COLUMN public.rul_losses_params.tue_work_hours IS 'Кол-во рабочих часов во вторник';
COMMENT ON COLUMN public.rul_losses_params.wed_work_hours IS 'Кол-во рабочих часов в среду';
COMMENT ON COLUMN public.rul_losses_params.thu_work_hours IS 'Кол-во рабочих часов в четверг';
COMMENT ON COLUMN public.rul_losses_params.fri_work_hours IS 'Кол-во рабочих часов в пятницу';
COMMENT ON COLUMN public.rul_losses_params.sat_work_hours IS 'Кол-во рабочих часов в субботу';
COMMENT ON COLUMN public.rul_losses_params.sun_work_hours IS 'Кол-во рабочих часов в воскресенье';
COMMENT ON COLUMN public.rul_losses_params.supply_temperature IS 'Температура подачи';
COMMENT ON COLUMN public.rul_losses_params.return_temperature IS 'Температура обратки';
COMMENT ON COLUMN public.rul_losses_params.recharge_temperature IS 'Температура подпитки';
COMMENT ON COLUMN public.rul_losses_params.generated IS 'Выработано';
