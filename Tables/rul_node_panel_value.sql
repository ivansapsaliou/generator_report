CREATE TABLE public.rul_node_panel_value (
    node_panel_value_id bigint DEFAULT nextval('rul_node_panel_value_node_panel_value_id_seq'::regclass) NOT NULL,
    value_number numeric,
    check_date timestamp without time zone NOT NULL,
    check_type_id bigint,
    node_panel_id bigint,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    is_correct numeric(1,0) DEFAULT 0 NOT NULL,
    changed_user_id bigint
    ,
    CONSTRAINT rul_meter_value_pkey_tmp PRIMARY KEY (node_panel_value_id, check_date),
    CONSTRAINT fk_changed_user_id FOREIGN KEY (changed_user_id) REFERENCES rul_user(user_id),
    CONSTRAINT fk_check_type_id FOREIGN KEY (check_type_id) REFERENCES rul_check_type(check_type_id),
    CONSTRAINT fk_node_panel_id FOREIGN KEY (node_panel_id) REFERENCES rul_node_panel(node_panel_id)
);

COMMENT ON COLUMN public.rul_node_panel_value.value_number IS 'Показание прибора учета';
COMMENT ON COLUMN public.rul_node_panel_value.check_date IS 'Дата получения показания';
COMMENT ON COLUMN public.rul_node_panel_value.check_type_id IS 'Сслыка на способ снятия показания';
COMMENT ON COLUMN public.rul_node_panel_value.node_panel_id IS 'Ссылка параметр измеряемый панелью в узле';
COMMENT ON COLUMN public.rul_node_panel_value.is_correct IS 'Флаг корректности';
COMMENT ON COLUMN public.rul_node_panel_value.changed_user_id IS 'Пользователь добавивший изменения';
