CREATE TABLE public.rul_connection (
    connection_id bigint DEFAULT nextval('rul_connection_connection_id_seq'::regclass) NOT NULL,
    connection_name character varying(256),
    agreement_id bigint,
    percent_consumption numeric,
    node_id bigint,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    service_type_id bigint,
    service_operation_template_id bigint,
    rate_id bigint,
    node_calculate_parameter_id bigint,
    canalized_part numeric,
    client_object_id bigint,
    unaccounted_source_consumption_id bigint,
    allocation_source_consumption_id bigint,
    resource_balance_attitude_id bigint,
    group_recalculation_attitude_id bigint,
    invoice_group_index bigint DEFAULT 1,
    percent_losses numeric,
    indexing_operation_template_id bigint,
    advance_operation_template_id bigint,
    losses_policy_id bigint,
    purpose_consumption_id bigint,
    losses_rate_id bigint
    ,
    CONSTRAINT rul_connection_pkey PRIMARY KEY (connection_id),
    CONSTRAINT fk_advance_operation_template_id FOREIGN KEY (advance_operation_template_id) REFERENCES rul_operation_template(operation_template_id),
    CONSTRAINT fk_agreement_id FOREIGN KEY (agreement_id) REFERENCES rul_agreement(agreement_id),
    CONSTRAINT fk_allocation_source_consumption_id FOREIGN KEY (allocation_source_consumption_id) REFERENCES rul_source_consumption(source_consumption_id),
    CONSTRAINT fk_group_recalculation_attitude_id FOREIGN KEY (group_recalculation_attitude_id) REFERENCES rul_group_recalculation_attitude(group_recalculation_attitude_id),
    CONSTRAINT fk_indexing_operation_template_id FOREIGN KEY (indexing_operation_template_id) REFERENCES rul_operation_template(operation_template_id),
    CONSTRAINT fk_losses_policy FOREIGN KEY (losses_policy_id) REFERENCES rul_losses_policy(losses_policy_id),
    CONSTRAINT fk_losses_rate_id FOREIGN KEY (losses_rate_id) REFERENCES rul_rate(rate_id),
    CONSTRAINT fk_node_calculate_paramater_id FOREIGN KEY (node_calculate_parameter_id) REFERENCES rul_node_calculate_parameter(node_calculate_parameter_id),
    CONSTRAINT fk_owner_node_id FOREIGN KEY (node_id) REFERENCES rul_node(node_id),
    CONSTRAINT fk_purpose_consumption_id FOREIGN KEY (purpose_consumption_id) REFERENCES rul_purpose_consumption(purpose_consumption_id),
    CONSTRAINT fk_rate_id FOREIGN KEY (rate_id) REFERENCES rul_rate(rate_id),
    CONSTRAINT fk_resource_balance_attitude_id FOREIGN KEY (resource_balance_attitude_id) REFERENCES rul_resource_balance_attitude(resource_balance_attitude_id),
    CONSTRAINT fk_service_operation_template_id FOREIGN KEY (service_operation_template_id) REFERENCES rul_operation_template(operation_template_id),
    CONSTRAINT fk_service_type_id FOREIGN KEY (service_type_id) REFERENCES rul_service_type(service_type_id),
    CONSTRAINT fk_target_consumer_client_object_id FOREIGN KEY (client_object_id) REFERENCES rul_client_object(client_object_id),
    CONSTRAINT fk_unaccounted_source_consumption_id FOREIGN KEY (unaccounted_source_consumption_id) REFERENCES rul_source_consumption(source_consumption_id)
);

COMMENT ON COLUMN public.rul_connection.connection_name IS 'Название для начислений';
COMMENT ON COLUMN public.rul_connection.agreement_id IS 'Ссылка на договор';
COMMENT ON COLUMN public.rul_connection.percent_consumption IS 'Процент расхода';
COMMENT ON COLUMN public.rul_connection.node_id IS 'Ссылка на узел потребителя';
COMMENT ON COLUMN public.rul_connection.start_date IS 'Дата подключения узла';
COMMENT ON COLUMN public.rul_connection.end_date IS 'Дата отключения узла';
COMMENT ON COLUMN public.rul_connection.service_type_id IS 'Ссылка на Вид услуги';
COMMENT ON COLUMN public.rul_connection.service_operation_template_id IS 'Ссылка на шаблон операций по услуге';
COMMENT ON COLUMN public.rul_connection.rate_id IS 'Ссылка на тарифную группу';
COMMENT ON COLUMN public.rul_connection.node_calculate_parameter_id IS 'Ссылка на Рассчетный параметр узла';
COMMENT ON COLUMN public.rul_connection.canalized_part IS 'Процент канализации';
COMMENT ON COLUMN public.rul_connection.client_object_id IS 'Ссылка на Котрагента размещенного на объекте (Не имеет отношения к договору, т.к. может быть отличным от него)';
COMMENT ON COLUMN public.rul_connection.unaccounted_source_consumption_id IS 'Ссылка на источник расходов при безучетном потреблении';
COMMENT ON COLUMN public.rul_connection.allocation_source_consumption_id IS 'Ссылка на источник расходов при распределении расходов ГПУ';
COMMENT ON COLUMN public.rul_connection.resource_balance_attitude_id IS 'Ссылка на справочник подлежит ли балансировке';
COMMENT ON COLUMN public.rul_connection.group_recalculation_attitude_id IS 'Ссылка на справочник подлежит ли пересчету по ГПУ';
COMMENT ON COLUMN public.rul_connection.invoice_group_index IS 'ИГС (Участвует в формировании счетов)';
COMMENT ON COLUMN public.rul_connection.percent_losses IS 'Процент потерь (только для воды кроде как)';
COMMENT ON COLUMN public.rul_connection.indexing_operation_template_id IS 'Ссылка на шаблон операции по индексации';
COMMENT ON COLUMN public.rul_connection.advance_operation_template_id IS 'Ссылка на шаблон операции по предоплате';
COMMENT ON COLUMN public.rul_connection.losses_policy_id IS 'Политика по потерям';
COMMENT ON COLUMN public.rul_connection.purpose_consumption_id IS 'Ссылка на Административное назначение потребления';
COMMENT ON COLUMN public.rul_connection.losses_rate_id IS 'Ссылка на тарифную группу для потерь';
