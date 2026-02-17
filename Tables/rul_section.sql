CREATE TABLE public.rul_section (
    section_id bigint DEFAULT nextval('rul_section_section_id_seq'::regclass) NOT NULL,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    op_user_id bigint,
    op_date timestamp without time zone,
    deleted smallint DEFAULT 0 NOT NULL,
    section_name character varying(256),
    installation_date timestamp without time zone,
    comment character varying(256),
    method_tubing_id bigint,
    construction_id bigint,
    line_id bigint
    ,
    CONSTRAINT rul_section_pkey PRIMARY KEY (section_id),
    CONSTRAINT fk_construction_id FOREIGN KEY (construction_id) REFERENCES rul_construction(construction_id),
    CONSTRAINT fk_line_id FOREIGN KEY (line_id) REFERENCES rul_line(line_id),
    CONSTRAINT fk_method_tubing_id FOREIGN KEY (method_tubing_id) REFERENCES rul_method_tubing(method_tubing_id)
);

COMMENT ON COLUMN public.rul_section.start_date IS 'Актуален для рассчетов с';
COMMENT ON COLUMN public.rul_section.end_date IS 'Актуален для расчетов по';
COMMENT ON COLUMN public.rul_section.section_name IS 'Описание участка';
COMMENT ON COLUMN public.rul_section.installation_date IS 'Дата монтажа';
COMMENT ON COLUMN public.rul_section.comment IS 'Примечание';
COMMENT ON COLUMN public.rul_section.method_tubing_id IS 'Ссылка на способ прокладки';
COMMENT ON COLUMN public.rul_section.construction_id IS 'Ссылка на конструкцию';
COMMENT ON COLUMN public.rul_section.line_id IS 'Ссылка на принадлежность линии';
