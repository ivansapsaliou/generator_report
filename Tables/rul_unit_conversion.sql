CREATE TABLE public.rul_unit_conversion (
    target_unit_id bigint NOT NULL,
    source_unit_id bigint NOT NULL,
    conversion_factor numeric(12,5) NOT NULL
    ,
    CONSTRAINT fk_rul_unit_conv_source FOREIGN KEY (source_unit_id) REFERENCES rul_unit(unit_id),
    CONSTRAINT fk_rul_unit_conv_target FOREIGN KEY (target_unit_id) REFERENCES rul_unit(unit_id)
);

COMMENT ON COLUMN public.rul_unit_conversion.target_unit_id IS 'Единица измерения, в которую нужно перевести значение по параметру';
COMMENT ON COLUMN public.rul_unit_conversion.source_unit_id IS 'Единица измерения, из которой нужно перевести значение по параметру';
COMMENT ON COLUMN public.rul_unit_conversion.conversion_factor IS 'Переводной коэффициент';
