CREATE TABLE public.rul_network_fragment_link (
    parent_network_fragment_id bigint,
    child_network_fragment_id bigint
    ,
    CONSTRAINT fk_child_network_fragment_id FOREIGN KEY (child_network_fragment_id) REFERENCES rul_network_fragment(network_fragment_id),
    CONSTRAINT fk_parent_network_fragment_id FOREIGN KEY (parent_network_fragment_id) REFERENCES rul_network_fragment(network_fragment_id)
);

COMMENT ON COLUMN public.rul_network_fragment_link.parent_network_fragment_id IS 'Фрагмент родитель';
COMMENT ON COLUMN public.rul_network_fragment_link.child_network_fragment_id IS 'Дочерний фрагмент';
