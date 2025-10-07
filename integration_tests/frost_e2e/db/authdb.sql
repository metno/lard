CREATE TABLE public.authorized_keys (
    owner_id bigint NOT NULL,
    client_id uuid NOT NULL,
    client_secret character varying(64) NOT NULL,
    email character varying(128) NOT NULL,
    active boolean DEFAULT true NOT NULL,
    created timestamp without time zone DEFAULT now(),
    last_use timestamp without time zone DEFAULT '1970-01-01 00:00:00'::timestamp without time zone
);

ALTER TABLE public.authorized_keys OWNER TO postgres;

INSERT INTO authorized_keys (owner_id, client_id, client_secret, email)
VALUES (1, 'a8adfa00-6680-49b3-bf94-caa8c3f1d823', 'a8adfa00-6680-49b3-bf94-caa8c3f1d823', 'test@met.no');
