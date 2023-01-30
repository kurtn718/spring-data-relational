DROP TABLE IF EXISTS dummy_entity;
DROP TABLE IF EXISTS SORT_EMBEDDED_ENTITY;
DROP TABLE IF EXISTS WITH_DOT_COLUMN;

CREATE TABLE dummy_entity
(
    id                  BIGINT IDENTITY PRIMARY KEY,
    TEST                VARCHAR(100),
    PREFIX2_ATTR        BIGINT,
    PREFIX_TEST         VARCHAR(100),
    PREFIX_PREFIX2_ATTR BIGINT
);

CREATE TABLE SORT_EMBEDDED_ENTITY
(
    id           BIGINT IDENTITY PRIMARY KEY,
    first_name   VARCHAR(100),
    address      VARCHAR(255),
    email        VARCHAR(255),
    phone_number VARCHAR(255)
);

CREATE TABLE WITH_DOT_COLUMN
(
    id             BIGINT IDENTITY PRIMARY KEY,
    "address.city" VARCHAR(255)
);