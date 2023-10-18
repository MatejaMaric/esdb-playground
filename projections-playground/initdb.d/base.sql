USE projected_models;
DROP TABLE IF EXISTS users;
CREATE TABLE users(
    username VARCHAR(255),
    login_count INT NOT NULL DEFAULT 0,
    version BIGINT NOT NULL,
    CONSTRAINT PRIMARY KEY (username)
);
