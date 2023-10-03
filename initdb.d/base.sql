USE projected_models;
DROP TABLE IF EXISTS users;
CREATE TABLE users(
    login_count INT,
    username VARCHAR(255)
);
