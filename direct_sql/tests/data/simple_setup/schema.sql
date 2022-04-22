CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    name TEXT,
    age INTEGER
);

INSERT INTO users (name, age)
VALUES
    ('sam', 30),
    ('coraline', 32);
