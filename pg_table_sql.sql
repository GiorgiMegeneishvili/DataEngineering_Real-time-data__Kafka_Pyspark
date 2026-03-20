CREATE TABLE IF NOT EXISTS weather_data1 (
    id          BIGSERIAL PRIMARY KEY,
    city        VARCHAR(100)        NOT NULL,
    temperature NUMERIC(6,2)        NOT NULL,     -- FloatType → 6 ციფრი სულ, 2 ათწილადი
    humidity    INTEGER             NOT NULL,
    weather     VARCHAR(150),                     -- nullable = True
    event_time  TIMESTAMP           NOT NULL,     -- StringType → to_timestamp() გარდაქმნის
    pressure    INTEGER,                          -- nullable = True
    wind_speed  NUMERIC(5,2),                     -- nullable = True
    source      VARCHAR(50),                       -- nullable = True
    
    ingested_at TIMESTAMP           DEFAULT CURRENT_TIMESTAMP
    
);


select * from weather_data1;
truncate table weather_data1;
