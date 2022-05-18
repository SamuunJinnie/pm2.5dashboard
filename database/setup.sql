CREATE TABLE IF NOT EXISTS raw_data (
    id serial PRIMARY KEY,
    stationID VARCHAR(10) NOT NULL,
    lat float NOT NULL,
    lng float NOT NULL,
    pm25 float,
    pm10 float,
    no2 float,
    so2 float,
    co float,
    rh float,
    temp float,
    -- datetime_aq timestamps with time zone NOT NULL
    datetime_aq VARCHAR(50) NOT NULL
)