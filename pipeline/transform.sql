-- Chicago Station Pulse — DuckDB transform layer.
-- status  : live availability per station (GBFS station_status)
-- info    : static metadata per station   (GBFS station_information)

-- Joined base: only stations that are installed and currently renting,
-- so KPIs reflect the system riders can actually use right now.
CREATE VIEW active AS
SELECT
    s.station_id,
    i.name,
    i.lat,
    i.lon,
    i.capacity,
    s.num_bikes_available    AS bikes,
    s.num_ebikes_available   AS ebikes,
    s.num_docks_available    AS docks
FROM status s
JOIN info i USING (station_id)
WHERE s.is_installed = 1
  AND s.is_renting = 1;

-- Citywide KPIs for the live dashboard header.
CREATE TABLE citywide AS
SELECT
    COUNT(*)                                             AS stations_online,
    SUM(bikes)                                           AS bikes_available,
    SUM(ebikes)                                          AS ebikes_available,
    SUM(docks)                                           AS docks_available,
    ROUND(100.0 * AVG(CASE WHEN bikes = 0 THEN 1 ELSE 0 END), 1) AS pct_empty,
    ROUND(100.0 * AVG(CASE WHEN docks = 0 THEN 1 ELSE 0 END), 1) AS pct_full,
    ROUND(100.0 * SUM(bikes) / NULLIF(SUM(capacity), 0), 1)      AS capacity_used_pct
FROM active;

-- Busiest docks right now: most bikes ready to ride.
CREATE TABLE top_stations AS
SELECT name, lat, lon, bikes, ebikes, docks, capacity
FROM active
ORDER BY bikes DESC, name
LIMIT 15;
