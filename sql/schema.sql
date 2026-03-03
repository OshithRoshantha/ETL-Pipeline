DROP TABLE IF EXISTS hotel_bookings;

CREATE TABLE hotel_bookings (
    booking_id INTEGER PRIMARY KEY,
    hotel_name VARCHAR(255) NOT NULL,
    category VARCHAR(50) NOT NULL,
    price DECIMAL(10, 2) NOT NULL CHECK (price > 0),
    rating DECIMAL(3, 2) CHECK (rating >= 0 AND rating <= 5),
    country VARCHAR(50) NOT NULL,
    created_date DATE NOT NULL,
    rooms_booked INTEGER NOT NULL CHECK (rooms_booked > 0),
    customer_email VARCHAR(255) NOT NULL,
    revenue DECIMAL(12, 2) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP INDEX IF EXISTS idx_country;
DROP INDEX IF EXISTS idx_category;
DROP INDEX IF EXISTS idx_created_date;
DROP INDEX IF EXISTS idx_rating;
DROP INDEX IF EXISTS idx_country_category;
DROP INDEX IF EXISTS idx_date_category;

CREATE INDEX idx_country ON hotel_bookings(country);
CREATE INDEX idx_category ON hotel_bookings(category);
CREATE INDEX idx_created_date ON hotel_bookings(created_date);
CREATE INDEX idx_rating ON hotel_bookings(rating);

CREATE INDEX idx_country_category ON hotel_bookings(country, category);
CREATE INDEX idx_date_category ON hotel_bookings(created_date, category);

COMMENT ON TABLE hotel_bookings IS 'Hotel booking records with cleaned and validated data';
COMMENT ON COLUMN hotel_bookings.revenue IS 'Calculated as price * rooms_booked';
COMMENT ON COLUMN hotel_bookings.loaded_at IS 'Timestamp when record was loaded into database';
