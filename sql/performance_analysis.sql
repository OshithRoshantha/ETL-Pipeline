EXPLAIN ANALYZE
SELECT 
    category,
    COUNT(*) as total_bookings,
    SUM(revenue) as total_revenue,
    AVG(rating) as avg_rating,
    AVG(price) as avg_price,
    MAX(price) as max_price,
    MIN(price) as min_price
FROM hotel_bookings
GROUP BY category
ORDER BY total_revenue DESC;

EXPLAIN ANALYZE
SELECT 
    TO_CHAR(created_date, 'YYYY-MM') as month,
    COUNT(*) as bookings,
    SUM(revenue) as monthly_revenue,
    AVG(price) as avg_price,
    SUM(rooms_booked) as total_rooms
FROM hotel_bookings
WHERE created_date >= '2023-01-01' AND created_date <= '2023-12-31'
GROUP BY TO_CHAR(created_date, 'YYYY-MM')
ORDER BY month;

EXPLAIN ANALYZE
SELECT 
    country,
    category,
    COUNT(*) as bookings,
    AVG(rating) as avg_rating,
    SUM(revenue) as total_revenue,
    AVG(price) as avg_price
FROM hotel_bookings
WHERE country = 'USA' AND category = 'Luxury'
GROUP BY country, category;

