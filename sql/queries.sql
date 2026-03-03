SELECT 
    category,
    COUNT(*) as total_bookings,
    SUM(revenue) as total_revenue,
    AVG(rating) as avg_rating,
    AVG(price) as avg_price
FROM hotel_bookings
GROUP BY category
ORDER BY total_revenue DESC
LIMIT 10;

SELECT 
    TO_CHAR(created_date, 'YYYY-MM') as month,
    COUNT(*) as bookings,
    SUM(revenue) as monthly_revenue,
    AVG(price) as avg_price,
    SUM(rooms_booked) as total_rooms
FROM hotel_bookings
GROUP BY TO_CHAR(created_date, 'YYYY-MM')
ORDER BY month;

SELECT 
    country,
    COUNT(*) as total_bookings,
    AVG(rating) as avg_rating,
    SUM(revenue) as total_revenue,
    AVG(price) as avg_price
FROM hotel_bookings
GROUP BY country
ORDER BY avg_rating DESC;

WITH ranked_hotels AS (
    SELECT 
        hotel_name,
        category,
        SUM(revenue) as total_revenue,
        COUNT(*) as bookings,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY SUM(revenue) DESC) as rank
    FROM hotel_bookings
    GROUP BY hotel_name, category
)
SELECT hotel_name, category, total_revenue, bookings
FROM ranked_hotels
WHERE rank <= 5
ORDER BY category, rank;

SELECT 
    TO_CHAR(created_date, 'YYYY-MM') as month,
    category,
    COUNT(*) as bookings,
    SUM(revenue) as revenue,
    AVG(rating) as avg_rating
FROM hotel_bookings
GROUP BY TO_CHAR(created_date, 'YYYY-MM'), category
ORDER BY month, revenue DESC;

SELECT 
    category,
    country,
    COUNT(*) as premium_bookings,
    AVG(price) as avg_price,
    SUM(revenue) as total_revenue
FROM hotel_bookings
WHERE rating >= 4.5 AND rooms_booked >= 3
GROUP BY category, country
HAVING COUNT(*) >= 10
ORDER BY total_revenue DESC;

SELECT 
    SUBSTRING(customer_email FROM '@(.*)$') as email_domain,
    COUNT(*) as bookings,
    AVG(revenue) as avg_revenue,
    SUM(revenue) as total_revenue
FROM hotel_bookings
GROUP BY SUBSTRING(customer_email FROM '@(.*)$')
HAVING COUNT(*) >= 50
ORDER BY bookings DESC
LIMIT 15;

EXPLAIN ANALYZE
SELECT category, COUNT(*), SUM(revenue)
FROM hotel_bookings
WHERE country = 'USA'
GROUP BY category;
