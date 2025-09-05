-- Simple test query
SELECT customer_id, customer_name, COUNT(*) as order_count
FROM customers c
INNER JOIN orders o ON c.customer_id = o.customer_id
GROUP BY customer_id, customer_name
ORDER BY order_count DESC;

