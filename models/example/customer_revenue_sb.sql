-- Customer churn dates
WITH data_monthly AS (
    SELECT * FROM {{ ref('scaffold') }}
),

 churn_dates
	AS
	(
		SELECT a.customer_id,
			a.product,
			a.month,
			a.invoice_start,
			a.invoice_end,
			a.sales,
			b.customer_join_month,
			b.customer_churn_month,
			b.customer_last_ARR
		FROM (
			SELECT customer_id,
				MIN(month) AS customer_join_month,
				MAX(month) AS customer_last_ARR,
				MAX(invoice_end) AS customer_churn_month
			FROM
				data_monthly
			GROUP BY customer_id
			) b
			RIGHT JOIN data_monthly a
			ON a.customer_id = b.customer_id
	) ,

	-- Product churn dates
	churn_dates_product
	AS
	(
		SELECT a.customer_id,
			a.product,
			a.month,
			a.invoice_start,
			a.invoice_end,
			a.sales,
			a.customer_join_month,
			a.customer_churn_month,
			a.customer_last_ARR,
			b.product_churn_month,
			b.product_join_month,
			b.product_last_ARR
		FROM (
			SELECT customer_id,
				product,
				MIN(month) AS product_join_month,
				MAX(month) AS product_last_ARR,
				MAX(invoice_end) AS product_churn_month
			FROM
				data_monthly
			GROUP BY customer_id,
	product
			) b
			RIGHT JOIN churn_dates a
			ON a.customer_id = b.customer_id
				AND a.product = b.product
	) ,

	-- Construct churn period
	-- leave out invoice_start and invoice_end
	churn_period
	AS
	(
					SELECT customer_id,
				product,
				product_join_month,
				product_join_month as month,
				DATEADD(MONTH, 12, product_last_ARR) as data_end,
				product_churn_month,
				product_last_ARR,
				customer_churn_month,
				customer_join_month,
				customer_last_ARR
			FROM
				churn_dates_product
			GROUP BY customer_id,
				product,
				product_join_month,
				product_last_ARR,
				product_churn_month,
				customer_churn_month,
				customer_join_month,
				customer_last_ARR
		UNION ALL
			SELECT
				customer_id,
				product,
				product_join_month,
				DATEADD(MONTH, 1, month),
				DATEADD(MONTH, 12, product_last_ARR) as data_end,
				product_churn_month,
				product_last_ARR,
				customer_churn_month,
				customer_join_month,
				customer_last_ARR
			FROM
				churn_period
			WHERE
				month < DATEADD(MONTH, 12, product_last_ARR)
	) ,

	-- Add churn period to main dataset
	data_monthly_with_cp
	AS
	(
		SELECT b.customer_id,
			b.product,
			b.month,
			b.customer_join_month,
			b.customer_churn_month,
			b.customer_last_ARR,
			b.product_churn_month,
			b.product_join_month,
			b.product_last_ARR,
			--a.invoice_start,
			--a.invoice_end,
			IFNULL(SUM(a.sales), 0) as sales
		FROM churn_dates_product a
			RIGHT JOIN churn_period b
			ON a.month = b.month
				AND a.customer_id = b.customer_id
				AND a.product = b.product

		GROUP BY b.customer_id,
			b.product,
			b.month,
			b.customer_join_month,
			b.customer_churn_month,
			b.customer_last_ARR,
			b.product_churn_month,
			b.product_join_month,
			b.product_last_ARR
	) ,

	-- ARR Revenue change & flags
	data_monthly_flags
	AS
	(
		SELECT customer_id,
			product,
			month,
			customer_join_month,
			customer_churn_month,
			customer_last_ARR,
			product_churn_month,
			product_join_month,
			product_last_ARR,
			sales as ARR,
			-- ARR last year
			IFNULL(lag(sales, 12) 
				OVER (PARTITION BY customer_id, product ORDER BY month), 0) as ARR_12mo_ago,
			-- ARR this month vs 12 months ago
			sales - IFNULL(lag(sales, 12) 
				OVER (PARTITION BY customer_id, product ORDER BY month), 0) as ARR_vs_12mo_ago,
			-- New Customer Flag
			CASE
				WHEN DATEDIFF(MONTH, customer_join_month, month) < 12 THEN 1
				ELSE 0 END as Flag_LTM_AddOrg,
			-- Churn Flag
			CASE
				WHEN DATEDIFF(MONTH, customer_churn_month, month) < 12
				AND DATEDIFF(MONTH, customer_churn_month, month) >= 0
				THEN 1 ELSE 0 END as Flag_LTM_Churn,
			-- Customer exists currently
			CASE
				WHEN DATEDIFF(MONTH, customer_join_month, month) >= 12
				AND month < customer_churn_month
				THEN 1 ELSE 0 END as Flag_LTM_Existing_Customer,
			-- Cross Sell
			CASE
				WHEN DATEDIFF(MONTH, customer_join_month, month) >= 12
				AND month < customer_churn_month -- when exists
				AND DATEDIFF(MONTH, product_join_month, month) < 12
				THEN 1 ELSE 0 END as Flag_LTM_Cross_Sell,
			-- Downgrade
			CASE
				WHEN DATEDIFF(MONTH, customer_join_month, month) >= 12
				AND month < customer_churn_month -- when exists
				AND DATEDIFF(MONTH, product_churn_month, month) < 12
				AND DATEDIFF(MONTH, product_churn_month, month) >= 0
				THEN 1 ELSE 0 END as Flag_LTM_Downgrade,
			-- Existing Product
			CASE 
				WHEN DATEDIFF(MONTH, customer_join_month, month) >= 12
				AND month < customer_churn_month -- when exists 
				AND DATEDIFF(MONTH, product_join_month, month) >= 12
				AND month < product_churn_month
				THEN 1 ELSE 0 END as Flag_LTM_Existing_Product

		FROM data_monthly_with_cp

	) ,

	-- Upsell Flags
	data_upsell_flags
	AS
	(
		SELECT a.customer_id,
			a.product,
			a.month,
			a.customer_join_month,
			a.customer_churn_month,
			a.customer_last_ARR,
			a.product_churn_month,
			a.product_join_month,
			a.product_last_ARR,
			a.ARR,
			a.ARR_12mo_ago,
			a.ARR_vs_12mo_ago,
			a.Flag_LTM_AddOrg,
			a.Flag_LTM_Churn,
			a.Flag_LTM_Existing_Customer,
			a.Flag_LTM_Cross_Sell,
			a.Flag_LTM_Downgrade,
			a.Flag_LTM_Existing_Product,
			b.Flag_LTM_Product_Grew,
			b.Flag_LTM_Product_Declined,
			CASE
				WHEN a.Flag_LTM_Existing_Customer = 1
				AND a.Flag_LTM_Existing_Product = 1
				AND b.Flag_LTM_Product_Grew = 1
				THEN 1 ELSE 0 END as Flag_LTM_Upsell,

			CASE
				WHEN a.Flag_LTM_Existing_Customer = 1
				AND a.Flag_LTM_Existing_Product = 1
				AND b.Flag_LTM_Product_Declined = 1
				THEN 1 ELSE 0 END as Flag_LTM_Downsell
		FROM
			(
			SELECT customer_id,
				product,
				month,
				SUM(ARR_vs_12mo_ago) as ARR_vs_12mo_ago,
				CASE
					WHEN SUM(ARR_vs_12mo_ago) > 0
					THEN 1 ELSE 0 END as Flag_LTM_Product_Grew,
				CASE
					WHEN SUM(ARR_vs_12mo_ago) < 0
					THEN 1 ELSE 0 END as Flag_LTM_Product_Declined
			FROM data_monthly_flags
			GROUP BY customer_id,
			product,
			month
		) b
			RIGHT JOIN data_monthly_flags a
			ON a.customer_id = b.customer_id
				AND a.product = b.product
				AND a.month = b.month
	) ,

	-- KPI Deltas
	data_deltas
	AS
	(
		SELECT customer_id,
			product,
			month,
			customer_join_month,
			customer_churn_month,
			customer_last_ARR,
			product_churn_month,
			product_join_month,
			product_last_ARR,
			ARR,
			ARR_12mo_ago,
			ARR_vs_12mo_ago,
			Flag_LTM_AddOrg,
			Flag_LTM_Churn,
			Flag_LTM_Existing_Customer,
			Flag_LTM_Cross_Sell,
			Flag_LTM_Downgrade,
			Flag_LTM_Existing_Product,
			Flag_LTM_Product_Grew,
			Flag_LTM_Product_Declined,
			Flag_LTM_Upsell,
			Flag_LTM_Downsell,
			YEAR(customer_join_month) as cohort,
			CASE
				WHEN Flag_LTM_AddOrg = 1 
				THEN ARR_vs_12mo_ago 
				ELSE 0 END as Delta_LTM_AddOrg,
			CASE
				WHEN Flag_LTM_Churn = 1 
				THEN ARR_12mo_ago * -1
				ELSE 0 END as Delta_LTM_Churn,
			CASE
				WHEN Flag_LTM_Cross_Sell = 1 
				THEN ARR 
				ELSE 0 END as Delta_LTM_Cross_Sell,
			CASE
				WHEN Flag_LTM_Downgrade = 1 
				THEN ARR_12mo_ago * -1 
				ELSE 0 END as Delta_LTM_Downgrade,
			CASE
				WHEN Flag_LTM_Upsell = 1 
				THEN ARR_vs_12mo_ago
				ELSE 0 END as Delta_LTM_Upsell,
			CASE
				WHEN Flag_LTM_Downsell = 1 
				THEN ARR_vs_12mo_ago
				ELSE 0 END as Delta_LTM_Downsell
		FROM data_upsell_flags

	),

	-- Bring in Firmographics

	data_final
	AS
	(
		SELECT a.customer_id,
			b.customer_name,
			a.product,
			b.product_category,
			b.market,
			b.segment,
			a.month,
			a.customer_join_month,
			a.customer_churn_month,
			a.customer_last_ARR,
			a.product_churn_month,
			a.product_join_month,
			a.product_last_ARR,
			a.ARR,
			a.ARR_12mo_ago,
			a.ARR_vs_12mo_ago,
			a.Flag_LTM_AddOrg,
			a.Flag_LTM_Churn,
			a.Flag_LTM_Existing_Customer,
			a.Flag_LTM_Cross_Sell,
			a.Flag_LTM_Downgrade,
			a.Flag_LTM_Existing_Product,
			a.Flag_LTM_Product_Grew,
			a.Flag_LTM_Product_Declined,
			a.Flag_LTM_Upsell,
			a.Flag_LTM_Downsell,
			a.cohort,
			a.Delta_LTM_AddOrg,
			a.Delta_LTM_Churn,
			a.Delta_LTM_Cross_Sell,
			a.Delta_LTM_Downgrade,
			a.Delta_LTM_Upsell,
			a.Delta_LTM_Downsell,
			a.ARR_12mo_ago 
				+ a.Delta_LTM_Churn
				+ a.Delta_LTM_Cross_Sell
				+ a.Delta_LTM_Downgrade
				+ a.Delta_LTM_Upsell
				+ a.Delta_LTM_Downsell as Delta_LTM_NRR
		FROM data_deltas a

			LEFT JOIN

			(SELECT customer_id,
				customer_name,
				sub_category as product,
				category as product_category,
				market,
				segment
			FROM "SNOWBALL"."PUBLIC"."SNOWBALL_INVOICES"
			GROUP BY customer_id,
			customer_name,
			sub_category,
			category,
			market,
			segment
			) b
			ON a.customer_id = b.customer_id
				AND a.product = b.product

	)


-- Write main table here
SELECT customer_id,
	customer_name,
	product,
	product_category,
	market,
	segment,
	month,
	customer_join_month,
	customer_churn_month,
	customer_last_ARR,
	product_churn_month,
	product_join_month,
	product_last_ARR,
	ARR,
	ARR_12mo_ago,
	ARR_vs_12mo_ago,
	Flag_LTM_AddOrg,
	Flag_LTM_Churn,
	Flag_LTM_Existing_Customer,
	Flag_LTM_Cross_Sell,
	Flag_LTM_Downgrade,
	Flag_LTM_Existing_Product,
	Flag_LTM_Product_Grew,
	Flag_LTM_Product_Declined,
	Flag_LTM_Upsell,
	Flag_LTM_Downsell,
	cohort,
	Delta_LTM_AddOrg,
	Delta_LTM_Churn,
	Delta_LTM_Cross_Sell,
	Delta_LTM_Downgrade,
	Delta_LTM_Upsell,
	Delta_LTM_Downsell,
	Delta_LTM_NRR
FROM data_final
