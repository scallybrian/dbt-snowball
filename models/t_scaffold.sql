{{ config(materialized='view', transient=true) }}

WITH
	-- Select intial fields to take forward
	init_table
	AS
	(
		SELECT
			customer_id,
			sub_category AS product,
			invoice_start,
			invoice_end,
			SUM(sales) AS sales
		FROM
			SNOWBALL.RAW.raw_invoices

		GROUP BY customer_id,
			sub_category,
			invoice_start,
			invoice_end
	),

	-- Generate rows for monthly dataset
	data_monthly
	AS
	(
			SELECT customer_id,
				product,
				invoice_start as month,
				invoice_end,
				sales,
				invoice_start
			-- duplicate to save in dataset
			FROM
				init_table
			GROUP BY customer_id,
				product,
				invoice_start,
				invoice_end,
				sales
		UNION ALL
			SELECT
				customer_id,
				product,
				DATEADD(MONTH, 1, month),
				invoice_end,
				sales,
				invoice_start
			FROM
				data_monthly
			WHERE
				DATEADD(MONTH, 1, month) < invoice_end


	)

SELECT * FROM data_monthly