#  Data Catalog for Gold Layer

## Overview
The Gold Layer represents the business-facing view of the data warehouse. It includes **dimension** and **fact** tables designed for advanced analytics, KPIs, and reporting use cases.

---

### 1. **gold.dim_customer**
- **Purpose:** Contains enriched customer information with demographic and identity attributes.
- **Columns:**

| Column Name   | Data Type | Nullable | Description |
|---------------|-----------|----------|-------------|
| customer_key  | BIGINT    | YES      | Surrogate key uniquely identifying each customer. |
| customer_id   | INTEGER   | YES      | Unique identifier assigned to each customer. |
| birthdate     | DATE      | YES      | Date of birth of the customer. |
| create_date   | DATE      | YES      | Record creation timestamp. |

---

### 2. **gold.dim_products**
- **Purpose:** Stores product metadata used in sales and inventory tracking.
- **Columns:**

| Column Name         | Data Type | Nullable | Description |
|---------------------|-----------|----------|-------------|
| product_key         | BIGINT    | YES      | Surrogate key for each product. |
| product_id          | INTEGER   | YES      | Unique product identifier. |
| product_number      | CHARACTER VARYING(50) | YES | Internal product code. |
| product_name        | CHARACTER VARYING(50) | YES | Human-readable product label. |
| category_id         | CHARACTER VARYING(50) | YES | Category ID associated with the product. |
| category            | CHARACTER VARYING(50) | YES | High-level grouping of the product. |
| subcategory         | CHARACTER VARYING(50) | YES | Subgroup classification of the product. |
| maintenance_required| CHARACTER VARYING(50) | YES | Whether product requires maintenance. |
| cost                | INTEGER   | YES      | Base cost of the product. |
| product_line        | CHARACTER VARYING(50) | YES | Line or series to which the product belongs. |
| start_date          | DATE      | YES      | Launch or availability date of the product. |

---

### 3. **gold.fact_sales**
- **Purpose:** Tracks transactional sales events and ties to product and customer dimensions.
- **Columns:**

| Column Name    | Data Type | Nullable | Description |
|----------------|-----------|----------|-------------|
| order_number   | CHARACTER VARYING(50) | YES | Unique identifier for each sales order. |
| product_key    | BIGINT    | YES      | Foreign key referencing `dim_products`. |
| customer_key   | BIGINT    | YES      | Foreign key referencing `dim_customer`. |
| order_date     | DATE      | YES      | When the order was placed. |
| shipping_date  | DATE      | YES      | When the product was shipped. |
| due_date       | DATE      | YES      | Expected due date for the order. |
| sales_amount   | INTEGER   | YES      | Total monetary value of the order. |
| quantity       | INTEGER   | YES      | Quantity of items sold. |
| price          | INTEGER   | YES      | Unit price at time of transaction. |
