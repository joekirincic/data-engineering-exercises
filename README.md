# data-engineering-exercises

This repository is an exercise in using PySpark to process and analyze data. We have a minimal Docker Compose stack consisting of a single container that runs a Python script `main.py`.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose

## Getting started

After cloning the repo, you can build the project using the following.

```bash
docker compose up -d --build
```

Once built, answers to some analysis questions can be found by running `docker compose logs app`. The test suite can be run by running `docker compose run app uv run pytest tests/`.

## The data

There are three CSV datasets in this exercise, `data/products.csv`, `data/sales_order_detail.csv`, and `data/sales_order_header.csv`. The primary and foreign keys for each table are given below. To determine the primary key of each table, the grain of the data was identified by locating appropriate ID columns and running diagnostic queries to determine which ID had a row count of 1 for all rows in the table.

| Table | Primary Key | Foreign Keys |
|---|---|---|
| `products` | `ProductID` | |
| `sales_order_detail` | `SalesOrderDetailID` | `SalesOrderID` -> `sales_order_header`, `ProductID` -> `products` |
| `sales_order_header` | `SalesOrderID` | `CustomerID` (Not used) |

## Analysis

There's a number of questions we can ask about this data, but we restrict ourselves to two for now.

### 1. What color had the most revenue each year?

The colors with the most revenue for each year can be found in the table below.

| Year | Color | Revenue |
|---|---|---|
| 2021-01-01 | Red | 6019613.15 |
| 2022-01-01 | Black | 14005216.19 |
| 2023-01-01 | Black | 15047626.43 |
| 2024-01-01 | Yellow | 6480720.07 |

This result was determined by aggregating yearly revenue for each color, and using a window function over that aggregated result to create an indicator variable that's true for the color with the highest revenue within that year. The final result is obtained by filtering down that result to only the rows where the indicator column is true. 


### 2. What is the average lead time (in business days) for each product category?

The average lead time in business days for each product category is shown in the table below.

| ProductCategoryName | AvgLeadTimeInBusinessDays |
|---|---|
| Bikes | 4.667897567632656 |
| NULL | 4.717621086432968 |
| Clothing | 4.709380234505863 |
| Accessories | 4.702787804316105 |
| Components | 4.667113624438874 |

This result was obtained by computing the average value of `LeadTimeInBusinessDays` grouped by `ProductCategoryName`. 