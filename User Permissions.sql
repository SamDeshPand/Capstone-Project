-- Databricks notebook source
GRANT SELECT ON TABLE capstone_data.bronze.customers TO `user_email@example.com`;
GRANT INSERT ON TABLE capstone_data.bronze.sales TO `user_email@example.com`;
GRANT UPDATE ON TABLE capstone_data.bronze.products TO `user_email@example.com`;
GRANT DELETE ON TABLE capstone_data.bronze.order_dates TO `user_email@example.com`;
