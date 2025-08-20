USE Facolos_Database;
GO

-- Ensure Facolos_Staging schema exists
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Facolos_Staging')
    EXEC('CREATE SCHEMA Facolos_Staging');
GO

-- Drop table if exists (for development)
IF OBJECT_ID('Facolos_Staging.data', 'U') IS NOT NULL
    DROP TABLE Facolos_Staging.data;
GO

-- Create staging table for data from various platforms
CREATE TABLE Facolos_Staging.data (
    -- ETL Metadata
    etl_batch_id UNIQUEIDENTIFIER DEFAULT NEWID(),
    etl_created_at DATETIME2 DEFAULT GETUTCDATE(),
    etl_updated_at DATETIME2 DEFAULT GETUTCDATE(),
    
    -- Order Information
    order_id NVARCHAR(50) NOT NULL,
    order_status NVARCHAR(50),
    buyer_message NVARCHAR(MAX),
    cancel_reason NVARCHAR(MAX),
    cancel_user NVARCHAR(50),
    collection_time BIGINT,
    create_time BIGINT,
    delivery_due_time BIGINT,
    delivery_time BIGINT,
    fulfillment_type NVARCHAR(50),
    order_line_type NVARCHAR(50),
    payment_method NVARCHAR(100),
    payment_method_name NVARCHAR(100),
    remark NVARCHAR(MAX),
    request_cancel_reason NVARCHAR(MAX),
    split_or_combine_tag NVARCHAR(50),
    update_time BIGINT,
    warehouse_id NVARCHAR(50),
    
    -- Order Amounts
    currency NVARCHAR(10),
    original_shipping_fee DECIMAL(18,4),
    original_total_product_price DECIMAL(18,4),
    seller_discount DECIMAL(18,4),
    shipping_fee DECIMAL(18,4),
    shipping_fee_platform_discount DECIMAL(18,4),
    shipping_fee_seller_discount DECIMAL(18,4),
    subtotal_after_seller_discounts DECIMAL(18,4),
    tax_amount DECIMAL(18,4),
    total_amount DECIMAL(18,4),
    
    -- Recipient Information
    recipient_address_detail NVARCHAR(MAX),
    recipient_address_region_code NVARCHAR(20),
    recipient_address_state NVARCHAR(100),
    recipient_address_city NVARCHAR(100),
    recipient_address_town NVARCHAR(100),
    recipient_address_district NVARCHAR(100),
    recipient_address_zipcode NVARCHAR(20),
    recipient_name NVARCHAR(200),
    recipient_phone NVARCHAR(50),
    recipient_phone_number NVARCHAR(50),
    
    -- Item Information (flattened structure)
    item_id NVARCHAR(50),
    item_name NVARCHAR(500),
    item_sku_id NVARCHAR(50),
    item_sku_image NVARCHAR(MAX),
    item_sku_name NVARCHAR(500),
    item_quantity INT,
    item_unit_price DECIMAL(18,4),
    item_currency NVARCHAR(10),
    item_is_gift NVARCHAR(10),
    item_platform_discount DECIMAL(18,4),
    item_seller_discount DECIMAL(18,4),
    
    -- Item SKU Sales Attributes (JSON or concatenated)
    item_sku_sales_attributes NVARCHAR(MAX),
    
    -- Constraints
    CONSTRAINT PK_facolos_staging_data PRIMARY KEY (etl_batch_id, order_id, item_id, item_sku_id)
);
GO

-- Create indexes for performance
CREATE INDEX IX_facolos_staging_data_order_id ON Facolos_Staging.data (order_id);
CREATE INDEX IX_facolos_staging_data_create_time ON Facolos_Staging.data (create_time);
CREATE INDEX IX_facolos_staging_data_update_time ON Facolos_Staging.data (update_time);
CREATE INDEX IX_facolos_staging_data_order_status ON Facolos_Staging.data (order_status);
CREATE INDEX IX_facolos_staging_data_etl_created_at ON Facolos_Staging.data (etl_created_at);
GO
