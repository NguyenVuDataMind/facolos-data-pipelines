-- =====================================================
-- MISA CRM STAGING TABLES - SQL SERVER VERSION
-- =====================================================
-- Tích hợp với TikTok Shop Infrastructure
-- Dựa trên kết quả testing API với 100% success rate
-- Compatible với existing staging schema patterns

USE Facolos_Staging;
GO

-- Ensure staging schema exists (shared với TikTok Shop)
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'staging')
    EXEC('CREATE SCHEMA staging');
GO

PRINT '🏢 Tạo MISA CRM staging tables...';

-- Drop existing MISA CRM tables if they exist
IF OBJECT_ID('staging.misa_sale_orders_flattened', 'U') IS NOT NULL
    DROP TABLE staging.misa_sale_orders_flattened;
IF OBJECT_ID('staging.misa_customers', 'U') IS NOT NULL
    DROP TABLE staging.misa_customers;
IF OBJECT_ID('staging.misa_contacts', 'U') IS NOT NULL
    DROP TABLE staging.misa_contacts;
IF OBJECT_ID('staging.misa_stocks', 'U') IS NOT NULL
    DROP TABLE staging.misa_stocks;
IF OBJECT_ID('staging.misa_products', 'U') IS NOT NULL
    DROP TABLE staging.misa_products;
GO

-- =====================================================
-- 1. MISA CUSTOMERS STAGING TABLE (81 fields from API)
-- =====================================================
PRINT '🏢 Tạo bảng staging.misa_customers...';

CREATE TABLE staging.misa_customers (
    -- Primary identifiers
    id BIGINT PRIMARY KEY,
    account_number NVARCHAR(50),
    account_code NVARCHAR(50),

    -- Basic information
    account_name NVARCHAR(500),
    account_short_name NVARCHAR(255),
    owner_name NVARCHAR(255),

    -- Contact information
    office_tel NVARCHAR(50),
    office_email NVARCHAR(255),
    website NVARCHAR(500),
    fax NVARCHAR(50),

    -- Address information
    billing_address NVARCHAR(MAX),
    billing_country NVARCHAR(100),
    billing_province NVARCHAR(100),
    billing_district NVARCHAR(100),
    billing_ward NVARCHAR(100),
    billing_street NVARCHAR(255),
    billing_code NVARCHAR(20),

    shipping_address NVARCHAR(MAX),
    shipping_country NVARCHAR(100),
    shipping_province NVARCHAR(100),
    shipping_district NVARCHAR(100),
    shipping_ward NVARCHAR(100),
    shipping_street NVARCHAR(255),
    shipping_code NVARCHAR(20),

    -- Business information
    business_type NVARCHAR(100),
    industry NVARCHAR(100),
    annual_revenue DECIMAL(15,2),
    tax_code NVARCHAR(50),
    bank_account NVARCHAR(100),
    bank_name NVARCHAR(255),

    -- Financial information
    debt DECIMAL(15,2),
    debt_limit DECIMAL(15,2),
    number_of_days_owed DECIMAL(10,2),

    -- Sales statistics
    number_orders DECIMAL(10,2),
    order_sales DECIMAL(15,2),
    average_order_value DECIMAL(15,2),
    average_number_of_days_between_purchases DECIMAL(10,2),
    number_days_without_purchase DECIMAL(10,2),

    -- Product information (JSON for complex data)
    list_product_category NVARCHAR(MAX),
    list_product NVARCHAR(MAX),

    -- Important dates
    purchase_date_recent DATETIME2,
    purchase_date_first DATETIME2,
    customer_since_date DATETIME2,
    last_interaction_date DATETIME2,
    last_visit_date DATETIME2,
    last_call_date DATETIME2,

    -- Personal information
    is_personal BIT DEFAULT 0,
    gender NVARCHAR(20),
    identification NVARCHAR(50),
    issued_on DATETIME2,
    place_of_issue NVARCHAR(255),
    celebrate_date DATETIME2,

    -- System fields
    organization_unit_name NVARCHAR(255),
    form_layout NVARCHAR(100),
    rating NVARCHAR(50),
    lead_source NVARCHAR(100),
    sector_name NVARCHAR(100),
    no_of_employee_name NVARCHAR(100),
    parent_account_name NVARCHAR(255),
    account_type NVARCHAR(100),

    -- Status flags
    inactive BIT DEFAULT 0,
    is_public BIT DEFAULT 0,
    is_distributor BIT DEFAULT 0,
    is_portal_access BIT DEFAULT 0,
    portal_username NVARCHAR(100),

    -- Geographic coordinates
    billing_long DECIMAL(10,7),
    billing_lat DECIMAL(10,7),
    shipping_long DECIMAL(10,7),
    shipping_lat DECIMAL(10,7),

    -- Custom fields
    custom_field13 NVARCHAR(MAX),
    custom_field14 NVARCHAR(MAX),
    description NVARCHAR(MAX),
    tag NVARCHAR(MAX),
    budget_code NVARCHAR(50),
    total_score DECIMAL(10,2),
    number_days_not_interacted NVARCHAR(50),
    related_users NVARCHAR(MAX),

    -- Audit fields (consistent with TikTok Shop pattern)
    created_date DATETIME2,
    created_by NVARCHAR(255),
    modified_date DATETIME2,
    modified_by NVARCHAR(255),
    last_modified_date DATETIME2,

    -- ETL metadata (consistent with TikTok Shop pattern)
    etl_created_at DATETIME2 DEFAULT GETUTCDATE(),
    etl_updated_at DATETIME2 DEFAULT GETUTCDATE(),
    etl_batch_id NVARCHAR(50),
    etl_source NVARCHAR(50) DEFAULT 'misa_crm_api'
);
GO

-- Create indexes for performance
CREATE INDEX IX_misa_customers_account_number ON staging.misa_customers(account_number);
CREATE INDEX IX_misa_customers_account_code ON staging.misa_customers(account_code);
CREATE INDEX IX_misa_customers_modified_date ON staging.misa_customers(modified_date);
CREATE INDEX IX_misa_customers_etl_updated_at ON staging.misa_customers(etl_updated_at);
CREATE INDEX IX_misa_customers_etl_batch_id ON staging.misa_customers(etl_batch_id);
GO

PRINT '✅ Đã tạo bảng staging.misa_customers thành công!';

-- =====================================================
-- 2. MISA SALE ORDERS FLATTENED STAGING TABLE (117+ fields)
-- =====================================================
PRINT '🛒 Tạo bảng staging.misa_sale_orders_flattened...';

CREATE TABLE staging.misa_sale_orders_flattened (
    -- Composite primary key from business logic
    order_id BIGINT NOT NULL,

    -- ORDER INFORMATION (prefixed with order_)
    order_sale_order_no NVARCHAR(50),
    order_account_name NVARCHAR(500),
    order_sale_order_name NVARCHAR(MAX),
    order_sale_order_amount DECIMAL(15,2),
    order_sale_order_date DATETIME2,
    order_due_date DATETIME2,
    order_status NVARCHAR(50),
    order_delivery_status NVARCHAR(50),
    order_pay_status NVARCHAR(50),
    order_revenue_status NVARCHAR(50),

    -- Order financial summary
    order_total_summary DECIMAL(15,2),
    order_tax_summary DECIMAL(15,2),
    order_discount_summary DECIMAL(15,2),
    order_to_currency_summary DECIMAL(15,2),
    order_total_receipted_amount DECIMAL(15,2),
    order_balance_receipt_amount DECIMAL(15,2),
    order_invoiced_amount DECIMAL(15,2),
    order_un_invoiced_amount DECIMAL(15,2),

    -- Order currency
    order_currency_type NVARCHAR(10),
    order_exchange_rate DECIMAL(10,4),
    order_is_use_currency BIT,

    -- Order addresses
    order_billing_address NVARCHAR(MAX),
    order_billing_country NVARCHAR(100),
    order_billing_province NVARCHAR(100),
    order_billing_district NVARCHAR(100),
    order_billing_ward NVARCHAR(100),
    order_billing_street NVARCHAR(255),
    order_billing_code NVARCHAR(20),

    order_shipping_address NVARCHAR(MAX),
    order_shipping_country NVARCHAR(100),
    order_shipping_province NVARCHAR(100),
    order_shipping_district NVARCHAR(100),
    order_shipping_ward NVARCHAR(100),
    order_shipping_street NVARCHAR(255),
    order_shipping_code NVARCHAR(20),

    -- Order contact info
    order_phone NVARCHAR(50),
    order_billing_contact NVARCHAR(255),
    order_shipping_contact_name NVARCHAR(255),

    -- Order business info
    order_organization_unit_name NVARCHAR(255),
    order_owner_name NVARCHAR(255),
    order_employee_code NVARCHAR(50),
    order_account_code NVARCHAR(50),
    order_contact_code NVARCHAR(50),

    -- Order important dates
    order_book_date DATETIME2,
    order_deadline_date DATETIME2,
    order_delivery_date DATETIME2,
    order_paid_date DATETIME2,
    order_invoice_date DATETIME2,
    order_production_date DATETIME2,

    -- ITEM INFORMATION (prefixed with item_)
    item_id BIGINT NOT NULL,
    item_product_code NVARCHAR(50),
    item_unit NVARCHAR(20),
    item_usage_unit NVARCHAR(20),
    item_price DECIMAL(15,2),
    item_amount DECIMAL(10,2),
    item_usage_unit_amount DECIMAL(10,2),
    item_usage_unit_price DECIMAL(15,2),
    item_total DECIMAL(15,2),
    item_to_currency DECIMAL(15,2),
    item_discount DECIMAL(15,2),
    item_tax DECIMAL(15,2),
    item_tax_percent NVARCHAR(10),
    item_discount_percent DECIMAL(5,2),
    item_price_after_tax DECIMAL(15,2),
    item_price_after_discount DECIMAL(15,2),
    item_to_currency_after_discount DECIMAL(15,2),

    -- Item product details
    item_description NVARCHAR(MAX),
    item_description_product NVARCHAR(MAX),
    item_stock_name NVARCHAR(255),
    item_batch_number NVARCHAR(100),
    item_serial_number NVARCHAR(100),
    item_expire_date DATETIME2,

    -- Item dimensions
    item_height DECIMAL(10,2),
    item_width DECIMAL(10,2),
    item_length DECIMAL(10,2),
    item_radius DECIMAL(10,2),
    item_mass DECIMAL(10,2),
    item_exist_amount DECIMAL(10,2),
    item_shipping_amount DECIMAL(10,2),

    -- Item business fields
    item_sort_order INT,
    item_ratio DECIMAL(10,4),
    item_operator NVARCHAR(20),
    item_promotion NVARCHAR(MAX),
    item_is_promotion BIT,
    item_custom_field1 DECIMAL(15,2),
    item_produced_quantity DECIMAL(10,2),
    item_quantity_ordered DECIMAL(10,2),
    item_sale_order_product NVARCHAR(MAX),

    -- Flattening metadata
    has_multiple_items BIT DEFAULT 0,
    total_items_in_order INT DEFAULT 0,

    -- ETL metadata (consistent with TikTok Shop pattern)
    etl_created_at DATETIME2 DEFAULT GETUTCDATE(),
    etl_updated_at DATETIME2 DEFAULT GETUTCDATE(),
    etl_batch_id NVARCHAR(50),
    etl_source NVARCHAR(50) DEFAULT 'misa_crm_api',

    -- Composite primary key constraint
    CONSTRAINT PK_misa_sale_orders_flattened PRIMARY KEY (order_id, item_id)
);
GO

-- Create indexes for performance
CREATE INDEX IX_misa_sale_orders_order_id ON staging.misa_sale_orders_flattened(order_id);
CREATE INDEX IX_misa_sale_orders_item_id ON staging.misa_sale_orders_flattened(item_id);
CREATE INDEX IX_misa_sale_orders_order_date ON staging.misa_sale_orders_flattened(order_sale_order_date);
CREATE INDEX IX_misa_sale_orders_product_code ON staging.misa_sale_orders_flattened(item_product_code);
CREATE INDEX IX_misa_sale_orders_account_name ON staging.misa_sale_orders_flattened(order_account_name);
CREATE INDEX IX_misa_sale_orders_etl_updated_at ON staging.misa_sale_orders_flattened(etl_updated_at);
CREATE INDEX IX_misa_sale_orders_etl_batch_id ON staging.misa_sale_orders_flattened(etl_batch_id);
GO

PRINT '✅ Đã tạo bảng staging.misa_sale_orders_flattened thành công!';

-- =====================================================
-- 3. MISA CONTACTS STAGING TABLE (62 fields from API)
-- =====================================================
PRINT '👥 Tạo bảng staging.misa_contacts...';

CREATE TABLE staging.misa_contacts (
    -- Primary identifiers
    id BIGINT PRIMARY KEY,
    contact_code NVARCHAR(50),
    account_code NVARCHAR(50),

    -- Name information
    contact_name NVARCHAR(255),
    first_name NVARCHAR(100),
    last_name NVARCHAR(100),
    salutation NVARCHAR(20),

    -- Contact information
    mobile NVARCHAR(50),
    office_tel NVARCHAR(50),
    other_phone NVARCHAR(50),
    office_email NVARCHAR(255),
    email NVARCHAR(255),
    facebook NVARCHAR(255),
    zalo NVARCHAR(100),

    -- Business information
    account_name NVARCHAR(500),
    title NVARCHAR(100),
    department NVARCHAR(100),
    account_type NVARCHAR(100),

    -- Address information
    mailing_address NVARCHAR(MAX),
    mailing_country NVARCHAR(100),
    mailing_province NVARCHAR(100),
    mailing_district NVARCHAR(100),
    mailing_ward NVARCHAR(100),
    mailing_street NVARCHAR(255),
    mailing_zip NVARCHAR(20),

    shipping_address NVARCHAR(MAX),
    shipping_country NVARCHAR(100),
    shipping_province NVARCHAR(100),
    shipping_district NVARCHAR(100),
    shipping_ward NVARCHAR(100),
    shipping_street NVARCHAR(255),
    shipping_zip NVARCHAR(20),

    -- Geographic coordinates
    mailing_long DECIMAL(10,7),
    mailing_lat DECIMAL(10,7),
    shipping_long DECIMAL(10,7),
    shipping_lat DECIMAL(10,7),

    -- Personal information
    date_of_birth DATETIME2,
    gender NVARCHAR(20),
    married_status NVARCHAR(50),

    -- Financial information
    bank_account NVARCHAR(100),
    bank_name NVARCHAR(255),

    -- Preferences
    email_opt_out BIT DEFAULT 0,
    phone_opt_out BIT DEFAULT 0,

    -- Business fields
    lead_source NVARCHAR(100),
    customer_since_date DATETIME2,
    organization_unit_name NVARCHAR(255),
    owner_name NVARCHAR(255),
    form_layout NVARCHAR(100),

    -- Status and scoring
    inactive BIT DEFAULT 0,
    total_score DECIMAL(10,2),

    -- Interaction tracking
    last_interaction_date DATETIME2,
    last_visit_date DATETIME2,
    last_call_date DATETIME2,
    number_days_not_interacted DECIMAL(10,2),

    -- System fields
    is_public BIT DEFAULT 0,
    tag NVARCHAR(MAX),
    related_users NVARCHAR(MAX),
    description NVARCHAR(MAX),

    -- Audit fields
    created_date DATETIME2,
    created_by NVARCHAR(255),
    modified_date DATETIME2,
    modified_by NVARCHAR(255),

    -- ETL metadata (consistent with TikTok Shop pattern)
    etl_created_at DATETIME2 DEFAULT GETUTCDATE(),
    etl_updated_at DATETIME2 DEFAULT GETUTCDATE(),
    etl_batch_id NVARCHAR(50),
    etl_source NVARCHAR(50) DEFAULT 'misa_crm_api'
);
GO

-- Create indexes for performance
CREATE INDEX IX_misa_contacts_account_code ON staging.misa_contacts(account_code);
CREATE INDEX IX_misa_contacts_contact_code ON staging.misa_contacts(contact_code);
CREATE INDEX IX_misa_contacts_modified_date ON staging.misa_contacts(modified_date);
CREATE INDEX IX_misa_contacts_etl_updated_at ON staging.misa_contacts(etl_updated_at);
CREATE INDEX IX_misa_contacts_etl_batch_id ON staging.misa_contacts(etl_batch_id);
GO

PRINT '✅ Đã tạo bảng staging.misa_contacts thành công!';

-- =====================================================
-- 4. MISA STOCKS STAGING TABLE (9 fields from API)
-- =====================================================
PRINT '📦 Tạo bảng staging.misa_stocks...';

CREATE TABLE staging.misa_stocks (
    -- Primary identifiers
    stock_code NVARCHAR(50) PRIMARY KEY,
    act_database_id UNIQUEIDENTIFIER,
    async_id UNIQUEIDENTIFIER,

    -- Basic information
    stock_name NVARCHAR(255),
    description NVARCHAR(MAX),

    -- Status
    inactive BIT DEFAULT 0,

    -- Audit fields
    created_date DATETIME2,
    created_by NVARCHAR(255),
    modified_date DATETIME2,
    modified_by NVARCHAR(255),

    -- ETL metadata (consistent with TikTok Shop pattern)
    etl_created_at DATETIME2 DEFAULT GETUTCDATE(),
    etl_updated_at DATETIME2 DEFAULT GETUTCDATE(),
    etl_batch_id NVARCHAR(50),
    etl_source NVARCHAR(50) DEFAULT 'misa_crm_api'
);
GO

-- Create indexes for performance
CREATE INDEX IX_misa_stocks_stock_code ON staging.misa_stocks(stock_code);
CREATE INDEX IX_misa_stocks_modified_date ON staging.misa_stocks(modified_date);
CREATE INDEX IX_misa_stocks_etl_updated_at ON staging.misa_stocks(etl_updated_at);
CREATE INDEX IX_misa_stocks_etl_batch_id ON staging.misa_stocks(etl_batch_id);
GO

PRINT '✅ Đã tạo bảng staging.misa_stocks thành công!';

-- =====================================================
-- 5. MISA PRODUCTS STAGING TABLE (35 fields from API)
-- =====================================================
PRINT '🛍️ Tạo bảng staging.misa_products...';

CREATE TABLE staging.misa_products (
    -- Primary identifiers
    id BIGINT PRIMARY KEY,
    product_code NVARCHAR(50) UNIQUE,

    -- Basic information
    product_name NVARCHAR(500),
    product_category NVARCHAR(100),
    usage_unit NVARCHAR(50),
    description NVARCHAR(MAX),
    sale_description NVARCHAR(MAX),

    -- Pricing information
    unit_price DECIMAL(15,2),
    purchased_price DECIMAL(15,2),
    unit_cost DECIMAL(15,2),
    unit_price1 DECIMAL(15,2),
    unit_price2 DECIMAL(15,2),
    unit_price_fixed DECIMAL(15,2),
    price_after_tax BIT DEFAULT 0,

    -- Tax information
    tax NVARCHAR(50),
    is_use_tax BIT DEFAULT 0,

    -- Product properties
    product_properties NVARCHAR(100),
    is_follow_serial_number BIT DEFAULT 0,
    is_set_product BIT DEFAULT 0,
    quantity_formula NVARCHAR(MAX),

    -- Inventory information
    default_stock NVARCHAR(100),

    -- Warranty information
    warranty_period NVARCHAR(100),
    warranty_description NVARCHAR(MAX),

    -- System fields
    organization_unit_name NVARCHAR(255),
    owner_name NVARCHAR(255),
    form_layout NVARCHAR(100),
    source NVARCHAR(100),

    -- Status
    inactive BIT DEFAULT 0,
    is_public BIT DEFAULT 0,

    -- Media
    avatar NVARCHAR(MAX),
    tag NVARCHAR(MAX),

    -- Audit fields
    created_date DATETIME2,
    created_by NVARCHAR(255),
    modified_date DATETIME2,
    modified_by NVARCHAR(255),

    -- ETL metadata (consistent with TikTok Shop pattern)
    etl_created_at DATETIME2 DEFAULT GETUTCDATE(),
    etl_updated_at DATETIME2 DEFAULT GETUTCDATE(),
    etl_batch_id NVARCHAR(50),
    etl_source NVARCHAR(50) DEFAULT 'misa_crm_api'
);
GO

-- Create indexes for performance
CREATE INDEX IX_misa_products_product_code ON staging.misa_products(product_code);
CREATE INDEX IX_misa_products_product_category ON staging.misa_products(product_category);
CREATE INDEX IX_misa_products_modified_date ON staging.misa_products(modified_date);
CREATE INDEX IX_misa_products_etl_updated_at ON staging.misa_products(etl_updated_at);
CREATE INDEX IX_misa_products_etl_batch_id ON staging.misa_products(etl_batch_id);
GO

PRINT '✅ Đã tạo bảng staging.misa_products thành công!';

-- =====================================================
-- SUMMARY & COMPLETION
-- =====================================================

PRINT '🎉 MISA CRM staging tables creation completed successfully!';
PRINT '';
PRINT '📊 Created tables:';
PRINT '   ✅ staging.misa_customers (81 fields)';
PRINT '   ✅ staging.misa_sale_orders_flattened (117+ fields)';
PRINT '   ✅ staging.misa_contacts (62 fields)';
PRINT '   ✅ staging.misa_stocks (9 fields)';
PRINT '   ✅ staging.misa_products (35 fields)';
PRINT '';
PRINT '🔍 All indexes created for optimal performance';
PRINT '🔗 Compatible with existing TikTok Shop infrastructure';
PRINT '📈 Ready for MISA CRM ETL pipeline deployment';
PRINT '';
PRINT '🚀 Next steps:';
PRINT '   1. Test database connection';
PRINT '   2. Run initial data backfill';
PRINT '   3. Deploy incremental ETL DAGs';
PRINT '   4. Setup monitoring and alerts';

-- =====================================================
-- VERIFICATION QUERIES (for testing)
-- =====================================================

-- Uncomment to verify table creation:
-- SELECT TABLE_NAME, TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES
-- WHERE TABLE_SCHEMA = 'staging' AND TABLE_NAME LIKE 'misa_%'
-- ORDER BY TABLE_NAME;

-- Uncomment to verify indexes:
-- SELECT
--     t.name AS TableName,
--     i.name AS IndexName,
--     i.type_desc AS IndexType
-- FROM sys.indexes i
-- INNER JOIN sys.tables t ON i.object_id = t.object_id
-- INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
-- WHERE s.name = 'staging' AND t.name LIKE 'misa_%'
-- ORDER BY t.name, i.name;
