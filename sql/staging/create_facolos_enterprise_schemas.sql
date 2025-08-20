-- Facolos Enterprise Data Warehouse
-- Main schema creation for all data sources
-- Created: 2024-01-20

USE Facolos_Database;
GO

-- ========================
-- ENTERPRISE SCHEMAS
-- ========================

-- E-Commerce Platforms
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Facolos_TikTok_Shop')
    EXEC('CREATE SCHEMA Facolos_TikTok_Shop');
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Facolos_Shopee')
    EXEC('CREATE SCHEMA Facolos_Shopee');
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Facolos_Lazada')
    EXEC('CREATE SCHEMA Facolos_Lazada');
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Facolos_Sendo')
    EXEC('CREATE SCHEMA Facolos_Sendo');
GO

-- Advertising Platforms  
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Facolos_TikTok_Ads')
    EXEC('CREATE SCHEMA Facolos_TikTok_Ads');
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Facolos_Facebook_Ads')
    EXEC('CREATE SCHEMA Facolos_Facebook_Ads');
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Facolos_Google_Ads')
    EXEC('CREATE SCHEMA Facolos_Google_Ads');
GO

-- Analytics Platforms
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Facolos_Google_Analytics')
    EXEC('CREATE SCHEMA Facolos_Google_Analytics');
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Facolos_Facebook_Pixel')
    EXEC('CREATE SCHEMA Facolos_Facebook_Pixel');
GO

-- CRM/ERP Systems
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Facolos_MISA_CRM')
    EXEC('CREATE SCHEMA Facolos_MISA_CRM');
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Facolos_Internal_ERP')
    EXEC('CREATE SCHEMA Facolos_Internal_ERP');
GO

-- Data Marts (Aggregated Business Intelligence)
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Facolos_Data_Mart')
    EXEC('CREATE SCHEMA Facolos_Data_Mart');
GO

-- ETL Control & Metadata
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Facolos_ETL_Control')
    EXEC('CREATE SCHEMA Facolos_ETL_Control');
GO

-- ========================
-- ETL CONTROL TABLES
-- ========================

-- ETL Batch tracking
IF OBJECT_ID('Facolos_ETL_Control.batch_runs', 'U') IS NOT NULL
    DROP TABLE Facolos_ETL_Control.batch_runs;
GO

CREATE TABLE Facolos_ETL_Control.batch_runs (
    batch_id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    data_source NVARCHAR(50) NOT NULL,
    table_name NVARCHAR(100) NOT NULL,
    start_time DATETIME2 DEFAULT GETUTCDATE(),
    end_time DATETIME2,
    status NVARCHAR(20) CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'CANCELLED')),
    records_extracted INT DEFAULT 0,
    records_loaded INT DEFAULT 0,
    error_message NVARCHAR(MAX),
    created_by NVARCHAR(50) DEFAULT 'facolos_etl_system'
);
GO

-- Data source configuration
IF OBJECT_ID('Facolos_ETL_Control.data_sources', 'U') IS NOT NULL
    DROP TABLE Facolos_ETL_Control.data_sources;
GO

CREATE TABLE Facolos_ETL_Control.data_sources (
    source_id NVARCHAR(50) PRIMARY KEY,
    source_name NVARCHAR(100) NOT NULL,
    source_type NVARCHAR(50) NOT NULL, -- 'ecommerce', 'advertising', 'analytics', 'crm'
    is_active BIT DEFAULT 1,
    last_extract_time DATETIME2,
    extract_frequency_hours INT DEFAULT 6,
    created_at DATETIME2 DEFAULT GETUTCDATE(),
    updated_at DATETIME2 DEFAULT GETUTCDATE()
);
GO

-- Insert initial data sources
INSERT INTO Facolos_ETL_Control.data_sources (source_id, source_name, source_type, extract_frequency_hours)
VALUES 
    ('tiktok_shop', 'TikTok Shop', 'ecommerce', 6),
    ('shopee', 'Shopee', 'ecommerce', 6),
    ('lazada', 'Lazada', 'ecommerce', 6),
    ('tiktok_ads', 'TikTok Ads', 'advertising', 24),
    ('facebook_ads', 'Facebook Ads', 'advertising', 24),
    ('google_analytics', 'Google Analytics', 'analytics', 24),
    ('misa_crm', 'MISA CRM', 'crm', 24);
GO

PRINT 'Facolos Enterprise schemas created successfully!';
PRINT 'Available schemas:';
PRINT '- E-Commerce: TikTok_Shop, Shopee, Lazada, Sendo';
PRINT '- Advertising: TikTok_Ads, Facebook_Ads, Google_Ads';
PRINT '- Analytics: Google_Analytics, Facebook_Pixel';
PRINT '- CRM/ERP: MISA_CRM, Internal_ERP';
PRINT '- Data Marts: Data_Mart';
PRINT '- ETL Control: ETL_Control';
GO
