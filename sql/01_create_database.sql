-- Create database if not exists
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'Facolos_Database')
BEGIN
    CREATE DATABASE Facolos_Database;
END;
GO

USE Facolos_Database;
GO

-- Create staging schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Facolos_Staging')
BEGIN
    EXEC('CREATE SCHEMA Facolos_Staging');
END;
GO
