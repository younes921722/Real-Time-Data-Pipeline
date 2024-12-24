--Drop Table[AdventureWorksLT2022].[SalesLT].[critical_users_data]

USE [AdventureWorksLT2022];


-- Create the SalesLT schema if it does not exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'SalesLT')
BEGIN
    EXEC('CREATE SCHEMA SalesLT');
END;

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'SalesLTSecret')
BEGIN
    EXEC('CREATE SCHEMA SalesLTSecret');
END;

CREATE TABLE SalesLTSecret.critical_users_data (
    id INT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    username VARCHAR(50) UNIQUE NOT NULL
);



CREATE TABLE SalesLT.non_critical_users_data (
    id INT IDENTITY(1,1) PRIMARY KEY,
    critical_user_id INT NOT NULL,  -- Added column for foreign key
    gender VARCHAR(10),
    address VARCHAR(MAX),
    postcode VARCHAR(50),
    dob DATETIME2,
    registered DATETIME2,
    phone VARCHAR(20),
    picture VARCHAR(MAX),
    FOREIGN KEY (critical_user_id) REFERENCES SalesLTSecret.critical_users_data(id)  -- Foreign key constraint
);
