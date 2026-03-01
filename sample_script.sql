-- Sample SQL script for testing SQL Batcher
-- This script creates a test database, tables, and inserts sample data

-- Create test table
CREATE TABLE TestBatch1 (
    ID INT PRIMARY KEY,
    Name NVARCHAR(100),
    CreatedDate DATETIME DEFAULT GETDATE()
)
GO

-- Insert sample data - Batch 1
INSERT INTO TestBatch1 (ID, Name) VALUES (1, 'First Record')
INSERT INTO TestBatch1 (ID, Name) VALUES (2, 'Second Record')
INSERT INTO TestBatch1 (ID, Name) VALUES (3, 'Third Record')
GO

-- Create another test table
CREATE TABLE TestBatch2 (
    ProductID INT PRIMARY KEY,
    ProductName NVARCHAR(100),
    Price DECIMAL(10,2)
)
GO

-- Insert product data
INSERT INTO TestBatch2 (ProductID, ProductName, Price) VALUES (1, 'Widget', 19.99)
INSERT INTO TestBatch2 (ProductID, ProductName, Price) VALUES (2, 'Gadget', 29.99)
INSERT INTO TestBatch2 (ProductID, ProductName, Price) VALUES (3, 'Doohickey', 39.99)
GO

-- Update records
UPDATE TestBatch1 SET Name = 'Updated First' WHERE ID = 1
GO

-- Create a view
CREATE VIEW vw_ProductSummary AS
SELECT 
    ProductID,
    ProductName,
    Price,
    CASE 
        WHEN Price < 25 THEN 'Budget'
        WHEN Price < 35 THEN 'Standard'
        ELSE 'Premium'
    END AS PriceCategory
FROM TestBatch2
GO

-- Select data to verify
SELECT * FROM TestBatch1
GO

SELECT * FROM vw_ProductSummary
GO

-- Clean up (optional - uncomment to include cleanup)
-- DROP VIEW vw_ProductSummary
-- GO
-- DROP TABLE TestBatch2
-- GO
-- DROP TABLE TestBatch1
-- GO
