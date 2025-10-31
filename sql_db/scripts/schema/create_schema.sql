IF (NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'test')) 
BEGIN
    EXEC ('CREATE SCHEMA [test]')
END