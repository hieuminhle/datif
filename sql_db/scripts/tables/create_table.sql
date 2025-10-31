IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[test].[testtable]') AND type in (N'U'))
DROP TABLE [test].[testtable]
GO

IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='testtable' and xtype='U')
CREATE TABLE [test].[testtable](
	[sourcesystem] [nvarchar](50) NOT NULL,
	[lfdnr] [nvarchar](20) NOT NULL,
	[group] [nvarchar](50) NOT NULL,
	[subgroup] [nvarchar](100) NULL
) ON [PRIMARY]
GO
