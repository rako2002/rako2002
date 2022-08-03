/****** Object:  Table [mtdta].[DataVaultHubTransform]    Script Date: 12/03/2020 12:26:56 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [mtdta].[DataVaultHubTransform](
	[SrcDatabase] [varchar](100) NULL,
	[SrcSchema] [varchar](100) NULL,
	[SrcObject] [varchar](100) NOT NULL,
	[SrcBusinessKey] [varchar](100) NOT NULL,
	[HubSchema] [varchar](100) NULL,
	[HubName] [varchar](100) NOT NULL,
	[HubBusinessKey] [varchar](100) NULL,
	[ColumnPosition] [int] NOT NULL,
	[RecordSource] [varchar](255) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [mtdta].[DataVaultLinkTransform]    Script Date: 12/03/2020 12:26:56 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [mtdta].[DataVaultLinkTransform](
	[SrcDatabase] [varchar](100) NULL,
	[SrcSchema] [varchar](100) NULL,
	[SrcObject] [varchar](100) NOT NULL,
	[SrcBusinessKey] [varchar](100) NOT NULL,
	[LinkSchema] [varchar](100) NULL,
	[LinkName] [varchar](100) NULL,
	[LinkHubHashKeyName] [varchar](100) NOT NULL,
	[LinkHubHashKeyColumnPosition] [int] NULL,
	[HubName] [varchar](100) NULL,
	[RecordSource] [varchar](255) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [mtdta].[DataVaultRefTransform]    Script Date: 12/03/2020 12:26:56 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [mtdta].[DataVaultRefTransform](
	[SrcDatabase] [varchar](100) NULL,
	[SrcSchema] [varchar](100) NULL,
	[SrcObject] [varchar](100) NOT NULL,
	[RefSchema] [varchar](100) NULL,
	[RefName] [varchar](100) NOT NULL,
	[RefBusinessKey] [varchar](100) NULL,
	[RecordSource] [varchar](255) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [mtdta].[DataVaultSatTransform]    Script Date: 12/03/2020 12:26:56 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [mtdta].[DataVaultSatTransform](
	[SrcDatabase] [varchar](100) NULL,
	[SrcSchema] [varchar](100) NULL,
	[SrcObject] [varchar](100) NOT NULL,
	[SrcColumn] [varchar](100) NOT NULL,
	[SatSchema] [varchar](100) NULL,
	[SatName] [varchar](100) NOT NULL,
	[SatColumn] [varchar](100) NULL,
	[ColumnPosition] [int] NOT NULL,
	[IsColumnBusinessKey] [bit] NOT NULL,
	[IsColumnPartOfHashDiffKey] [bit] NOT NULL,
	[RecordSource] [varchar](255) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [mtdta].[SrcFKConstraint]    Script Date: 12/03/2020 12:26:56 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [mtdta].[SrcFKConstraint](
	[ParentTable] [varchar](100) NULL,
	[ParentColumn] [varchar](100) NULL,
	[ChildTable] [varchar](100) NOT NULL,
	[ChildColumn] [varchar](100) NULL,
	[ColumnPosition] [int] NOT NULL,
	[FKConstraintName] [varchar](255) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [mtdta].[SrcPKConstraint]    Script Date: 12/03/2020 12:26:56 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [mtdta].[SrcPKConstraint](
	[SrcTable] [varchar](100) NULL,
	[SrcColumn] [varchar](100) NULL,
	[ColumnPosition] [int] NOT NULL,
	[PKConstraintName] [varchar](255) NULL
) ON [PRIMARY]
GO
ALTER TABLE [mtdta].[DataVaultSatTransform] ADD  DEFAULT ((0)) FOR [IsColumnBusinessKey]
GO
ALTER TABLE [mtdta].[DataVaultSatTransform] ADD  DEFAULT ((0)) FOR [IsColumnPartOfHashDiffKey]
GO
