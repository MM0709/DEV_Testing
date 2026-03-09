USE [DGDB_PL]
GO
/****** Object:  StoredProcedure [dbo].[Fx_Updates_Region_count]    Script Date: 2/25/2026 2:13:26 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

Alter PROCEDURE [dbo].[Fx_Updates_Region_count](  @group_Id INT, @Counts NVARCHAR(250))
AS  
BEGIN  
	UPDATE DGDB_PL..FX_UPDATES_COUNTS SET Counts = @Counts WHERE Group_Id = @group_Id 
	print '15'
END
Go