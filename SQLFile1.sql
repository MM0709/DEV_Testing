USE [DGDB_PL]
GO
/****** Object:  StoredProcedure [dbo].[spProcess10SourceFileProcessing]    Script Date: 2/20/2026 1:03:27 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



/*
Jan van Berghem - Nov 2014
Source Update Process, run this procedure with as input parameter the Source number from table cfSource, 
2nd Input parameter will be Until what Step to process, default = all Steps
Steps:
1. Create Database for Source if not exist based on cfSource
2. Update Source in local DB with Rules from cfSourceRules
3. Generate entries / Update table cfSourceMapRules 
4. Generate Sourcetable for fields mapped in cfSourceMapRules set zActive = 1
5. Generate SourceFileExtract Based on SourceFile ( = SourceFile Transaposed with overview of changes)
*/


/*

exec spProcess10SourceFileProcessing 623
*/
ALTER PROCEDURE [dbo].[spProcess10SourceFileProcessing] 


	
	@srcID  INT = 731,				-- srcIFD from cfSource Table	
	@DebugMode BIT = 0,				-- Print additional Info

	@RecordCount INTEGER = 0 OUTPUT	-- RETURNS Active records, marked fort Processing (zActive in Sourcefile = 1)
	
AS 
	Declare
	@srcName NVARCHAR(50)='',
	@srcSrtCode NVARCHAR(4),
	@srcVendorCode NVARCHAR(50)='',
	@srcTableName NVARCHAR(50)='', 
	@srcGroup NVARCHAR(50)='',	
	@srcSptCode NVARCHAR(4),
	@srcSupplierPlantMapOverride BIT = 0, 
	@srcAdhocFlag BIT,
	@srcIncrementalFlag BIT,
	@srcFilterCompany	BIT,
	@srgID INTEGER,
	@DBName NVARCHAR(40)='',
	@SourceTable NVARCHAR(80),	
	@Source NVARCHAR(40),
	@SQL NVARCHAR(4000)='',
	@SQL2 NVARCHAR(4000)='',
	@SQL3 NVARCHAR(4000)='',
	@Columns NVARCHAR(4000) = '',
	@ColumnsAlias NVARCHAR(4000)='',
	@srrID BIGINT,
	@srrUpdateType NVARCHAR(1),
	@srrFieldname NVARCHAR(50),
	@srrUpdate NVARCHAR(4000),
	@srrWhere NVARCHAR(4000),
	@srrActive  BIT,
	@srrFieldType NVARCHAR(255),
	@ResetSourceTableValue NVARCHAR(255)='',
	@DatType NVARCHAR(10),
	@zFieldReset NVARCHAR(4000)='',
	@LastRecord INTEGER,
	@RowCount INTEGER,	
	@RowCount2 INTEGER,	
	@Total INTEGER,
	@fldID INTEGER,
	@fldName NVARCHAR(50),
	@Material NVARCHAR(40),
	@MaterialGroup NVARCHAR(40),
	@ProductHierarchy NVARCHAR(40),
	@tbfRequired BIT,
	@tbfSeq INTEGER,
	@tblPK AS NVARCHAR(255),
	@srmID BIGINT,
	@srmSourceMap NVARCHAR(4000),	
	@srmSourceMap2 NVARCHAR(4000),	
	@srmFldName NVARCHAR(50),
	@srmFldID INTEGER,
	@srmPKSourceMap NVARCHAR(4000),
	@srdID BIGINT,
	@srdValueOld NVARCHAR(255),
	@srdSrmPK NVARCHAR(255),
	@StartDate DATETIME,
	@StatusFlag SMALLINT,
	@DateDiff NUMERIC(8,2),
	@SAPClientDataDB NVARCHAR(40),
	@srcSystem NVARCHAR(3),
	@srcClient NVARCHAR(3),
	@srcPHOverride	BIT
	
	

	DECLARE @TableExists TABLE ( Exist BIT)
	DECLARE @TableGroupByValues TABLE ( KeyValue NVARCHAR(255),Done BIT)
	DECLARE @TableDataType TABLE ( DatType NVARCHAR(255))
	DECLARE @TableRowCount TABLE (Number INT)
	DECLARE @TableCHeckID TABLE (ID INT)
	DECLARE @TableZFields TABLE ( Field NVARCHAR(40),done BIT)
	DECLARE @TableStats TABLE (StatusFlag NVARCHAR(40), Number INT)

	SET NOCOUNT ON
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED 
	SET @startDate = GETDATE()
			
	PRINT '-------------------------------------------------------------------------'
	PRINT ' 1 Initialize Database'
	PRINT '-------------------------------------------------------------------------'


    SELECT	@srcName = srcName,
			@srcSrtCode = srcSrtCode,
			@srcVendorCode = srcVendorCode ,
            @srcTableName = srcTableName,
			@srcClient = srcClient,
			@srcGroup = srcGroup,
			@srcSystem = srcSystem,
			@srcSptCode = srcSptCode,
			@srcAdhocFlag = srcAdhocFlag,
			@srcIncrementalFlag = srcIncrementalFlag,
			@srcSupplierPlantMapOverride = srcSupplierPlantMapOverride,
			@srcFilterCompany = srcFilterCompany,
			@srcPHOverride = srcPHOverride
        FROM cfSource
        WHERE srcid = @srcID
	SET @Source = CAST(@srcID AS CHAR(5)) + ' - ' + @SrcName + ''''

	SELECT @SAPClientDataDB = trdDatabase FROM dbo.cfTargetData WHERE trdSystem = @srcSystem AND trdClient = @srcClient	

	IF @srcName = ''
		BEGIN
			PRINT 'No Valid Source Number (srcID) submitted, aborting procedure'
			RETURN
		END
		
	/*CREATE Source Specific DATABSE IF Needed*/
	SET @DBName =  @srcSrtCode + @srcVendorCode + '_' + @srcName + '_' + CAST(@srcID AS NVARCHAR)
	SET @SourceTable = @DBName + '.dbo.['+ @srcTableName + ']'
	
	-- Update adhoc EMEA with Suppliers same as supplierplantmap (Diego)
		IF @srcID IN ('497','577','623','624','640','732','733')
		BEGIN
			SET @SQL =
				'UPDATE '+@SourceTable+'
				 SET zSalesOrg=REPLACE(RTRIM(LTRIM(zSalesOrg)), '' '', '''') ,
				 zDefaultPlant=REPLACE(RTRIM(LTRIM(zDefaultPlant)), '' '', ''''),
				 zSupplierPlantMap=REPLACE(RTRIM(LTRIM(zSupplierPlantMap)), '' '', ''''),
				 MatGroup=REPLACE(RTRIM(LTRIM(MatGroup)), '' '', ''''),
				 zSupplierCurrency=REPLACE(RTRIM(LTRIM(zSupplierCurrency)), '' '', ''''),
				 zFreightUplift=REPLACE(RTRIM(LTRIM(zFreightUplift)), '' '', ''''),
				 SERNP=REPLACE(RTRIM(LTRIM(SERNP)), '' '', ''''),
				 MatGroup1=CASE WHEN REPLACE(RTRIM(LTRIM(MatGroup1)), '' '', '''')=''HWD'' THEN ''HDW'' ELSE REPLACE(RTRIM(LTRIM(MatGroup1)), '' '', '''') END
				
				 UPDATE '+@SourceTable+'
				SET zSupplierPlantMap= STUFF((SELECT distinct ''|'' + CASE 
																	WHEN RIGHT(Z.StringValue,6) IN (''300637'',''301220'') 
																	AND LEFT(Z.StringValue,4) IN (''4001'',''4601'',''4611'',''5001'',''5501'',''5521'') 
																	AND dgdb.dbo.RegexIsMatch(''4600|4100|5000|5500|5520'', [zSalesOrg],1) = 1
																	AND [MatGroup]=''10000''
																	THEN Z.StringValue+''|''+LEFT(Z.StringValue,4)+''=301084''
																	ELSE Z.StringValue
																	END
									   FROM  '+@SourceTable+' N
									   CROSS APPLY dgdb.dbo.fnSplitStringToTable(zSupplierPlantMap,''|'') Z
									   WHERE N.Material=M.Material
									   FOR XML PATH('''')), 1, 1, '''') 
				FROM  '+@SourceTable+' M
				CROSS APPLY dgdb.dbo.fnSplitStringToTable(zSupplierPlantMap,''|'') A 
				WHERE M.Material=Material

								 UPDATE '+@SourceTable+'
				SET zSupplierPlantMap= STUFF((SELECT distinct ''|'' + CASE 
																	WHEN RIGHT(Z.StringValue,6) IN (''301546'',''300637'',''301220'') 
																	AND LEFT(Z.StringValue,4) IN (''4001'',''4611'') 
																	AND dgdb.dbo.RegexIsMatch(''4010|5100|5110|5120|5130|5140|5160'', [zSalesOrg],1) = 1
																	AND [MatGroup]=''10000''
																	THEN Z.StringValue+''|''+LEFT(Z.StringValue,4)+''=301700''
																	ELSE Z.StringValue
																	END
									   FROM  '+@SourceTable+' N
									   CROSS APPLY dgdb.dbo.fnSplitStringToTable(zSupplierPlantMap,''|'') Z
									   WHERE N.Material=M.Material
									   FOR XML PATH('''')), 1, 1, '''') 
				FROM  '+@SourceTable+' M
				CROSS APPLY dgdb.dbo.fnSplitStringToTable(zSupplierPlantMap,''|'') A 
				WHERE M.Material=Material


				
				UPDATE '+@SourceTable+'
				SET zSupplierPlantMap= STUFF((SELECT distinct ''|'' + Z.StringValue
									   FROM  '+@SourceTable+' N
									   CROSS APPLY dgdb.dbo.fnSplitStringToTable(zSupplierPlantMap,''|'') Z
									   WHERE N.Material=M.Material
									   FOR XML PATH('''')), 1, 1, '''') 
				FROM  '+@SourceTable+' M
				CROSS APPLY dgdb.dbo.fnSplitStringToTable(zSupplierPlantMap,''|'') A 
				WHERE M.Material=Material

				'

				/*SET @SQL2 =
				'
				UPDATE '+@SourceTable+'
				SET zSupplierPlantMap= STUFF((SELECT distinct ''|'' + CASE 
																	WHEN LEFT(Z.StringValue,4) IN (''4611'') THEN ''''
																	WHEN RIGHT(Z.StringValue,4) IN (''4611'') THEN ''''
																	ELSE Z.StringValue
																	END
									   FROM  '+@SourceTable+' N
									   CROSS APPLY dgdb.dbo.fnSplitStringToTable(zSupplierPlantMap,''|'') Z
									   WHERE N.Material=M.Material
									   FOR XML PATH('''')), 1, 1, '''') 
				FROM  '+@SourceTable+' M
				CROSS APPLY dgdb.dbo.fnSplitStringToTable(zSupplierPlantMap,''|'') A 
				WHERE M.Material=Material	

				UPDATE '+@SourceTable+' SET zSupplierPlantMap=RIGHT(zSupplierPlantMap, LEN(zSupplierPlantMap)-1) WHERE LEFT(zSupplierPlantMap,1)=''|''
							
				'*/
				 
			SET @SQL3 =
				 'UPDATE '+@SourceTable+'
				 SET zSupplier= STUFF((SELECT distinct ''|'' + CASE WHEN RIGHT(Z.StringValue,6) LIKE ''%=%'' AND RIGHT(Z.StringValue,5) NOT IN (''60040'') THEN RIGHT(Z.StringValue,4) 
																	WHEN RIGHT(Z.StringValue,6) LIKE ''%=%'' AND RIGHT(Z.StringValue,5) IN (''60040'') THEN RIGHT(Z.StringValue,5)
																	ELSE RIGHT(Z.StringValue,6) 
																END
									   FROM  '+@SourceTable+' N
									   CROSS APPLY dgdb.dbo.fnSplitStringToTable(zSupplierPlantMap,''|'') Z
									   WHERE N.Material=M.Material
									   FOR XML PATH('''')), 1, 1, '''') 
				 FROM  '+@SourceTable+' M
				 CROSS APPLY dgdb.dbo.fnSplitStringToTable(zSupplierPlantMap,''|'') A 
				 WHERE M.Material=Material
				 
				 UPDATE '+@SourceTable+'
				 SET zPlant= STUFF((SELECT distinct ''|'' + LEFT(Z.StringValue,4)
									   FROM  '+@SourceTable+' N
									   CROSS APPLY dgdb.dbo.fnSplitStringToTable(zSupplierPlantMap,''|'') Z
									   WHERE N.Material=M.Material
									   FOR XML PATH('''')), 1, 1, '''') 
				 FROM  '+@SourceTable+' M
				 CROSS APPLY dgdb.dbo.fnSplitStringToTable(zSupplierPlantMap,''|'') A 
				 WHERE M.Material=Material
				 
				 UPDATE '+@SourceTable+'
				 SET zSalesOrg=
				 CASE
					WHEN (zPlant LIKE ''%4001%'' OR zPlant LIKE ''%4003%'') AND zSalesOrg NOT LIKE ''%4000%'' THEN zSalesOrg+''|4000''
					WHEN (zPlant LIKE ''%4101%'' OR zPlant LIKE ''%4103%'') AND zSalesOrg NOT LIKE ''%4100%'' THEN zSalesOrg+''|4100''				
					WHEN (zPlant LIKE ''%4601%'' OR zPlant LIKE ''%4603%'') AND zSalesOrg NOT LIKE ''%4600%'' THEN zSalesOrg+''|4600''
					WHEN (zPlant LIKE ''%4611%'' OR zPlant LIKE ''%4613%'') AND zSalesOrg NOT LIKE ''%4600%'' THEN zSalesOrg+''|4600''
					WHEN (zPlant LIKE ''%5501%'' OR zPlant LIKE ''%5503%'') AND zSalesOrg NOT LIKE ''%5500%'' THEN zSalesOrg+''|5500''
					WHEN (zPlant LIKE ''%5521%'' OR zPlant LIKE ''%5523%'') AND zSalesOrg NOT LIKE ''%5520%'' THEN zSalesOrg+''|5520''
					WHEN (zPlant LIKE ''%5001%'' OR zPlant LIKE ''%5003%'') AND zSalesOrg NOT LIKE ''%5000%'' THEN zSalesOrg+''|5000''
					ELSE zSalesOrg
				 END'
				 
			EXEC (@SQL+/*@SQL2+*/@SQL3)
			PRINT (@SQL+/*@SQL2+*/@SQL3)
		END	

/*	IF @srcID IN ('497','577','623','624','640','732','733')
		BEGIN
			SET @SQL =
				'UPDATE '+@SourceTable+'
				 SET zSalesOrg=REPLACE(RTRIM(LTRIM(zSalesOrg)), '' '', '''') ,
				 zDefaultPlant=REPLACE(RTRIM(LTRIM(zDefaultPlant)), '' '', ''''),
				 zSupplierPlantMap=REPLACE(RTRIM(LTRIM(zSupplierPlantMap)), '' '', ''''),
				 MatGroup=REPLACE(RTRIM(LTRIM(MatGroup)), '' '', ''''),
				 zSupplierCurrency=REPLACE(RTRIM(LTRIM(zSupplierCurrency)), '' '', ''''),
				 zFreightUplift=REPLACE(RTRIM(LTRIM(zFreightUplift)), '' '', ''''),
				 SERNP=REPLACE(RTRIM(LTRIM(SERNP)), '' '', ''''),
				 MatGroup1=CASE WHEN REPLACE(RTRIM(LTRIM(MatGroup1)), '' '', '''')=''HWD'' THEN ''HDW'' ELSE REPLACE(RTRIM(LTRIM(MatGroup1)), '' '', '''') END
				
				
				 UPDATE '+@SourceTable+'
				 SET zSupplier= STUFF((SELECT distinct ''|'' + CASE WHEN RIGHT(Z.StringValue,6) LIKE ''%=%'' AND RIGHT(Z.StringValue,5) NOT IN (''60040'') THEN RIGHT(Z.StringValue,4) 
																	WHEN RIGHT(Z.StringValue,6) LIKE ''%=%'' AND RIGHT(Z.StringValue,5) IN (''60040'') THEN RIGHT(Z.StringValue,5)
																	ELSE RIGHT(Z.StringValue,6) 
																END
									   FROM  '+@SourceTable+' N
									   CROSS APPLY dgdb.dbo.fnSplitStringToTable(zSupplierPlantMap,''|'') Z
									   WHERE N.Material=M.Material
									   FOR XML PATH('''')), 1, 1, '''') 
				 FROM  '+@SourceTable+' M
				 CROSS APPLY dgdb.dbo.fnSplitStringToTable(zSupplierPlantMap,''|'') A 
				 WHERE M.Material=Material
				 
				 UPDATE '+@SourceTable+'
				 SET zPlant= STUFF((SELECT distinct ''|'' + LEFT(Z.StringValue,4)
									   FROM  '+@SourceTable+' N
									   CROSS APPLY dgdb.dbo.fnSplitStringToTable(zSupplierPlantMap,''|'') Z
									   WHERE N.Material=M.Material
									   FOR XML PATH('''')), 1, 1, '''') 
				 FROM  '+@SourceTable+' M
				 CROSS APPLY dgdb.dbo.fnSplitStringToTable(zSupplierPlantMap,''|'') A 
				 WHERE M.Material=Material
				 
				 UPDATE '+@SourceTable+'
				 SET zSalesOrg=
				 CASE
					WHEN (zPlant LIKE ''%4001%'' OR zPlant LIKE ''%4003%'') AND zSalesOrg NOT LIKE ''%4000%'' THEN zSalesOrg+''|4000''
					WHEN (zPlant LIKE ''%4101%'' OR zPlant LIKE ''%4103%'') AND zSalesOrg NOT LIKE ''%4100%'' THEN zSalesOrg+''|4100''				
					WHEN (zPlant LIKE ''%4601%'' OR zPlant LIKE ''%4603%'') AND zSalesOrg NOT LIKE ''%4600%'' THEN zSalesOrg+''|4600''
					WHEN (zPlant LIKE ''%5501%'' OR zPlant LIKE ''%5503%'') AND zSalesOrg NOT LIKE ''%5500%'' THEN zSalesOrg+''|5500''
					WHEN (zPlant LIKE ''%5521%'' OR zPlant LIKE ''%5523%'') AND zSalesOrg NOT LIKE ''%5520%'' THEN zSalesOrg+''|5520''
					WHEN (zPlant LIKE ''%5001%'' OR zPlant LIKE ''%5003%'') AND zSalesOrg NOT LIKE ''%5000%'' THEN zSalesOrg+''|5000''
					ELSE zSalesOrg
				 END'
				 
			EXEC (@SQL)
			PRINT (@SQL)
		END	*/
		
	IF NOT EXISTS (SELECT name FROM master..sysdatabases WHERE name = @DBName)
		BEGIN						
			SET @SQL = 'CREATE DATABASE ' + @DBName + '
				ON 
				( NAME = ' + @DBName + ',
					FILENAME = ''F:\MSSQL\data\' + @DbName + '.mdf'',
					SIZE = 50,	
					MAXSIZE = UNLIMITED,				
					FILEGROWTH = 50 )
				LOG ON
				( NAME = ' + @DBName + '_log,
					FILENAME = ''I:\MSSQL\LOG\' + @DbName + 'log.ldf'',
					SIZE = 50MB,	
					MAXSIZE = UNLIMITED,				
					FILEGROWTH = 50MB );
				ALTER DATABASE ' + @DBName + ' SET RECOVERY SIMPLE;' 
			PRINT @SQL
			EXEC( @SQL)
		END 

	SET @SQL = 'SELECT COUNT(*) FROM ' + CASE WHEN @SrcAdhocFlag = 0 THEN @SourceTable ELSE @DBName + '.dbo.['+ @srcTableName + '_Adhoc]' END
	DELETE FROM @TableRowCount
	INSERT INTO @TableRowCount EXEC (@SQL)
	SELECT @RowCount = Number FROM @TableRowCount

	IF @srcSptCode = 'SPMV'  --Run SalesView only
		BEGIN
			PRINT ''
			PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
			PRINT ' Start of Running Salesview Extensions (from _SalesOrgExtensions)'
			PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
			PRINT ''
			PRINT 'Have you run ''EXEC dbo.spProcess07SalesViewExtension ' + CAST(@srcID AS NVARCHAR) + ''''
			PRINT ''			
			PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
			PRINT ' END of Running Salesview Extensions'
			PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
			PRINT ''			
			PRINT ''			
			PRINT ''			
		END
		IF @srcSptCode = 'SPMC'  --Run Plant &Stor Loca & WH View only
		BEGIN
			PRINT ''
			PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
			PRINT ' Start of Running Plantview Extensions (from _PlantOrgExtensions)'
			PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
			PRINT ''
			EXEC dbo.spProcess07PlantViewExtension @srcID
			PRINT ''			
			PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
			PRINT ' END of Running Plantview Extensions'
			PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
			PRINT ''			
			PRINT ''			
			PRINT ''			
		END


	PRINT ''
	PRINT ' Working with SourceTable: ' + CASE WHEN @SrcAdhocFlag = 0 THEN @SourceTable ELSE @DBName + '.dbo.['+ @srcTableName + '_Adhoc]' END 	
	PRINT ''
	PRINT ' If you have materials in the override Table than please run SP10 TWICE to establish link between MFRPN and SAP Material!'
	PRINT ''


	PRINT CAST( GETDATE() AS CHAR(30)) + ' - Duration: ' + CAST(DATEDIFF(SECOND,@startDate,GETDATE()) AS NVARCHAR)	
	PRINT '-------------------------------------------------------------------------'
	PRINT ' 2. EXECUTE UPDATE / INSERT / DELETE RULES FROM cfSourceRules'
	PRINT '-------------------------------------------------------------------------'
	PRINT ''
	PRINT CAST('TOTAL' AS CHAR(8)) + CAST(dgdb.dbo.fnReturnThousandSeperator(@RowCount) AS CHAR(12)) + ' Records in ' + CASE WHEN @SrcAdhocFlag = 0 THEN @srcTableName ELSE @srcTableName + '_Adhoc]' END 		
	PRINT ''


	
	SET @SQL = 'SELECT 1 FROM ' + @DBName + '.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ''' + @srcTableName + ''''
	INSERT INTO @TableExists EXEC (@SQL)	
	IF ISNULL(@@ROWCOUNT,0) = 0
		BEGIN
			PRINT ''
			PRINT 'Cannot find srcTableName ''' +  @SourceTable +  ''' listed in table Source'
			PRINT 'Please Import Source File into table ''' + @srcTableName + '''  (Database = ' + @DBName + ')'
			RETURN
		END

	SET @SQL = 'SELECT ssgSrgID FROM cfSourceSourceGroup WHERE ssgSrcID = ' + CAST(@srcID AS NVARCHAR) + ';'
	DELETE FROM @TableCHeckID
	INSERT INTO @TableCHeckID EXEC (@SQL)	
	IF ISNULL(@@ROWCOUNT,0) = 0
		BEGIN
			PRINT ''
			PRINT 'SourceID not linked to a SourceGroup'
			PRINT 'Please link the SourceID to a SourceGroup in cfSourceSourceGroup'
			RETURN
		END	
	ELSE
		BEGIN
			SELECT @srgID = [ID] FROM @TableCHeckID
		END
	DELETE FROM @TableCHeckID

	/*Determine if Adhoc Table Needs to be used and/or Created*/
	IF @SrcAdhocFlag = 1 		
		BEGIN 
			SET @SQL = 'SELECT 1 FROM ' + @DBName + '.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ''' + @srcTableName + '_Adhoc'''	
			DELETE FROM @TableExists
			INSERT INTO @TableExists EXEC (@SQL)	
			IF ISNULL(@@ROWCOUNT,0) = 0
				BEGIN
					SET @SQL = 'SELECT TOP (1) * INTO ' + @DBName + '..' + @srcTableName + '_Adhoc FROM ' + @DBName + '.dbo.[' + @srcTableName + ']; TRUNCATE TABLE ' + @DBName + '.dbo.' + @srcTableName + '_Adhoc;'
					EXEC (@SQL)					
					PRINT ''
					PRINT 'Created Adhoc Table ' + @DBName + '.dbo.' + @srcTableName + '_Adhoc'
					PRINT 'Please Import the required fields into table ''' + @srcTableName + '  _Adhoc'' (Database = ' + @DBName + ')'
					RETURN
				END
			SET @srcTableName = @srcTableName + '_Adhoc'
			SET @SourceTable = @DBName + '.dbo.['+ @srcTableName + ']'			
		END
	
	/*Create Override Table if Needed */	
	SET @SQL = 'SELECT 1 FROM ' + @DBName + '.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ''' + REPLACE(@srcTableName,'_Adhoc','') + '_Override'''	
	DELETE FROM @TableExists
	INSERT INTO @TableExists EXEC (@SQL)	
	IF ISNULL(@@ROWCOUNT,0) = 0
		BEGIN
			SET @SQL = 'CREATE TABLE ' + @DBName + '..[' + REPLACE(@srcTableName,'_Adhoc','') + '_Override] 
				(SAPMaterial NVARCHAR(40) NOT NULL,SalesOrg NVARCHAR(4) DEFAULT('''') NOT NULL,Plant NVARCHAR(4) DEFAULT('''') NOT NULL,ProdTypeCode NVARCHAR(4) DEFAULT('''') NOT NULL, zComment NVARCHAR(255) DEFAULT('''') NOT NULL);
				CREATE UNIQUE INDEX UIX_MATNR ON ' + @DBName + '..' + REPLACE(@srcTableName,'_Adhoc','') + '_Override (SAPMaterial,Plant);'
			EXEC (@SQL)								
		END


	/*Create TMP tables with current content of config tables and other*/
    SELECT * , 0 AS Done
        INTO #cfSourceRules
        FROM [dgdb_PL].[dbo].[cfSourceRules]
		INNER JOIN dbo.cfSourceSourceRules ON srrID = ssrSrrID
        WHERE ssrSrcID = @srcID
		AND srrActive = 1
		
	/*Add FreightUplift rules for EMEA if srcID 99999*/
	IF @srcVendorCode IN ('99999') 
	BEGIN	
		CREATE TABLE #cfMatGroups (MatGroup NVARCHAR(5));
		SET @SQL = '
		INSERT INTO #cfMatGroups
		SELECT DISTINCT MatGroup 
		FROM ' + @DBName + '..Adhoc'	
		PRINT(@SQL)
		EXEC (@SQL)
		
		IF @srcGroup IN ('EMEA') and @srcID NOT IN ('641','795','811')
		BEGIN
		INSERT INTO #cfSourceRules
		SELECT A.*,C.*, 0
		FROM [dbo].[cfSourceRules] A 
		INNER JOIN  [dbo].[cfSourceSourceRules] C on A.srrID=C.ssrSrrID
		INNER JOIN  [dbo].[cfSource] B ON C.ssrSrcID=B.srcID
		WHERE A.srrFieldName in ('FreightUplift','GoodsReceiptDays','DefaultPlant')
		AND B.srcGroup IN ('EMEA')
		AND B.srcVendorCode IN (SELECT * FROM #cfMatGroups)
		AND scrComment NOT IN ('NO','MDM')
		AND a.srrActive='1'
		order by srcVendorCode ASC
		
		UPDATE #cfSourceRules SET srrWhere=srrWhere+' AND MatGroup='+''''+srrVendorCode+'''' WHERE srrFieldName IN ('FreightUplift','GoodsReceiptDays','DefaultPlant')	
		END
	
		IF @srcVendorCode IN ('99999') AND @srcGroup IN ('APAC')
		BEGIN		
		INSERT INTO #cfSourceRules
		SELECT A.*,C.*, 0
		FROM [dbo].[cfSourceRules] A 
		INNER JOIN  [dbo].[cfSourceSourceRules] C on A.srrID=C.ssrSrrID
		INNER JOIN  [dbo].[cfSource] B ON C.ssrSrcID=B.srcID
		WHERE A.srrFieldName in ('FreightUplift')
		AND B.srcGroup IN ('APAC')
		AND B.srcVendorCode IN (SELECT * FROM #cfMatGroups)
		AND scrComment NOT IN ('NO','MDM')
		AND a.srrActive='1'
		order by srcVendorCode ASC
		
		UPDATE #cfSourceRules SET srrWhere=srrWhere+' AND MatGroup='+''''+srrVendorCode+'''' WHERE srrFieldName='FreightUplift'	
		END 
		END


	/*New section to be able to filter for specific salesOrgs*/
	IF @srcFilterCompany = 1 
		BEGIN
			PRINT ''
			PRINT ' **** srcFilterCompany = 1 ****'
			PRINT ''
			DECLARE @cfValidPlantsSalesOrgs TABLE (ruleID INTEGER, Value NVARCHAR(4000))
			INSERT INTO @cfValidPlantsSalesOrgs EXEC spProcess12FilterValidSalesOrgsPlants @srcID
	        
			UPDATE #cfSourceRules SET srrUpdate = Value FROM #cfSourceRules INNER JOIN @cfValidPlantsSalesOrgs ON srrID = ruleID

			PRINT ''
		END

    SELECT * ,0 AS Done
        INTO #cfSourceMapRules
        FROM cfSourceMapRules
		WHERE srmSrgID = @srgID				

    SELECT fldID,fldName,fldShortDescription ,tbfRequired,tbfSeq,dgdb.dbo.RegexReplace(tblPK,'{TblPreFix}|' + tbltable + '.','') AS tblPK,0 AS Done
        INTO #SourceFields
        FROM dgdb..TableField
            INNER JOIN dgdb..Field ON dgDB.dbo.Field.fldID = dgDB.dbo.TableField.tbfFldID
			INNER JOIN dgdb..[Table] ON dgDB.dbo.[Table].tblID = dgDB.dbo.TableField.tbfTblID 
        WHERE tbfTblID = ( SELECT srtTblID FROM dbo.cfSourceType INNER JOIN dbo.cfSource ON srcSrtCode = srtCode WHERE srcID = @srcID)

	IF NOT EXISTS (SELECT TOP 1 1 FROM #cfSourceRules)
		BEGIN
			PRINT ''
			PRINT 'No Source rules setup or linked to Source'
			PRINT 'Please SET-UP your rules in SourceRules and LINK them to your Scource in SourceSourceRules '			
			PRINT ''
			GOTO Clean_UP
		END

	BEGIN TRY
		/*CREATE zProcessed on Vendor Source*/
		IF COL_LENGTH(@SourceTable,'zProcessed') IS NULL AND @srcIncrementalFlag = 1
			BEGIN
				SET @SQL = 'ALTER TABLE ' +  @SourceTable + ' ADD zProcessed BIT DEFAULT(0) WITH VALUES'					
				EXEC (@SQL)
			END					
		ELSE
			BEGIN
				IF @srcIncrementalFlag = 0 AND COL_LENGTH(@SourceTable,'zProcessed') IS NOT NULL 
					BEGIN
						SET @SQL = 'UPDATE ' +  @SourceTable + ' SET zProcessed = 0'					
					END
			END

		/*CREATE zActive, zInserted on Vendor Source*/
		IF COL_LENGTH(@SourceTable,'zActive') IS NULL
					BEGIN
						SET @SQL = 'ALTER TABLE ' +  @SourceTable + ' ADD zActive BIT DEFAULT(1) WITH VALUES'					
						EXEC (@SQL)
					END	
				ELSE
					BEGIN
						SET @SQL = 'UPDATE ' +  @SourceTable + ' SET zActive = 1'					
						EXEC (@SQL)					
					END	

		/*CREATE zsrcName on Vendor Source*/
		IF COL_LENGTH(@SourceTable,'zsrcName') IS NULL
					BEGIN
						SET @SQL = 'ALTER TABLE ' +  @SourceTable + ' ADD zSrcName NVARCHAR(40) DEFAULT(''' + @srcName + ''');'					
						EXEC (@SQL)
					END					
		SET @SQL = 'UPDATE ' +  @SourceTable + ' SET zSrcName = ''' + @srcName + ''' WHERE ISNULL(zSrcName,'''') <> ''' + @srcName + ''';'				
		EXEC (@SQL)			
				
		/*CREATE zOverride, zInserted on Vendor Source*/
		IF COL_LENGTH(@SourceTable,'zOverride') IS NULL
					BEGIN
						SET @SQL = 'ALTER TABLE ' +  @SourceTable + ' ADD zOverride BIT DEFAULT(0) WITH VALUES'					
						EXEC (@SQL)
					END	
				ELSE
					BEGIN
						SET @SQL = 'UPDATE ' +  @SourceTable + ' SET zOverride = 0'					
						EXEC (@SQL)					
					END	
			UPDATE dbo.cfSourceMapRules SET srmSourceMap = 'zOverride',srmActive = 1 WHERE srmFldID = 1122 AND srmSourceMap <> 'zOverride'
		
		/*CREATE and Fill zPlantSupplierMAp id exists zPlant & zSupplier */
		
				IF COL_LENGTH(@SourceTable,'zPlant') IS NOT NULL AND COL_LENGTH(@SourceTable,'zSupplier') IS NOT NULL
					BEGIN
						IF COL_LENGTH(@SourceTable,'zSupplierPlantMap') IS NULL 
							BEGIN
								SET @SQL = 'ALTER TABLE ' +  @SourceTable + ' ADD zSupplierPlantMap NVARCHAR(750) DEFAULT ('''');'					
								EXEC (@SQL)
							END
						-- populate with all possible combinations of Supplier(s) & Plant(s) if zSupplierPlantMap = ''
						--lookup Material FieldName for SrcID
						IF @srcSupplierPlantMapOverride = 0
							BEGIN
								SELECT TOP 1 @Material = srmSourceMap FROM #cfSourceMapRules INNER JOIN dbo.cfSourceSourceGroup ON ssgSrcID = @SrcID AND srmFldName = 'PK'
								SET @SQL = '
											UPDATE ' + @SourceTable + ' 
												SET zSupplierPlantMap =	ISNULL(										
														REPLACE(STUFF((SELECT '' '' + REPLACE(	Convert(nvarchar(MAX),zplant)+ ''|'', 
																							''|'',
																							''='' + StringValue  + ''|'')
																	FROM ' + @SourceTable + ' B
																		CROSS APPLY dgdb.dbo.fnSplitStringToTable(zSupplier,
																						''|'') C
																	WHERE A.' + @Material + ' = B.' + @Material + '
																		FOR XML PATH('''') ,
																		TYPE).value(''.'', ''NVARCHAR(MAX)''),
																	1, 1, ''''), '' '', ''''),'''')
												FROM ' + @SourceTable + ' A --WHERE ISNULL(zSupplierPlantMap,'''') = ''''
											'
								--PRINT @SQL
								EXEC (@SQL)
								/* Update cfSourceMapRules for zSupplierPlantMap where not mapped */	
								UPDATE dbo.cfSourceMapRules SET srmSourceMap = 'zSupplierPlantMap',srmActive = 1 WHERE srmFldID = 1099 AND srmSourceMap <> 'zSupplierPlantMap'
							END
					END		
			
					

		IF EXISTS (SELECT 1 FROM #cfSourceRules WHERE srrUpdateType = 'I')
			BEGIN			
				IF COL_LENGTH(@SourceTable,'zInserted') IS NULL
					BEGIN
						SET @SQL = 'ALTER TABLE ' +  @SourceTable + ' ADD zInserted BIT DEFAULT(0) WITH VALUES'					
						EXEC (@SQL)
					END	
				ELSE
					BEGIN
						SET @SQL = 'DELETE FROM ' +  @SourceTable + ' WHERE zInserted = 1'					
						EXEC (@SQL)					
					END	
			END
		SET @zFieldReset ='Active,Inserted'
		END TRY

		BEGIN CATCH
			PRINT ERROR_MESSAGE() + '  ' + @SQL

	--SET @zFieldReset ='Active,Inserted'

		END CATCH

		/*Insert source materails into xtMatnr table for 10353 vendor code*/
		IF (@srcVendorCode = '10353')
		BEGIN
			SET @SQL = 'UPDATE A
			SET xmtInSAP = 1
			FROM xtMatnr A
			INNER JOIN ' + @SAPClientDataDB + '..MATERIALID MD ON MATNR_EXT = xmtWestconMaterial;
			
			INSERT INTO DGDB_PL..xtMatnr(xmtVendorCode, xmtVendorMaterial, xmtActive, xmtCreated, xmtInSAP)
			SELECT DISTINCT ''' + @srcVendorCode + ''', ProductName, 1,  GETDATE(), 0
			FROM ' + @SourceTable + ' A
			LEFT OUTER JOIN xtMATNR B ON B.xmtVendorMaterial = A.ProductName
			--LEFT OUTER JOIN ' + @SAPClientDataDB + '..MATERIALID MD WITH (NOLOCK) ON MATNR_EXT = xmtWestconMaterial
			WHERE B.xmtVendorMaterial IS NULL;'
			
			PRINT @SQL;
			EXEC (@SQL);
		END


	/*NEW, Track chnages on Pricleist*/
	IF (@srcID NOT IN (481,722))
	BEGIN
		EXEC spProcess09PrepSourceFileProcessing @srcID
	END
	
	/* Apply rules from cfSourceRules*/
	WHILE (SELECT TOP 1 1 FROM #cfSourceRules WHERE Done = 0 ORDER BY srrFieldSeq ASC,srrUpdateSeq ASC ) = 1
		BEGIN
            SELECT TOP 1 @srrID = srrID,
					@srrUpdateType = srrUpdateType,
					@srrFieldname = srrFieldname,
					@srrUpdate = srrUpdate,
					@srrWhere = srrWhere,
					@srrFieldType = srrFieldType,
					@srrActive = srrActive
                FROM #cfSourceRules
                WHERE Done = 0
                ORDER BY srrFieldSeq ASC ,
                    srrUpdateSeq ASC 

			/*Add zField to Vendor Table*/			
			SET @srrFieldname =	REPLACE(REPLACE(@srrFieldname,'[',''),']','')
			SET @srrFieldType =	CASE WHEN  ISNULL(@srrFieldType,'') = '' THEN 'NVARCHAR(255) DEFAULT('''') WITH VALUES' ELSE @srrFieldType END

			/* Deetermine Data type if field already exists to create field or reset to 0 or '' */
			SET @SQL = 'SELECT DATA_TYPE FROM ' + @DBName + '.INFORMATION_SCHEMA.COLUMNS IC WHERE TABLE_NAME = ''' + @srcTableName + ''' AND COLUMN_NAME = ''z' + @srrFieldname + ''''
			DELETE FROM @TableDataType
			INSERT INTO @TableDataType EXEC (@SQL)
			SET @DatType = NULL
			SELECT @DatType = DatType FROM @TableDataType												
			BEGIN TRY			
				/*Create zField if needed or reset Current Values*/
				IF @srrUpdateType = 'U' AND @srrActive = 1
					BEGIN
						IF @DatType IS NULL 
							BEGIN
								SET @SQL = 'ALTER TABLE ' +  @SourceTable + ' ADD [z' + @srrFieldname + '] ' + @srrFieldType --+ ' END'	
							END
						ELSE
							BEGIN
								SET @SQL = ''
								IF dgdb.dbo.RegExIsMatch(@srrFieldname,@zFieldReset,1) = 0  AND @srrID <> '9903'
									BEGIN
										SET @ResetSourceTableValue =	CASE WHEN dgdb.dbo.RegexIsMatch('BIT|NUMERIC|DEC|MONEY|FLOAT', @datType,1) = 1 THEN '0' ELSE '''''' END						
										SET @SQL = 'UPDATE ' +  @SourceTable +' SET [z' + @srrFieldname + '] = LTRIM(RTRIM(' + @ResetSourceTableValue + ')) WHERE [z' + @srrFieldname + '] <> ' + @ResetSourceTableValue 						
										SET @zFieldReset = @zFieldReset + @srrFieldname
									END
							END		
						EXEC (@SQL)						
					END

				/*Do the UPDATE statement*/
				IF @srrUpdateType = 'U' -- SQL update statement
					BEGIN
						IF @srrUpdate <> 'FILL'
							BEGIN								
								SET @SQL = 'UPDATE ' +  @SourceTable +' SET [z' + @srrFieldname + '] = ' + @srrUpdate 
							END
						IF ISNULL(@srrWhere,'') <> ''
							BEGIN
							IF LEFT(@srrWhere,4) = 'FROM' -- TableJoin
								BEGIN
									SET @srrWhere = REPLACE(@srrWhere,'{SAPdb}',@SAPClientDataDB)
									SET @srrWhere = REPLACE(@srrWhere,'{dbName}',@dbName)
									SET @SQL = @SQL +  ' ' + @srrWhere								
								END
							ELSE
								BEGIN
									SET @srrWhere = REPLACE(@srrWhere,'{dbName}',@dbName)
									SET @SQL = @SQL +  ' WHERE ' + @srrWhere								
								END
							END
					END
						ELSE
							BEGIN -- Fill up NULL and empty cells ('') with the previous value								
								SET @SQL = 'DECLARE @v NVARCHAR(4000); UPDATE ' + @SourceTable + ' WITH(TABLOCKX) '
								SET @SQL = @SQL + ' SET @v = z' + @srrFieldname + ' = CASE WHEN ISNULL(' + @srrFieldname + ','''') = '''' THEN @v ELSE ' + @srrFieldname + ' END OPTION(MAXDOP 1); '								
							END		
															
					

				/*Do the INSERT statement & SET zInserted = 1*/
				IF @srrUpdateType = 'I' -- SQL INSERT / DUPLICATE records statement
					BEGIN										
						SET @SQL = 'SELECT * INTO #TMP FROM ' + @SourceTable + ' WHERE ' + @srrWhere + '; '
						SET @SQL = @SQL + 'UPDATE #TMP SET zInserted = 1; ' 
						SET @SQL = @SQL + 'INSERT INTO  ' +  @SourceTable +' SELECT * FROM #TMP; '					
					END
				

				/*Do the DELETE statement*/
				IF @srrUpdateType = 'D' -- SQL INSERT / DUPLICATE records statement
					BEGIN
						SET @SQL = 'DELETE FROM ' + @SourceTable + ' WHERE ' + @srrWhere 						
					END				
				IF @srrActive = 1
					BEGIN	
						--IF @Debugmode = 1 BEGIN PRINT @SQL END
						EXEC (@SQL)
						SET @RowCount = @@ROWCOUNT
					END
				ELSE
					BEGIN
						SET @SQL = 'Not Active - ' + @SQL
					END

				PRINT CAST(@srrID AS CHAR(10)) + CAST(dgdb.dbo.fnReturnThousandSeperator(@RowCOUNT) AS CHAR(10))+ ' ' + CAST(@SQL AS NVARCHAR(2200))  
			END TRY
            
			BEGIN CATCH
				PRINT  CAST(@srrID AS CHAR(10)) + 'ERROR:     ' + ERROR_MESSAGE()  + '  - '	+	@SQL
			END CATCH  		
			          

			UPDATE #cfSourceRules SET done =1 WHERE srrID = @srrID
		END

		/* SET zOVerride Flag*/
		SET @SQL = 'SELECT 1 FROM ' + @DBName + '.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ''SourceFileExtract '''
		INSERT INTO @TableExists EXEC (@SQL)	
		IF ISNULL(@@ROWCOUNT,0) <> 0
			BEGIN	
				Select @srmSourceMap = srmSourceMap FROM dbo.cfSourceMapRules WHERE srmFldID = 921 AND srmSrgID = @srgID		--PK Fioeld		
				Select @srmSourceMap2 = srmSourceMap FROM dbo.cfSourceMapRules WHERE srmFldID = 914 AND srmSrgID = @srgID		--prodTypeFioeld		
				SET @SQL = 'UPDATE ' + @DBName + '.dbo.[' + @srcTableName + '] SET [zOverride] = 1, ' + @srmSourceMap2 + ' = B.ProdTypeCode FROM ' + @DBName + '..[' + @srcTableName + '] A INNER JOIN ' + @DBName + '..SourceFileExtract C ON A.' + @srmSourceMap + ' = C.PK INNER JOIN ' + @DBName + '..' + REPLACE(@srcTableName,'_Adhoc','') + '_Override B ON B.SAPMaterial = C.Material' 		
				IF @Debugmode = 1 BEGIN PRINT @SQL END
				--EXEC (@SQL)
				SET @RowCOunt2 = @@RowCount
				IF @RowCount2 > 0
					BEGIN
						PRINT CAST('Override' AS CHAR(10)) + CAST(dgdb.dbo.fnReturnThousandSeperator(@RowCOUNT2) AS CHAR(10))+ ' ' + CAST(@SQL AS NVARCHAR(2200))  
					END
				--IF @RowCount > 0 
				--	BEGIN				
				--		/*Set Override OBsoletes*/						
				--		SET @SQL = 'UPDATE ' + @DBName + '.dbo.[' + @srcTableName + '_Override] SET [zObsolete] = 1 FROM ' + @DBName + '..[' + @srcTableName + '_Override] A RIGHT OUTER JOIN ' + @DBName + '..SourceFileExtract B ON A.' + @srmSourceMap + ' = B.PK WHERE A.zActive IS NULL ' 		
				--		IF @DebugMode = 1 BEGIN PRINT @SQL END 
				--		EXEC (@SQL)
				--		SET @SQL2 = @SQL
				--		SET @SQL = 'SELECT COUNT(*) FROM ' + @DBName + '.dbo.[' + @srcTableName + '_Override] WHERE zObsolete = 1'		
				--		DELETE FROM @TableRowCount
				--		INSERT INTO @TableRowCount EXEC (@SQL)
				--		SELECT @RowCOUNT = Number FROM @TableRowCount
				--		IF @ROWCOUNT > 0
				--			BEGIN
				--				PRINT CAST('Over.obs' AS CHAR(10)) + CAST(dgdb.dbo.fnReturnThousandSeperator(@RowCOUNT) AS CHAR(10))+ ' ' + CAST(@SQL AS NVARCHAR(2200))  
				--			END
				--	END
			END

		Select @srmSourceMap = srmSourceMap FROM dbo.cfSourceMapRules WHERE srmFldID = 918 AND srmSrgID = @srgID		  --ListPrice
		SET @SQL = 'UPDATE ' + @SourceTable + ' SET zActive = 0 WHERE CAST(' + @srmSourceMap + ' AS MONEY) >=  1000000000'
		IF @Debugmode = 1 BEGIN PRINT @SQL END
		EXEC (@SQL)

		SET @RowCount2 = @@ROWCOUNT
		IF @RowCount2 > 0
			BEGIN
				PRINT CAST('Bad LP' AS CHAR(10)) + CAST(dgdb.dbo.fnReturnThousandSeperator(@RowCOUNT2) AS CHAR(10))+ ' ' + CAST(@SQL AS NVARCHAR(2200))  
			END

		/*Overrrule PH Values From MARA if column ProductHierarchy Exists*/
		IF @srcPHOverride = 1 
			BEGIN					
				IF EXISTS (SELECT TOP 1 1 FROM  DGDB_PL..cfSourceMapRules WHERE srmFldName = 'ProductHierarchy' AND  srmSrgID = @srgID AND LEN(LTRIM(srmSourceMAp)) > 1)
					BEGIN
						SELECT TOP 1 @Material = srmSourceMap FROM dbo.cfSourceMapRules WHERE srmSrgID = @srgID AND srmFldName = 'Material'
						SELECT TOP 1 @MaterialGroup = srmSourceMap FROM dbo.cfSourceMapRules WHERE srmSrgID = @srgID AND srmFldName = 'MaterialGroup'
						SELECT TOP 1 @ProductHierarchy = srmSourceMap FROM dbo.cfSourceMapRules WHERE srmSrgID = @srgID AND srmFldName = 'ProductHierarchy'
						SET @SQL = '
							USE ' + @dbName + '; 
							UPDATE A SET ' + @Material + ' = dgdb.dbo.RegExReplace(' + @Material + ',''^ +|$ +|\t|\r|\n|\e|\a|\f|\v\|\s|\xa0'','''') FROM ' + @srcTableName + ' A;
							UPDATE A SET ' + @ProductHierarchy + ' = ISNULL(PRDHA,' + @MaterialGroup + ') FROM ' + @SAPClientDataDB + '..MARA S RIGHT JOIN ' + @srcTableName + ' A ON S.MFRPN = A.' + @Material + ' AND A.MatGroup = S.MATKL'	
						--PRINT 'Overwrite PH: ' + @SQL
						EXEC (@SQL)
					END
			END

		SET @SQL = 'SELECT COUNT(*) FROM ' + CASE WHEN @SrcAdhocFlag = 0 THEN @SourceTable ELSE @DBName + '.dbo.['+ @srcTableName + ']' END + ' WHERE zActive = 1'
		DELETE FROM @TableRowCount
		INSERT INTO @TableRowCount EXEC (@SQL)
		SELECT @RecordCount = Number FROM @TableRowCount
		PRINT ''
		PRINT CAST('TOTAL' AS CHAR(10)) + CAST(dgdb.dbo.fnReturnThousandSeperator(@RecordCount) AS CHAR(12)) + ' Records marked for processing'		
		PRINT ''


		
		PRINT CAST( GETDATE() AS CHAR(30)) + ' - Duration: ' + CAST(DATEDIFF(SECOND,@startDate,GETDATE()) AS NVARCHAR)		
		PRINT ''
		PRINT '--------------------------------------------------------------------'
		PRINT ' 3. EXECUTE UPDATE / INSERT Fields IN cfSourceMapRules'
		PRINT '--------------------------------------------------------------------'

		/* Start with Updating cfSourceMapRules */
		WHILE (SELECT TOP 1 1 FROM #SourceFields WHERE done = 0 AND tbfSeq < 9000 ORDER BY tbfSeq ASC, fldID ASC) = 1
			BEGIN
                SELECT TOP 1 
						@fldID = fldID,
						@fldName = fldName,
						@tbfRequired = tbfRequired,
						@tbfSeq = tbfSeq,
						@tblPK = tblPK
                    FROM #SourceFields
                    WHERE done = 0 AND tbfSeq < 9000
                    ORDER BY tbfSeq ASC ,
                        fldID ASC

				/*Check if Field Is already available in MAP #cfSourceMapRules*/
				IF NOT EXISTS ( SELECT 1 FROM #cfSourceMapRules WHERE srmSrgID = @srgID AND srmFldID = @fldID	)
					BEGIN
						INSERT INTO dbo.cfSourceMapRules ( srmSrgID ,srmTbfSeq,srmFldID,srmFldName , srmSourceMap ,srmComment ,srmActive )
							VALUES
							    (	@srgID , -- srmSrgID - int
									@tbfSeq, -- srmTbfSeq - int
							        @fldID , -- srmFldID - int
							        @fldName , -- srmFldName - nvarchar(50)
							        '' , -- srmSourceMap - nvarchar(4000)
							        '' , -- srmComment - nvarchar(255)
							        @tbfRequired   -- srmActive - bit
							    )
						PRINT CAST(@fldID AS CHAR(10)) + CAST(0 AS CHAR(10)) + ' Added   cfSourceMapRules: ' + CAST(@fldName AS CHAR(50))  
					END
				/* Check for changes in fldName*/
				IF EXISTS ( SELECT 1 FROM #cfSourceMapRules WHERE srmSrgID = @srgID AND srmFldID = @fldID	AND (srmFldName <> @fldName OR srmTbfSeq <> @TbfSeq))
					BEGIN
						UPDATE #cfSourceMapRules SET srmFldName = @fldName, srmTbfSeq = @TbfSeq WHERE srmSrgID = @srgID AND srmFldID = @fldID	
						UPDATE cfSourceMapRules SET srmFldName = @fldName, srmTbfSeq = @TbfSeq WHERE srmSrgID = @srgID AND srmFldID = @fldID	
						PRINT CAST(@fldID AS CHAR(10)) + CAST(0 AS CHAR(10)) + ' Updated cfSourceMapRules: ' + CAST(@fldName AS CHAR(50))  
					END

				UPDATE #SourceFields SET done = 1 WHERE fldID = @fldID
			END

		/* Update Plants in cfSourceSourceGroup*/
		UPDATE cfSourceSourceGroup
			SET ssgPlants = srrUpdate
			FROM dbo.cfSourceRules
				INNER JOIN cfSourceSourceRules ON srrID = ssrSrrID
				INNER JOIN dbo.cfSourceSourceGroup ON ssgSrcID = ssrSrcID
			WHERE srrFieldName = 'plant'
				AND LEFT(srrUpdate, 1) = ''''
				AND ssgPlants <> srrUpdate
				AND ssgSrcID = @srcID
				AND srrActive = 1

		/* update UOM/MEINS to each to support diff UOM - mapped to MARA-MEINS*/
		/*if nothing specified will default to EACH*/
		UPDATE dbo.cfSourceMapRules SET srmSourceMap = '''EA''',srmActive = 1 WHERE srmFldID = 737 AND srmSourceMap = ''

		/*List of required sourceMapFields*/
		SELECT fldID,fldName INTO #RequiredFields FROM dbo.vVendorPriceListsFields WHERE  tbfRequired = 1


		EXEC spSYSExecSourceProc @srcID,'130'
		
		
		PRINT CAST( GETDATE() AS CHAR(30)) + ' - Duration: ' + CAST(DATEDIFF(SECOND,@startDate,GETDATE()) AS NVARCHAR)		
		PRINT '--------------------------------------------------------------------'
		PRINT ' 3B.New Check SourceFile from Vendor'
		PRINT '--------------------------------------------------------------------'
		IF (@srcTableName NOT LIKE '%BundleHeader%' AND @srcVendorCode NOT IN ('10090')) BEGIN
		 
		SET @SQL = '			
			IF EXISTS (SELECT TOP 1 1 
					FROM ' + @SourceTable + ' S
						CROSS APPLY ( SELECT Data FROM DGDB_PL..fnSplitString(S.zPlant, ''|'')) x
					WHERE data BETWEEN 4000 AND 5999 AND data NOT IN (''4611'',''4612'',''4613'',''4610'') AND zActive=''1''
						AND CHARINDEX(LEFT(x.Data,3) + ''0'',S.zSalesOrg,1)  = 0)
				BEGIN					
					SELECT 1
				END
			ELSE
				BEGIN
					SELECT 0
				END
			'	
			PRINT @SQL
				
		DELETE FROM @TableRowCount
		INSERT INTO @TableRowCount EXEC (@SQL)		
		SELECT @RowCount =  Number FROM @TableRowCount		
		IF @RowCount = 1 
			BEGIN
					PRINT ''
					PRINT 'Sales Org extensions (zSalesOrg) missing based on plants(zPlant) set-up '					
					PRINT '*** Aborting SP10 ***'
					PRINT ''
					GOTO Clean_Up
				END

		END
		
				
		PRINT CAST( GETDATE() AS CHAR(30)) + ' - Duration: ' + CAST(DATEDIFF(SECOND,@startDate,GETDATE()) AS NVARCHAR)	
		
	
			
		PRINT '--------------------------------------------------------------------'
		PRINT ' 4. Update / Create SourceData Table (Exexcute active cfSourceMapRules)'
		PRINT '--------------------------------------------------------------------'

		IF @DebugMode = 1 BEGIN PRINT 'Create SourceData' END 
		
				-- Custom code added for Pure Storage srcID only to handle the duplicate material records
		--IF @srcID = 133 
		--	BEGIN 
  -- 				 UPDATE a
		--		 SET zProductName = a.zProductName + '-' + CONVERT(VARCHAR,Seq) 
		--		 FROM VPL10353_PureStorage_SG_NZ_133.dbo.[PureStorage] AS A
		--		 INNER JOIN (SELECT   zProductName, [ProductName] ,ROW_NUMBER() OVER (PARTITION BY zProductName ORDER BY zProductName ASC) AS Seq  
		--		 FROM  VPL10353_PureStorage_SG_NZ_133.dbo.[PureStorage]) AS b ON a.[ProductName] = b.[ProductName]
		--		 WHERE b.Seq > 1		  		      

		--	END 

		IF @srcVendorCode = '10353'
		BEGIN 
			
			DECLARE @MaxID INT;

			SELECT @MaxID = MAX(RIGHT(xmtWestconMaterial,5)) 
			FROM dbo.xtMatnr 
			WHERE xmtVendorCode = @srcVendorCode 
				AND xmtType = 'T' 
				AND LEN(xmtWestconMaterial) = 40

			UPDATE xtMATNR
			SET xmtWestconMaterial = CONCAT((REPLACE((SUBSTRING(dgdb.dbo.RegExReplace(xmtVendorMaterial,'[^(A-z\d=*)]+',''), 1
										, (LEN(dgdb.dbo.RegExReplace(xmtVendorMaterial,'[^(A-z\d=*)]+',''))-1))), '=',''))
											, RIGHT(dgdb.dbo.RegExReplace(xmtVendorMaterial,'[^(A-z\d=*)]+',''),1))
			WHERE xmtInSAP = 0;

			/*Normal Matwerial, 40 or less characters*/
			--UPDATE dbo.xtMatnr 
			--SET xmtWestconMaterial = dgdb.dbo.RegExReplace(xmtVendorMaterial,'[^(A-z\d=)]+','')
			--	, xmtType = 'N' 
			--WHERE LEN(xmtVendorMaterial) < 41 AND xmtInSAP = 0

			/* Material, 40 or less characters after being stripped of non numeric or non characters */
			--UPDATE dbo.xtMatnr 
			--SET xmtWestconMaterial = dgdb.dbo.RegExReplace(xmtVendorMaterial,'[^(A-z\d=)]+','')
			--	, xmtType = 'S' 
			--WHERE LEN(dgdb.dbo.RegExReplace(xmtVendorMaterial,'[^(A-z\d=)]+','')) < 41 
			--	AND xmtWestconMaterial IS NULL
			--	AND xmtInSAP = 0;

			--/*Remaining materials > 40 chars, ate left 35 + add next number*/
			--UPDATE dbo.xtMatnr 
			--SET xmtWestconMaterial = LEFT(DGDB.dbo.RegExReplace(xmtVendorMaterial, '[^(A-z\d=)]+', ''), 35) 
			--	, xmtType = 'T'  
			--WHERE LEN(DGDB.dbo.RegExReplace(xmtVendorMaterial, '[^(A-z\d=)]+', '')) > 40 
			--	AND xmtWestconMaterial IS NULL
			--	AND xmtInSAP = 0;

			UPDATE xtMatnr
			SET xmtType = 'T'
			WHERE LEN(xmtWestconMaterial) > 40
				AND xmtInSAP = 0;

			WITH CTE_MaxNumber
			AS(
				SELECT ROW_NUMBER() OVER (ORDER BY xmtVendorMaterial) AS ID
					, xmtVendorMaterial
					, xmtWestconMaterial  
				FROM dbo.xtMatnr 
				WHERE xmtType = 'T' 
					AND LEN(xmtWestconMaterial) > 34
					AND xmtInSAP = 0
			)

			UPDATE dbo.xtMatnr
			SET xmtWestconMaterial = SUBSTRING(C.xmtWestconMaterial, 1, 35) + RIGHT('0000' + CAST(ID  + ISNULL(@MaxID,0) AS NVARCHAR),5)
			FROM dbo.xtMatnr X
			JOIN CTE_MaxNumber C ON C.xmtVendorMaterial = X.xmtVendorMaterial;

			SET @SQL = 'UPDATE A
			SET A.zProductName =  B.xmtWestconMaterial
			FROM ' + @SourceTable + ' A
			INNER JOIN xtMatnr B ON B.xmtVendorMaterial = A.ProductName
			WHERE zActive = 1;'

			PRINT @SQL;
			EXEC(@SQL);
		END
			SELECT @srmSourceMap = srmSourceMap,@srmPKSourceMap = srmSourceMap FROM #cfSourceMapRules WHERE srmFldName = 'PK'
		
			IF NOT EXISTS (SELECT 1 FROM #cfSourceMapRules WHERE ISNULL([srmSourceMap],'') <> '' AND srmSrgID = @srgID)
				BEGIN
					PRINT ''
					PRINT 'No SourceMap rules have been set-up yet'
					PRINT 'Please Map the required Fields of Table ''' + @srcTableName + ''' To the predifined fields of Table cfSourceMapRules'
					PRINT ''
					GOTO Clean_Up
				END

			SELECT fldID , fldNAme ,fldShortDescription
				INTO #Fields
				FROM dbo.vVendorPriceListsFields
					LEFT OUTER JOIN cfSourceMapRules ON fldID = srmFldID
				WHERE tbfRequired = 1 AND ISNULL(srmSourceMap, '') = '' AND srmSrgID = @srgID
			IF EXISTS ( SELECT 1 FROM #Fields )
				BEGIN
					SELECT 'Please map all required fields in SourceMapRules before continuing' , * FROM #Fields
					GOTO Clean_Up
				END

		/* Check,Create and SET DataType for PK  */
			IF @DebugMode = 1 BEGIN PRINT 'Create PK Sourcefile' END 
			BEGIN TRY
				SET @SQL = ' 
							DECLARE @PKName NVARCHAR(255),@SQL NVARCHAR(4000),@PKField NVARCHAR(255);
							SELECT @PKName = o.Name FROM ' + @dbName + '.sys.objects AS o
								INNER JOIN ' + @dbName + '.sys.tables AS t ON t.object_id = o.parent_object_id
									WHERE o.type = ''PK'' AND t.name = ''' + @srcTableName + ''';			
							SELECT @PKField = srmSourceMap FROM cfSourceMapRules WHERE srmSrgID = ' + CAST(@srgID AS NVARCHAR) + ' AND srmFldID = ''903'';
			
							SET @SQL = ''ALTER TABLE ' + @SourceTable + ' DROP CONSTRAINT '' +  @PKName + '''' 						
							EXEC (@SQL)
							set @SQL  =''ALTER TABLE ' + @SourceTable + ' ALTER COLUMN '' +  @PKField + '' NVARCHAR(255) NOT NULL''
							exec (@SQL)
							SET @SQL = ''ALTER TABLE ' + @SourceTable + ' ADD PRIMARY KEY ( '' + @PKField + '');''			
							EXEC (@SQL)
						'
		
				PRINT ''
				EXEC (@SQL)
			END TRY

			BEGIN CATCH
				PRINT 'Warning:  PK Creation on VendorFile failed becuase of Duplicate Partnumber(s)'
				PRINT''
			END CATCH
		BEGIN TRY

		
		
		IF @DebugMode = 1 BEGIN PRINT 'Start Cleaning' END 
		/*Clean Material number before checking for dupes*/		
			SET @SQL = 'IF OBJECT_ID(''tempdb..##SourceTable_' + @dbName + ''') IS NOT NULL BEGIN DROP TABLE ##SourceTable_' + @dbName + ' END' EXEC (@SQL)
			SET @SQL = 'SELECT * INTO ##SourceTable_' +  @DBName + ' FROM ' + @SourceTable + ' WHERE zActive = 1 ' + CASE WHEN @SrcIncrementalFlag = 1 THEN ' AND zProcessed = 0 ' ELSE '' END
			EXEC (@SQL)
IF @DebugMode = 1 BEGIN PRINT @SQL END
			/*Get PK Field to get rid of crap characters*/
				SELECT @srdSrmPK = srmSourceMap FROM dbo.cfSourceMapRules WHERE srmSrgID = @SrgID AND srmFldID = 921
				/*Do Data governance Clean Up PK for non Printable characters like tab, line break etc..*/
				SET @SQL = 'UPDATE  ##SourceTable_' + @dbName + ' SET ' + @srdSrmPK + ' = dgdb.dbo.RegExReplace(' + @srdSrmPK + ',''\t|\r|\n|\e|\a|\f|\v\|\s|\xa0'','''') '
				SET @SQL = @SQL + 'WHERE dgdb.dbo.RegExIsMatch(''\t|\r|\n|\e|\a|\f|\v\|\s|\xa0'',' + @srdSrmPK + ',1) = 1; '			
				SET @SQL = @SQL + 'PRINT CAST(CASE WHEN @@ROWCOUNT = 0 THEN '' '' ELSE ''>>>'' END  AS CHAR(20))  + CAST(''Non Printable characters   removed from ' + @srdSrmPK + ''' AS CHAR(100))	+ CAST(dgdb.dbo.fnReturnThousandSeperator(@@RowCOUNT)  AS CHAR(10))'											
				EXEC (@SQL)			
				/* Trailing/Leading spaces PK*/
				SET @SQL = 'UPDATE  ##SourceTable_' + @dbName + ' SET ' + @srdSrmPK + ' =  dgdb.dbo.RegExReplace(' + @srdSrmPK + ',''^ +| +$'','''') '
				SET @SQL = @SQL + ' WHERE dgdb.dbo.RegExIsMatch(''^ +| +$'',' + @srdSrmPK + ',1) = 1; '			
				SET @SQL = @SQL + 'PRINT CAST(CASE WHEN @@ROWCOUNT = 0 THEN '' '' ELSE ''>>>'' END  AS CHAR(20))  + CAST(''Trailing Or Leading spaces removed from ' + @srdSrmPK + ''' AS CHAR(100))	+ CAST(dgdb.dbo.fnReturnThousandSeperator(@@RowCOUNT)  AS CHAR(10))'		
				EXEC (@SQL)							
				PRINT ''

			/*Test existance & Uniqueness of PK from #cfSourceMapRules, If not unique ABORT*/
			IF EXISTS (SELECT TOP 1 1 FROM #cfSourceMapRules WHERE srmFldName = 'PK') 			
				BEGIN
					/*Test if there are duplicates*/				
					SET @SQL = 'SELECT COUNT(*) FROM ##SourceTable_' +  @DBName + ' WHERE zActive = 1 GROUP BY ' + @srmSourceMap + ' HAVING COUNT(*) > 1 ;'
					INSERT INTO @TableCHeckID EXEC (@SQL)
					IF EXISTS (SELECT 1 FROM @TableCHeckID WHERE ID <> 0 )
						BEGIN						
							PRINT ' ERROR: Duplicate Primary Key(PK) found in ' + @SourceTable 
							PRINT ' *** Aborting ***'
							PRINT ''
							--PRINT '     SELECT ' + @srmSourceMap + '  AS DuplicatePK,COUNT(*) as Lines FROM ##SourceTable_' +  @DBName + ' WHERE zActive = 1 GROUP BY ' + @srmSourceMap + ' HAVING COUNT(*) > 1'
							SET @SQL = 'SELECT ''Duplicate partnumber(s) found'' AS Error,' + @srmSourceMap + '  AS DuplicatePartNumbers,COUNT(*) as Lines FROM ##SourceTable_' +  @DBName + '  WHERE zActive = 1 GROUP BY ' + @srmSourceMap + ' HAVING COUNT(*) > 1' 
							--PRINT @SQL
							EXEC (@SQL)
							PRINT ''
							GOTO Clean_Up
						END
					ELSE
						BEGIN	-- Next Check
							SET @SQL = 'SELECT TOP 1 1 FROM ' + @SourceTable + ' WHERE ISNULL(' + @srmSourceMap + ','''')  = '''';'								
							DELETE FROM @TableCHeckID
							INSERT INTO @TableCHeckID EXEC (@SQL)
							IF EXISTS (SELECT 1 FROM @TableCHeckID WHERE ID <> 0 )
								BEGIN
									PRINT ' ERROR: NULL or Empty Value Primary Key(PK) found in ''' + @SourceTable + '.' + @srmSourceMap + ''''						
									PRINT ''
									GOTO Clean_Up
								END
						END
				END
			ELSE
				BEGIN
					PRINT ' ERROR: PK (primary Key) Field Not set-up in cfSourceMapRules, Please set-up de PK (Which Field(s) has/have the value(s) that will identify a single row as Unique)'
					GOTO Clean_UP
				END


			/*INSERT or UPDATE NEW RECORDS*/
			/*
			-1 = MISSING IN PRICELIST
			0  = NO CHANGE
			1  = INSERT / NEW RECORD 
			2  = UPDATE 
			*/

			SET @SQL = 'IF OBJECT_ID(''tempdb..##SourceData_' + @dbName + ''') IS NOT NULL BEGIN DROP TABLE ##SourceData_' + @dbName + ' END' EXEC (@SQL)			
			EXEC dgdb..spGenDropOrCheckTable 'SourceData',@DBName,1
			SET @SQL = '
				CREATE TABLE ' + @DBName + '.dbo.SourceData([srdID] [bigint] NOT NULL,	[srdSrmPK] [nvarchar](255) NOT NULL,[srdSRCID] [int] NULL,	[srdSrmID] [int] NULL,	[srdFldID] [int] NULL,	[srdValue] [nvarchar](4000) NULL,	[srdValueOld] [nvarchar](255) NULL,
				[srdCreated] [datetime] NULL,[srdCreatedBy] [nvarchar](40) NULL,[srdModified] [datetime] NULL,	[srdModifiedBy] [nvarchar](40) NULL,[StatusFlag] [int] NOT NULL,	[RunDate] [datetime] NOT NULL)'			
			SET @SQL = @SQL + 'CREATE INDEX IX_srdSrmPK ON ' + @DBName + '.dbo.SourceData(srdSrmPK);'
			EXEC (@SQL)

			SELECT @srmSourceMap = srmSourceMap,@srmPKSourceMap = srmSourceMap FROM #cfSourceMapRules WHERE srmFldName = 'PK'
			SET @SQL = 'SELECT '         		
			WHILE (SELECT TOP 1 1 FROM #cfSourceMapRules WHERE done = 0 AND ISNULL(srmSourceMap,'') <> '' AND srmActive = 1 ORDER BY srmTbfSeq ASC) = 1
				BEGIN
					SELECT TOP 1 
							@srmID = srmID,
							@srmFldID = srmFldID,
							@srmSourceMap = srmSourceMap,
							@srmFldName = srmFldName
						FROM #cfSourceMapRules
						WHERE done = 0
						AND ISNULL(srmSourceMap,'') <> ''
						AND srmActive = 1 
						ORDER BY srmTbfSeq ASC 						

					/*CREATE #SourceData*/				
					SET @SQL = 'DECLARE @SQL NVARCHAR(1000);SELECT CAST(0 AS INTEGER) as srdID,' + CAST(@srmID AS NVARCHAR) + ' AS srdSrmID, '  +  @srmPKSourceMap + ' AS srdSrmPK, '  + CAST(@srmFldID AS NVARCHAR) + ' as srdFldID, LTRIM(RTRIM(' + @srmSourceMap  				
					SET @SQL = @SQL + ')) AS srdValue, 0 AS StatusFlag, ' + CAST(@srgID AS NVARCHAR) + ' AS srdSrgID, SYSTEM_USER as srdCreatedBy,GETDATE() as Rundate INTO ##SourceData_' + @dbName + ' FROM ##SourceTable_' +  @DBName + ' WHERE zActive = 1;'
					SET @SQL = @SQL + 'UPDATE ##SourceData_' + @dbName + ' SET srdID = B.srdID FROM ##SourceData_' + @dbName + ' A INNER JOIN ' + @DBName + '.dbo.SourceData AS B ON '
					SET @SQL = @SQL + ' A.srdSrmID = B.srdSrmID AND A.srdFldID = B.srdFldID AND A.srdSrmPK = B.srdSrmPK AND b.srdSrmID = ' + CAST(@srmID AS NVARCHAR) + ';'
					SET @SQL = @SQL + 'CREATE INDEX IX_srdID ON ##SourceData_' + @dbName + '(srdID); CREATE INDEX IX_srdSrmPK ON ##SourceData_' + @dbName + '(srdSrmPK);'
					BEGIN TRY
						EXEC(@SQL)										

					
						/* UPDATE MATERIAL & DESCRIPTION & K*/

						IF @srmFldID = 903 --Material
							BEGIN							
											
								/* Check for Duplicate Materials*/																											
									SET @SQL = 
											' Declare  @cnt int 
													select @cnt = count(1)  
																FROM ##SourceData_' + @dbName + ' A 
																	INNER JOIN ' + @dbName + '..SourceData B On A.srdSrmPK = B.srdSrmPK 
																	INNER JOIN ' + @SAPClientDataDB + '..MARA ON CAST(A.srdValue AS NVARCHAR) = CAST(MFRPN as NVARCHAR) AND MATKL <> B.srdValue 
																	INNER JOIN ' + @SAPClientDataDB + '..MATERIALID ID ON MARA.MATNR = MATNR_INT AND A.srdSrmPK  = ltrim(rtrim(MATNR_EXT))  
																WHERE A.srdFldID = 903 AND B.srdFldID = 905
											if @cnt > 0
													BEGIN
											
														SELECT M1.MFRPN,M1.MATKL,'' MFRPN exists in diff mat.group >>> '', LEFT(CAST(A.srdValue as NVARCHAR),26) + ''^'' + RIGHT(B.srdValue,3)  as New_Material, MATNR_EXT as Existing_New_Material
																FROM ##SourceData_' + @dbName + ' A 
																	INNER JOIN ' + @dbName + '..SourceData B On A.srdSrmPK = B.srdSrmPK 
																	INNER JOIN ' + @SAPClientDataDB + '..MARA M1 ON CAST(A.srdValue AS NVARCHAR) = CAST(M1.MFRPN as NVARCHAR) AND M1.MATKL <> B.srdValue 																
																	INNER JOIN ' + @SAPClientDataDB + '..MATERIALID ID ON M1.MATNR = MATNR_INT AND A.srdSrmPK = ltrim(rtrim(MATNR_EXT))  	
																WHERE A.srdFldID = 903 AND B.srdFldID = 905			
																order by mfrpn													
														UPDATE A SET srdVAlue =  LEFT(CAST(A.srdValue as NVARCHAR),26) + ''^'' + RIGHT(B.srdValue,3) 
															FROM ##SourceData_' + @dbName + ' A 
																INNER JOIN ' + @dbName + '..SourceData B On A.srdSrmPK = B.srdSrmPK 
																INNER JOIN ' + @SAPClientDataDB + '..MARA ON CAST(A.srdValue AS NVARCHAR) = CAST(MFRPN as NVARCHAR) AND MATKL <> B.srdValue 
																INNER JOIN ' + @SAPClientDataDB + '..MATERIALID ID ON MARA.MATNR = MATNR_INT AND A.srdSrmPK  = ltrim(rtrim(MATNR_EXT))  	
															WHERE A.srdFldID = 903 AND B.srdFldID = 905; 
														PRINT CAST(CASE WHEN @@ROWCOUNT = 0 THEN '' '' ELSE ''*** WARNING ***'' END  AS CHAR(20))  + CAST(''DUPLICATE MATERIALS FOUND (same Material(MFRPN), different Material Group(MATKL))'' AS CHAR(100))	+ CAST(dgdb.dbo.fnReturnThousandSeperator(@@RowCOUNT)  AS CHAR(10))
													END'										
									EXEC (@SQL)	
									--PRINT @SQL								
									PRINT ''

								/*Do Data governance on Field Material for Illegal Characters*/
									DECLARE @IllegalCharacter AS CHAR(1) = ''
									DECLARE @IllegalCharacterNew AS CHAR(1) = ''
									DECLARE @SubstitutionCharacter AS CHAR(1) = ''
									DECLARE @StartingPositionForCHeck AS SMALLINT = ''	
									DECLARE @EndingPositionForCHeck AS SMALLINT = ''						                                    
									PRINT ''
									WHILE (SELECT TOP 1 1 FROM dgDB.dbo.xtMATNR_IllegaclCharactersSubstitution WHERE IllegalCharacter > @IllegalCharacter ORDER BY IllegalCharacter) = 1
										BEGIN
				
												SELECT TOP 1 @IllegalCharacterNew = IllegalCharacter ,
														@SubstitutionCharacter = SubstitutionCharacter ,
														@StartingPositionForCHeck = StartingPositionForCHeck,
														@EndingPositionForCHeck = EndingPositionForCheck
													FROM dgDB.dbo.xtMATNR_IllegaclCharactersSubstitution
													WHERE IllegalCharacter > @IllegalCharacter
													ORDER BY IllegalCharacter				

												SET @IllegalCharacter = @IllegalCharacterNew

												SET @SQL = 'UPDATE  ##SourceData_' + @dbName + ' SET srdVAlue = REPLACE(srdValue, ''' + @IllegalCharacter + ''', ''' + LTRIM(rtrim(@SubstitutionCharacter)) + ''' ) '
												SET @SQL = @SQL + 'WHERE srdFldID = 903 AND CHARINDEX(''' + @IllegalCharacter + ''', srdValue) > 0 AND LEN(srdValue) >= ' + CAST(@StartingPositionForCHeck AS NVARCHAR) + ' AND CHARINDEX(''' + @IllegalCharacter + ''', srdValue) <= ' + CAST(@EndingPositionForCHeck AS NVARCHAR) + ';'												
												SET @SQL = @SQL + 'PRINT CAST(CASE WHEN @@ROWCOUNT = 0 THEN '' '' ELSE ''>>>'' END AS CHAR(20)) + CAST(''Character ' + @IllegalCharacter + ' in Material replaced with ' + @SubstitutionCharacter + ''' AS CHAR(100)) + CAST(dgdb.dbo.fnReturnThousandSeperator(@@RowCOUNT)  AS CHAR(10));'																	
						
												--SET @SQL = 'UPDATE  ##SourceData_' + @dbName + ' SET srdVAlue = REPLACE(srdValue, ''' + @IllegalCharacter + ''', ''' + @SubstitutionCharacter + ''' ) '
												--SET @SQL = @SQL + 'WHERE srdFldID = 903 AND PATINDEX(''%'' + ''' + @IllegalCharacter + ''' + ''%'', CAST(srdValue AS NVARCHAR)) > 0 AND LEN(srdValue) >= ''' + CAST(@StartingPositionForCHeck AS NVARCHAR) + ''';'												
												--SET @SQL = @SQL + 'PRINT CAST(CASE WHEN @@ROWCOUNT = 0 THEN '' '' ELSE ''>>>'' END AS CHAR(20)) + CAST(''Character ' + @IllegalCharacter + ' in Material replaced with ' + @SubstitutionCharacter + ''' AS CHAR(100)) + CAST(dgdb.dbo.fnReturnThousandSeperator(@@RowCOUNT)  AS CHAR(10));'											
												--PRINT @SQL
										EXEC (@SQL)						
										END			
								END
							IF @srmFldID = 904 --DESCRIPTION
								BEGIN								
									PRINT ''
									/*Do Data governance Clean Up Description for non Printable characters like tab, line break etc..*/
										SET @SQL = 'UPDATE  ##SourceData_' + @dbName + ' SET srdVAlue = dgdb.dbo.RegExReplace(srdValue,''[^\x00-\x7F]'','''') '
										SET @SQL = @SQL + 'WHERE srdFldID = 904 AND dgdb.dbo.RegExIsMatch(''[^\x00-\x7F]'',srdValue,1) = 1; '			
										SET @SQL = @SQL + 'PRINT CAST(CASE WHEN @@ROWCOUNT = 0 THEN '' '' ELSE ''>>>'' END  AS CHAR(20))  + CAST(''1st pass Non Printable characters removed from Description'' AS CHAR(100))	+ CAST(dgdb.dbo.fnReturnThousandSeperator(@@RowCOUNT)  AS CHAR(10))'												
										EXEC (@SQL)			
									/*Do Data governance Clean Up Description for non Printable characters like tab, line break etc..*/
										SET @SQL = 'UPDATE  ##SourceData_' + @dbName + ' SET srdVAlue = dgdb.dbo.RegExReplace(srdValue,''\t|\r|\n|\e|\a|\f|\v\|\s|\xa0'','' '') '
										SET @SQL = @SQL + 'WHERE srdFldID = 904 AND dgdb.dbo.RegExIsMatch(''\t|\r|\n|\e|\a|\f|\v\|\s|\xa0'',srdValue,1) = 1; '			
										SET @SQL = @SQL + 'PRINT CAST(CASE WHEN @@ROWCOUNT = 0 THEN '' '' ELSE ''>>>'' END  AS CHAR(20))  + CAST(''2nd Pass Non Printable characters removed from Description'' AS CHAR(100))	+ CAST(dgdb.dbo.fnReturnThousandSeperator(@@RowCOUNT)  AS CHAR(10))'												
										EXEC (@SQL)		
										
									/*Do Data governance Clean Up Description starting with '/'..*/
										SET @SQL = 'UPDATE  ##SourceData_' + @dbName + ' SET srdVAlue = dgdb.dbo.RegExReplace(srdValue,''^/'','''') '
										SET @SQL = @SQL + 'WHERE srdFldID = 904 AND dgdb.dbo.RegExIsMatch(''^/'',srdValue,1) = 1; '			
										SET @SQL = @SQL + 'PRINT CAST(CASE WHEN @@ROWCOUNT = 0 THEN '' '' ELSE ''>>>'' END  AS CHAR(20))  + CAST(''Descriptions starting with / characters removed from Description'' AS CHAR(100))	+ CAST(dgdb.dbo.fnReturnThousandSeperator(@@RowCOUNT)  AS CHAR(10))'												
										EXEC (@SQL)	
											
									/*Truncate Description if more than 4000 chars.*/
										SET @SQL = 'UPDATE  ##SourceData_' + @dbName + ' SET srdVAlue = LEFT(srdValue,4000)'
										SET @SQL = @SQL + 'WHERE srdFldID = 904 AND LEN(srdValue) > 4000; '			
										SET @SQL = @SQL + 'PRINT CAST(CASE WHEN @@ROWCOUNT = 0 THEN '' '' ELSE ''>>>'' END  AS CHAR(20))  + CAST(''Truncate Description if more than 4000 chars'' AS CHAR(100))	+ CAST(dgdb.dbo.fnReturnThousandSeperator(@@RowCOUNT)  AS CHAR(10))'												
										EXEC (@SQL)
									/*Do Data governance Clean Up Description for multiple spaces.*/
										SET @SQL = 'UPDATE  ##SourceData_' + @dbName + ' SET srdVAlue = dgdb.dbo.RegExReplace(srdValue,'' +|\|'','' '') '
										SET @SQL = @SQL + 'WHERE srdFldID = 904 AND dgdb.dbo.RegExIsMatch('' +|\|'',srdValue,1) = 1; '
										SET @SQL = @SQL + 'PRINT CAST(CASE WHEN @@ROWCOUNT = 0 THEN '' '' ELSE ''>>>'' END  AS CHAR(20))  + CAST(''Multiple Spaces removed from Description'' AS CHAR(100))	+ CAST(dgdb.dbo.fnReturnThousandSeperator(@@RowCOUNT)  AS CHAR(10))'												
										EXEC (@SQL)										
									/*Do Data governance Clean Up Description for Leading trailing spaces.*/
										SET @SQL = 'UPDATE  ##SourceData_' + @dbName + ' SET srdVAlue = LTRIM(RTRIM(srdValue))'
										SET @SQL = @SQL + 'WHERE srdFldID = 904 AND (LEFT(srdValue,1) = '' '' OR RIGHT(srdValue,1) = '' '')'
										SET @SQL = @SQL + 'PRINT CAST(CASE WHEN @@ROWCOUNT = 0 THEN '' '' ELSE ''>>>'' END  AS CHAR(20))  + CAST(''Leading/Trailing Spaces removed from Description'' AS CHAR(100))	+ CAST(dgdb.dbo.fnReturnThousandSeperator(@@RowCOUNT)  AS CHAR(10))'												
										EXEC (@SQL)
										PRINT ''									
								END
			
						/*StatusFlag = 1 - INSERT NEW Records INTo SourceData*/
						SET @SQL =	'BEGIN TRY DECLARE @fldName NVARCHAR(40);DECLARE @RowCount INT;SET @FldName = ''' + @srmFldName + ''';'											
						SET @SQL = @SQL + 'INSERT INTO ' + @DBName + '.dbo.SourceData WITH (TABLOCKX) (srdID,srdSrmID, srdSrmPK,srdFldID, srdValue,srdCreated,srdCreatedBy, StatusFlag, RunDate) '
						SET @SQL = @SQL + 'SELECT srdID,srdSrmID, srdSrmPK,srdFldID, srdValue,RunDate,srdCreatedBy, StatusFlag, RUndate FROM ##SourceData_' + @dbName + ' WHERE srdID = 0;SET @Rowcount = ISNULL(@@Rowcount,0)'					
						SET @SQL = @SQL + 'IF @Rowcount > 0 BEGIN PRINT CAST('' '' AS CHAR(10)) + CAST(dgdb.dbo.fnReturnThousandSeperator(@RowCOUNT) AS CHAR(10)) + ''New Records Inserted for '' + CAST(@fldName AS CHAR(80))  + ''   Duration:'  +  CAST(DATEDIFF(SECOND,@startDate,GETDATE()) AS NVARCHAR) + ''' END;'
						
			
						/* ##SourceData Records where srdID = 0 are new and can be excluded as the have been inserted above*/							
						SET @SQL = @SQL + 'DELETE FROM ##SourceData_' + @dbName + ' WHERE srdID = 0;'					
						/*StatusFlag = -1 - Records not on ScourceList anymore*/							
						SET @SQL = @SQL + 'SELECT srdID,StatusFlag INTO #Existing FROM ' + @DBName + '.dbo.SourceData WHERE srdID <> 0 AND srdSrmID = ' + CAST(@SrmID AS NVARCHAR)+ ';'
						SET @SQL = @SQL + 'UPDATE #Existing SET StatusFlag = -1 FROM #Existing A LEFT OUTER JOIN ##SourceData_' + @dbName + ' AS B ON A.srdID = B.srdID WHERE B.srdID IS NULL ;'
						SET @SQL = @SQL + 'UPDATE ' + @DBName + '.dbo.SourceData WITH (TABLOCKX) SET StatusFlag = -1 FROM ' + @DBName + '.dbo.SourceData A INNER JOIN #Existing AS B ON A.srdID = B.srdID WHERE B.StatusFlag = -1 ;'					
						SET @SQL = @SQL + 'SET @RowCount = @@ROWCOUNT; IF @RowCount > 0 BEGIN PRINT CAST('' '' AS CHAR(10)) + CAST(dgdb.dbo.fnReturnThousandSeperator(@RowCOUNT) AS CHAR(10)) + ''Old Records Missing for '' + CAST(@fldName AS CHAR(80))  + ''   Duration:'  +  CAST(DATEDIFF(SECOND,@startDate,GETDATE()) AS NVARCHAR) + ''' END;'
						
						/*StatusFlag = 2 - Update Changes*/					
						
						SET @SQL = @SQL + 'UPDATE ' + @DBName + '.dbo.SourceData WITH (TABLOCKX) SET srdModifiedBy = SYSTEM_USER, srdModified = GETDATE(),StatusFlag = 2,srdValue = B.srdValue,srdValueOld = A.srdValue FROM ' + @DBName + '.dbo.SourceData A INNER JOIN ##SourceData_' + @dbName + ' AS B '
						SET @SQL = @SQL + 'ON A.srdID = B.srdID WHERE CAST(A.srdValue AS NVARCHAR) <> CAST(B.srdValue AS NVARCHAR); '
						SET @SQL = @SQL + 'END TRY '
						SET @SQL = @SQL + 'BEGIN CATCH PRINT  CAST('''' AS CHAR(10)) + ''ERROR:     '' + ERROR_MESSAGE()    END CATCH;'	
					
						/*UPDATE SQL*/
						EXEC sp_executesql @SQL
						SET @Rowcount = @@Rowcount				
						IF @Rowcount > 0 BEGIN PRINT CAST('' AS CHAR(10)) + CAST(dgdb.dbo.fnReturnThousandSeperator(@RowCOUNT) AS CHAR(10)) + '    Records Updated  for ' + CAST(@srmFldName AS CHAR(80)) + '   Duration:'  +  CAST(DATEDIFF(SECOND,@startDate,GETDATE()) AS NVARCHAR) + '' END
					END TRY

					BEGIN CATCH
						PRINT CAST('' AS CHAR(10)) + CAST(0 AS CHAR(10)) + @srmFldName + ' - ' + @srmSourceMap + ' - ' + ERROR_MESSAGE() 						
						IF (SELECT 1 FROM #RequiredFields WHERE fldID = @srmFldID) = 1
							BEGIN
								SELECT 	'******************** ' + @srmFldName + ' not mapped (correctly) aborting procedure ! ***********************'
								PRINT ''
								PRINT '******************** ' + @srmFldName + ' not mapped (correctly) aborting procedure ! ***********************'
								PRINT ''
								PRINT 'Disregard error message below on transaction count after EXECUTE but fix the error above >>>>>  ***** ERROR *****'								
								PRINT ''
								RETURN
							END 
					END CATCH
	

					SET @Columns = @Columns + '[' + CAST(@srmFldID AS NVARCHAR) + '], '				
					SET @ColumnsAlias = @ColumnsAlias + '[' + CAST(@srmFldID AS NVARCHAR) + '] AS ' + CAST(ISNULL(@srmFldName,'') AS NVARCHAR) + ', '		

					SET @SQL = 'IF OBJECT_ID(''tempdb..##SourceData_' + @dbName + ''') IS NOT NULL BEGIN DROP TABLE ##SourceData_' + @dbName + ' END'				
					
					EXEC (@SQL)
					UPDATE #cfSourceMapRules SET done = 1 WHERE srmID = @srmID 		
	
				END				
	
			SET @SQL = 'CREATE INDEX IK_Sourcedata_' + 	@dbName + ' ON ' + @dbName + '..SourceData(srdSrmPK);'
			SET @SQL = @SQL + 'CREATE UNIQUE INDEX UIX_Sourcedata_' + 	@dbName + ' ON ' + @dbName + '..SourceData(srdSrmPK,srdSrmID)'
			EXEC (@SQL)
					
			SET @Columns = dgdb.dbo.RegexReplace(@Columns,', $','')  
			SET @ColumnsAlias = dgdb.dbo.RegexReplace(@ColumnsAlias,', $','')  

			SET @SQl = 'SELECT COUNT(*) FROM ' + @dbName + '..SourceData WHERE StatusFlag <> -1'		
			DELETE FROM @TableRowCount 
			INSERT INTO @TableRowCount EXEC (@SQL)
			SELECT @Total = Number FROM @TableRowCount
			PRINT''						
			PRINT CAST('TOTAL' AS CHAR(8)) + CAST(dgdb.dbo.fnReturnThousandSeperator(@Total) AS CHAR(12)) + 'Records IN ' + @dbName + '..SourceData'
			PRINT ''
			PRINT CAST( GETDATE() AS CHAR(30)) + ' - Duration: ' + CAST(DATEDIFF(SECOND,@startDate,GETDATE()) AS NVARCHAR)
			
			EXEC spSYSExecSourceProc @srcID,'160'
			
			PRINT ''
			PRINT '--------------------------------------------------------------------'
			PRINT ' 5. Create SourceFile Extract - Structure for Review'
			PRINT '--------------------------------------------------------------------'
			/* CREATE Transposed / Pivot Table from SourceData*/
	
				EXEC dgdb..spGenDropOrCheckTable 'SourceFileExtract',@DBName,1
					SET @SQL = 'SELECT DISTINCT' + @ColumnsAlias + ' INTO ' + @DBName + '.dbo.SourceFileExtract  
							FROM 
							(
							SELECT srdSrmPK,srdFldID, srdValue
							FROM ' + @DBName + '..SourceData
						) x
						PIVOT 
						(
							MAX(srdValue)
							FOR srdFldID IN (' + @Columns + ')
						) p' 
				EXEC (@SQL)
				PRINT @SQL

				/*Add column zStatusFlag*/
				SET @SQL = 'ALTER TABLE ' + @DBName + '.dbo.SourceFileExtract  Add zStatusFlag SMALLINT DEFAULT(0) WITH VALUES'		
				EXEC (@SQL)
				/*Add column zMDMTicketNumber*/
				SET @SQL = 'ALTER TABLE ' + @DBName + '.dbo.SourceFileExtract  ADD zMDMTicketNumber NVARCHAR(100);'
				EXEC (@SQL)
				
				SET @SQL = 'UPDATE SF SET zMDMTicketNumber = dbo.udf_RemoveSplCharacters_Swathi(S.srcMDMTicketNumber) FROM ' + @DBName + '.dbo.SourceFileExtract SF CROSS JOIN cfSource S WHERE S.srcID = ' + CAST(@srcID AS NVARCHAR)						
				--PRINT @SQL				
				EXEC (@SQL)

				SET @SQL = 'IF ' + convert(varchar,@srcID) + ' in( 481,722,745,25,796,300,563) BEGIN UPDATE SF SET Material = PK FROM ' + @DBName + '.dbo.SourceFileExtract SF END ' 
					PRINT @SQL	EXEC (@SQL)

				/*Update column ZStatusFlag*/
				SET @SQL = 'UPDATE '+ @DBName + '.dbo.SourceFileExtract SET zStatusFlag = StatusFlag FROM ' + @DBName + '.dbo.SourceFileExtract AS E INNER JOIN ' + @DBName + '.dbo.SourceData AS D ON E.PK = D.srdSrmPK '
				SET @SQL = @SQL + 'WHERE StatusFlag = 2 '
				EXEC (@SQL)

				SET @SQL = 'UPDATE '+ @DBName + '.dbo.SourceFileExtract SET zStatusFlag = StatusFlag FROM ' + @DBName + '.dbo.SourceFileExtract AS E INNER JOIN ' + @DBName + '.dbo.SourceData AS D ON E.PK = D.srdSrmPK '
				SET @SQL = @SQL + 'WHERE StatusFlag = -1 '
				EXEC (@SQL)

				SET @SQL = 'UPDATE '+ @DBName + '.dbo.SourceFileExtract SET zStatusFlag = StatusFlag FROM ' + @DBName + '.dbo.SourceFileExtract AS E INNER JOIN ' + @DBName + '.dbo.SourceData AS D ON E.PK = D.srdSrmPK '
				SET @SQL = @SQL + 'WHERE StatusFlag = 1 '
				EXEC (@SQL)

				/*Add column zSource*/
				SET @SQL = 'ALTER TABLE ' + @DBName + '.dbo.SourceFileExtract  Add zSource NVARCHAR(50) DEFAULT(''' + @Source + ') WITH VALUES'		
				EXEC (@SQL)

				SET @SQL = 'SELECT CAST(zStatusFlag as nvarchar) as StatusFlag,COUNT(*) FROM ' + @dbName + '..SourceFileExtract GROUP BY zStatusFlag'	
				PRINT @SQL;			
				INSERT INTO @TableStats EXEC (@SQL)
				SELECT @Total = SUM(Number) FROM @TableStats
				PRINT''
				PRINT CAST('TOTAL' AS CHAR(8)) + CAST(dgdb.dbo.fnReturnThousandSeperator(@Total) AS CHAR(12)) + 'Records IN ' + @dbName + '..SourceFileExtract'
				PRINT ''

	
				/*Add ZFields For Fields With Changed Values*/
				SET @SQL = 'SELECT DISTINCT [srmFldName], 0 AS Done FROM '  + @DBName + '.dbo.SourceData INNER JOIN #cfSourceMapRules ON srdSrmID = srmID  WHERE StatusFlag = 2'				
				PRINT @SQL;
				INSERT INTO @TableZFields EXEC (@SQL)			
				
				WHILE (SELECT TOP 1 1 FROM @TableZFields WHERE done = 0) = 1
					BEGIN							
						SELECT TOP 1 @srmFldName = Field FROM @TableZFields WHERE done = 0
						SET @SQL = 'IF COL_LENGTH( ''' + @DBName + '.dbo.SourceFileExtract'',''z' + @srmFldName + ''') IS NULL BEGIN ALTER TABLE ' + @DBName + '.dbo.SourceFileExtract  Add z' + @srmFldName + ' NVARCHAR(255)  END;'		
						PRINT @SQL;
						EXEC (@SQL)
						PRINT CAST ('' AS CHAR(20)) + 'Added Field z' + @srmFldName
						UPDATE @TableZFields SET Done =  1 WHERE Field = @srmFldName
					END
		
				/*Update zFields With Changed Values*/
				/*SELECT srdID,srdSrmID,srdSrmPK, srdValue,srdValueOld,srmFldName, StatusFlag,0 AS Done INTO #SourceDataChanges FROM ' + @DBName + '..SourceData INNER JOIN #cfSourceMapRules ON srdSrmID = srmID  WHERE StatusFlag =2
				WHILE (SELECT TOP 1 1 FROM #SourceDataChanges WHERE DOne = 0 ORDER BY srdSrmPK) = 1
					BEGIN
						SELECT TOP 1 @srdID = srdID ,
								@srmFldName = srmFldName ,
								@srdValueOld = srdValueOld,
								@srdSrmPK = srdSrmPK
							FROM #SourceDataChanges
							WHERE DOne = 0
							ORDER BY srdSrmPK

						SET @SQL = ''UPDATE '+ @DBName + '.dbo.SourceFileExtract SET z'+ @srmFldName + ' = ''' + @srdValueOld + ''' WHERE PK = ''' + @srdSrmPK + '''
						EXEC (@SQL)

						UPDATE #SourceDataChanges SET Done = 1 WHERE srdID = @srdID
					END		
				DROP TABLE #SourceDataChanges
				*/

				/*Set STATS for OutPUT*/
				-- Total				
					/*WHILE (SELECT TOP 1 1 FROM @TableStats WHERE Number <> 0) = 1
						BEGIN
							SELECT TOP 1 @StatusFlag = StatusFlag ,
									@RowCOunt = Number
								FROM @TableStats
								WHERE Number <> 0
						
							PRINT CAST ('' AS CHAR(20)) + 'Records with StatusFlag '+ @StatusFlag +  ' = ' + CAST(@RowCount AS NVARCHAR) 

							UPDATE @TableStats SET Number = 0 WHERE statusFlag = @StatusFlag
						END*/
					--SELECT @RowCount = Number FROM @TableRowCount
	
					
		IF (SELECT 1 FROM dbo.cfSourceMapRules WHERE srmSrgID = @srgID AND srmFldName = 'ProdTypeCode') = 1 AND @srcSrtCode = 'VPL' AND @srcVendorCode <> '99999'
			BEGIN	
			PRINT 'Line 1268'						
				EXEC dbo.spProcess15SourceFileComparison @srcID, 'ProdTypeCode'							
				IF COL_LENGTH(@dbName+'..SourceFileExtract','zLegacyPH2') > 0 BEGIN EXEC dbo.spProcess15SourceFileComparison @srcID, 'zLegacyPH2' END
				IF COL_LENGTH(@dbName+'..SourceFileExtract','zLegacyPH3') > 0 BEGIN EXEC dbo.spProcess15SourceFileComparison @srcID, 'zLegacyPH3' END
				IF COL_LENGTH(@dbName+'..SourceFileExtract','SERNP') > 0 BEGIN EXEC dbo.spProcess15SourceFileComparison @srcID, 'SERNP' END
			END
		PRINT 'Calling SP'
		EXEC spSYSExecSourceProc @srcID,'190'
	END TRY

		
		

		BEGIN CATCH
			PRINT 'ERROR !!!!'
			PRINT ''
			PRINT CAST(ISNULL(@srmFldName,'') AS CHAR(20)) + ERROR_MESSAGE() + '  ' + @SQL

		END CATCH

IF @srcGroup='EMEA'
	BEGIN
		/*SET @SQL =
				'
					UPDATE '+@dbName+'..SourceFileExtract
					SET Plant=CASE
					WHEN Plant LIKE ''%4101%'' AND Plant LIKE ''%4001%'' THEN Plant
					WHEN Plant LIKE ''%4101%'' AND Plant NOT LIKE ''%4001%'' THEN Plant+''|4001''
					WHEN Plant LIKE ''%4001%'' AND Plant NOT LIKE ''%4101%'' THEN Plant+''|4101''
					WHEN Plant NOT LIKE ''%4001%'' AND Plant NOT LIKE ''%4101%'' THEN Plant+''|4001|4101''
					END,
					Supplier=CASE
					WHEN Supplier LIKE ''%4101%'' AND Supplier LIKE ''%4001%'' THEN Supplier
					WHEN Supplier LIKE ''%4101%'' AND Supplier NOT LIKE ''%4001%'' THEN Supplier+''|4001''
					WHEN Supplier LIKE ''%4001%'' AND Supplier NOT LIKE ''%4101%'' THEN Supplier+''|4101''
					WHEN Supplier NOT LIKE ''%4001%'' AND Supplier NOT LIKE ''%4101%'' THEN Supplier+''|4001|4101''
					END,
					SupplierPlantMap=CASE
					WHEN SupplierPlantMap LIKE ''%4001=4101%'' AND SupplierPlantMap LIKE ''%4101=4001%'' THEN SupplierPlantMap
					WHEN SupplierPlantMap LIKE ''%4001=4101%'' AND SupplierPlantMap NOT LIKE ''%4101=4001%'' THEN SupplierPlantMap+''|4101=4001''
					WHEN SupplierPlantMap LIKE ''%4101=4001%'' AND SupplierPlantMap NOT LIKE ''%4001=4101%'' THEN SupplierPlantMap+''|4001=4101''
					WHEN SupplierPlantMap NOT LIKE ''%4101=4001%'' AND SupplierPlantMap NOT LIKE ''%4001=4101%'' THEN SupplierPlantMap+''|4001=4101|4101=4001''
					END
					WHERE ProdTypeCode IN (''HWBB'',''HWST'',''SWBB'',''SWST'')
					AND (Plant LIKE (''%4001%'') OR Plant LIKE (''%4101%''))
					AND '''+@srcVendorCode+''' NOT IN (''10003'',''10018'',''10021'',
													   ''10265'',''10025'',''10237'',
													   ''10011'',''10317'',''10164'',
													   ''10220'',''10301'',''99999'',
													   ''10160'',''10232'',''10220'',
													   ''10330'',''10332'',''10031'',
													   ''10344'',''10018'',''10300'',
													   ''10124'',''10113'',''10343'',
													   ''10016'')
					AND '''+CAST(@srcID AS NVARCHAR)+''' NOT IN (''46'',''399'',''559'',''724'',''680'',''310'')
					AND Plant LIKE ''%4001%'' AND Plant LIKE ''%4101%''
				'
		EXEC (@SQL)
		PRINT (@SQL) */	
	
	-- South Africa Hardware stock transfers from 4001
	-- South Africa Hardware stock transfers from 4001
	/*
	IF @srcID IN ('51','680')
		BEGIN
			SET @SQL =
				'
					UPDATE '+@dbName+'..SourceFileExtract
					SET Plant=
					CASE
					WHEN Plant LIKE ''%5501%'' THEN Plant
					WHEN Plant NOT LIKE ''%5501%'' THEN Plant+''|5501''
					END,
					SupplierPlantMap=LTRIM(RTRIM(SupplierPlantMap))+''|5501=IC4000''
					WHERE ProdTypeCode IN (''HWBB'',''HWST'',''SWBB'',''SWST'')
				'
			EXEC (@SQL)
			PRINT (@SQL)
		END	
	*/
	IF @srcID IN ('484') --Infoblox
		BEGIN
			SET @SQL =
				'
					UPDATE '+@dbName+'..SourceFileExtract
					SET Plant=
					CASE
					WHEN Plant LIKE ''%5501%'' THEN Plant
					WHEN Plant NOT LIKE ''%5501%'' THEN Plant+''|5501''
					END,
					SupplierPlantMap=LTRIM(RTRIM(SupplierPlantMap))+''|5501=4001''
					WHERE ProdTypeCode IN (''HWBB'',''HWST'',''SWBB'',''SWST'')
				'
			EXEC (@SQL)
			PRINT (@SQL)
		END

	IF @srcID IN ('62') --Infoblox
	BEGIN
			SET @SQL =
				'
					UPDATE '+@dbName+'..SourceFileExtract
					SET PK=[SKU]
					FROM '+@dbName+'..Infoblox
					WHERE SourceFileExtract.PK=[Partner SKU]
				'
			EXEC (@SQL)
			PRINT (@SQL)
	END

	END

			PRINT ''
			PRINT '--------------------------------------------------------------------'
			PRINT ' 6. Update Supplier Plant Map Extensions '
			PRINT '--------------------------------------------------------------------'

	IF (ISNULL((SELECT srcSupplierExtFlag FROM cfSOurce WHERE srcId = @SrcId),0) = 1)
		BEGIN
			EXEC spGetSupplierPlantMapExtensions @dbName,@SAPClientDataDB;
		END


Clean_UP:				
		IF OBJECT_ID('tempdb..#cfSourceRules') IS NOT NULL BEGIN DROP TABLE #cfSourceRules END				
		IF OBJECT_ID('tempdb..#cfSourceMapRules') IS NOT NULL BEGIN DROP TABLE #cfSourceMapRules END				
		IF OBJECT_ID('tempdb..#SourceFields') IS NOT NULL BEGIN DROP TABLE #SourceFields END				
		IF OBJECT_ID('tempdb..#Fields') IS NOT NULL BEGIN DROP TABLE #Fields END				
		
		SET @DateDiff = CAST(DATEDIFF(SECOND,@startDate,GETDATE()) AS NUMERIC(8,2))
		PRINT ''
		PRINT ''
		PRINT '--------------------------------------------------------------------'
		PRINT CAST( GETDATE() AS CHAR(30)) + ' - Total Duration: ' + CAST(DATEDIFF(SECOND,@startDate,GETDATE()) AS NVARCHAR) + ' Seconds - ' + CAST(CAST(@DateDiff/60 AS NUMERIC(8,2)) AS NVARCHAR) + ' Minutes)'
		Print 'END';
		
		




