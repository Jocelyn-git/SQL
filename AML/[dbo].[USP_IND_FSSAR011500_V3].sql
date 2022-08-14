USE [AML_SARDB]
GO
/****** Object:  StoredProcedure [dbo].[USP_IND_FSSAR011500]    Script Date: 2018/4/3 下午 02:50:12 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[USP_IND_FSSAR011500_V2]
(
    @DATACAT         VARCHAR(100),         --XBATCHFLOW.DATACAT
    @PROCESS_TYPE    CHAR(3),              --XBATCHFLOW.PROCESS_TYPE
    @ETL_BEGIN_DATE  DATETIME,             --作業起始日期
    @ETL_END_DATE    DATETIME,             --作業起始日期
    @PARAM           VARCHAR(4000) OUTPUT  --回傳參數
)
AS
------------------------------
-- 版本控制
------------------------------
-- Data Source     : 
-- Comment         : SAR-01-15-00
-- 情境規則        : 同一客戶在過去(0)天匯入或匯出匯款交易分別>=台幣(1)萬元
--					 且
--					 這些交易來自或匯至高風險國家或地區
/*****************************************************
-- 版本說明(Version Description)
----------------------------------------------
01).2019/12/23 Jocelyn, Initial
02).2020/02/20 Jocelyn, 依照新訂客戶分群調整
*****************************************************/

    ------------------------------
    -- 宣告變數
    ------------------------------
    -- 以下為執行狀態相關變數
    DECLARE @JOB_START_TIME  DATETIME;       --程式起始時間
    DECLARE @JOB_END_TIME    DATETIME;       --程式結束時間
    DECLARE @SRC_TBLNM       VARCHAR(500);   --來源 TABLE，Sample：SourceTable1||SourceTable2。
    DECLARE @TGT_TBLNM       VARCHAR(500);   --目地 TABLE，Sample：TargetTable。
    DECLARE @INS_CNT         DECIMAL(10,0);  --新增資料筆數
    DECLARE @UPD_CNT         DECIMAL(10,0);  --更新資料筆數
    DECLARE @DEL_CNT         DECIMAL(10,0);  --刪除資料筆數
	DECLARE @EXEC_DESC       VARCHAR(4000);  --執行資訊

    -- 以下為 PROCEDURE 內自訂變數
    DECLARE @SAR_CATE    VARCHAR(10)	--客群分類
    DECLARE @SAR_Version INT            --變數版本
    DECLARE @SAR_Id      VARCHAR(18)    --SAR_ID
	DECLARE @Par1 INT  					--天數
    DECLARE @Par2 INT  					--金額
    DECLARE @Data_Dt DATE
    DECLARE @Cycle_Dt DATE
	DECLARE @Par1_Dt DATE
    ------------------------------
    -- 設定必輸入變數
    ------------------------------
    -- 執行狀態
    SET @SRC_TBLNM = 'FB_Txn||FB_Cust||Cd_Txn||Cd_SAR_Par||Cd_Risk_Cntry';  --有多個 Source Table 時使用<||>分隔符號區分
    SET @TGT_TBLNM = 'FS_SAR_01_15_00';  --通常只會填一個 Target Table    

    ------------------------------
    -- 設定變數
    ------------------------------
    -- 執行狀態
    SET @JOB_START_TIME = GETDATE();
    SET @INS_CNT = 0;
    SET @UPD_CNT = 0;
    SET @DEL_CNT = 0;
    -- OUT 回傳值
    SET @PARAM = ISNULL(@PARAM,'');
    -------------------------------------------------------------------------------
    -- 程式開始
    -------------------------------------------------------------------------------
    -------------------------------------------------------------------------------
    -- Step1. 暫存TABLE&參數初始化
    -------------------------------------------------------------------------------
    SET @SAR_Id = 'SAR_01_15_00'
	IF OBJECT_ID('Tempdb.dbo.#SAR_CATE') IS NOT NULL BEGIN DROP TABLE #SAR_CATE END
	IF OBJECT_ID('Tempdb.dbo.#SAR_CATE_VALUE') IS NOT NULL BEGIN DROP TABLE #SAR_CATE_VALUE END
	
	/* 態樣參數表 */
	CREATE TABLE #SAR_CATE_VALUE 
	 (SAR_CATE VARCHAR(10)
	 ,Par1_SDt DATE 
	 ,Par1_EDt DATE
	 ,Par2_SDt DATE
	 ,Par3 DECIMAL(18,2))


	/* 設定迴圈(依客群分類) */
	SELECT ROW_NUMBER() OVER (ORDER BY SAR_ID,SAR_CATE) AS SEQ,*
	  INTO #SAR_CATE 
	  FROM (SELECT DISTINCT * FROM Cd_SAR_Par_V2 WHERE SAR_Id = @SAR_Id) S1 
	  WHERE S1.SAR_ID = 'SAR_01_15_00'--指定SAR_ID
	    AND S1.SAR_CATE NOT IN ('DEFAULT') --指定客群

	DECLARE @ST_SEQ INT = 1
	DECLARE @EN_SEQ INT = (SELECT MAX(SEQ) AS SEQ FROM #SAR_CATE)

    WHILE (@ST_SEQ <= @EN_SEQ)
	BEGIN
	  
			SELECT @SAR_CATE = SAR_CATE FROM #SAR_CATE
			WHERE SEQ = @ST_SEQ

			SELECT @SAR_Version = SAR_Version
					,@Par1 = Par1    --天數
					,@Par2 = Par2    --金額
			  FROM #SAR_CATE
		     WHERE SAR_Id = @SAR_Id 
		       AND SAR_CATE = @SAR_CATE

	   SET @Cycle_Dt = CONVERT(DATE, @ETL_END_DATE)
    SELECT @Data_Dt = LBSDT FROM AML_MetaFlow..CB_DT WHERE DATADT = @Cycle_Dt --資料日期為前一個營業日
	   SET @Par1_Dt = DATEADD(D,-@Par1+1,@Data_Dt)
	    
	   INSERT INTO #SAR_CATE_VALUE SELECT @SAR_CATE,@Par1_Dt,@Par2  

    SET @ST_SEQ += 1  
    END

	 --PRINT CONCAT('@Par1_Dt=',@Par1_Dt)
	 --PRINT CONCAT('@Data_Dt=',@Data_Dt)

    IF OBJECT_ID('TEMPDB..#TMP_DataC') IS NOT NULL BEGIN
		DROP TABLE #TMP_Data
	END

    IF OBJECT_ID('TEMPDB..#TMP_DataD') IS NOT NULL BEGIN
		DROP TABLE #TMP_Data
	END

    --IF OBJECT_ID('TEMPDB..#TMP_SAR') IS NOT NULL BEGIN
	--	DROP TABLE #TMP_SAR
	--END

    CREATE TABLE #TMP_DataC
    (
         Cust_No       VARCHAR(50)   --客戶編號
        ,AMT_SUM_IN     DECIMAL(25,5) --過去N日總匯入交易金額
    )
    CREATE TABLE #TMP_DataD
    (
         Cust_No       VARCHAR(50)   --客戶編號
		,AMT_SUM_OUT    DECIMAL(25,5) --過去N日總匯出交易金額
    )
    CREATE TABLE #TMP_SAR
    (
         Cust_No       VARCHAR(50)   --客戶編號
    )
    -------------------------------------------------------------------------------
    -- Step2. 刪除已轉入資料
    -------------------------------------------------------------------------------
    --主TABLE
    DELETE FS_SAR_01_15_00 WHERE Data_Dt = @Cycle_Dt
    --明細TABLE
    DELETE FS_SAR_01_15_00_Dtl WHERE Data_Dt = @Cycle_Dt
    --案件管理Table
	DELETE FS_SAR_Case_Info WHERE Data_Dt = @Cycle_Dt and SAR_Id = 'SAR_01_15_00'    
    -------------------------------------------------------------------------------
    -- Step3. 交易監控
    -------------------------------------------------------------------------------
	SET @ST_SEQ = 1
	SET @EN_SEQ = (SELECT MAX(SEQ) AS SEQ FROM #SAR_CATE)

    WHILE (@ST_SEQ <= @EN_SEQ)
	BEGIN
	  SELECT @SAR_CATE = SAR_CATE FROM #SAR_CATE
	  WHERE SEQ = @ST_SEQ
	  
        --匯入
        INSERT INTO #TMP_DataC (Cust_No,AMT_SUM_IN)  
        SELECT  B.Cust_No            AS 'Cust_No'
               ,SUM(A.CCY_Amt)       AS 'AMT_SUM_IN'
          FROM FB_Txn A
	      JOIN FB_Cust_Acct AA ON A.Acct_No = AA.Acct_No
          JOIN FB_Cust B ON B.Cust_No = AA.Cust_No
          JOIN Cd_Txn C ON A.Txn_key = C.Txn_key
          join Cd_Risk_Cntry E on A.Cntry_Cd_3 = E.Risk_Cntry_Cd and E.Risk_Lvl = 'H'
		  JOIN FD_Cust_Cate F ON B.Cust_No= F.Cust_No        --客群分群
		  JOIN #SAR_CATE_VALUE G ON G.Cust_Cate = F.SAR_CATE --態樣分群
         WHERE C.AC_DC_Typ = 'C'
		   AND F.Cust_Cate = @SAR_CATE
           and C.AML_Prod_Typ ='RM'
           AND A.Txn_Dt BETWEEN G.Par1_Dt AND @Data_Dt
		   AND Ccy_Amt >= G.Par2
           --AND B.Cust_Typ_Cd IN ('D','NP','FP')    --個人客戶身份別(待確認)
         GROUP BY B.Cust_No

        --匯出
        INSERT INTO #TMP_DataD (Cust_No,AMT_SUM_OUT)  
        SELECT  B.Cust_No            AS 'Cust_No'
			   ,SUM(A.CCY_Amt)       AS 'AMT_SUM_OUT'
          FROM FB_Txn A
	      JOIN FB_Cust_Acct AA ON A.Acct_No = AA.Acct_No
          JOIN FB_Cust B ON B.Cust_No = AA.Cust_No
          JOIN Cd_Txn C ON A.Txn_key = C.Txn_key
          join Cd_Risk_Cntry E on A.Cntry_Cd_3 = E.Risk_Cntry_Cd and E.Risk_Lvl = 'H'
		  JOIN FD_Cust_Cate F ON B.Cust_No= F.Cust_No        --客群分群
		  JOIN #SAR_CATE_VALUE G ON G.Cust_Cate = F.SAR_CATE --態樣分群
         WHERE C.AC_DC_Typ = 'D'
		   AND F.Cust_Cate = @SAR_CATE
           and C.AML_Prod_Typ ='RM'
           AND A.Txn_Dt BETWEEN G.Par1_Dt AND @Data_Dt
		   AND Ccy_Amt >= G.Par2
		   --AND B.Cust_Typ_Cd IN ('D','NP','FP')   
         GROUP BY B.Cust_No
    -------------------------------------------------------------------------------
    -- Step4. 存入報表TABLE
    ------------------------------------------------------------------------------- 
    INSERT INTO FS_SAR_01_15_00
    (
           Data_Dt
          ,Cust_No
          ,Cust_Nm
		  ,Alt_Dt
          ,Txn_Amt_In
          ,Txn_Amt_O
          ,Lst_Maint_Usr
          ,Lst_Maint_DT
    )
        select @Cycle_Dt
              ,C.Cust_No
              ,C.Cust_Nm
			  ,@Cycle_Dt
              ,AMT_SUM_IN as 'Txn_Amt_In'
              ,AMT_SUM_OUT as 'Txn_Amt_O'
			  ,'FB_Txn' as 'Lst_Maint_Usr'
			  ,getdate() as 'Lst_Maint_DT'
      FROM #TMP_DataC A
      JOIN #TMP_DataD B ON A.Cust_No = B.Cust_No
      JOIN FB_Cust C ON A.Cust_No = C.Cust_No

    SET @INS_CNT = @INS_CNT + @@ROWCOUNT
    -------------------------------------------------------------------------------
    -- Step5. 存入明細資料檔
    ------------------------------------------------------------------------------- 
    INSERT INTO FS_SAR_01_15_00_Dtl
    (
		   Case_Id,[Data_Dt]
		  ,[Txn_Ref_No]
		  ,[Acct_No]
		  ,[Txn_Key]
		  ,[Txn_Time]
		  ,[Txn_Dt]
		  ,[Br_No]
		  ,[Ccy_Amt]
		  ,[Ccy_Cd]
		  ,[Ccy_Amt_In_Txn_Ccy]
		  ,[Txn_Tool_Typ]
		  ,AC_DC_Typ
          ,AML_Prod_Typ
          ,Cntry_Cd_3
    )
    SELECT Case_Id,@Cycle_Dt
		,[Txn_Ref_No]
		,C.[Acct_No]
		,C.[Txn_Key]
		,[Txn_Time]
		,[Txn_Dt]
		,[Br_No]
		,[Ccy_Amt]
		,[Ccy_Cd]
		,[Ccy_Amt_In_Txn_Ccy]
		,C.[Txn_Tool_Typ]
		,D.AC_DC_Typ
        ,D.AML_Prod_Typ
        ,C.Cntry_Cd_3
     FROM FS_SAR_01_15_00 S
	  JOIN #TMP_DataC A ON S.Cust_No = A.Cust_No
      JOIN #TMP_DataD B ON S.Cust_No = B.Cust_No
	 join FB_Cust_Acct AA on S.Cust_No = AA.Cust_No
     JOIN FB_Txn C ON AA.Acct_No = C.Acct_No
     JOIN Cd_Txn D ON C.Txn_Key = D.Txn_Key
	 join Cd_Risk_Cntry E on C.Cntry_Cd_3 = E.Risk_Cntry_Cd and E.Risk_Lvl = 'H'
	 JOIN FD_Cust_Cate F ON B.Cust_No= F.Cust_No        --客群分群
	 JOIN #SAR_CATE_VALUE G ON G.Cust_Cate = F.SAR_CATE --態樣分群
    WHERE F.Cust_Cate = @SAR_CATE
	  and D.AC_DC_Typ in ('C','D')
      and C.AML_Prod_Typ ='RM'
      AND C.Txn_Dt >= G.Par1_Dt AND C.Txn_Dt < @Data_Dt
	  AND Ccy_Amt >= G.Par2
	  and S.Data_Dt = @Cycle_Dt 
	  
   SET @ST_SEQ += 1
 END	  
    -------------------------------------------------------------------------------
    -- Step6. 存入案件管理TABLE(包TRY CATCH)
    -------------------------------------------------------------------------------   
    BEGIN TRY 
		BEGIN TRAN 
	insert into [FS_SAR_Case_Info]
	(  Case_Id,[Cust_Nm]
      ,[Cust_No]
      ,[SAR_Id]
      ,[SAR_Version]
      ,[Data_Dt]
      ,[Confirm_Status]
      ,[Rpt_Dtl_Src_Table]        
      ,[Lst_UpDt_Src]
      ,[Lst_UpDt_Dtm]
	  ,Case_Br_No
	)
	select Case_Id
	      ,A.[Cust_Nm]
		  ,A.[Cust_No]
		  ,D.SAR_Id,D.[SAR_Version]
		  ,@Cycle_Dt
		  ,'1'
		  ,'FS_SAR_01_15_00_Dtl'      
          ,'AML'
		  ,GETDATE()
		  ,C.Cust_Lst_Br_No
		  from FS_SAR_01_15_00 A
		  join FD_Cust_Stat C on A.Cust_No = C.Cust_No
		  join Cd_SAR_Par_V2 D on D.SAR_Id = 'SAR_01_15_00'
		 where A.Data_Dt = @Cycle_Dt  
        COMMIT TRAN
	END TRY 
	BEGIN CATCH
	    ROLLBACK;
		SET @EXEC_DESC = CONCAT(ERROR_NUMBER(),':',ERROR_MESSAGE())
		RAISERROR (@EXEC_DESC, 16, 1);  
	END CATCH
    -------------------------------------------------------------------------------
    -- 程式結束
    -------------------------------------------------------------------------------

    ------------------------------
    -- 執行結果紀錄
    ------------------------------
    -- 執行狀態
    SET @JOB_END_TIME = GETDATE();
    -- OUT 回傳值
    IF @PARAM <> '' BEGIN SET @PARAM = @PARAM + ';' END
    IF ISNULL(@EXEC_DESC,'') <> '' BEGIN SET @PARAM = @PARAM + 'EXEC_DESC=' + @EXEC_DESC + ';'; END
    SET @PARAM = @PARAM + 'JOB_START_TIME=' + CONVERT(VARCHAR(30),@JOB_START_TIME,121) + ';';
    SET @PARAM = @PARAM + 'JOB_END_TIME=' + CONVERT(VARCHAR(30),@JOB_END_TIME,121) + ';';
    SET @PARAM = @PARAM + 'SRC_TBLNM=' + @SRC_TBLNM + ';';
    SET @PARAM = @PARAM + 'TGT_TBLNM=' + @TGT_TBLNM + ';';
    SET @PARAM = @PARAM + 'INS_CNT=' + CAST(@INS_CNT AS VARCHAR (100)) + ';';
    SET @PARAM = @PARAM + 'UPD_CNT=' + CAST(@UPD_CNT AS VARCHAR (100)) + ';';
    SET @PARAM = @PARAM + 'DEL_CNT=' + CAST(@DEL_CNT AS VARCHAR (100)) + ';';



GO