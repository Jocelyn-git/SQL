USE [AML_SARDB]
GO
/****** Object:  StoredProcedure [dbo].[USP_IND_FSSAR011200]    Script Date: 2018/4/3 下午 02:48:43 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[USP_IND_FSSAR011200_V2]
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
-- Comment         : SAR-01-12-00
-- 情境規則        : 同一客戶於當天以現金辦理匯款其合計金額>=台幣(0)萬元
--					 且
--					 單筆<=(1)萬元之上述交易合計筆數>=(2)筆
/*****************************************************
-- 版本說明(Version Description)
----------------------------------------------
01).2019/10/01 Jocelyn, Initial
03).2020/04/19 Jocelyn, 依照新訂客戶分群調整
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
	DECLARE @Par1 INT 					--合計金額
    DECLARE @Par2 INT					--單筆金額
    DECLARE @Par3 INT 					--筆數

    DECLARE @Data_Dt DATE
    DECLARE @Cycle_Dt DATE
    ------------------------------
    -- 設定必輸入變數
    ------------------------------
    -- 執行狀態
    SET @SRC_TBLNM = 'FB_Txn||FB_Cust||Cd_Txn||Cd_SAR_Par';  --有多個 Source Table 時使用<||>分隔符號區分
    SET @TGT_TBLNM = 'FS_SAR_01_12_00';  --通常只會填一個 Target Table    

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
	SET @SAR_Id = 'SAR_01_12_00'
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
	  WHERE S1.SAR_ID = 'SAR_01_12_00'--指定SAR_ID
	    AND S1.SAR_CATE NOT IN ('DEFAULT') --指定客群

	DECLARE @ST_SEQ INT = 1
	DECLARE @EN_SEQ INT = (SELECT MAX(SEQ) AS SEQ FROM #SAR_CATE)

    WHILE (@ST_SEQ <= @EN_SEQ)
	BEGIN
	  
		SELECT @SAR_CATE = SAR_CATE FROM #SAR_CATE
		WHERE SEQ = @ST_SEQ   

		SELECT @SAR_Version = SAR_Version
				,@Par1 = Par1    --合計金額
				,@Par2 = Par2    --單筆金額
				,@Par3 = Par3    --筆數
		  FROM #SAR_CATE 
		 WHERE SAR_Id = @SAR_Id
	       AND SAR_CATE = @SAR_CATE

		SET @Cycle_Dt = CONVERT(DATE, @ETL_END_DATE)
		SELECT @Data_Dt = LBSDT FROM AML_MetaFlow..CB_DT WHERE DATADT = @Cycle_Dt --資料日期為前一個營業日

		INSERT INTO #SAR_CATE_VALUE SELECT @SAR_CATE,@Par1,@Par2,@Par3  

      SET @ST_SEQ += 1
    END
	 --PRINT CONCAT('@Par1_Dt=',@Par1_Dt)
	 --PRINT CONCAT('@Data_Dt=',@Data_Dt)

    IF OBJECT_ID('TEMPDB..#TMP_Data') IS NOT NULL BEGIN
		DROP TABLE #TMP_Data
	END

    IF OBJECT_ID('TEMPDB..#TMP_SAR') IS NOT NULL BEGIN
		DROP TABLE #TMP_SAR
	END

    CREATE TABLE #TMP_Data
    (
         Cust_No				VARCHAR(50)   --客戶編號
		,Cust_Nm				VARCHAR(200)   --客戶名稱
        ,AMT_SUM                DECIMAL(25,5) --總交易金額
        ,AMT_CNT                INT           --總交易筆數
    )    

    CREATE TABLE #TMP_SAR
    (
         Cust_No     VARCHAR(50)       --客戶編號
		,Cust_Nm	  VARCHAR(200)      --客戶名稱
		,AMT_CNT	  int				--總交易筆數
		,AMT_SUM	  int				--總交易金額
    )

    -------------------------------------------------------------------------------
    -- Step2. 刪除已轉入資料
    -------------------------------------------------------------------------------
    --主TABLE
    DELETE FS_SAR_01_12_00 WHERE Data_Dt = @Cycle_Dt
    --明細TABLE
    DELETE FS_SAR_01_12_00_Dtl WHERE Data_Dt = @Cycle_Dt
    --案件管理Table
    DELETE FS_SAR_Case_Info WHERE Data_Dt = @Cycle_Dt and SAR_Id = 'SAR_01_12_00'
    -------------------------------------------------------------------------------
    -- Step3. 交易監控
    -------------------------------------------------------------------------------
	SET @ST_SEQ = 1
	SET @EN_SEQ = (SELECT MAX(SEQ) AS SEQ FROM #SAR_CATE)

    WHILE (@ST_SEQ <= @EN_SEQ)
	BEGIN
	  SELECT @SAR_CATE = SAR_CATE FROM #SAR_CATE
	  WHERE SEQ = @ST_SEQ
        
		
		INSERT INTO #TMP_Data (Cust_No,Cust_Nm,AMT_SUM,AMT_CNT)  
        SELECT  B.Cust_No			 AS 'Cust_No'
			   ,B.Cust_Nm			 AS 'Cust_Nm'
               ,SUM(A.CCY_Amt)       AS 'AMT_SUM'
               ,COUNT(*)             AS 'AMT_CNT'
          FROM FB_Txn A
		  join FB_Cust_Acct AA on A.Acct_No = AA.Acct_No
          JOIN FB_Cust B ON  AA.Cust_No = B.Cust_No 
		  JOIN Cd_Txn C ON A.Txn_key = C.Txn_key
		  JOIN FD_Cust_Cate D ON AA.Cust_No= D.Cust_No        --客群分群
	      JOIN #SAR_CATE_VALUE E ON D.Cust_Cate = E.SAR_CATE --態樣分群
         WHERE A.Txn_Tool_Typ = 1
		   AND E.Cust_Cate = @SAR_CATE
           and A.AML_Prod_Typ in ('RM')
		   AND C.AC_DC_Typ IN ('C','D')
           AND A.Txn_Dt = @Data_Dt
		   AND Ccy_Amt <= E.Par2
           --AND B.Cust_Typ_Cd IN ('D','NP','FP')    
         GROUP BY B.Cust_No,B.Cust_Nm

        INSERT INTO #TMP_SAR(Cust_No,Cust_Nm,AMT_CNT,AMT_SUM)
        SELECT Cust_No
		      ,Cust_Nm
			  ,A.AMT_CNT
			  ,A.AMT_SUM
          FROM #TMP_Data A
		  JOIN FD_Cust_Cate B ON A.Cust_No= B.Cust_No        --客群分群
	      JOIN #SAR_CATE_VALUE C ON B.Cust_Cate = C.SAR_CATE --態樣分群
         WHERE B.Cust_Cate = @SAR_CATE
           and AMT_SUM >= C.Par1
		   and AMT_CNT >= C.Par3
    -------------------------------------------------------------------------------
    -- Step4. 存入報表TABLE
    ------------------------------------------------------------------------------- 
    INSERT INTO FS_SAR_01_12_00
    (
           Data_Dt
          ,Cust_No
          ,Cust_Nm
		  ,Alt_Dt
		  --,Txn_Amt_Cash_In
          ,Txn_Cnt
          ,Txn_Amt
          ,Lst_Maint_Usr
          ,Lst_Maint_DT
    )
        select @Cycle_Dt
              ,S2.Cust_No
              ,S2.Cust_Nm
			  ,@Cycle_Dt
			  --,sum(s2.AMT_SUM)	as 'Txn_Amt_Cash_In'
              ,s2.AMT_CNT			as 'Txn_Cnt'
              ,S2.AMT_SUM			as 'Txn_Amt'
			  ,'FB_Txn'		as 'Lst_Maint_Usr'
			  ,getdate()			as 'Lst_Maint_DT'
      FROM #TMP_SAR S1
      JOIN #TMP_Data S2 ON S1.Cust_No = S2.Cust_No

    SET @INS_CNT = @INS_CNT + @@ROWCOUNT
       -------------------------------------------------------------------------------
    -- Step5. 存入明細資料檔
    ------------------------------------------------------------------------------- 
    INSERT INTO FS_SAR_01_12_00_Dtl
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
		,B.[Acct_No]
		,B.[Txn_Key]
		,[Txn_Time]
		,[Txn_Dt]
		,[Br_No]
		,[Ccy_Amt]
		,[Ccy_Cd]
		,[Ccy_Amt_In_Txn_Ccy]
		,B.[Txn_Tool_Typ]
		,C.AC_DC_Typ
        ,C.AML_Prod_Typ
        ,B.Cntry_Cd_3
     FROM FS_SAR_01_12_00 S
	 join FB_Cust_Acct AA on S.Cust_No = AA.Cust_No
     JOIN FB_Txn B ON AA.Acct_No = B.Acct_No
     JOIN Cd_Txn C ON C.Txn_Key = B.Txn_Key
	 join FB_Cust D on S.Cust_No = D.Cust_No
	 JOIN FD_Cust_Cate E ON S.Cust_No= E.Cust_No        --客群分群
	 JOIN #SAR_CATE_VALUE F ON E.Cust_Cate = F.SAR_CATE --態樣分群
    WHERE B.Txn_Tool_Typ = 1
	  AND E.Cust_Cate = @SAR_CATE
      and B.AML_Prod_Typ in ('RM')
	  AND C.AC_DC_Typ IN ('C','D')
	--AND D.Cust_Typ_Cd IN ('D','NP','FP')    
      AND B.Txn_Dt = @Data_Dt
	  AND Ccy_Amt <= F.Par2
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
		  ,'FS_SAR_01_12_00_Dtl'      
          ,'AML'
		  ,GETDATE()
		  ,C.Cust_Lst_Br_No
		  from FS_SAR_01_12_00 A
		  join FD_Cust_Stat C on A.Cust_No = C.Cust_No 
		  join Cd_SAR_Par_V2 D on D.SAR_Id = 'SAR_01_12_00'
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