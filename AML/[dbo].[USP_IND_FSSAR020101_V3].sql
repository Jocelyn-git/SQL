USE [AML_SARDB]
GO
/****** Object:  StoredProcedure [dbo].[USP_IND_FSSAR020101]    Script Date: 2018/10/5 下午 02:01:58 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[USP_IND_FSSAR020101_V2]
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
-- Comment         : SAR-02-01-01
-- 情境規則        : 客戶當日還款(含一般放款、進口貸款, 聯貸案, 應收帳款賣方還款且放款狀態為正常)
--					 且撥款日至實際還款日天數/撥款日至訂約到期日天數<=(0)%
/*****************************************************
-- 版本說明(Version Description)
----------------------------------------------
01).2019/09/20 Jocelyn, Initial
02).2020/05/18 Jocelyn, 依照新訂客戶分群調整
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
	DECLARE @Par1 decimal(4,2)
    DECLARE @Data_Dt DATE
    DECLARE @Cycle_Dt DATE
	DECLARE @Data_Dt1 DATE
    ------------------------------
    -- 設定必輸入變數
    ------------------------------
    -- 執行狀態
    SET @SRC_TBLNM = 'FB_Acct||FB_party||FB_Acct_Bal||Cd_SAR_Par||FB_AcctBal||FB_Party_Acct';  --有多個 Source Table 時使用<||>分隔符號區分
    SET @TGT_TBLNM = 'FS_SAR_02_01_01';  --通常只會填一個 Target Table    

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
    SET @SAR_Id = 'SAR_02_01_01'
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
	  WHERE S1.SAR_ID = 'SAR_02_01_01'--指定SAR_ID
	    AND S1.SAR_CATE IN ('HCLN','CLN') --指定客群 

	DECLARE @ST_SEQ INT = 1
	DECLARE @EN_SEQ INT = (SELECT MAX(SEQ) AS SEQ FROM #SAR_CATE)

    WHILE (@ST_SEQ <= @EN_SEQ)
	BEGIN
	  
			SELECT @SAR_CATE = SAR_CATE FROM #SAR_CATE
			WHERE SEQ = @ST_SEQ
	
			SELECT  @SAR_Version = SAR_Version
					,@Par1 = Par1    --比例
			  FROM #SAR_CATE
			 WHERE SAR_Id = @SAR_Id
			   AND SAR_CATE = @SAR_CATE

			SET @Cycle_Dt = CONVERT(DATE, @ETL_END_DATE)
			SELECT @Data_Dt = LBSDT FROM AML_MetaFlow..CB_DT WHERE DATADT = @Cycle_Dt --資料日期為前一個營業日
			SET @Data_Dt1 = DATEADD(D,-1, @Data_Dt) --資料日期為@Data_Dt-1天
			
		INSERT INTO #SAR_CATE_VALUE SELECT @SAR_CATE,@Par1
		
	 SET @ST_SEQ += 1
    END

    IF OBJECT_ID('TEMPDB..#TMP_Data') IS NOT NULL BEGIN
		DROP TABLE #TMP_SAR
	END

    IF OBJECT_ID('TEMPDB..#TMP_Data') IS NOT NULL BEGIN
		DROP TABLE #TMP_SAR
	END

	CREATE TABLE #TMP_Data
    (
         Cust_No          VARCHAR(50)	   --客戶編號
		,Cust_Nm          VARCHAR(200)	   --客戶名稱
        ,Acct_No          VARCHAR(200)	   --帳戶編號
        ,Payment_Days     INT              --撥款日至還款日天數
		,Appropriate_Days INT              --撥款日至到期日天數
		,Txn_Amt          DECIMAL(25,5)    --當日還款
    ) 

    CREATE TABLE #TMP_SAR
    (
        Acct_No          VARCHAR(50)	   --帳戶編號
	   ,Rt               DECIMAL(8,4)      --撥款日至還款日天數除以撥款日至到期日天數比例
    )
    -------------------------------------------------------------------------------
    -- Step2. 刪除已轉入資料
    -------------------------------------------------------------------------------
    --主TABLE
    DELETE FS_SAR_02_01_01 WHERE Data_Dt = @Cycle_Dt
    --明細TABLE
    DELETE FS_SAR_02_01_01_Dtl WHERE Data_Dt = @Cycle_Dt
    --案件管理Table
	DELETE FS_SAR_Case_Info WHERE Data_Dt = @Cycle_Dt and SAR_Id = 'SAR_02_01_01'
    -------------------------------------------------------------------------------
    -- Step3. 交易監控
    -------------------------------------------------------------------------------
	SET @ST_SEQ = 1
	SET @EN_SEQ = (SELECT MAX(SEQ) AS SEQ FROM #SAR_CATE)

    WHILE (@ST_SEQ <= @EN_SEQ)
	BEGIN
	  SELECT @SAR_CATE = SAR_CATE FROM #SAR_CATE
	  WHERE SEQ = @ST_SEQ
	  
	  
        INSERT INTO #TMP_Data
        SELECT S3.Cust_No
			  ,S3.Cust_Nm
              ,S1.Acct_No
			  ,DATEDIFF(D,S1.Acct_Opn_Dt,S6.posted_Dt)	  As 'Payment_Days'
			  ,DATEDIFF(D,S1.Acct_Opn_Dt,S1.Mtur_Dt)      AS 'Appropriate_Days'
			  ,SUM(S5.Acct_Bal)-SUM(S4.Acct_Bal)          as 'Txn_Amt'
          FROM FB_Acct S1
          JOIN FB_Cust_Acct S2 ON S1.Acct_No = S2.Acct_No
          JOIN FB_Cust S3 ON S2.Cust_No = S3.Cust_No
          JOIN FB_Acct_Bal S4 ON S4.Acct_No = S1.Acct_No 
		  JOIN FB_Acct_Bal S5 ON S5.Acct_No = S1.Acct_No 
		  JOIN FB_Txn S6 ON S1.Acct_NO = S6.Acct_No
          JOIN Cd_Txn S7 ON S6.Txn_Key = S7.Txn_Key 
		 WHERE S1.AML_Prod_Typ ='LN'
		   AND S1.Clt_Ind <> 'Y'
		   AND S1.Over_Due_Ind <> 'Y' 
           AND S4.Data_Dt = @Data_Dt
           AND S5.Data_Dt = @Data_Dt1
           AND S6.Txn_Dt = @Data_Dt
           AND S7.AML_Prod_Typ = 'LN'
           AND ((S4.Acct_Bal > 0 OR S5.Acct_Bal > 0) AND S4.Acct_Bal <> S5.Acct_Bal)
         GROUP BY S3.Cust_No
				 ,S3.Cust_Nm
                 ,S1.Acct_No
				 ,S1.Acct_Opn_Dt
				 ,S6.posted_Dt
				 ,S1.Mtur_Dt

        INSERT INTO #TMP_SAR
        SELECT  Acct_No
			   ,CAST(Payment_Days AS DECIMAL(6,1))/CAST(Appropriate_Days AS DECIMAL(6,1)) as'Rt'
          FROM #TMP_Data  
         WHERE Txn_Amt > 0        
    -------------------------------------------------------------------------------
    -- Step4. 存入報表TABLE
    ------------------------------------------------------------------------------- 
    INSERT INTO FS_SAR_02_01_01
    (
		   [Data_Dt]
		  ,[Cust_No]
		  ,[Cust_Nm]
          ,[Acct_No]
		  ,[Alt_Dt]
		  ,[Txn_Amt]
		  ,[Payment_Days]
		  ,[Appropriate_Days]
		  ,[Lst_Maint_Usr]
		  ,[Lst_Maint_DT]
    )
        select @Cycle_Dt
              ,S2.Cust_No
              ,S2.Cust_Nm
              ,S2.Acct_No
              ,@Cycle_Dt
              ,S2.Txn_Amt
			  ,S2.Payment_Days
			  ,S2.Appropriate_Days
			  ,'FB_Acct'
			  ,getdate()
      FROM #TMP_SAR S1
      JOIN #TMP_Data S2 ON S1.Acct_no = S2.Acct_no
	  JOIN FB_Cust_Acct S3 ON S3.Acct_No = S1.Acct_No
	  JOIN FD_Cust_Cate S4 ON S3.Cust_No= S4.Cust_No        --客群資料
	  JOIN #SAR_CATE_VALUE S5 ON S4.Cust_Cate = S5.SAR_CATE --態樣分群
	 WHERE S1.Rt <= S5.Par1
	   AND S4.Cust_Cate = @SAR_CATE

   SET @ST_SEQ += 1
 END
    
	SET @INS_CNT = @INS_CNT + @@ROWCOUNT
    -------------------------------------------------------------------------------
    -- Step5. 存入明細資料檔
    ------------------------------------------------------------------------------- 
    INSERT INTO FS_SAR_02_01_01_Dtl
    (
           Case_Id
          ,Data_Dt
          ,Txn_Dt
          ,Acct_No
          ,Segment_Id
          ,Acct_Bal
    )
    SELECT Case_Id
           ,@Cycle_Dt
           ,S2.Data_Dt
		   ,S1.Acct_No
		   ,S2.Segment_Id
		   ,S2.Acct_Bal
     FROM FS_SAR_02_01_01 S1
	 JOIN FB_Acct_Bal S2 on S1.Acct_No = S2.Acct_No 
	WHERE S1.Data_Dt = @Cycle_Dt 
      AND S2.Data_Dt BETWEEN @Data_Dt1 AND @Data_Dt
    -------------------------------------------------------------------------------
    -- Step6. 存入案件管理TABLE(包TRY CATCH)
    -------------------------------------------------------------------------------   
    BEGIN TRY 
		BEGIN TRAN 
	        INSERT INTO [FS_SAR_Case_Info]
	        (  
                   [Case_Id]
                   ,[Cust_Nm]
                   ,[Cust_No]
                   ,[SAR_Id]
                   ,[SAR_Version]
                   ,[Data_Dt]
                   ,[Confirm_Status]
                   ,[Rpt_Dtl_Src_Table]        
                   ,[Lst_UpDt_Src]
                   ,[Lst_UpDt_Dtm]
	               ,[Case_Br_No]
	        )
	        SELECT S1.Case_Id
	              ,S1.[Cust_Nm]
		          ,S1.[Cust_No]
		          ,S3.SAR_Id
                  ,S3.[SAR_Version]
		          ,@Cycle_Dt
		          ,'1'
		          ,'FS_SAR_02_01_01_Dtl'      
                  ,'AML'
		          ,GETDATE()
		          ,S2.Cust_Lst_Br_No
		      FROM FS_SAR_02_01_01 S1
		      JOIN FD_Cust_Stat S2 ON S1.Cust_No = S2.Cust_No
		      JOIN Cd_SAR_Par_V2 S3 ON S3.SAR_Id = 'SAR_02_01_01'
		     WHERE S1.Data_Dt = @Cycle_Dt  
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