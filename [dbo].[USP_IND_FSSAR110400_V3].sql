USE [AML_SARDB]
GO
/****** Object:  StoredProcedure [dbo].[USP_IND_FSSAR110400]    Script Date: 2018/4/9 下午 03:24:46 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[USP_IND_FSSAR110400_V2]
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
-- DATA SOURCE     : 
-- COMMENT         : SAR-11-04-00
-- 情境規則         : 同一客戶過去(0)天之跨境交易匯入或匯出自高避稅風險或高金融保密黑名單國家筆數分別>=(1)筆
--                   且累計金額分別>=菲幣(2)萬元
/*****************************************************
-- 版本說明(VERSION DESCRIPTION)
----------------------------------------------
01).2019/11/05 Jocelyn, Initial
03).2020/05/06 Jocelyn, 依照新訂客戶分群調整Procedure
*****************************************************/

    ------------------------------
    -- 宣告變數
    ------------------------------
    -- 以下為執行狀態相關變數
    DECLARE @JOB_START_TIME  DATETIME;       --程式起始時間
    DECLARE @JOB_END_TIME    DATETIME;       --程式結束時間
    DECLARE @SRC_TBLNM       VARCHAR(500);   --來源 TABLE，SAMPLE：SOURCETABLE1||SOURCETABLE2。
    DECLARE @TGT_TBLNM       VARCHAR(500);   --目地 TABLE，SAMPLE：TARGETTABLE。
    DECLARE @INS_CNT         DECIMAL(10,0);  --新增資料筆數
    DECLARE @UPD_CNT         DECIMAL(10,0);  --更新資料筆數
    DECLARE @DEL_CNT         DECIMAL(10,0);  --刪除資料筆數
	DECLARE @EXEC_DESC       VARCHAR(4000);  --執行資訊

    -- 以下為 PROCEDURE 內自訂變數
    DECLARE @SAR_CATE    VARCHAR(10)	 --客群分類
	DECLARE @SAR_Version INT             --變數版本
    DECLARE @SAR_Id      VARCHAR(18)     --SAR_ID
    DECLARE @PAR1 INT           	     --天數
    DECLARE @PAR2 INT           		 --交易次數
    DECLARE @PAR3 INT           		 --金額(累計)
    DECLARE @DATA_DT DATE       		 --資料日期
    DECLARE @CYCLE_DT DATE      		 --作業日期
    DECLARE @PAR1_DT DATE
    ------------------------------
    -- 設定必輸入變數
    ------------------------------
    -- 執行狀態
    SET @SRC_TBLNM = 'FB_Txn||FB_Cust||CD_TXN||CD_SAR_PAR||FB_Cust_Acct';  --有多個 SOURCE TABLE 時使用<||>分隔符號區分
    SET @TGT_TBLNM = 'FS_SAR_11_04_00';  --通常只會填一個 TARGET TABLE    

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
    -- STEP1. 暫存TABLE&參數初始化
    -------------------------------------------------------------------------------
    SET @SAR_Id = 'SAR_11_04_00'
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
	  WHERE S1.SAR_ID = 'SAR_11_04_00'--指定SAR_ID
	    AND S1.SAR_CATE NOT IN ('DEFAULT') --指定客群 

	DECLARE @ST_SEQ INT = 1
	DECLARE @EN_SEQ INT = (SELECT MAX(SEQ) AS SEQ FROM #SAR_CATE)

    WHILE (@ST_SEQ <= @EN_SEQ)
	BEGIN
	  
		SELECT @SAR_CATE = SAR_CATE FROM #SAR_CATE
		WHERE SEQ = @ST_SEQ
	
		SELECT @SAR_Version = SAR_Version
                ,@PAR1  = PAR1      --天數
                ,@PAR2 = PAR2      --交易次數
                ,@PAR3 = PAR3      --金額(累計)
          FROM #SAR_CATE
         WHERE SAR_Id = @SAR_Id
	       AND SAR_CATE = @SAR_CATE

		SET @CYCLE_DT = CONVERT(DATE, @ETL_END_DATE)
		SELECT @DATA_DT = LBSDT FROM AML_MetaFlow..CB_DT WHERE DATADT = @Cycle_Dt --資料日期為前一個營業日
		SET @PAR1_DT = DATEADD(D,-@PAR1+1,@DATA_DT)
	
	   INSERT INTO #SAR_CATE_VALUE SELECT @SAR_CATE,@PAR1_DT,@PAR2,@PAR3

      SET @ST_SEQ += 1
    END
		
 
    IF OBJECT_ID('TEMPDB..#TMP_DATA_IN') IS NOT NULL BEGIN
        DROP TABLE #TMP_DATA_IN
    END

    IF OBJECT_ID('TEMPDB..#TMP_DATA_OUT') IS NOT NULL BEGIN
        DROP TABLE #TMP_DATA_OUT
    END 

    IF OBJECT_ID('TEMPDB..#TMP_DATA') IS NOT NULL BEGIN
        DROP TABLE #TMP_DATA
    END

    IF OBJECT_ID('TEMPDB..#TMP_SAR') IS NOT NULL BEGIN
        DROP TABLE #TMP_SAR
    END

     CREATE TABLE #TMP_DATA_T
    (
         Cust_No        VARCHAR(50)      --帳戶編號
        ,AMT     DECIMAL(25,5)           --交易金額
        ,AC_DC_Typ     VARCHAR(50)       --交易類型
    )    

    CREATE TABLE #TMP_DATA_IN
    (
         Cust_No        VARCHAR(50)      --帳戶編號
        ,AMT_SUM     DECIMAL(25,5)       --總交易金額
        ,AMT_CNT     INT --筆數
    )    

    CREATE TABLE #TMP_DATA_OUT
    (
         Cust_No        VARCHAR(50)      --帳戶編號
        ,AMT_SUM     DECIMAL(25,5)       --總交易金額
        ,AMT_CNT     INT --筆數
    )

    CREATE TABLE #TMP_DATA
    (
         Cust_No        VARCHAR(50)      --帳戶編號
    )

    CREATE TABLE #TMP_SAR
    (
        Cust_No     VARCHAR(50)       --客戶編號
    )
    -------------------------------------------------------------------------------
    -- STEP2. 刪除已轉入資料
    -------------------------------------------------------------------------------
    --主TABLE
    DELETE FS_SAR_11_04_00 WHERE DATA_DT = @CYCLE_DT
    --明細TABLE
    DELETE FS_SAR_11_04_00_Dtl WHERE DATA_DT = @CYCLE_DT
    --案件管理TABLE
	DELETE FS_SAR_Case_Info WHERE DATA_DT = @CYCLE_DT AND SAR_ID = @SAR_Id        
    -------------------------------------------------------------------------------
    -- STEP3. 交易監控
    -------------------------------------------------------------------------------
    SET @ST_SEQ = 1
	SET @EN_SEQ = (SELECT MAX(SEQ) AS SEQ FROM #SAR_CATE)

    WHILE (@ST_SEQ <= @EN_SEQ)
	BEGIN
	  SELECT @SAR_CATE = SAR_CATE FROM #SAR_CATE
	  WHERE SEQ = @ST_SEQ

        --過去N天跨境交易
        INSERT INTO #TMP_DATA_T
        (
         Cust_No 
        ,AMT
        ,AC_DC_Typ
        ) 
        SELECT B.Cust_No 
               ,A.CCY_AMT
               ,C.AC_DC_Typ
         FROM DBO.FB_Txn AS A
            JOIN DBO.CD_TXN AS C                    ON A.TXN_KEY  = C.TXN_KEY
            JOIN DBO.FB_Cust_Acct AS AC        ON A.ACCT_NO  = AC.ACCT_NO
            JOIN DBO.FB_Cust AS B                    ON B.Cust_No  = AC.Cust_No
            JOIN DBO.Cd_Risk_Cntry AS E         ON A.CNTRY_CD_3  = E.Risk_Cntry_Cd
		    JOIN FD_Cust_Cate F ON B.Cust_No= F.Cust_No        --客群分群
	        JOIN #SAR_CATE_VALUE G ON F.Cust_Cate = G.SAR_CATE --態樣分群
         WHERE E.Risk_Tax_Evasion_Ind = 'Y' AND C.AML_PROD_TYP = 'RM'
           AND A.TXN_DT BETWEEN G.PAR1_DT AND @DATA_DT
		   AND F.Cust_Cate = @SAR_CATE
           --AND B.Cust_Typ_Cd IN ('D','NP','FP') 
           AND C.AC_DC_Typ IN ('C','D')

        --過去N天跨境匯入
        INSERT INTO #TMP_DATA_IN
        (
         Cust_No 
        ,AMT_SUM
        ,AMT_CNT
        ) 
        SELECT Cust_No 
               ,SUM(AMT)
               ,COUNT(AMT)
         FROM #TMP_DATA_T
         WHERE AC_DC_Typ = 'C'
         GROUP BY Cust_No
		 
		 
		INSERT INTO #TMP_DATA(Cust_No)
        SELECT A.Cust_No
          FROM #TMP_DATA_IN A
		  JOIN FD_Cust_Cate B ON A.Cust_No= B.Cust_No        --客群分群
	      JOIN #SAR_CATE_VALUE C ON B.Cust_Cate = C.SAR_CATE --態樣分群
         WHERE B.Cust_Cate = @SAR_CATE
           AND AMT_CNT >= C.PAR2
		   AND AMT_SUM >= C.PAR3
         --PRINT('過去N天跨境匯入-個人')

        --過去N天跨境匯款匯出
        INSERT INTO #TMP_DATA_OUT
        (
         Cust_No 
        ,AMT_SUM
        ,AMT_CNT 
        ) 
        SELECT Cust_No 
               ,SUM(AMT)
               ,COUNT(AMT) 
         FROM #TMP_DATA_T
         WHERE AC_DC_Typ = 'D'
         GROUP BY Cust_No
		
		
		INSERT INTO #TMP_DATA(Cust_No)
        SELECT Cust_No
         FROM #TMP_DATA_OUT A
		 JOIN FD_Cust_Cate B ON A.Cust_No= B.Cust_No        --客群分群
	     JOIN #SAR_CATE_VALUE C ON B.Cust_Cate = C.SAR_CATE --態樣分群
         WHERE B.Cust_Cate = @SAR_CATE
              AND AMT_CNT >= C.PAR2
			  AND AMT_SUM >= C.PAR3
         --PRINT('過去N天跨境匯款匯出-個人')

		INSERT INTO #TMP_SAR(Cust_No)
        SELECT DISTINCT Cust_No    FROM #TMP_DATA
           --PRINT '個人'
    -------------------------------------------------------------------------------
    -- STEP4. 存入報表TABLE
    ------------------------------------------------------------------------------- 
    INSERT INTO FS_SAR_11_04_00
    (
            DATA_DT
            ,Cust_No
            ,Cust_Nm
            ,ALT_DT
            ,TXN_CNT
            ,TXN_AMT
            ,LST_MAINT_USR
            ,LST_MAINT_DT
    )
    SELECT DISTINCT @CYCLE_DT
                    ,SS.Cust_No
                    ,S4.Cust_Nm 
                    ,@CYCLE_DT
                    ,S1.AMT_CNT+S2.AMT_CNT AS 'TXN_CNT'
                    ,S1.AMT_SUM+S2.AMT_SUM AS 'TXN_AMT'
                    ,'FB_Txn' AS 'LST_MAINT_USR'
                    ,GETDATE() AS 'LST_MAINT_DT'
     FROM #TMP_SAR AS SS
     JOIN #TMP_DATA_IN AS S1  ON SS.Cust_No = S1.Cust_No
     JOIN #TMP_DATA_OUT AS S2 ON S2.Cust_No = SS.Cust_No
     JOIN FB_Cust AS S4       ON S4.Cust_No = SS.Cust_No    
    
    SET @INS_CNT = @INS_CNT + @@ROWCOUNT   
    -------------------------------------------------------------------------------
    -- STEP5. 存入明細資料檔
    ------------------------------------------------------------------------------- 
    INSERT INTO FS_SAR_11_04_00_DTL
    (
          CASE_ID
          ,DATA_DT
          ,TXN_REF_NO
          ,ACCT_NO
          ,TXN_KEY
          ,TXN_TIME
          ,TXN_DT
          ,BR_NO
          ,CCY_AMT
          ,CCY_CD
          ,CCY_AMT_IN_TXN_CCY
          ,TXN_TOOL_TYP
          ,AC_DC_Typ
          ,AML_Prod_Typ
          ,Cntry_Cd_3
    )
    SELECT 
           CASE_ID
           ,@CYCLE_DT
           ,A.TXN_REF_NO
           ,A.ACCT_NO
           ,A.TXN_KEY
           ,TXN_TIME
           ,TXN_DT
           ,BR_NO
           ,CCY_AMT
           ,CCY_CD
           ,CCY_AMT_IN_TXN_CCY
           ,A.TXN_TOOL_TYP
           ,C.AC_DC_Typ
           ,C.AML_Prod_Typ
           ,A.Cntry_Cd_3
     FROM FS_SAR_11_04_00 AS S
       JOIN FB_Cust_Acct AS D  ON S.Cust_No = D.Cust_No
       JOIN FB_Txn AS A  ON A.ACCT_NO = D.ACCT_NO
       JOIN CD_TXN AS C ON A.TXN_KEY = C.TXN_KEY     
       JOIN DBO.Cd_Risk_Cntry AS E  ON A.CNTRY_CD_3  = E.Risk_Cntry_Cd
	   JOIN FD_Cust_Cate F          ON S.Cust_No= F.Cust_No        --客群分群
	   JOIN #SAR_CATE_VALUE G       ON F.Cust_Cate = G.SAR_CATE    --態樣分群
    WHERE A.TXN_DT BETWEEN G.PAR1_DT AND @DATA_DT
	  AND F.Cust_Cate = @SAR_CATE
      AND C.AC_DC_TYP IN ('C','D')  AND E.Risk_Tax_Evasion_Ind = 'Y'
      AND A.AML_Prod_Typ = 'RM'
      AND S.Data_Dt = @CYCLE_DT    
	  
     SET @ST_SEQ += 1
    END
    -------------------------------------------------------------------------------
    -- STEP6. 存入案件管理TABLE(包TRY CATCH)
    -------------------------------------------------------------------------------   
        BEGIN TRY 
		BEGIN TRAN 
	INSERT INTO FS_SAR_Case_Info
            (
                  SAR_Id
                  ,Data_Dt
                  ,Case_Id
                  ,Cust_Nm
                  ,Cust_No
                  ,SAR_Version
                  ,Confirm_Status
                  ,Rpt_Dtl_Src_Table
                  ,Lst_UpDt_Src
                  ,Lst_UpDt_Dtm
                  ,Case_Br_No
            )
            SELECT @SAR_Id
                   ,@Cycle_Dt
                   ,S.Case_Id
                   ,S.Cust_Nm
                   ,S.Cust_No
                   ,@SAR_Version
                   ,'1'
                   ,'FS_SAR_11_04_00_Dtl'
                   ,'AML'
                   ,GETDATE()
                   ,D.Cust_Lst_Br_No
              FROM FS_SAR_11_04_00 S
              JOIN FD_Cust_Stat D ON S.Cust_No = D.Cust_No
             WHERE S.Data_Dt = @Cycle_Dt     
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