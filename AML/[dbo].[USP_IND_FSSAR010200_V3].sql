USE [AML_SARDB]
GO
/****** Object:  StoredProcedure [dbo].[USP_IND_FSSAR010200]    Script Date: 2019/7/26 上午 10:59:34 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[USP_IND_FSSAR010200_V2]
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
-- COMMENT         : SAR-01-02-00
--情境規則         : 同一客戶於{0}天內辦理多筆現金存、提、匯款交易，分別累計>=台幣{1}元且現金存、提、匯款交易次數各別>={2}次
/*****************************************************
-- 版本說明(VERSION DESCRIPTION)
----------------------------------------------
01).2019/09/06 Jocelyn, Initial
02).2020/03/02 Jocelyn, 依照新訂客戶分群調整
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
    DECLARE @SAR_CATE    VARCHAR(10)	--客群分類
	DECLARE @SAR_Version INT            --變數版本
    DECLARE @SAR_Id      VARCHAR(18)    --SAR_ID
    DECLARE @PAR1 INT           --天數
    DECLARE @PAR2 INT           --金額
    DECLARE @PAR3 INT           --交易次數
    DECLARE @DATA_DT DATE       --資料日期
    DECLARE @CYCLE_DT DATE      --作業日期
    DECLARE @PAR1_DT DATE

    ------------------------------
    -- 設定必輸入變數
    ------------------------------
    -- 執行狀態
    SET @SRC_TBLNM = 'FB_Txn||FB_Cust||CD_TXN||CD_SAR_PAR||FB_Cust_Acct';  --有多個 SOURCE TABLE 時使用<||>分隔符號區分
    SET @TGT_TBLNM = 'FS_SAR_01_02_00';  --通常只會填一個 TARGET TABLE    

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
    SET @SAR_Id = 'SAR_01_02_00'
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
	  WHERE S1.SAR_ID = 'SAR_01_02_00'--指定SAR_ID
	    AND S1.SAR_CATE NOT IN ('DEFAULT') --指定客群

	DECLARE @ST_SEQ INT = 1
	DECLARE @EN_SEQ INT = (SELECT MAX(SEQ) AS SEQ FROM #SAR_CATE)

    WHILE (@ST_SEQ <= @EN_SEQ)
	BEGIN
	  
		SELECT @SAR_CATE = SAR_CATE FROM #SAR_CATE
		WHERE SEQ = @ST_SEQ
		
		SELECT @SAR_Version = SAR_Version
          ,@PAR1 = PAR1      --天數
          ,@PAR2 = PAR2      --金額
          ,@PAR3 = PAR3      --交易次數
		  FROM #SAR_CATE
         WHERE SAR_Id = @SAR_Id
	       AND SAR_CATE = @SAR_CATE

		SET @CYCLE_DT = CONVERT(DATE, @ETL_END_DATE)
		SELECT @DATA_DT = LBSDT FROM AML_MetaFlow..CB_DT WHERE DATADT = @Cycle_Dt --資料日期為前一個營業日
		SET @PAR1_DT = DATEADD(D,-@PAR1+1,@DATA_DT)

		INSERT INTO #SAR_CATE_VALUE SELECT @SAR_CATE,@PAR1_DT,@Par2,@Par3  

      SET @ST_SEQ += 1
    END


    IF OBJECT_ID('TEMPDB..#TMP_DATA_I') IS NOT NULL BEGIN
        DROP TABLE #TMP_DATA_I
    END

    IF OBJECT_ID('TEMPDB..#TMP_DATA_O') IS NOT NULL BEGIN
        DROP TABLE #TMP_DATA_O
    END   

    IF OBJECT_ID('TEMPDB..#TMP_SAR') IS NOT NULL BEGIN
        DROP TABLE #TMP_SAR
    END

    IF OBJECT_ID('TEMPDB..#TMP_SAR') IS NOT NULL BEGIN
        DROP TABLE #TMP_SAR
    END

    CREATE TABLE #TMP_DATA_I
    (
         Cust_No     VARCHAR(50)   --客戶編號
        ,AMT_SUM     DECIMAL(25,5) --總存入交易金額
        ,AMT_CNT     INT           --總存入交易筆數
    )    

    CREATE TABLE #TMP_DATA_O
    (
         Cust_No     VARCHAR(50)   --客戶編號
        ,AMT_SUM     DECIMAL(25,5) --總移出交易金額
        ,AMT_CNT     INT           --總移出交易筆數
    )     

    CREATE TABLE #TMP_DATA_RM
    (
         Cust_No     VARCHAR(50)   --客戶編號
        ,AMT_SUM     DECIMAL(25,5) --總移出交易金額
        ,AMT_CNT     INT           --總移出交易筆數
    )   

    CREATE TABLE #TMP_SAR
    (
        Cust_No     VARCHAR(50)    --客戶編號
       ,AMT_SUM     DECIMAL(25,5) --帳戶總交易金額
       ,AMT_CNT     INT           --帳戶總交易筆數
    )
    -------------------------------------------------------------------------------
    -- STEP2. 刪除已轉入資料
    -------------------------------------------------------------------------------
    --主TABLE
    DELETE FS_SAR_01_02_00 WHERE DATA_DT = @CYCLE_DT
    --明細TABLE
    DELETE FS_SAR_01_02_00_Dtl WHERE DATA_DT = @CYCLE_DT
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
	  
        --存款
		
        INSERT INTO  #TMP_DATA_I(Cust_No,AMT_SUM,AMT_CNT)    
        SELECT B.Cust_No 
               ,SUM(A.CCY_AMT) 
               ,COUNT(A.CCY_AMT)
         FROM DBO.FB_Txn AS A
         JOIN DBO.CD_TXN AS C
           ON A.TXN_KEY  = C.TXN_KEY
         JOIN DBO.FB_Cust_Acct AS AC
           ON A.ACCT_NO  = AC.ACCT_NO
         JOIN DBO.FB_Cust AS B
           ON B.Cust_No  = AC.Cust_No
		 JOIN FD_Cust_Cate D      --客群分群
		   ON AC.Cust_No= D.Cust_No    
	     JOIN #SAR_CATE_VALUE E   --態樣分群
		   ON D.Cust_Cate = E.SAR_CATE 
        WHERE C.AC_DC_TYP = 'C'
		  AND S4.Cust_Cate = @SAR_CATE
		  and A.Txn_Tool_Typ = '1'
          and C.AML_Prod_Typ in ('PB','CK','CT','NCD')
          AND A.TXN_DT BETWEEN E.PAR1_DT AND @DATA_DT
          --AND B.Cust_Typ_Cd IN ('D','NP','FP')   
        GROUP BY B.Cust_No
       HAVING COUNT(*) >= E.PAR4
          AND SUM(A.CCY_AMT) >= E.PAR2

        --提款

        INSERT INTO  #TMP_DATA_O(Cust_No,AMT_SUM,AMT_CNT)    
        SELECT B.Cust_No 
               ,SUM(A.CCY_AMT) 
               ,COUNT(A.CCY_AMT)
         FROM DBO.FB_Txn AS A
         JOIN DBO.CD_TXN AS C
           ON A.TXN_KEY  = C.TXN_KEY
         JOIN DBO.FB_Cust_Acct AS AC
           ON A.ACCT_NO  = AC.ACCT_NO
         JOIN DBO.FB_Cust AS B
           ON B.Cust_No  = AC.Cust_No
         JOIN #TMP_DATA_I TMP
           ON TMP.Cust_No = B.Cust_No 
		 JOIN FD_Cust_Cate D       --客群分群
		   ON AC.Cust_No= D.Cust_No    
	     JOIN #SAR_CATE_VALUE E    --態樣分群
		   ON D.Cust_Cate = E.SAR_CATE 
        WHERE C.AC_DC_TYP = 'D'
		  AND E.Cust_Cate = @SAR_CATE
		  and A.Txn_Tool_Typ = '1'
          and C.AML_Prod_Typ in ('PB','CK','CT','NCD')
          AND A.TXN_DT BETWEEN @PAR1_DT AND @DATA_DT
          --AND B.Cust_Typ_Cd IN ('D','NP','FP')  
        GROUP BY B.Cust_No
       HAVING COUNT(*) >= E.PAR4
          AND SUM(A.CCY_AMT) >= E.PAR2

        --匯款

        INSERT INTO  #TMP_DATA_RM(Cust_No,AMT_SUM,AMT_CNT)    
        SELECT B.Cust_No 
               ,SUM(A.CCY_AMT) 
               ,COUNT(A.CCY_AMT) 
         FROM DBO.FB_Txn AS A
         JOIN DBO.CD_TXN AS C
           ON A.TXN_KEY  = C.TXN_KEY
         JOIN DBO.FB_Cust_Acct AS AC
           ON A.ACCT_NO  = AC.ACCT_NO
         JOIN DBO.FB_Cust AS B
           ON B.Cust_No  = AC.Cust_No
         JOIN #TMP_DATA_O TMP
           ON TMP.Cust_No = B.Cust_No
		 JOIN FD_Cust_Cate D       --客群分群
		   ON AC.Cust_No= D.Cust_No    
	     JOIN #SAR_CATE_VALUE E    --態樣分群
		   ON D.Cust_Cate = E.SAR_CATE
        WHERE C.AML_Prod_Typ = 'RM'
		  AND E.Cust_Cate = @SAR_CATE
          and A.Txn_Tool_Typ = '1'
          AND A.TXN_DT BETWEEN E.PAR1_DT AND @DATA_DT
          --AND B.Cust_Typ_Cd IN ('D','NP','FP')   
        GROUP BY B.Cust_No
       HAVING COUNT(*) >= E.PAR4
          AND SUM(A.CCY_AMT) >= E.PAR2


        INSERT INTO #TMP_SAR(Cust_No,AMT_CNT,AMT_SUM)
        SELECT T1.Cust_No
		      ,T1.AMT_CNT+T2.AMT_CNT+T3.AMT_CNT as 'AMT_CNT'
			  ,T1.AMT_SUM+T2.AMT_SUM+T3.AMT_SUM as 'AMT_SUM'
         FROM #TMP_DATA_O AS T1
         JOIN #TMP_DATA_I AS T2 ON T1.Cust_No = T2.Cust_No
		 JOIN #TMP_DATA_RM AS T3 ON T1.Cust_No = T3.Cust_No
       
    -------------------------------------------------------------------------------
    -- STEP4. 存入報表TABLE
    ------------------------------------------------------------------------------- 
    INSERT INTO FS_SAR_01_02_00
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
        SELECT @CYCLE_DT
              ,S3.Cust_No 
              ,S3.Cust_Nm 
              ,@CYCLE_DT
              ,S1.AMT_CNT AS 'TXN_CNT'
              ,S1.AMT_SUM AS 'TXN_AMT'
              ,'FB_Txn' AS 'LST_MAINT_USR'
              ,GETDATE() AS 'LST_MAINT_DT'
          FROM #TMP_SAR S1
          JOIN FB_Cust S3 ON S1.Cust_No = S3.Cust_No      

    SET @INS_CNT = @INS_CNT + @@ROWCOUNT   
    -------------------------------------------------------------------------------
    -- STEP5. 存入明細資料檔
    ------------------------------------------------------------------------------- 
    INSERT INTO FS_SAR_01_02_00_DTL
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
       FROM FS_SAR_01_02_00 AS S
       JOIN FB_Cust_Acct AS D  ON S.Cust_No = D.Cust_No
       JOIN FB_Txn AS A  ON A.ACCT_NO = D.ACCT_NO
       JOIN CD_TXN AS C ON A.TXN_KEY = C.TXN_KEY
	   JOIN FD_Cust_Cate E ON S.Cust_No= E.Cust_No      --客群分群
	   JOIN #SAR_CATE_VALUE F ON E.Cust_Cate = F.SAR_CATE   --態樣分群   	   
      WHERE A.TXN_DT BETWEEN F.PAR1_DT AND @DATA_DT
	    AND E.Cust_Cate = @SAR_CATE
        AND C.AC_DC_TYP IN ('C','D')
	    and C.Txn_Tool_Typ = '1'
	    and C.AML_Prod_Typ in ('PB','CK','CT','NCD')
        AND S.Data_Dt = @CYCLE_DT

    INSERT INTO FS_SAR_01_02_00_DTL
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
     FROM FS_SAR_01_02_00 AS S
       JOIN FB_Cust_Acct AS D  ON S.Cust_No = D.Cust_No
       JOIN FB_Txn AS A  ON A.ACCT_NO = D.ACCT_NO
       JOIN CD_TXN AS C ON A.TXN_KEY = C.TXN_KEY
	   JOIN FD_Cust_Cate E ON S.Cust_No= E.Cust_No      --客群分群
	   JOIN #SAR_CATE_VALUE F ON E.Cust_Cate = F.SAR_CATE   --態樣分群    	   
    WHERE A.TXN_DT BETWEEN F.PAR1_DT AND @DATA_DT
	  AND E.Cust_Cate = @SAR_CATE
	  and C.Txn_Tool_Typ = '1'
	  and C.AML_Prod_Typ = 'RM'
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
                   ,'FS_SAR_01_02_00_Dtl'
                   ,'AML'
                   ,GETDATE()
                   ,D.Cust_Lst_Br_No
              FROM FS_SAR_01_02_00 S
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