


//***********************************
// Choose warehouse,database and schema 
//***********************************

//selecting warehouse,database and schema
use WAREHOUSE DEV_ENGINEER_WH;
use DATABASE EDM_CONFIRMED_DEV;
use schema EDM_CONFIRMED_DEV.SCRATCH;


CREATE OR REPLACE PROCEDURE sp_GetItemMasterPrice_To_BIM_load_Pending_Retail_Item_Price(src_wrk_tbl varchar,cnf_db varchar,cnf_schema varchar,wrk_schema varchar)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    AS 
    $$ 
        var src_wrk_tbl = SRC_WRK_TBL;
        var cnf_db = CNF_DB;
        var cnf_schema = CNF_SCHEMA;
        var wrk_schema = WRK_SCHEMA;
        
        // **************        Load for Pending_UPC_Price BEGIN *****************
        // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
        var tgt_wrk_tbl = cnf_db + "." + wrk_schema + ".Pending_Retail_Item_Price_wrk";
        var tgt_tbl = cnf_db + "." + cnf_schema + ".Pending_Retail_Item_Price";
        var sql_command = `CREATE OR REPLACE TABLE ` + tgt_wrk_tbl + ` as 
        SELECT src.Price_Method_Cd
			  ,src.Item_Limt_Qty
			  ,src.Item_Price_Amt
			  ,src.Item_Price_Fctr
			  ,src.Alternate_Price_Amt
			  ,src.Alternate_Price_Fctr
			  ,src.Price_Change_Reason_Cd
			  ,src.Long_Term_Special_Ind
			  ,src.Origin_Txt
			  ,src.Common_Retail_Cd
			  ,src.Unit_Price_Table_Nbr
			  ,src.Unit_Price_Measure_Cd
			  ,src.Unit_Price_Measure_Nbr
			  ,src.Unit_Price_Label_Unit_Cd
			  ,src.Unit_Price_Multiplication_Fctr
			  ,src.UPC_Nbr
			  ,src.Rog_Id
			  ,src.Price_Area_Cd
			  ,src.Price_Effective_Start_Dt
			  ,src.Price_Effective_End_Dt
			  ,src.Link_Dt
			  ,src.Old_Item_Price_Amt
			  ,src.Old_Item_Price_Fctr
			  ,src.Point_Of_Sale_Price_Send_Ind
			  ,src.Master_Update_Process_Ind
			  ,src.Change_Ts
			  ,src.Billing_Dt
			  ,src.Unit_Type_Nbr
			  ,src.Pending_Price_Reason_Cd
			  ,src.Common_Cd
			  ,src.Ad_Select_Id  
			  ,src.BODNm 
			  ,src.DW_Logical_delete_ind                       
              ,CASE WHEN tgt.Rog_Id is null or tgt.Price_Area_Cd is null or tgt.UPC_Nbr is null  then 'I' ELSE 'U' END as DML_Type
              ,CASE WHEN ( DW_First_Effective_dt < CURRENT_DATE or DW_First_Effective_dt is null)  THEN 0 Else 1 END as Sameday_chg_ind
        FROM (
            SELECT DISTINCT
            	 PENDINGPRICE_PRICEMETHODCD AS Price_Method_Cd
				,PENDINGPRICE_ITEMLIMTQTY AS Item_Limt_Qty
				,PENDINGPRICE_ITEMPRICEAMT AS Item_Price_Amt
				,PENDINGPRICE_ITEMPRICEFACTOR AS Item_Price_Fctr
				,PENDINGPRICE_ALTERNATEPRICEAMT AS Alternate_Price_Amt
				,PENDINGPRICE_ALTERNATEPRICEFACTOR AS Alternate_Price_Fctr
				,PENDINGPRICE_REASONCD AS Price_Change_Reason_Cd
				,PENDINGPRICE_LongTermSpecialInd AS Long_Term_Special_Ind
				,PENDINGPRICE_ORIGINTXT AS Origin_Txt
				,CommonCd AS Common_Retail_Cd //
				,UnitPriceTableNumber AS Unit_Price_Table_Nbr
				,UnitPriceMeasure AS Unit_Price_Measure_Cd
				,SellingUnitSize AS Unit_Price_Measure_Nbr
				,LabelPriceUnit AS Unit_Price_Label_Unit_Cd
				,UnitPriceMultiplicationFactor AS Unit_Price_Multiplication_Fctr
				,ITEM_UPCNBR AS UPC_Nbr
				,RogCD AS Rog_Id
				,PriceareaCd AS Price_Area_Cd
				,PENDINGPRICE_EFFECTIVESTARTDT AS Price_Effective_Start_Dt
				,PENDINGPRICE_EFFECTIVEENDDT AS Price_Effective_End_Dt
				,LinkDate AS Link_Dt
				,OldItemPriceAmt AS Old_Item_Price_Amt
				,OldItemPriceFactor AS Old_Item_Price_Fctr
				,POSPriceSendInd AS Point_Of_Sale_Price_Send_Ind
				,MasterUpdateProcessInd AS Master_Update_Process_Ind
				,ChangeDtTm AS Change_Ts
				,BillingDt AS Billing_Dt
				,UnitTypeNbr AS Unit_Type_Nbr
				,PendingPriceReasonCd AS Pending_Price_Reason_Cd
				,CommonCd AS Common_Cd
				,AdSelectId AS Ad_Select_Id
            	,CASE WHEN upper(ActionTypeCd) = 'DELETE' THEN TRUE ELSE FALSE END as DW_Logical_delete_ind
            	,BODNm
            FROM  ` + src_wrk_tbl +`
            WHERE   Rog_Id is not null and Price_Area_Cd is not null and UPC_Nbr is not null  
        ) as src
        LEFT JOIN(
            SELECT Price_Method_Cd
				,Item_Limt_Qty
				,Item_Price_Amt
				,Item_Price_Fctr
				,Alternate_Price_Amt
				,Alternate_Price_Fctr
				,Price_Change_Reason_Cd
				,Long_Term_Special_Ind
				,Origin_Txt
				,Common_Retail_Cd
				,Unit_Price_Table_Nbr
				,Unit_Price_Measure_Cd
				,Unit_Price_Measure_Nbr
				,Unit_Price_Label_Unit_Cd
				,Unit_Price_Multiplication_Fctr
				,UPC_Nbr
				,Rog_Id
				,Price_Area_Cd
				,Price_Effective_Start_Dt
				,Price_Effective_End_Dt
				,Link_Dt
				,Old_Item_Price_Amt
				,Old_Item_Price_Fctr
				,Point_Of_Sale_Price_Send_Ind
				,Master_Update_Process_Ind
				,Change_Ts
				,Billing_Dt
				,Unit_Type_Nbr
				,Pending_Price_Reason_Cd
				,Common_Cd
				,Ad_Select_Id
				,DW_First_Effective_dt
            FROM  ` + tgt_tbl + `
            WHERE DW_CURRENT_VERSION_IND = TRUE
            )  as tgt on tgt.Rog_Id = src.Rog_Id
				and tgt.Price_Area_Cd = src.Price_Area_Cd
				and tgt.UPC_Nbr = src.UPC_Nbr
                where (tgt.Rog_id is  null and tgt.Price_Area_cd is null and tgt.UPC_Nbr is  null) or  
                (tgt.Price_Method_Cd <> src.Price_Method_Cd OR
                tgt.Item_Limt_Qty <> src.Item_Limt_Qty or
                tgt.Item_Price_Amt <> src.Item_Price_Amt OR
                tgt.Item_Price_Fctr <> src.Item_Price_Fctr OR
                tgt.Alternate_Price_Amt <> src.Alternate_Price_Amt OR 
                tgt.Alternate_Price_Fctr <> src.Alternate_Price_Fctr OR 
                tgt.Price_Change_Reason_Cd <> src.Price_Change_Reason_Cd OR
                tgt.Long_Term_Special_Ind <> src.Long_Term_Special_Ind OR 
                tgt.Origin_Txt <> src.Origin_Txt OR 
                tgt.Common_Retail_Cd <> src.Common_Retail_Cd OR
                tgt.Unit_Price_Table_Nbr <> src.Unit_Price_Table_Nbr OR
                tgt.Unit_Price_Measure_Cd <> src.Unit_Price_Measure_Cd OR 
                tgt.Unit_Price_Measure_Nbr <> src.Unit_Price_Measure_Nbr OR
                tgt.Unit_Price_Label_Unit_Cd <> src.Unit_Price_Label_Unit_Cd OR 
                tgt.Unit_Price_Multiplication_Fctr <> src.Unit_Price_Multiplication_Fctr OR
                tgt.Price_Effective_Start_Dt <> src.Price_Effective_Start_Dt OR
                tgt.Price_Effective_End_Dt <> src.Price_Effective_End_Dt OR 
                tgt.Link_Dt <> src.Link_Dt OR
                tgt.Old_Item_Price_Amt <> src.Old_Item_Price_Amt 
                OR tgt.Old_Item_Price_Fctr <> src.Old_Item_Price_Fctr OR 
                tgt.Point_Of_Sale_Price_Send_Ind <> src.Point_Of_Sale_Price_Send_Ind
                OR tgt.Master_Update_Process_Ind <> src.Master_Update_Process_Ind OR
                tgt.Change_Ts <> src.Change_Ts OR 
                tgt.Billing_Dt <> src.Billing_Dt OR 
                tgt.Unit_Type_Nbr <> src.Unit_Type_Nbr OR 
                tgt.Pending_Price_Reason_Cd <> src.Pending_Price_Reason_Cd OR
                tgt.Common_Cd <> src.Common_Cd OR 
                tgt.Ad_Select_Id <> src.Ad_Select_Id OR 
                tgt.Price_Effective_Start_Dt <> src.Price_Effective_Start_Dt OR
                tgt.Price_Effective_End_Dt <> src.Price_Effective_End_Dt) `;
        try {
            snowflake.execute (
                {sqlText: sql_command}
            );
        }
        catch (err) {
            return "Creation of Pending_Retail_Item_Price work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }

        //SCD Type2 transaction begins
        var sql_begin = "BEGIN"
        // Processing Updates of Type 2 SCD
        var sql_updates = `UPDATE ` + tgt_tbl + ` as tgt
            SET DW_Last_Effective_dt = CURRENT_DATE
            ,DW_CURRENT_VERSION_IND = FALSE
            ,DW_LAST_UPDATE_TS = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
            ,DW_SOURCE_UPDATE_NM = BODNm
        FROM (
            SELECT Price_Method_Cd
				,Item_Limt_Qty
				,Item_Price_Amt
				,Item_Price_Fctr
				,Alternate_Price_Amt
				,Alternate_Price_Fctr
				,Price_Change_Reason_Cd
				,Long_Term_Special_Ind
				,Origin_Txt
				,Common_Retail_Cd
				,Unit_Price_Table_Nbr
				,Unit_Price_Measure_Cd
				,Unit_Price_Measure_Nbr
				,Unit_Price_Label_Unit_Cd
				,Unit_Price_Multiplication_Fctr
				,UPC_Nbr
				,Rog_Id
				,Price_Area_Cd
				,Price_Effective_Start_Dt
				,Price_Effective_End_Dt
				,Link_Dt
				,Old_Item_Price_Amt
				,Old_Item_Price_Fctr
				,Point_Of_Sale_Price_Send_Ind
				,Master_Update_Process_Ind
				,Change_Ts
				,Billing_Dt
				,Unit_Type_Nbr
				,Pending_Price_Reason_Cd
				,Common_Cd
				,Ad_Select_Id 
				,BOdnm
            FROM ` + tgt_wrk_tbl + `
            WHERE DML_Type = 'U'
            AND Sameday_chg_ind = 0
        ) src
        WHERE   tgt.Rog_Id = src.Rog_Id
				and tgt.Price_Area_Cd = src.Price_Area_Cd
				and tgt.UPC_Nbr = src.UPC_Nbr
				AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
        
        // Processing Sameday updates
        var sql_sameday = `UPDATE ` + tgt_tbl + ` as tgt
        SET Price_Method_Cd = src.Price_Method_Cd
			,Item_Limt_Qty = src.Item_Limt_Qty
			,Item_Price_Amt = src.Item_Price_Amt
			,Item_Price_Fctr = src.Item_Price_Fctr
			,Alternate_Price_Amt = src.Alternate_Price_Amt
			,Alternate_Price_Fctr = src.Alternate_Price_Fctr
			,Price_Change_Reason_Cd = src.Price_Change_Reason_Cd
			,Long_Term_Special_Ind = src.Long_Term_Special_Ind
			,Origin_Txt = src.Origin_Txt
			,Common_Retail_Cd = src.Common_Retail_Cd
			,Unit_Price_Table_Nbr = src.Unit_Price_Table_Nbr
			,Unit_Price_Measure_Cd = src.Unit_Price_Measure_Cd
			,Unit_Price_Measure_Nbr = src.Unit_Price_Measure_Nbr
			,Unit_Price_Label_Unit_Cd = src.Unit_Price_Label_Unit_Cd
			,Unit_Price_Multiplication_Fctr = src.Unit_Price_Multiplication_Fctr
			,Price_Effective_Start_Dt = src.Price_Effective_Start_Dt
			,Price_Effective_End_Dt = src.Price_Effective_End_Dt
			,Link_Dt = src.Link_Dt
			,Old_Item_Price_Amt = src.Old_Item_Price_Amt
			,Old_Item_Price_Fctr = src.Old_Item_Price_Fctr
			,Point_Of_Sale_Price_Send_Ind = src.Point_Of_Sale_Price_Send_Ind
			,Master_Update_Process_Ind = src.Master_Update_Process_Ind
			,Change_Ts = src.Change_Ts
			,Billing_Dt = src.Billing_Dt
			,Unit_Type_Nbr = src.Unit_Type_Nbr
			,Pending_Price_Reason_Cd = src.Pending_Price_Reason_Cd
			,Common_Cd = src.Common_Cd
			,Ad_Select_Id = src.Ad_Select_Id
            ,DW_Logical_delete_ind = src.DW_Logical_delete_ind
            ,DW_LAST_UPDATE_TS = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
            ,DW_SOURCE_UPDATE_NM = BODNm
        FROM (              
            SELECT 
				 Price_Method_Cd
				,Item_Limt_Qty
				,Item_Price_Amt
				,Item_Price_Fctr
				,Alternate_Price_Amt
				,Alternate_Price_Fctr
				,Price_Change_Reason_Cd
				,Long_Term_Special_Ind
				,Origin_Txt
				,Common_Retail_Cd
				,Unit_Price_Table_Nbr
				,Unit_Price_Measure_Cd
				,Unit_Price_Measure_Nbr
				,Unit_Price_Label_Unit_Cd
				,Unit_Price_Multiplication_Fctr
				,UPC_Nbr
				,Rog_Id
				,Price_Area_Cd
				,Price_Effective_Start_Dt
				,Price_Effective_End_Dt
				,Link_Dt
				,Old_Item_Price_Amt
				,Old_Item_Price_Fctr
				,Point_Of_Sale_Price_Send_Ind
				,Master_Update_Process_Ind
				,Change_Ts
				,Billing_Dt
				,Unit_Type_Nbr
				,Pending_Price_Reason_Cd
				,Common_Cd
				,Ad_Select_Id 
				,DW_Logical_delete_ind
                ,BODNm
            FROM  ` + tgt_wrk_tbl + `   
            WHERE DML_Type = 'U'
            AND Sameday_chg_ind = 1
        ) src
        WHERE tgt.Rog_Id = src.Rog_Id
				and tgt.Price_Area_Cd = src.Price_Area_Cd
				and tgt.UPC_Nbr = src.UPC_Nbr
				AND tgt.DW_CURRENT_VERSION_IND = TRUE `;

        // Processing Inserts
        var sql_inserts = `INSERT INTO ` + tgt_tbl + `
            (Price_Method_Cd
			,Item_Limt_Qty
			,Item_Price_Amt
			,Item_Price_Fctr
			,Alternate_Price_Amt
			,Alternate_Price_Fctr
			,Price_Change_Reason_Cd
			,Long_Term_Special_Ind
			,Origin_Txt
			,Common_Retail_Cd
			,Unit_Price_Table_Nbr
			,Unit_Price_Measure_Cd
			,Unit_Price_Measure_Nbr
			,Unit_Price_Label_Unit_Cd
			,Unit_Price_Multiplication_Fctr
			,UPC_Nbr
			,Rog_Id
			,Price_Area_Cd
			,Price_Effective_Start_Dt
			,Price_Effective_End_Dt
			,Link_Dt
			,Old_Item_Price_Amt
			,Old_Item_Price_Fctr
			,Point_Of_Sale_Price_Send_Ind
			,Master_Update_Process_Ind
			,Change_Ts
			,Billing_Dt
			,Unit_Type_Nbr
			,Pending_Price_Reason_Cd
			,Common_Cd
			,Ad_Select_Id 
            ,DW_First_Effective_Dt 
            ,DW_Last_Effective_Dt
            ,DW_LAST_UPDATE_TS
            ,DW_CREATE_TS          
            ,DW_LOGICAL_DELETE_IND  
            ,DW_SOURCE_CREATE_NM   
            ,DW_SOURCE_UPDATE_NM
            ,DW_CURRENT_VERSION_IND
        )
        SELECT
            Price_Method_Cd
			,Item_Limt_Qty
			,Item_Price_Amt
			,Item_Price_Fctr
			,Alternate_Price_Amt
			,Alternate_Price_Fctr
			,Price_Change_Reason_Cd
			,Long_Term_Special_Ind
			,Origin_Txt
			,Common_Retail_Cd
			,Unit_Price_Table_Nbr
			,Unit_Price_Measure_Cd
			,Unit_Price_Measure_Nbr
			,Unit_Price_Label_Unit_Cd
			,Unit_Price_Multiplication_Fctr
			,UPC_Nbr
			,Rog_Id
			,Price_Area_Cd
			,Price_Effective_Start_Dt
			,Price_Effective_End_Dt
			,Link_Dt
			,Old_Item_Price_Amt
			,Old_Item_Price_Fctr
			,Point_Of_Sale_Price_Send_Ind
			,Master_Update_Process_Ind
			,Change_Ts
			,Billing_Dt
			,Unit_Type_Nbr
			,Pending_Price_Reason_Cd
			,Common_Cd
			,Ad_Select_Id   
            ,CURRENT_DATE + 1
            ,'31-DEC-9999'
            ,TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
            ,TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
            ,DW_Logical_delete_ind
            ,BODNm
            ,BODNm
            ,TRUE
        FROM   `+ tgt_wrk_tbl +`
        WHERE Sameday_chg_ind = 0`; 

        var sql_commit = "COMMIT"
        var sql_rollback = "ROLLBACK"
        try {
            snowflake.execute (
                {sqlText: sql_begin}
            );
            snowflake.execute (
                {sqlText: sql_updates}
            );
            snowflake.execute (
                {sqlText: sql_sameday}
            );
            snowflake.execute (
                {sqlText: sql_inserts}
            );
            snowflake.execute (
                {sqlText: sql_commit}
            );    
        }
        catch (err) {
            snowflake.execute (
                {sqlText: sql_rollback}
            );
            return "Loading of Pending_Retail_Item_Price" + tgt_tbl + " Failed with error: " + err;   // Return a error message.
        }
                // **************        Load for Pending_UPC_Price ENDs *****************
                
        return "SUCCESS"
    $$;
