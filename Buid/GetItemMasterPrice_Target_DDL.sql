


//***********************************
// Choose warehouse,database and schema 
//***********************************

//selecting warehouse,database and schema
use WAREHOUSE DEV_ENGINEER_WH;
use DATABASE EDM_CONFIRMED_DEV;
use schema EDM_CONFIRMED_DEV.SCRATCH;



//***********************************
// Create Target Tables 
//***********************************
CREATE OR REPLACE TABLE Pending_Retail_Item_Price
(
 Rog_Id                VARCHAR  NOT NULL ,
 Price_Area_Cd         VARCHAR(20)  NOT NULL ,
 UPC_Nbr               NUMBER(14)  NOT NULL ,
 DW_First_Effective_Dt  DATE  NOT NULL ,
 DW_Last_Effective_Dt  DATE  NOT NULL ,
 Price_Effective_Start_Dt  DATE  ,
 Price_Effective_End_Dt  DATE  ,
 Common_Retail_Cd      NUMBER(6)  ,
 Unit_Price_Table_Nbr  NUMBER  ,
 Unit_Price_Measure_Cd  NUMBER(7)  ,
 Unit_Price_Measure_Nbr  VARCHAR(40)  ,
 Unit_Price_Label_Unit_Cd  VARCHAR(10)  ,
 Unit_Price_Multiplication_Fctr  NUMBER(10,10)  ,
 Price_Method_Cd       VARCHAR(10)  ,
 Item_Limt_Qty         NUMBER  ,
 Item_Price_Amt        NUMBER(12,4)  ,
 Item_Price_Fctr       NUMBER  ,
 Alternate_Price_Amt   NUMBER(12,4)  ,
 Alternate_Price_Fctr  NUMBER  ,
 Price_Change_Reason_Cd  VARCHAR  ,
 Long_Term_Special_Ind  VARCHAR  ,
 Origin_Txt            VARCHAR  ,
 Link_Dt               DATE  ,
 Old_Item_Price_Amt    NUMBER(12,4)  ,
 Old_Item_Price_Fctr   NUMBER  ,
 Point_Of_Sale_Price_Send_Ind  VARCHAR  ,
 Master_Update_Process_Ind  VARCHAR  ,
 Change_Ts             TIMESTAMP  ,
 Billing_Dt            DATE  ,
 Unit_Type_Nbr         NUMBER  ,
 Pending_Price_Reason_Cd  VARCHAR  ,
 Common_Cd             NUMBER(5,0)  ,
 Ad_Select_Id          VARCHAR  ,
 DW_CREATE_TS          TIMESTAMP  ,
 DW_LAST_UPDATE_TS     TIMESTAMP  ,
 DW_LOGICAL_DELETE_IND  BOOLEAN  ,
 DW_SOURCE_CREATE_NM   VARCHAR(255)  ,
 DW_SOURCE_UPDATE_NM   VARCHAR(255)  ,
 DW_CURRENT_VERSION_IND  BOOLEAN  
);

ALTER TABLE Pending_Retail_Item_Price
 ADD CONSTRAINT XPKPendingPrice PRIMARY KEY (Rog_Id, Price_Area_Cd, UPC_Nbr, DW_First_Effective_Dt, DW_Last_Effective_Dt);
