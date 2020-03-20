pipeline {
   agent any

   stages {
      stage('DDL_defination') {
          
         steps {
            bat 'snowsql -c myconnection --config C:\\Users\\91827\\.snowsql\\config -f GetItemMasterPrice_Target_DDL.sql'
         }
         
      }
      
      stage('StoredProcedured_build') {
          
         steps {
            bat 'snowsql -c myconnection --config C:\\Users\\91827\\.snowsql\\config -f ItemMasterPrice_Pending_Retail_Item_Price.sql'
         }
         
      }
      stage('RunChildThroughMaster'){
          steps{
              bat 'snowsql -c myconnection --config C:\\Users\\91827\\.snowsql\\config -f sp_GetItemMasterPrice_To_BIM_load.sql'
             
          }  
          
      }
   }
}
