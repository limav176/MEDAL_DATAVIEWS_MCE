{{ 
	standard_config(
		model_name='stg_inapp_sfi',
		zone='bronze',
		materialized='ephemeral'
	)
}}
select 
	lower(concat(messageid, deviceid, systemtoken, requestid)) as primkey
	,appname       
	,messagename         
	,messageid           
	,campaigns           
	,lower(deviceid) as deviceid            
	,try_cast (datetimesend AS timestamp)  as datetimesend   
	,messagecontent      
	,messageopened       
	,try_cast (opendate AS timestamp)  as opendate               
	,timeinapp           
	,platformversion     
	,status              
	,serviceresponse     
	,geofencename        
	,template            
	,format              
	,pagename            
	,lower(pushjobid) as pushjobid           
	,systemtoken         
	,inboxmessagedownloaded
	,inboxmessageopened  
	,iosmediaurl         
	,androidmediaurl     
	,mediaalt            
	,contactkey          
	,requestid           
	,plataform           
	,ingestion_date      
	,ingestion_year      
	,ingestion_month     
	,ingestion_day       
	,execution_date      
	,execution_year      
	,execution_month     
	,execution_day   
FROM  
	{{ source('comunicacoes_bronze', 'inapp_sfi') }}