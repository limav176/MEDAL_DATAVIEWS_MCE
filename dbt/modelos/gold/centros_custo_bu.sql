{{ standard_config(
	model_name='centros_custo_bu',
	zone='gold',
	materialized='table'
) }}
select
centro_custo, disparado_por, bu, bu_rateio 
from  {{ ref('ccusto_sfmc') }}