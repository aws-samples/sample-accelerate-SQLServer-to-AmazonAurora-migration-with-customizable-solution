CREATE OR REPLACE PROCEDURE stage.stage_ctas_null_upd_cell_perf_stats(
	target_schema text,
    stage_table text
) 
LANGUAGE 'plpgsql'
SECURITY DEFINER
AS $$

BEGIN
	
    RAISE NOTICE 'DROP TABLE %.%',target_schema,stage_table;
    -- Droping table if exists
    
    EXECUTE 'DROP TABLE IF EXISTS '||target_schema||'.' || stage_table || ';' ;
    
    RAISE NOTICE 'CREATE TABLE %.%',target_schema,stage_table;
    -- Create new table in target_schema with updated column values 

    EXECUTE 'CREATE TABLE '||target_schema||'.' || stage_table || ' AS SELECT 
            cell_location          ,
            cell_id      ,
            bandwidth_usage_total        ,
            signal_strength_indicator_1           ,
            dropped_calls_total          ,
            device_id      ,
            data_usage_total         ,
            network_quality_indicator_1        ,
            (CASE WHEN handover_attempts = -2147400000 THEN NULL  ELSE handover_attempts END)	as handover_attempts
            data_timestamp         
            FROM stage.' || stage_table ;

    RAISE NOTICE 'ALTER TABLE %.% ADD CONSTRAINT',target_schema,stage_table;
    -- Add the primay key constraint back
    EXECUTE 'ALTER TABLE '||target_schema||'.'||stage_table||' ADD CONSTRAINT '||stage_table||'_pkey PRIMARY KEY (cell_id, data_timestamp);';

END;
$$;


COMMENT ON PROCEDURE stage.stage_ctas_null_upd_cell_perf_stats (text,text) IS 'Handles the logic of performing the updates to change the columns back to NULL from the default value';
