
SELECT 
    'SELECT ' || 
    LISTAGG(
        'CASE WHEN TRY_TO_NUMBER(' || column_name || ') IS NULL THEN ''' || column_name || ''' END AS ' || column_name,
        ', '
    ) WITHIN GROUP (ORDER BY ordinal_position) 
    || ' FROM your_table' AS validation_query
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_name = 'YOUR_TABLE_NAME'
  AND table_schema = 'YOUR_SCHEMA_NAME';









SELECT 
    TO_DATE('2024-09-01', 'YYYY-MM-DD') AS V_EFF_DATE,
    TRUNC(TO_DATE('2024-09-01', 'YYYY-MM-DD'), 'DD') AS TRUNC_EFF_DT,
    CASE 
        WHEN CUR_TERM_EFF_DT >= TRUNC(TO_DATE('2024-09-01', 'YYYY-MM-DD'), 'DD') THEN 
            (LTRIM(RTRIM(CAST(OUT_FIRE_ALRM_TYPES_CODE AS VARCHAR2(100)))) || 
             LTRIM(RTRIM(CAST(OUT_SPRINKLER_TYPES AS VARCHAR2(100)))))
        WHEN ISO_PROTECTIVE_DEVICES = 'Does Not Apply' THEN 
            'Does Not Apply'
        ELSE 
            CAST(ISO_PROTECTIVE_DEVICES AS VARCHAR2(100))
    END AS NEW_PROTECTION_DEVICE_CODE
FROM 
    TRANS_EXP_POST_PT_DEVICES_FSV_LOOKUP;








               TRANS_LKP_PT_DVCS_CDS_SPRINKLERS_SYSTEMS_ISO_PROTECTION_DEVICE_CODE AS OUT_FIRE_ALRM_TYPES_CODE,
               ISO_PROTECTION_DEVICE_CODE AS OUT_SPRINKLER_TYPES  ,  
               TO_DATE('2024-09-01', 'YYYY-MM-DD') AS V_EFF_DATE,
             -- EXP_HO_SPECIFIC_ISO_PREM_FIELDS_DW.V_EFF_DATE,
               TRUNC(V_EFF_DATE,'DD') AS TRUNC_EFF_DT,
             -- EXP_HO_SPECIFIC_ISO_PREM_FIELDS_DW.TRUNC_EFF_DT,
               DECODE (TRUE, CUR_TERM_EFF_DT >=TRUNC_EFF_DT, (LTRIM(RTRIM(OUT_FIRE_ALRM_TYPES_CODE)) || LTRIM(RTRIM(OUT_SPRINKLER_TYPES))), ISO_PROTECTIVE_DEVICES) AS NEW_PROTECTION_DEVICE_CODE
          FROM TRANS_EXP_POST_PT_DEVICES_FSV_LOOKUP


Numeric value 'Does Not Apply' is not recognized







CASE 
    WHEN IN_DWELLING_FORM_CODE IS NULL THEN ' '
    WHEN RTRIM(IN_DWELLING_FORM_CODE) = 'HO_5' THEN ' '
    WHEN RTRIM(IN_DWELLING_FORM_CODE) = 'HO_3' AND v_COV_A_LIMIT_AMT <> 0 THEN
        CASE 
            WHEN (v_COV_C_LIMIT_AMT / v_COV_A_LIMIT_AMT) > 0.4 THEN
                DECODE((v_COV_C_LIMIT_AMT / v_COV_A_LIMIT_AMT),
                    0.4, '2',
                    0.5, '3',
                    0.6, '4',
                    0.7, '4',
                    0.75, '6',
                    1, '7',
                    '9')
            ELSE '1'
        END
    ELSE NULL
END AS OUT_COV_C_PCT_CD






 IFF( IN_DWELLING_FORM_CODE IS NULL,' ',
               IFF(RTRIM(IN_DWELLING_FORM_CODE)='HO_5',' ', 
               IFF(RTRIM(IN_DWELLING_FORM_CODE)='HO_3' AND v_COV_A_LIMIT_AMT<>0 ,
               IFF((v_COV_C_LIMIT_AMT/v_COV_A_LIMIT_AMT)>0.4, 
               DECODE((v_COV_C_LIMIT_AMT/v_COV_A_LIMIT_AMT),0.4,'2',0.5,'3', 0.6,'4',0.7,'4',0.75,'6',1,'7','9'),'1'
               )))) AS OUT_COV_C_PCT_CD,
















CASE 
    WHEN RTRIM(LTRIM(IN_COV_CD)) = 'FRAUD' THEN 'E'
    WHEN (RTRIM(IN_DWELLING_FORM_CODE) IN ('HO_3', 'HO_4', 'HO_5') 
          AND IN_PERS_PROP_COUNT < 1 
          AND RTRIM(IN_DF_DWELLING_USE_OCCUP_CD) = 'P') THEN
        CASE 
            WHEN IN_STATUS_COV_COUNT < 1 AND RTRIM(IN_SEC_OR_SEASONAL_IND) = 'N' THEN '1'
            WHEN IN_STATUS_COV_COUNT >= 1 AND RTRIM(IN_SEC_OR_SEASONAL_IND) = 'N' THEN '2'
            ELSE '1'
        END
    WHEN (RTRIM(IN_DWELLING_FORM_CODE) IN ('HO_3', 'HO_4', 'HO_5') 
          AND IN_PERS_PROP_COUNT < 1 
          AND RTRIM(IN_DF_DWELLING_USE_OCCUP_CD) <> 'P') THEN
        CASE 
            WHEN IN_STATUS_COV_COUNT < 1 AND RTRIM(IN_SEC_OR_SEASONAL_IND) <> 'E' THEN '3'
            WHEN IN_STATUS_COV_COUNT >= 1 AND RTRIM(IN_SEC_OR_SEASONAL_IND) = 'E' THEN '4'
            ELSE '1'
        END
    WHEN RTRIM(IN_DWELLING_FORM_CODE) = 'HO_3' AND IN_PERS_PROP_COUNT >= 1 THEN '7'
    WHEN RTRIM(IN_DWELLING_FORM_CODE) = 'HO_6' AND IN_BUILD_ADD1_COV_COUNT < 1 THEN
        CASE 
            WHEN IN_STATUS_COV_COUNT < 1 AND IN_RNTL_OTHR_COV_COUNT < 1 
                 AND RTRIM(IN_SEC_OR_SEASONAL_IND) = 'N' 
                 AND RTRIM(IN_DF_DWELLING_USE_OCCUP_CD) = 'P' THEN '1'
            WHEN IN_STATUS_COV_COUNT >= 1 AND IN_RNTL_OTHR_COV_COUNT < 1 
                 AND RTRIM(IN_SEC_OR_SEASONAL_IND) = 'N' 
                 AND RTRIM(IN_DF_DWELLING_USE_OCCUP_CD) = 'P' THEN '2'
            WHEN IN_STATUS_COV_COUNT < 1 AND IN_RNTL_OTHR_COV_COUNT >= 1 
                 AND RTRIM(IN_DF_DWELLING_USE_OCCUP_CD) = 'R' THEN '5'
            WHEN IN_STATUS_COV_COUNT >= 1 AND IN_RNTL_OTHR_COV_COUNT >= 1 
                 AND RTRIM(IN_DF_DWELLING_USE_OCCUP_CD) = 'R' THEN '6'
            WHEN RTRIM(IN_DF_DWELLING_USE_OCCUP_CD) = 'S' 
                 AND RTRIM(IN_SEC_OR_SEASONAL_IND) <> 'E' THEN '3'
            WHEN RTRIM(IN_DF_DWELLING_USE_OCCUP_CD) = 'S' 
                 AND RTRIM(IN_SEC_OR_SEASONAL_IND) = 'E' THEN '4'
            ELSE '1'
        END
    WHEN RTRIM(IN_DWELLING_FORM_CODE) = 'HO_6' AND IN_BUILD_ADD1_COV_COUNT >= 1 THEN '7'
    WHEN RTRIM(IN_DWELLING_FORM_CODE) = 'DP_3' THEN
        CASE 
            WHEN RTRIM(IN_DF_DWELLING_USE_OCCUP_CD) = 'P' 
                 AND RTRIM(IN_SEC_OR_SEASONAL_IND) = 'N' THEN '1'
            WHEN RTRIM(IN_DF_DWELLING_USE_OCCUP_CD) = 'P' 
                 AND RTRIM(IN_SEC_OR_SEASONAL_IND) <> 'N' THEN '3'
            WHEN RTRIM(IN_DF_DWELLING_USE_OCCUP_CD) <> 'P' 
                 AND RTRIM(IN_SEC_OR_SEASONAL_IND) = 'N' THEN '5'
            WHEN RTRIM(IN_DF_DWELLING_USE_OCCUP_CD) <> 'P' 
                 AND RTRIM(IN_SEC_OR_SEASONAL_IND) <> 'N' THEN '7'
            ELSE '1'
        END
    ELSE NULL
END AS OUT_STATUS_CODE













SQL compilation error: syntax error line 1,984 at position 15 unexpected 'IFF'. syntax error line 1,984 at position 19 unexpected 'RTRIM'. syntax error line 1,984 at position 25 unexpected 'IN_DWELLING_FORM_CODE'. syntax error line 1,989 at position 18 unexpected ')'.




IFF(RTRIM(LTRIM(IN_COV_CD)) = 'FRAUD','E', 
               IFF((RTRIM(IN_DWELLING_FORM_CODE) ='HO_3' OR RTRIM(IN_DWELLING_FORM_CODE) ='HO_4' OR RTRIM(IN_DWELLING_FORM_CODE) ='HO_5') 
               AND IN_PERS_PROP_COUNT<1 
               AND RTRIM(IN_DF_DWELLING_USE_OCCUP_CD)='P', DECODE(TRUE, IN_STATUS_COV_COUNT<1 
               AND RTRIM(IN_SEC_OR_SEASONAL_IND)='N','1', IN_STATUS_COV_COUNT>=1 
               AND RTRIM(IN_SEC_OR_SEASONAL_IND)='N','2', '1'), 
               IFF((RTRIM(IN_DWELLING_FORM_CODE) ='HO_3' OR RTRIM(IN_DWELLING_FORM_CODE) ='HO_4' OR RTRIM(IN_DWELLING_FORM_CODE) ='HO_5') 
               AND IN_PERS_PROP_COUNT<1 
               AND RTRIM(IN_DF_DWELLING_USE_OCCUP_CD)<>'P', DECODE(TRUE, IN_STATUS_COV_COUNT<1 
               AND RTRIM(IN_SEC_OR_SEASONAL_IND)<>'E','3', IN_STATUS_COV_COUNT>=1 
               AND RTRIM(IN_SEC_OR_SEASONAL_IND)='E','4', '1'), 
               IFF(RTRIM(IN_DWELLING_FORM_CODE)='HO_3'
               AND IN_PERS_PROP_COUNT>=1,'7', 
               IFF(RTRIM(IN_DWELLING_FORM_CODE)='HO_6' 
               AND IN_BUILD_ADD1_COV_COUNT<1, DECODE(TRUE, IN_STATUS_COV_COUNT<1 
               AND IN_RNTL_OTHR_COV_COUNT<1 AND RTRIM(IN_SEC_OR_SEASONAL_IND)='N' 
               AND RTRIM(IN_DF_DWELLING_USE_OCCUP_CD)='P','1', IN_STATUS_COV_COUNT>=1 
               AND IN_RNTL_OTHR_COV_COUNT<1
               AND RTRIM(IN_SEC_OR_SEASONAL_IND)='N' 
               AND RTRIM(IN_DF_DWELLING_USE_OCCUP_CD)='P','2', IN_STATUS_COV_COUNT<1 
               AND IN_RNTL_OTHR_COV_COUNT>=1 
               AND RTRIM(IN_DF_DWELLING_USE_OCCUP_CD)='R','5', IN_STATUS_COV_COUNT>=1 
               AND IN_RNTL_OTHR_COV_COUNT>=1 
               AND RTRIM(IN_DF_DWELLING_USE_OCCUP_CD)='R','6', RTRIM(IN_DF_DWELLING_USE_OCCUP_CD)='S' 
               AND RTRIM(IN_SEC_OR_SEASONAL_IND)<>'E','3', RTRIM(IN_DF_DWELLING_USE_OCCUP_CD)='S' 
               AND RTRIM(IN_SEC_OR_SEASONAL_IND)='E','4', '1'), 
               IFF(RTRIM(IN_DWELLING_FORM_CODE)='HO_6' 
               AND IN_BUILD_ADD1_COV_COUNT>=1,'7'
              
               IFF(RTRIM(IN_DWELLING_FORM_CODE)='DP_3', DECODE(TRUE, RTRIM(IN_DF_DWELLING_USE_OCCUP_CD)='P' 
               AND RTRIM(IN_SEC_OR_SEASONAL_IND)='N','1',  RTRIM(IN_DF_DWELLING_USE_OCCUP_CD)='P' 
               AND RTRIM(IN_SEC_OR_SEASONAL_IND)<>'N','3',  RTRIM(IN_DF_DWELLING_USE_OCCUP_CD)<>'P' 
               AND RTRIM(IN_SEC_OR_SEASONAL_IND)='N','5',  RTRIM(IN_DF_DWELLING_USE_OCCUP_CD)<>'P' 
               AND RTRIM(IN_SEC_OR_SEASONAL_IND)<>'N','7', '1')
               )))))) AS OUT_STATUS_CODE,
