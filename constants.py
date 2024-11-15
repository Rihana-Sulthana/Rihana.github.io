# Oracle
ORACLE_QUERY = "select * from INSPY_AP.INV_HEADER where  to_date(scan_date,'DD-MM-YY')" \
               " between to_date('01-07-24','DD-MM-YY') and  to_date('30-09-24','DD-MM-YY')   order by header_id"

ORACLE_IGNORE_COLUMNS = ['CAPTURE_END', 'CAPTURE_START', 'CONTENT_CREATE', 'EXTRACTION_END', 'EXTRACTION_START',
                         'INGESTION_END', 'INGESTION_START', 'INV_DATE_TEXT', 'ML_END', 'ML_RULES_ENGINE_RUN',
                         'ML_START', 'OCR_END', 'OCR_LANGUAGE',
                         'OCR_OUT_LOC', 'OCR_START', 'REJECT_TO_USER', 'V_VENDOR_NAME', 'UPDATED_DATE']


# Snowflake
SNOWFLAKE_QUERY = "Select * from RAW.INV_HEADER where scan_date between '2024-07-01' and" \
                  " '2024-09-30' order by header_id"

SNOWFLAKE_FLOAT_COLUMNS = ['MISC_CHARGE', 'TAX', 'DISCOUNT', 'LINE_SUBTOTAL', 'TOTAL', 'APPROVAL_EXCHANGE_RATE',
                           'APPROVAL_CURRENCY_TOTAL']

SNOWFLAKE_IGNORE_COLUMNS = ['SCAN_DATE_TS', 'UPDATED_DATE']

# Common constants

TIMESTAMP_COLUMNS = ["PAYMENT_DETECTED_DATE", "UPDATE_INVERP_IMPORTED_DATE", "BATCH_MATCHING_UPDATED",
                     "PO_PAIRING_UPDATED", "PAYMENT_IMPORTED_DATE", "DISCOUNT_DATE",
                     "VALIDATION_DATE", "UPDATE_INVERP_DETECTED_DATE", "PO_AUTO_FLIPPED", "PAYMENT_DATE",
                     "ML_CODING_UPDATED_DATE", "SUPPLY_DATE",
                     "BATCH_MATCHING_DATE"]

DATE_COLUMNS = ["INV_DATE", "SCAN_DATE", "DECISION_DATE", "INVOICE_DUE_DATE", "TERMS_DATE", "ERP_TERMS_DATE", "GL_DATE",
                "CREATED_DATE",
                "APPROVAL_EXCHANGE_RATE_DATE", "ERP_GL_DATE", "FUNC_EXCHANGE_RATE_DATE"]

FLOAT_TO_INT_COLUMNS = ["TAX", "LINE_SUBTOTAL", "TOTAL", "FUNC_CURRENCY_TOTAL", "APPROVAL_CURRENCY_TOTAL"]