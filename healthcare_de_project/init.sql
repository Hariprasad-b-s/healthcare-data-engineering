-- Silver layer
CREATE OR REPLACE VIEW silver_claim_diagnosis    AS SELECT * FROM read_parquet('/Users/hariprasad/Downloads/Career/Projects/healthcare-data-engineering/healthcare_de_project/data_lake/silver/silver_claim_diagnosis/*.parquet');
CREATE OR REPLACE VIEW silver_claim_header       AS SELECT * FROM read_parquet('/Users/hariprasad/Downloads/Career/Projects/healthcare-data-engineering/healthcare_de_project/data_lake/silver/silver_claim_header/*.parquet');
CREATE OR REPLACE VIEW silver_claim_line_item    AS SELECT * FROM read_parquet('/Users/hariprasad/Downloads/Career/Projects/healthcare-data-engineering/healthcare_de_project/data_lake/silver/silver_claim_line_item/*.parquet');
CREATE OR REPLACE VIEW silver_claim_procedure    AS SELECT * FROM read_parquet('/Users/hariprasad/Downloads/Career/Projects/healthcare-data-engineering/healthcare_de_project/data_lake/silver/silver_claim_procedure/*.parquet');
CREATE OR REPLACE VIEW silver_condition          AS SELECT * FROM read_parquet('/Users/hariprasad/Downloads/Career/Projects/healthcare-data-engineering/healthcare_de_project/data_lake/silver/silver_condition/*.parquet');
CREATE OR REPLACE VIEW silver_encounter_bundle   AS SELECT * FROM read_parquet('/Users/hariprasad/Downloads/Career/Projects/healthcare-data-engineering/healthcare_de_project/data_lake/silver/silver_encounter_bundle/*.parquet');
CREATE OR REPLACE VIEW silver_eob_header         AS SELECT * FROM read_parquet('/Users/hariprasad/Downloads/Career/Projects/healthcare-data-engineering/healthcare_de_project/data_lake/silver/silver_eob_header/*.parquet');
CREATE OR REPLACE VIEW silver_eob_line_item      AS SELECT * FROM read_parquet('/Users/hariprasad/Downloads/Career/Projects/healthcare-data-engineering/healthcare_de_project/data_lake/silver/silver_eob_line_item/*.parquet');
CREATE OR REPLACE VIEW silver_location           AS SELECT * FROM read_parquet('/Users/hariprasad/Downloads/Career/Projects/healthcare-data-engineering/healthcare_de_project/data_lake/silver/silver_location/*.parquet');
CREATE OR REPLACE VIEW silver_organization       AS SELECT * FROM read_parquet('/Users/hariprasad/Downloads/Career/Projects/healthcare-data-engineering/healthcare_de_project/data_lake/silver/silver_organization/*.parquet');
CREATE OR REPLACE VIEW silver_patient            AS SELECT * FROM read_parquet('/Users/hariprasad/Downloads/Career/Projects/healthcare-data-engineering/healthcare_de_project/data_lake/silver/silver_Patient/*.parquet');
CREATE OR REPLACE VIEW silver_practitioner       AS SELECT * FROM read_parquet('/Users/hariprasad/Downloads/Career/Projects/healthcare-data-engineering/healthcare_de_project/data_lake/silver/silver_practitioner/*.parquet');
CREATE OR REPLACE VIEW silver_procedure_bundle   AS SELECT * FROM read_parquet('/Users/hariprasad/Downloads/Career/Projects/healthcare-data-engineering/healthcare_de_project/data_lake/silver/silver_procedure_bundle/*.parquet');
CREATE OR REPLACE VIEW silver_dim_date           AS SELECT * FROM read_parquet('/Users/hariprasad/Downloads/Career/Projects/healthcare-data-engineering/healthcare_de_project/data_lake/silver/dim_date/*.parquet');

-- Gold layer
CREATE OR REPLACE VIEW dim_condition             AS SELECT * FROM read_parquet('/Users/hariprasad/Downloads/Career/Projects/healthcare-data-engineering/healthcare_de_project/data_lake/gold/dim_condition/*.parquet');
CREATE OR REPLACE VIEW dim_date                  AS SELECT * FROM read_parquet('/Users/hariprasad/Downloads/Career/Projects/healthcare-data-engineering/healthcare_de_project/data_lake/gold/dim_date/*.parquet');
CREATE OR REPLACE VIEW dim_encounter             AS SELECT * FROM read_parquet('/Users/hariprasad/Downloads/Career/Projects/healthcare-data-engineering/healthcare_de_project/data_lake/gold/dim_encounter/*.parquet');
CREATE OR REPLACE VIEW dim_location              AS SELECT * FROM read_parquet('/Users/hariprasad/Downloads/Career/Projects/healthcare-data-engineering/healthcare_de_project/data_lake/gold/dim_location/*.parquet');
CREATE OR REPLACE VIEW dim_organization          AS SELECT * FROM read_parquet('/Users/hariprasad/Downloads/Career/Projects/healthcare-data-engineering/healthcare_de_project/data_lake/gold/dim_organization/*.parquet');
CREATE OR REPLACE VIEW dim_patient               AS SELECT * FROM read_parquet('/Users/hariprasad/Downloads/Career/Projects/healthcare-data-engineering/healthcare_de_project/data_lake/gold/dim_patient/*.parquet');
CREATE OR REPLACE VIEW dim_practitioner          AS SELECT * FROM read_parquet('/Users/hariprasad/Downloads/Career/Projects/healthcare-data-engineering/healthcare_de_project/data_lake/gold/dim_practitioner/*.parquet');
CREATE OR REPLACE VIEW dim_procedure             AS SELECT * FROM read_parquet('/Users/hariprasad/Downloads/Career/Projects/healthcare-data-engineering/healthcare_de_project/data_lake/gold/dim_procedure/*.parquet');
CREATE OR REPLACE VIEW dim_time                  AS SELECT * FROM read_parquet('/Users/hariprasad/Downloads/Career/Projects/healthcare-data-engineering/healthcare_de_project/data_lake/gold/dim_time/*.parquet');
CREATE OR REPLACE VIEW fact_claim_header         AS SELECT * FROM read_parquet('/Users/hariprasad/Downloads/Career/Projects/healthcare-data-engineering/healthcare_de_project/data_lake/gold/fact_claim_header/*.parquet');
CREATE OR REPLACE VIEW fact_claim_line_item      AS SELECT * FROM read_parquet('/Users/hariprasad/Downloads/Career/Projects/healthcare-data-engineering/healthcare_de_project/data_lake/gold/fact_claim_line_item/*.parquet');

SELECT 'Views loaded: silver (13) + gold (11)' AS status;
