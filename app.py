from flask import Flask, request, jsonify
import json
import requests
from flask_cors import CORS
from dotenv import load_dotenv
import os

load_dotenv()

app = Flask(__name__)
CORS(app)

api_key = os.getenv("DOMO_DEVELOPER_TOKEN")
API_URL = os.getenv("API_URL")

system_instruction = """
You are a Snowflake SQL expert and DOMO Magic ETL expert.

TASK:

1. **Analyze** the provided DOMO Magic ETL JSON:
   - It contains a data transformation pipeline and schema details of all involved tables.

2. **Generate** a complete Snowflake stored procedure named `SP_INSERT_COMPLETE_SALES_FINAL`:
   - Replicate the transformations exactly as per the JSON.
   - Insert the final result into the table `COMPLETE_SALES_FINAL`.
   - Use this exact template:

     USE DATABASE INFORMATION;
     USE SCHEMA PUBLIC;
     USE WAREHOUSE COMPUTE_WH;

     CREATE OR REPLACE PROCEDURE SP_INSERT_COMPLETE_SALES_FINAL()
     RETURNS STRING
     LANGUAGE SQL
     AS
     $$
     BEGIN
         -- Step 1: Create staging tables based on JSON logic
         CREATE OR REPLACE TEMP TABLE ... AS
         SELECT ... FROM ...;

         -- Step 2: Apply joins, filters, or transformations
         CREATE OR REPLACE TEMP TABLE ... AS
         SELECT ... FROM ...;

         -- Step 3: Insert results into COMPLETE_SALES_FINAL
         INSERT INTO COMPLETE_SALES_FINAL (...) SELECT ... FROM ...;

         RETURN 'Success';
     END;
     $$;

   - Only use Snowflake-compatible SQL syntax.
   - Add comments describing each step’s transformation.
   - Use only fields present in the JSON — do NOT invent columns or tables.
   - If a step is unclear or missing data, skip it with: `-- Skipped due to missing fields`.

3. **Extract** all `sourceName` values linked to `dataSourceName` fields from the JSON (no duplicates).

4. **Output strictly in JSON** (no extra text), in this format:

{
  "sql": "<full stored procedure code here>",
  "datasourceName": ["<source1>", "<source2>", ...]
}
"""

@app.route('/generate-sql', methods=['POST'])
def generate_sql():
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No file part in the request"}), 400

        file = request.files['file']

        if file.filename == '':
            return jsonify({"error": "No selected file"}), 400

        if not file.filename.endswith('.json'):
            return jsonify({"error": "Only .json files are allowed"}), 400

        file_content = file.read()

        if not file_content:
            return jsonify({"error": "Uploaded file is empty"}), 400

        try:
            input_json = json.loads(file_content)

        except json.JSONDecodeError as e:
            return jsonify({
                "error": "Invalid JSON format in the uploaded file",
                "details": str(e)
            }), 400

        payload = {
            "model": "domo.openai.gpt-4o-mini",
            "input": system_instruction + "DOMO's MAGIC ETL JSON :" +json.dumps(input_json),
            "parameters": {
                "temperature": 0.2
            }
        }

        headers = {
            "Content-Type": "application/json",
            "X-DOMO-Developer-Token": api_key
        }

        response = requests.post(API_URL, headers=headers, json=payload)

        if response.status_code != 200:
            return jsonify({"error": "API call failed", "details": response.text}), 500
        
        response_content = response.json().get("output", "")

        if not response_content or response_content.strip() == "":
            return jsonify({
                "error": "DOMO API returned empty output",
                "raw_output": response_content
            }), 500

        try:
            response_json = json.loads(response_content)

            sql = response_json.get("sql")
            datasources = response_json.get("datasourceName")

            if not sql and not datasources:
                return jsonify({
                    "error": "DOMO API output is missing required fields",
                    "raw_output": response_content
                }), 500

            return jsonify({
                "Output": sql if sql else "No SQL found.",
                "Inputs": datasources if datasources else "No datasource provided."
            })

        except json.JSONDecodeError as e:
            return jsonify({
                "error": "Invalid JSON returned from DOMO API",
                "raw_output": response_content,
                "details": str(e)
            }), 500


    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/", methods=['GET', 'POST'])
def home():
    return "Running"

if __name__ == '__main__':
    app.run(debug=True)
