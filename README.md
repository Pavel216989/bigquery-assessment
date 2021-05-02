# Coding Challenge V3
## Task
Build a Python application to produce a weekly financial usage report. 
###  Requirements
1. The report only considers plays with >= 30 seconds duration.
2. The report only considers tracks with a valid owner during the playing time.
3. The report contains the following columns:
    1. reporting_start_date: the start date of reporting week.
    2. reporting_end_date: the end date of reporting week.
    3. track_id
    4. track_title
    5. owner_id
    6. price_per_play: the weekly payout of the owner divided by total plays of all tracks
    belonged to that owner.
    7. total_plays: total plays for this combination of track_id and owner_id.
4. The report should be stored as a table in BigQuery.
## Solution description

## Usage
### Prerequisites
1. Before running the program, make sure to install all the requirements:
    ```
    pip install -r requirements.txt
    ```
2. Make sure to set bigquery credentials file path as environment variable called **GOOGLE_APPLICATION_CREDENTIALS**.
### Execution
The solution can be executed as a python program from the command line. The entrypoint is called **main.py**
**3 arguments are expected:**
1. Reporting start date (string in %Y-%m-%d format).
2. Reporting end date (string in %Y-%m-%d format).
3. Destination BiqQuery table name (string).
### Example:
```
python3 main.py "2019-05-13" "2019-05-19" "report"
```