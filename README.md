# Coding Challenge V3

## Task
Build a Python application to produce a weekly financial usage report. 
The application needs to read data from multiple BigQuery tables, apply filters, calculate rates, create a report and write it back to BigQuery.

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
![Schema](./img/schema.png)
### Design Decisions:

1. I decided to select data from multiple tracks plays tables in the following way:
    * In a for loop, for each date between reporting_date_from and reporting_date_to
      we select all the plays and append them as dataframes to a list.
    * After getting all the plays dataframes, we create a concatenated pandas dataframe of all plays.
    * Finally, we filter  all the plays with duration < 30 out.

   In my opinion, filtering plays by duration after all plays are concatenated should be faster than running 
      ```SELECT ... WHERE duration >= 30``` multiple times. Also, concatenating multiple dataframes in the end should be 
      faster than running ```SELECT .. UNION ALL``` multiple times on the SQL side.
2. The execution speed can be increased by using BigQuery Storage.

## Usage

### Prerequisites

1. Before running the program, make sure to install all the requirements:
    ```
    pip install -r requirements.txt
    ```
2. Make sure to set bigquery credentials file path as environment variable called **GOOGLE_APPLICATION_CREDENTIALS**.

### Execution
The solution can be executed as a python program from the command line. The entrypoint is called **main.py**.

**3 arguments are expected:**
1. Reporting start date (string in %Y-%m-%d format).
2. Reporting end date (string in %Y-%m-%d format). There has to be a payouts table in the source dataset for this date.
3. Destination BiqQuery table name (non-empty string).

**Example:**
```
python3 main.py "2019-05-13" "2019-05-19" "report"
```
**NOTE**: 
   
   The correctness of the reporting end date is validate on the stage of getting payouts data.
   
   We don't require it to be either 2019-05-12 or 2019-05-19, 
   so that the system could work on a bigger dataset with other dates too.
