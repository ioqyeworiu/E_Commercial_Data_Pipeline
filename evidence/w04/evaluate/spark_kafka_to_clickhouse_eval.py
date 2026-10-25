from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQueryListener
import csv
import os

class CsvMetricsListener(StreamingQueryListener):
    def __init__(self, csv_file_path):
        self.csv_file_path = csv_file_path
        if not os.path.exists(self.csv_file_path):
            with open(self.csv_file_path, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(['Timestamp', 'Batch_ID', 'Batch_Duration_ms', 'Input_Rows'])
                file.close()

    def onQueryStarted(self, event):
        print(f"Query started: {event.id}")

    def onQueryProgress(self, event):
        timestamp = event.progress.timestamp
        batch_id = event.progress.batchId
        batch_duration = event.progress.batchDuration
        num_input_rows = event.progress.numInputRows

        # Ghi nối tiếp vào file CSV
        with open(self.csv_file_path, mode='a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([timestamp, batch_id, batch_duration, num_input_rows])

    def onQueryTerminated(self, event):
        print(f"Query terminated: {event.id}")