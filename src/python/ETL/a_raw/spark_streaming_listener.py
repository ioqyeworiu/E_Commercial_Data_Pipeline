from pyspark.sql.streaming import StreamingQueryListener
import csv
import os

class CSVStreamingQueryListener(StreamingQueryListener):
    
    def __init__(self, csv_file_path):
        self.csv_file_path = csv_file_path
        if not os.path.exists(self.csv_file_path):
            with open(self.csv_file_path, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(['ProgressId', 'Timestamp', 'BatchID', 'BatchDuration', 'BatchDurationMs', 'NumInputRows', 'InputRowsPerSecond', 'ProcessedRowsPerSecond'])
    
    def onQueryStarted(self, event):
        print(f"Query started: {event.id}")

    def onQueryProgress(self, event):
        with open(self.csv_file_path, mode='a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([
                event.progress.id,
                event.progress.timestamp,
                event.progress.batchId,
                event.progress.batchDuration,
                event.progress.durationMs,
                event.progress.numInputRows,
                event.progress.inputRowsPerSecond,
                event.progress.processedRowsPerSecond
            ])

    def onQueryTerminated(self, event):
        print(f"Query terminated: {event.id}")