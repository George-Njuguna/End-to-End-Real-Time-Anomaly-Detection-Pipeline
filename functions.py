 # Libraries
import pandas as pd

 # Creating Timestamp Column
def feat_eng( data ):
    start_time = pd.to_datetime("2025-09-20 00:00:00")
    data["Timestamp"] = start_time + pd.to_timedelta( data["Time"], unit="s" )
    data = data.rename( columns = {'Time' : 'Time_elapsed_sec'} ) 