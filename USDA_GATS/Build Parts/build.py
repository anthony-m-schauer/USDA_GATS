from datetime import datetime 
import time 

start_time = datetime.now()
print("\nStarted", end="")
time.sleep(11)

def get_run_time(start_time):
    end_time = datetime.now()
    elapsed = (end_time - start_time)
    total_seconds = int(elapsed.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    print(f" Runtime: {int(hours)}h {int(minutes)}m {int(seconds)}s.")

get_run_time(start_time)