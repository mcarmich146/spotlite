import schedule
import time
from subscriptionUtils import check_and_notify  # Assuming check_and_notify is in subscManager.py
import config

def monitor_subscriptions():
    # This function will be called periodically
    check_and_notify()

def main():
    # Run the search right away to help with debugging.    
    monitor_subscriptions()

    # Set up the period for the monitor.
    schedule.every(config.SUBC_MON_FREQUENCY).minutes.do(monitor_subscriptions)

    # Keep running the schedule in a loop
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
