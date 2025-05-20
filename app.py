from flask import Flask, render_template, jsonify
import threading
import time
import logging
from datetime import datetime
import os

# Import the autoposter functionality
from autoposter.lambda_function import lambda_handler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('social_media_autoposter_app')

app = Flask(__name__)

# Tracking variables for background task
background_task_status = {
    "running": False,
    "last_run": None,
    "next_scheduled_run": None,
    "result": None
}


def run_autoposter():
    """Background task to run the autoposter script"""
    global background_task_status

    try:
        logger.info("Starting autoposter background task")
        background_task_status["running"] = True

        # Run the autoposter function (formerly Lambda handler)
        result = lambda_handler(None, None)

        # Update status
        background_task_status["result"] = result
        background_task_status["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Autoposter task completed with result: {result}")
    except Exception as e:
        logger.error(f"Error in autoposter task: {e}")
        background_task_status["result"] = {"statusCode": 500, "body": str(e)}
    finally:
        background_task_status["running"] = False


def schedule_daily_run():
    """Function to schedule the daily run of the autoposter"""
    global background_task_status

    while True:
        # Get current time
        now = datetime.now()

        # Schedule for noon UTC (or adjust as needed)
        target_hour = 12

        # Calculate time until next run
        if now.hour < target_hour:
            # Run today at noon
            next_run = now.replace(hour=target_hour, minute=0, second=0, microsecond=0)
        else:
            # Run tomorrow at noon
            import datetime as dt
            next_run = (now + dt.timedelta(days=1)).replace(hour=target_hour, minute=0, second=0, microsecond=0)

        # Update next scheduled run in status
        background_task_status["next_scheduled_run"] = next_run.strftime("%Y-%m-%d %H:%M:%S")

        # Calculate seconds until next run
        seconds_until_run = (next_run - now).total_seconds()
        logger.info(f"Next autoposter run scheduled for {next_run.strftime('%Y-%m-%d %H:%M:%S')} "
                    f"({seconds_until_run / 3600:.2f} hours from now)")

        # Wait until scheduled time
        time.sleep(seconds_until_run)

        # Only proceed if not already running
        if not background_task_status["running"]:
            # Start a new thread for the actual run
            autoposter_thread = threading.Thread(target=run_autoposter)
            autoposter_thread.daemon = True
            autoposter_thread.start()

        # Sleep for 60 seconds to avoid busy waiting
        time.sleep(60)


@app.route('/')
def index():
    """Main status page"""
    return render_template('index.html', status=background_task_status)


@app.route('/status')
def status():
    """API endpoint for status"""
    return jsonify(background_task_status)


@app.route('/run-now', methods=['POST'])
def run_now():
    """Endpoint to trigger an immediate run"""
    if background_task_status["running"]:
        return jsonify({"status": "error", "message": "Autoposter is already running"})

    # Start a new thread for the run
    autoposter_thread = threading.Thread(target=run_autoposter)
    autoposter_thread.daemon = True
    autoposter_thread.start()

    return jsonify({"status": "success", "message": "Autoposter started"})


if __name__ == '__main__':
    # Start scheduler in a separate thread
    scheduler_thread = threading.Thread(target=schedule_daily_run)
    scheduler_thread.daemon = True
    scheduler_thread.start()

    # Set host to 0.0.0.0 to make it accessible externally
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=False)