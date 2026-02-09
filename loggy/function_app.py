# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

import azure.functions as func
import logging
import uuid
import json

app = func.FunctionApp()


def _get_message() -> str:
    """Generate a message with a unique request ID."""
    curr_id = str(uuid.uuid4())
    message = f"Request ID: {curr_id} Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
    logging.info(message)
    return message


@app.function_name(name="TimerTrigger")
@app.timer_trigger(schedule="0 */5 * * * *", arg_name="timer", run_on_startup=True)
def timer(timer: func.TimerRequest):
    _get_message()


@app.function_name(name="HttpTrigger")
@app.route(route="HttpTrigger")
def http(req: func.HttpRequest) -> str:
    return _get_message()


@app.function_name(name="CustomLog")
@app.route(route="CustomLog", methods=["GET", "POST"])
def custom_log(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP trigger that allows custom log messages and log levels."""

    # Get parameters from query string or request body
    try:
        req_body = req.get_json() if req.method == "POST" else {}
    except ValueError:
        req_body = {}

    # Get message from query params or body
    message = req.params.get('message') or req_body.get('message', 'Custom log entry')
    level = req.params.get('level') or req_body.get('level', 'info')
    count = int(req.params.get('count') or req_body.get('count', 1))

    # Validate count
    if count > 100:
        count = 100  # Limit to prevent abuse

    # Generate logs based on level
    responses = []
    for i in range(count):
        log_id = str(uuid.uuid4())
        full_message = f"[{i+1}/{count}] Request ID: {log_id} - {message}"

        if level.lower() == 'debug':
            logging.debug(full_message)
        elif level.lower() == 'warning':
            logging.warning(full_message)
        elif level.lower() == 'error':
            logging.error(full_message)
        elif level.lower() == 'critical':
            logging.critical(full_message)
        else:
            logging.info(full_message)

        responses.append({
            'index': i + 1,
            'id': log_id,
            'level': level,
            'message': full_message
        })

    return func.HttpResponse(
        body=json.dumps({
            'status': 'success',
            'logs_generated': count,
            'logs': responses
        }),
        status_code=200,
        headers={'Content-Type': 'application/json'}
    )
