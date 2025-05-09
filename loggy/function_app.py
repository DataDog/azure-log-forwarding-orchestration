# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

import azure.functions as func
import logging
import uuid

app = func.FunctionApp()


@app.function_name(name="TimerTrigger")
@app.timer_trigger(schedule="0 */5 * * * *", arg_name="timer", run_on_startup=True)
def timer(timer: func.TimerRequest):
    curr_id = str(uuid.uuid4())
    message = f"Request ID: {curr_id} Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
    logging.info(message)


@app.function_name(name="HttpTrigger")
@app.route(route="HttpTrigger")
def http(req: func.HttpRequest) -> str:
    curr_id = str(uuid.uuid4())
    message = f"Request ID: {curr_id} Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
    logging.info(message)
    return message
