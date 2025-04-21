import azure.functions as func
import logging
import uuid

app = func.FunctionApp()


@app.function_name(name="HttpTrigger")
@app.route(route="HttpTrigger")
def main(req: func.HttpRequest) -> str:
    curr_id = str(uuid.uuid4())
    message = f"Request ID: {curr_id} Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
    logging.info(message)
    return message
