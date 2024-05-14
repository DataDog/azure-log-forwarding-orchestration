from os import environ

environ.setdefault("AzureWebJobsStorage", "TEST_CONNECTION_STRING")
environ.setdefault("EVENT_HUB_NAME", "TEST_EVENT_HUB")
environ.setdefault("EVENT_HUB_NAMESPACE", "TEST_EVENT_HUB_NAMESPACE")
