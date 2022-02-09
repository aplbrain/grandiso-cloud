import json
import pathlib

DEFAULT_CONFIGURATION = {
    "dynamodb_table": "motif-search-results",
    "sqs_queue_name": "motif-search-queue",
}


def configuration(configuration_filepath: str = "config.json") -> dict:

    try:
        contents = json.load(
            pathlib.Path(configuration_filepath).expanduser().resolve().open()
        )
    except:
        contents = DEFAULT_CONFIGURATION

    return contents
