from pprint import pprint

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from datetime import date, timedelta


credentials = GoogleCredentials.get_application_default()

service = discovery.build('bigquery', 'v2', credentials=credentials)


projectId = 'XYZ'

job_body = {

    "configuration": {
        "query": {
            "query": """ SELECT event_dim.user_id FROM [XYZ_PQR_app_events_20161222] WHERE event_dim.user_id IS NOT NULL """,
            "destinationTable": {
                "projectId": "XYZ",
                "datasetId": "PQR",
                "tableId": "destination_table_name"
            },
            "createDisposition": "CREATE_IF_NEEDED",
            "writeDisposition": "WRITE_APPEND",
        }
    }

}

request = service.jobs().insert(projectId=projectId, body=job_body)
response = request.execute()

pprint(response)
