import json
import boto3


def get_secret_manger_values(secret_id):
    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager",
        region_name="us-west-2",
    )
    get_secret_value_response = client.get_secret_value(SecretId=secret_id)
    secret_json = json.loads(get_secret_value_response["SecretString"])
    return secret_json