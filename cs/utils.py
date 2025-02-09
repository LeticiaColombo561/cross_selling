from google.cloud import secretmanager


def access_secret_version(
    project_id, secret_id, version_id="latest", credentials=None
):

    client = secretmanager.SecretManagerServiceClient(credentials=credentials)

    name = client.secret_version_path(project_id, secret_id, version_id)
    response = client.access_secret_version(name)
    payload = response.payload.data.decode("UTF-8")

    return payload
