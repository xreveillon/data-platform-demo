# chargy

## Getting started

### Installing dependencies

Ensure [`uv`](https://docs.astral.sh/uv/) is installed following their [official documentation](https://docs.astral.sh/uv/getting-started/installation/).

Install the required dependencies using _sync_:

```bash
uv sync
```

Then, activate the virtual environment:

| OS | Command |
| --- | --- |
| Linux | ```source .venv/bin/activate``` |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

### Create the .env file

The .env file contains variable definitions to configure the execution, file to be created at the root of the project (sibling of this `README.md`)..
```dotenv
S3_ENDPOINT_URL=urlValue
S3_ACCESS_KEY_ID=value
S3_SECRET_KEY=value
```

| Variable | Description | Example |
| --- | --- | --- |
| S3_ENDPOINT_URL | Indicate the on-premise S3 storage | `https://s3.example.com:9000` |
| S3_ACCESS_KEY_ID | The access key to "log in" to the s3 storage | `lskjhvuiezbcqiuh87364` |
| S3_SECRET_KEY | The secret key to "log in" to the s3 storage | `dksjvbkegfzu873458lskdjf` |

### Running Dagster

Start the Dagster UI web server:

```bash
dg dev
```

Open http://localhost:3000 in your browser to see the project.

## Learn more

To learn more about this template and Dagster in general:

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)
