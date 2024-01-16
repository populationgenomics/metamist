# Contributing

We love contributions from the community! This document explains how you can contribute to the project.

## How to Contribute
#### Generate and install the python installable API

After making any changes to the logic, it is worth regenerating the API with the OpenAPI Generator. 

Generating the installable APIs (Python + Typescript) involves running the server, getting the `/openapi.json`, and running `openapi-generator`.

The `regenerate_api.py` script does this for us in a few ways:

1. Uses a running server on `localhost:8000`
2. Runs a docker container from the `SM_DOCKER` environment variable.
3. Spins up the server itself.

You can simply run:
```bash
# this will start the api.server, so make sure you have the dependencies installed,
python regenerate_api.py \
    && pip install .
```

or if you prefer the Docker approach (eg: for CI), this command will build the docker container and supply it to `regenerate_api.py`:
```bash
# SM_DOCKER is a known env variable to regenerate_api.py
export SM_DOCKER="cpg/metamist-server:dev"
docker build --build-arg SM_ENVIRONMENT=local -t $SM_DOCKER -f deploy/api/Dockerfile .
python regenerate_api.py
```

#### Developing the UI
```bash
# Ensure you have started metamist server locally on your computer already, then in another tab open the UI.
# This will automatically proxy request to the server.
cd web
npm install
npm run compile
npm start
```
This will start a web server using Vite, running on `localhost:5173`.

### OpenAPI and Swagger

The Web API uses `apispec` with OpenAPI3 annotations on each route to describe interactions with the server. We can generate a swagger UI and an installable
python module based on these annotations.

Some handy links:

- [OpenAPI specification](https://swagger.io/specification/)
- [Describing parameters](https://swagger.io/docs/specification/describing-parameters/)
- [Describing request body](https://swagger.io/docs/specification/describing-request-body/)
- [Media types](https://swagger.io/docs/specification/media-types/)

The web API exposes this schema in two ways:

- Swagger UI: `http://localhost:8000/docs`
  - You can use this to construct requests to the server
  - Make sure you fill in the Bearer token (at the top right )
- OpenAPI schema: `http://localhost:8000/schema.json`
  - Returns a JSON with the full OpenAPI 3 compliant schema.
  - You could put this into the [Swagger editor](https://editor.swagger.io/) to see the same "Swagger UI" that `/api/docs` exposes.
  - We generate the metamist installable Python API based on this schema.

## Deployment

The CPG deploy is managed through Cloud Run on the Google Cloud Platform.
The deploy github action builds the container, and is deployed.

Additionally you can access metamist through the identity-aware proxy (IAP),
which handles the authentication through OAuth, allowing you to access the
front-end.