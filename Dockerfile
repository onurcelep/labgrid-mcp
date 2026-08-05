# Container image for labgrid-mcp (stdio MCP server).
#
# The server needs a reachable labgrid coordinator to do real work; without
# one it still starts, serves MCP introspection, and retries the connection
# in the background -- so `docker run -i` works for tool discovery and
# registry health checks out of the box.
#
#   docker build -t labgrid-mcp .
#   docker run -i -e LG_COORDINATOR=host.docker.internal:20408 labgrid-mcp
#
# Note for redistributors: this image contains labgrid (LGPL-2.1-or-later)
# installed from PyPI as a dependency; distributing the built image carries
# the corresponding LGPL obligations (see README/License).
FROM python:3.12-slim
WORKDIR /app
COPY . .
RUN pip install --no-cache-dir .
ENTRYPOINT ["labgrid-mcp"]
