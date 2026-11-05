FROM ghcr.io/astral-sh/uv:python3.10-bookworm

# Use the image's Python instead of downloading the one in .python-version
ENV UV_PYTHON=/usr/local/bin/python3 UV_PYTHON_DOWNLOADS=never

WORKDIR /code

# Install dependencies before copying the source so this layer stays cached
# when only code changes
COPY pyproject.toml uv.lock /code/
RUN uv sync --frozen --no-dev --no-install-project

# Cache GADM boundaries inside dep_tools so pods don't download them at runtime
RUN /code/.venv/bin/python -c "from dep_tools.grids import gadm_union; gadm_union()"

ADD . /code

RUN uv sync --frozen --no-dev

# Argo calls `python dep_wofs/...`, so the project venv must come first
ENV PATH="/code/.venv/bin:$PATH"
