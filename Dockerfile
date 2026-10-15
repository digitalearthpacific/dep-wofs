FROM ghcr.io/astral-sh/uv:python3.10-bookworm

# Use the image's Python instead of downloading the one in .python-version
ENV UV_PYTHON=/usr/local/bin/python3 UV_PYTHON_DOWNLOADS=never

ADD . /code

WORKDIR /code

RUN uv sync --frozen --no-dev

# Argo calls `python dep_wofs/...`, so the project venv must come first
ENV PATH="/code/.venv/bin:$PATH"
