FROM astral/uv:python3.12-alpine

WORKDIR /app

COPY pyproject.toml README.md LICENSE.md ./
COPY src ./src

RUN uv pip install --system --editable .

ENV PYTHONUNBUFFERED=1

CMD ["solarlog2mqtt"]
