FROM python:3.12-slim

WORKDIR /workspace

COPY requirements.txt ./
RUN python -m pip install --no-cache-dir --disable-pip-version-check -r requirements.txt

COPY . .

ENV DBT_PROFILES_DIR=/workspace

CMD ["dbt", "build", "--project-dir", "/workspace", "--profiles-dir", "/workspace"]
