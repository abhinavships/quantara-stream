FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1
# pypi.org blocks the Condense builder IPs (organiser notice); use the mirror they gave. Env var also covers pip's isolated build env.
ENV PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
# build without pip's isolated sub-environment (the builder's network fails there); setuptools comes from the main env
RUN pip install --no-cache-dir --retries 10 --timeout 60 setuptools wheel && pip install --no-cache-dir --retries 10 --timeout 60 --no-build-isolation -r requirements.txt
COPY . .
CMD ["python", "app.py"]
