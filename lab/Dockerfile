FROM python:3.12-slim

WORKDIR /app

# build tools only for pyfarmhash (optional dependency; install failure is tolerated)
COPY lab/requirements.txt /tmp/requirements.txt
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential faketime \
 && pip install --no-cache-dir "temporalio>=1.9,<2" \
 && (pip install --no-cache-dir "pyfarmhash>=0.3" || echo "pyfarmhash unavailable: lab.probe shard will be disabled") \
 && apt-get purge -y build-essential && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*   # faketime is kept for the clock-skew experiment

COPY lab/ /app/lab/
ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "lab.worker"]
