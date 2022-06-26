FROM agrigorev/zoomcamp-model:mlops-3.9.7-slim

RUN pip install -U pip
RUN pip install pipenv

ADD /output /app/output

COPY [ "Pipfile", "Pipfile.lock", "homework.py", "./"]

RUN pipenv install --system --deploy

ENTRYPOINT ["python", "homework.py", "2021", "4"]
