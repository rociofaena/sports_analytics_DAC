FROM public.ecr.aws/lambda/python:3.11

COPY requirements.txt .
RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir --prefer-binary -r requirements.txt

COPY app.py ${LAMBDA_TASK_ROOT}/

CMD ["app.lambda_handler"]
